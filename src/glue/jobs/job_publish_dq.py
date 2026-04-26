"""AWS Glue PySpark job that runs post-curation Glue Data Quality checks.

This job validates curated master and transaction outputs with Glue DQ rulesets,
publishes the results to the Glue Data Quality dashboard, and persists the DQ
findings without failing the job when business-rule violations are detected.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from urllib.parse import urlparse


def join_path(root: str, *parts: str) -> str:
    """Join local paths and S3 URIs without adding double slashes."""

    cleaned_root = root.rstrip("/")
    cleaned_parts = [part.strip("/") for part in parts]
    return "/".join([cleaned_root] + cleaned_parts)


def resolve_args() -> dict[str, str]:
    """Resolve arguments from Glue runtime or local CLI execution."""

    try:
        from awsglue.utils import getResolvedOptions

        return getResolvedOptions(sys.argv, ["JOB_NAME", "processed_root", "business_date"])
    except ImportError:
        parser = argparse.ArgumentParser()
        parser.add_argument("--JOB_NAME", default="local_dq_publish_job")
        parser.add_argument("--processed_root", "--processed-root", dest="processed_root", default="generated_data/processed")
        parser.add_argument("--business_date", "--business-date", dest="business_date", required=True)
        return vars(parser.parse_args())


def build_customer_ruleset() -> str:
    """Return Glue DQ rules for the curated customer snapshot."""

    return """
Rules = [
    IsComplete "customer_id",
    IsComplete "customer_name",
    IsComplete "risk_band",
    IsUnique "customer_id",
    ColumnValues "risk_band" in ["LOW", "MEDIUM", "HIGH"],
    ColumnValues "kyc_status" in ["PENDING", "VERIFIED"]
]
""".strip()


def build_account_ruleset() -> str:
    """Return Glue DQ rules for the curated account snapshot."""

    return """
Rules = [
    IsComplete "account_id",
    IsComplete "customer_id",
    IsComplete "advisor_id",
    IsUnique "account_id",
    ColumnValues "account_status" in ["ACTIVE", "DORMANT"],
    ColumnValues "account_segment" in ["EQUITY_DELIVERY", "INTRADAY", "FNO"]
]
""".strip()


def build_order_ruleset() -> str:
    """Return Glue DQ rules for curated orders."""

    return """
Rules = [
    IsComplete "order_id",
    IsComplete "account_id",
    IsComplete "instrument_id",
    IsUnique "order_id",
    ColumnValues "order_status" in ["COMPLETED", "CANCELLED", "REJECTED"],
    ColumnValues "ordered_quantity" > 0,
    ColumnValues "limit_price" > 0
]
""".strip()


def build_trade_ruleset() -> str:
    """Return Glue DQ rules for curated trades."""

    return """
Rules = [
    IsComplete "trade_id",
    IsComplete "order_id",
    IsComplete "account_id",
    IsUnique "trade_id",
    ColumnValues "executed_quantity" > 0,
    ColumnValues "executed_price" > 0,
    ColumnValues "gross_trade_amount" > 0,
    ColumnValues "net_trade_amount" > 0
]
""".strip()


def validate_required_columns(actual_columns: list[str], required_columns: list[str], dataset_name: str) -> None:
    """Raise a clear error when a curated dataset is missing expected columns."""

    missing_columns = [column for column in required_columns if column not in actual_columns]
    if missing_columns:
        raise RuntimeError(
            f"Dataset '{dataset_name}' is missing required columns {missing_columns}. "
            f"Available columns: {actual_columns}"
        )


def read_csv_dynamic_frame(glue_context, dynamic_frame_class, path: str, transformation_ctx: str, required_columns: list[str]):
    """Read curated CSV data through Spark first, then convert to DynamicFrame."""

    dataframe = (
        glue_context.spark_session.read.option("header", "true").option("inferSchema", "false").csv(path)
    )
    validate_required_columns(dataframe.columns, required_columns, transformation_ctx)
    return dynamic_frame_class.fromDF(dataframe, glue_context, transformation_ctx)


def evaluate_dataset(evaluate_data_quality, frame, ruleset: str, evaluation_context: str):
    """Run Glue Data Quality and publish the result to the Glue DQ dashboard."""

    return evaluate_data_quality.apply(
        frame=frame,
        ruleset=ruleset,
        publishing_options={
            "dataQualityEvaluationContext": evaluation_context,
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={
            "performanceTuning.caching": "CACHE_NOTHING",
        },
    )


def delete_s3_prefix(path: str) -> None:
    """Delete every object under an S3 prefix so reruns can write cleanly."""

    import boto3

    parsed = urlparse(path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/").rstrip("/") + "/"
    client = boto3.client("s3")
    continuation_token = None

    while True:
        request = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            request["ContinuationToken"] = continuation_token

        response = client.list_objects_v2(**request)
        contents = response.get("Contents", [])
        if contents:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": item["Key"]} for item in contents], "Quiet": True},
            )

        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")


def clear_output_path(glue_context, output_path: str) -> None:
    """Remove an existing output path before writing fresh job output."""

    if output_path.startswith("s3://"):
        try:
            glue_context.purge_s3_path(output_path.rstrip("/") + "/", {"retentionPeriod": 0})
            return
        except Exception as error:
            print(f"Falling back to boto3 delete for {output_path} because purge_s3_path failed: {error}")
            delete_s3_prefix(output_path)
            return

    destination = Path(output_path)
    if destination.exists():
        shutil.rmtree(destination)


def write_json_dataframe(glue_context, dataframe, output_path: str) -> None:
    """Write Glue DQ result rows for audit/debugging."""

    clear_output_path(glue_context, output_path)
    dataframe.coalesce(1).write.mode("overwrite").json(output_path)


def summarize_glue_dq_result(result_dataframe, dataset_name: str) -> dict[str, int | str]:
    """Summarize Glue DQ findings without failing the job for rule violations."""

    if "Outcome" not in result_dataframe.columns:
        raise RuntimeError(
            f"Glue DQ result for '{dataset_name}' does not contain the expected 'Outcome' column. "
            f"Available columns: {result_dataframe.columns}"
        )

    failed_count = result_dataframe.filter("Outcome <> 'Passed'").count()
    return {
        "dataset_name": dataset_name,
        "failed_rule_outcomes": failed_count,
        "dataset_status": "FAILED_RULES" if failed_count else "PASSED_RULES",
    }


def main() -> None:
    """Execute the post-curation Glue Data Quality job.

    The job keeps the flow intentionally simple:
    1. read curated datasets produced by upstream Glue jobs
    2. run Glue Data Quality rulesets on those curated datasets
    3. publish the DQ results to Glue Data Quality dashboards and CloudWatch
    4. write the raw DQ result rows to S3 for audit/debugging
    5. summarize the Glue DQ findings without failing the job for rule violations
    6. commit the Glue run
    """

    args = resolve_args()

    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.job import Job
    from awsgluedq.transforms import EvaluateDataQuality
    from pyspark.context import SparkContext
    from pyspark.sql import Row

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    business_date = args["business_date"]
    curated_day_root = join_path(args["processed_root"], "curated", f"business_date={business_date}")
    dq_root = join_path(curated_day_root, "dq")

    print(f"Starting Glue DQ publish job for business_date={business_date}")

    # Step 1: Read curated outputs produced by the upstream master and transaction jobs.
    customers_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(curated_day_root, "master", "customers_snapshot"),
        "dq_customers_frame",
        [
            "customer_id",
            "customer_name",
            "email",
            "mobile_number",
            "city_name",
            "state_name",
            "country_name",
            "risk_band",
            "kyc_status",
        ],
    )
    accounts_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(curated_day_root, "master", "accounts_snapshot"),
        "dq_accounts_frame",
        [
            "account_id",
            "customer_id",
            "advisor_id",
            "account_segment",
            "account_status",
            "opened_date",
        ],
    )
    orders_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(curated_day_root, "transactions", "orders"),
        "dq_orders_frame",
        [
            "order_id",
            "business_date",
            "account_id",
            "customer_id",
            "instrument_id",
            "order_timestamp",
            "order_side",
            "order_type",
            "ordered_quantity",
            "limit_price",
            "order_status",
        ],
    )
    trades_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(curated_day_root, "transactions", "trades"),
        "dq_trades_frame",
        [
            "trade_id",
            "business_date",
            "order_id",
            "account_id",
            "customer_id",
            "instrument_id",
            "trade_side",
            "trade_timestamp",
            "executed_quantity",
            "executed_price",
            "brokerage_fee",
            "tax_amount",
            "gross_trade_amount",
            "net_trade_amount",
        ],
    )

    # Step 2: Run Glue Data Quality rulesets on curated outputs and publish to Glue DQ dashboards.
    customer_dq_results = evaluate_dataset(
        EvaluateDataQuality,
        customers_frame,
        build_customer_ruleset(),
        f"brokerage_curated_customers_{business_date}",
    )
    account_dq_results = evaluate_dataset(
        EvaluateDataQuality,
        accounts_frame,
        build_account_ruleset(),
        f"brokerage_curated_accounts_{business_date}",
    )
    order_dq_results = evaluate_dataset(
        EvaluateDataQuality,
        orders_frame,
        build_order_ruleset(),
        f"brokerage_curated_orders_{business_date}",
    )
    trade_dq_results = evaluate_dataset(
        EvaluateDataQuality,
        trades_frame,
        build_trade_ruleset(),
        f"brokerage_curated_trades_{business_date}",
    )

    # Step 3: Persist the raw Glue DQ outputs to S3 for audit/debugging.
    write_json_dataframe(glue_context, customer_dq_results.toDF(), join_path(dq_root, "curated_customers_dq_results"))
    write_json_dataframe(glue_context, account_dq_results.toDF(), join_path(dq_root, "curated_accounts_dq_results"))
    write_json_dataframe(glue_context, order_dq_results.toDF(), join_path(dq_root, "curated_orders_dq_results"))
    write_json_dataframe(glue_context, trade_dq_results.toDF(), join_path(dq_root, "curated_trades_dq_results"))

    # Step 4: Summarize Glue DQ findings without failing the job for rule violations.
    dq_summary_rows = [
        summarize_glue_dq_result(customer_dq_results.toDF(), "curated_customers"),
        summarize_glue_dq_result(account_dq_results.toDF(), "curated_accounts"),
        summarize_glue_dq_result(order_dq_results.toDF(), "curated_orders"),
        summarize_glue_dq_result(trade_dq_results.toDF(), "curated_trades"),
    ]
    total_failed_rule_outcomes = sum(int(row["failed_rule_outcomes"]) for row in dq_summary_rows)
    summary_df = glue_context.spark_session.createDataFrame([Row(**row) for row in dq_summary_rows])
    write_json_dataframe(glue_context, summary_df, join_path(dq_root, "dq_publish_summary"))

    # Step 5: Commit after the Glue DQ validation stage succeeds.
    print(
        {
            "business_date": business_date,
            "pipeline_status": "DQ_FINDINGS_PRESENT" if total_failed_rule_outcomes else "PASSED",
            "total_failed_rule_outcomes": total_failed_rule_outcomes,
        }
    )
    job.commit()


if __name__ == "__main__":
    main()
