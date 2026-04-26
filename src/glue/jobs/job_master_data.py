"""AWS Glue PySpark job for brokerage master snapshots.

This job reads the daily customer and account snapshots from S3, evaluates data
quality using AWS Glue Data Quality, and writes curated master outputs plus DQ
result datasets back to S3.
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

        return getResolvedOptions(sys.argv, ["JOB_NAME", "raw_root", "processed_root", "business_date"])
    except ImportError:
        parser = argparse.ArgumentParser()
        parser.add_argument("--JOB_NAME", default="local_master_data_job")
        parser.add_argument("--raw_root", "--raw-root", dest="raw_root", default="generated_data/raw")
        parser.add_argument("--processed_root", "--processed-root", dest="processed_root", default="generated_data/processed")
        parser.add_argument("--business_date", "--business-date", dest="business_date", required=True)
        return vars(parser.parse_args())

#IsComplete-->No null values
#ColumnValues "risk_band" in ["LOW", "MEDIUM", "HIGH"]-->Only these values are allowed in column risk_band
def build_customer_ruleset() -> str:
    """Return the Glue Data Quality ruleset for the customer snapshot."""

    return """
Rules = [
    IsComplete "customer_id", 
    IsComplete "customer_name",
    IsComplete "risk_band",
    IsComplete "kyc_status",
    IsUnique "customer_id",
    ColumnValues "risk_band" in ["LOW", "MEDIUM", "HIGH"],
    ColumnValues "kyc_status" in ["PENDING", "VERIFIED"]
]
""".strip()


def build_account_ruleset() -> str:
    """Return the Glue Data Quality ruleset for the account snapshot."""

    return """
Rules = [
    IsComplete "account_id",
    IsComplete "customer_id",
    IsComplete "account_status",
    IsComplete "account_segment",
    IsUnique "account_id",
    ColumnValues "account_status" in ["ACTIVE", "DORMANT"],
    ColumnValues "account_segment" in ["EQUITY_DELIVERY", "INTRADAY", "FNO"]
]
""".strip()


def validate_required_columns(actual_columns: list[str], required_columns: list[str], dataset_name: str) -> None:
    """Raise a clear error when a dataset is missing expected columns."""

    missing_columns = [column for column in required_columns if column not in actual_columns]
    if missing_columns:
        raise RuntimeError(
            f"Dataset '{dataset_name}' is missing required columns {missing_columns}. "
            f"Available columns: {actual_columns}"
        )


def read_csv_dynamic_frame(glue_context, dynamic_frame_class, path: str, transformation_ctx: str, required_columns: list[str]):
    """Read CSV data through Spark first, then convert it into a DynamicFrame.

    Reading through Spark keeps CSV header handling consistent across local runs
    and Glue runtime. The explicit column validation helps fail fast with a
    clear message before downstream transforms reference missing columns.
    """

    dataframe = (
        glue_context.spark_session.read.option("header", "true").option("inferSchema", "false").csv(path)
    )
    validate_required_columns(dataframe.columns, required_columns, transformation_ctx)
    return dynamic_frame_class.fromDF(dataframe, glue_context, transformation_ctx)


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


def write_csv_dynamic_frame(glue_context, frame, output_path: str, transformation_ctx: str) -> None:
    """Write a DynamicFrame as CSV to S3 or local storage."""

    clear_output_path(glue_context, output_path)

    if output_path.startswith("s3://"):
        glue_context.write_dynamic_frame.from_options(
            frame=frame,
            connection_type="s3",
            connection_options={"path": output_path},
            format="csv",
            format_options={"withHeader": True},
            transformation_ctx=transformation_ctx,
        )
        return

    frame.toDF().coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)


def write_json_dataframe(glue_context, dataframe, output_path: str) -> None:
    """Write a Spark DataFrame as JSON."""

    clear_output_path(glue_context, output_path)
    dataframe.coalesce(1).write.mode("overwrite").json(output_path)


def evaluate_dataset(evaluate_data_quality, frame, ruleset: str, evaluation_context: str):
    """Run AWS Glue Data Quality on a DynamicFrame."""

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


def main() -> None:
    """Execute the master-data Glue job.

    The job keeps the flow intentionally simple:
    1. read the daily master snapshots
    2. evaluate Glue Data Quality rulesets
    3. write curated master outputs
    4. persist DQ result datasets
    5. commit the Glue run
    """

    args = resolve_args()

    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.job import Job
    from awsgluedq.transforms import EvaluateDataQuality
    from pyspark.context import SparkContext

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    business_date = args["business_date"]
    raw_day_root = join_path(args["raw_root"], f"business_date={business_date}")
    curated_day_root = join_path(args["processed_root"], "curated", f"business_date={business_date}")

    print(f"Starting master-data Glue job for business_date={business_date}")

    # Step 1: Read the current-day master snapshots from the raw landing area.
    customers_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(raw_day_root, "customers_snapshot.csv"),
        "customers_frame",
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
        join_path(raw_day_root, "accounts_snapshot.csv"),
        "accounts_frame",
        [
            "account_id",
            "customer_id",
            "advisor_id",
            "account_segment",
            "account_status",
            "opened_date",
        ],
    )

    # Step 2: Evaluate data quality rules for customer and account datasets.
    customer_dq_results = evaluate_dataset(
        EvaluateDataQuality,
        customers_frame,
        build_customer_ruleset(),
        f"brokerage_master_customers_{business_date}",
    )
    account_dq_results = evaluate_dataset(
        EvaluateDataQuality,
        accounts_frame,
        build_account_ruleset(),
        f"brokerage_master_accounts_{business_date}",
    )

    # Step 3: Write the curated master snapshots that downstream Redshift loads consume.
    write_csv_dynamic_frame(
        glue_context,
        customers_frame,
        join_path(curated_day_root, "master", "customers_snapshot"),
        "write_customers_snapshot",
    )
    write_csv_dynamic_frame(
        glue_context,
        accounts_frame,
        join_path(curated_day_root, "master", "accounts_snapshot"),
        "write_accounts_snapshot",
    )

    # Step 4: Persist detailed Glue Data Quality results for audit and debugging.
    write_json_dataframe(
        glue_context,
        customer_dq_results.toDF(),
        join_path(curated_day_root, "dq", "customers_dq_results"),
    )
    write_json_dataframe(
        glue_context,
        account_dq_results.toDF(),
        join_path(curated_day_root, "dq", "accounts_dq_results"),
    )

    # Step 5: Commit the Glue job after all master outputs are written successfully.
    print("Master-data Glue job completed successfully.")
    job.commit()


if __name__ == "__main__":
    main()
