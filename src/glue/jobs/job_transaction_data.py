"""AWS Glue PySpark job for brokerage transaction datasets.

This job reads raw order, trade, fund movement, settlement, market price, and
holdings files from S3, evaluates data quality, derives trade fact measures,
and writes curated transaction outputs.
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
        parser.add_argument("--JOB_NAME", default="local_transaction_data_job")
        parser.add_argument("--raw_root", "--raw-root", dest="raw_root", default="generated_data/raw")
        parser.add_argument("--processed_root", "--processed-root", dest="processed_root", default="generated_data/processed")
        parser.add_argument("--business_date", "--business-date", dest="business_date", required=True)
        return vars(parser.parse_args())


def build_order_ruleset() -> str:
    """Return the Glue Data Quality ruleset for orders."""

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
    """Return the Glue Data Quality ruleset for trades."""

    return """
Rules = [
    IsComplete "trade_id",
    IsComplete "order_id",
    IsComplete "account_id",
    IsUnique "trade_id",
    ColumnValues "executed_quantity" > 0,
    ColumnValues "executed_price" > 0,
    ColumnValues "brokerage_fee" >= 0
]
""".strip()


def build_holdings_fact_columns() -> list[str]:
    """Return the Redshift-ready holdings fact column order."""

    return [
        "business_date",
        "account_id",
        "instrument_id",
        "holding_quantity",
        "average_cost",
        "market_price",
        "market_value",
    ]


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

    This avoids Glue CSV schema surprises on reruns and produces a predictable
    set of columns before DQ and downstream transformations start.
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
    """Execute the transaction-data Glue job.

    The job keeps the flow intentionally simple:
    1. read the daily transactional datasets
    2. evaluate Glue Data Quality rulesets
    3. derive trade measures needed downstream
    4. write curated transaction outputs
    5. persist DQ and operational metrics
    6. commit the Glue run
    """

    args = resolve_args()

    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.job import Job
    from awsgluedq.transforms import EvaluateDataQuality
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    business_date = args["business_date"]
    raw_day_root = join_path(args["raw_root"], f"business_date={business_date}")
    curated_day_root = join_path(args["processed_root"], "curated", f"business_date={business_date}")
    spark = glue_context.spark_session

    print(f"Starting transaction-data Glue job for business_date={business_date}")

    # Step 1: Read all transaction-style datasets for the target business date.
    orders_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(raw_day_root, "orders.csv"),
        "orders_frame",
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
        join_path(raw_day_root, "trades.csv"),
        "trades_frame",
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
        ],
    )
    fund_movements_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(raw_day_root, "fund_movements.csv"),
        "fund_movements_frame",
        [
            "fund_movement_id",
            "business_date",
            "account_id",
            "movement_type",
            "amount",
            "currency_code",
        ],
    )
    settlements_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(raw_day_root, "settlements.csv"),
        "settlements_frame",
        [
            "settlement_id",
            "trade_id",
            "business_date",
            "settlement_date",
            "settlement_status",
        ],
    )
    market_prices_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(raw_day_root, "market_prices.csv"),
        "market_prices_frame",
        [
            "business_date",
            "instrument_id",
            "close_price",
        ],
    )
    holdings_frame = read_csv_dynamic_frame(
        glue_context,
        DynamicFrame,
        join_path(raw_day_root, "holdings_snapshot.csv"),
        "holdings_frame",
        [
            "account_id",
            "instrument_id",
            "holding_quantity",
            "average_cost",
            "market_price",
            "market_value",
        ],
    )

    # Step 2: Run Glue Data Quality rules on orders and trades before warehouse loading.
    orders_dq_results = evaluate_dataset(
        EvaluateDataQuality,
        orders_frame,
        build_order_ruleset(),
        f"brokerage_transactions_orders_{business_date}",
    )
    trades_dq_results = evaluate_dataset(
        EvaluateDataQuality,
        trades_frame,
        build_trade_ruleset(),
        f"brokerage_transactions_trades_{business_date}",
    )

    # Step 3: Derive additional fact measures that analysts will query in Redshift.
    trades_df = trades_frame.toDF().withColumn(
        "gross_trade_amount",
        F.round(F.col("executed_quantity").cast("double") * F.col("executed_price").cast("double"), 2),
    ).withColumn(
        "net_trade_amount",
        F.round(
            (
                F.col("executed_quantity").cast("double") * F.col("executed_price").cast("double")
            )
            + F.col("brokerage_fee").cast("double")
            + F.col("tax_amount").cast("double"),
            2,
        ),
    )
    trade_facts_frame = DynamicFrame.fromDF(trades_df, glue_context, "trade_facts_frame")

    # Step 3b: Add the business date to holdings so the curated output matches
    # the Redshift fact_daily_holdings table shape exactly.
    holdings_df = holdings_frame.toDF().withColumn("business_date", F.lit(business_date)).select(*build_holdings_fact_columns())
    holdings_facts_frame = DynamicFrame.fromDF(holdings_df, glue_context, "holdings_facts_frame")

    # Step 4: Write all curated transaction outputs back to S3 for downstream Redshift COPY.
    write_csv_dynamic_frame(glue_context, orders_frame, join_path(curated_day_root, "transactions", "orders"), "write_orders")
    write_csv_dynamic_frame(glue_context, trade_facts_frame, join_path(curated_day_root, "transactions", "trades"), "write_trades")
    write_csv_dynamic_frame(
        glue_context,
        fund_movements_frame,
        join_path(curated_day_root, "transactions", "fund_movements"),
        "write_fund_movements",
    )
    write_csv_dynamic_frame(
        glue_context,
        settlements_frame,
        join_path(curated_day_root, "transactions", "settlements"),
        "write_settlements",
    )
    write_csv_dynamic_frame(
        glue_context,
        market_prices_frame,
        join_path(curated_day_root, "transactions", "market_prices"),
        "write_market_prices",
    )
    write_csv_dynamic_frame(
        glue_context,
        holdings_facts_frame,
        join_path(curated_day_root, "transactions", "holdings_snapshot"),
        "write_holdings",
    )

    # Step 5: Save dataset-level DQ results and transaction metrics for operational visibility.
    write_json_dataframe(glue_context, orders_dq_results.toDF(), join_path(curated_day_root, "dq", "orders_dq_results"))
    write_json_dataframe(glue_context, trades_dq_results.toDF(), join_path(curated_day_root, "dq", "trades_dq_results"))

    metrics_rows = [
        {
            "business_date": business_date,
            "order_count": orders_frame.count(),
            "trade_count": trade_facts_frame.count(),
            "fund_movement_count": fund_movements_frame.count(),
            "brokerage_fee_total": float(
                trades_df.agg(F.round(F.sum(F.col("brokerage_fee").cast("double")), 2).alias("brokerage_fee_total")).collect()[0][
                    "brokerage_fee_total"
                ]
                or 0.0
            ),
        }
    ]
    metrics_df = spark.createDataFrame(metrics_rows)
    write_json_dataframe(glue_context, metrics_df, join_path(curated_day_root, "dq", "transaction_metrics"))

    # Step 6: Commit the Glue job only after all curated transaction outputs are stored.
    print("Transaction-data Glue job completed successfully.")
    job.commit()


if __name__ == "__main__":
    main()
