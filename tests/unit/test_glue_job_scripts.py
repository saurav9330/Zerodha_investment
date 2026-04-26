"""Tests for the self-contained Glue PySpark job scripts."""

import csv
from pathlib import Path

from src.glue.jobs import job_master_data, job_publish_dq, job_redshift_load, job_transaction_data


def test_master_job_rulesets_include_glue_dq_checks() -> None:
    customer_ruleset = job_master_data.build_customer_ruleset()
    account_ruleset = job_master_data.build_account_ruleset()

    assert 'IsComplete "customer_id"' in customer_ruleset
    assert 'IsUnique "customer_id"' in customer_ruleset
    assert 'ColumnValues "account_status" in ["ACTIVE", "DORMANT"]' in account_ruleset


def test_transaction_job_rulesets_include_numeric_and_status_rules() -> None:
    order_ruleset = job_transaction_data.build_order_ruleset()
    trade_ruleset = job_transaction_data.build_trade_ruleset()

    assert 'ColumnValues "ordered_quantity" > 0' in order_ruleset
    assert 'ColumnValues "order_status" in ["COMPLETED", "CANCELLED", "REJECTED"]' in order_ruleset
    assert 'ColumnValues "executed_price" > 0' in trade_ruleset


def test_transaction_job_holdings_fact_columns_match_redshift_fact_order() -> None:
    assert job_transaction_data.build_holdings_fact_columns() == [
        "business_date",
        "account_id",
        "instrument_id",
        "holding_quantity",
        "average_cost",
        "market_price",
        "market_value",
    ]


def test_redshift_load_sql_targets_serverless_copy_paths() -> None:
    sql_statements = job_redshift_load.build_sql_statements(
        business_date="2026-04-19",
        iam_role_arn="arn:aws:iam::123456789012:role/demo-role",
        data_bucket="demo-curated-bucket",
        raw_root="s3://demo-raw-bucket/raw",
    )
    sql_text = job_redshift_load.build_sql_bundle(sql_statements)

    assert "copy brokerage_dw.fact_orders" in sql_text.lower()
    assert "dim_customer_scd2" in sql_text.lower()
    assert "dim_country" in sql_text.lower()
    assert "dim_exchange" in sql_text.lower()
    assert "s3://demo-curated-bucket/curated/business_date=2026-04-19" in sql_text
    assert "dim_account_scd2" in sql_text.lower()


def test_redshift_load_bootstraps_warehouse_tables() -> None:
    sql_statements = job_redshift_load.build_sql_statements(
        business_date="2026-04-19",
        iam_role_arn="arn:aws:iam::123456789012:role/demo-role",
        data_bucket="demo-curated-bucket",
        raw_root="s3://demo-raw-bucket/raw",
    )
    sql_text = job_redshift_load.build_sql_bundle(sql_statements).lower()

    assert "create table if not exists brokerage_dw.dim_date" in sql_text
    assert "create table if not exists brokerage_dw.fact_orders" in sql_text
    assert "create table if not exists brokerage_dw.dim_customer_scd2" in sql_text


def test_redshift_load_chunks_large_statement_lists_for_data_api() -> None:
    sql_batches = job_redshift_load.chunk_sql_statements([f"select {index}" for index in range(95)])

    assert len(sql_batches) == 3
    assert len(sql_batches[0]) == 40
    assert len(sql_batches[1]) == 40
    assert len(sql_batches[2]) == 15


def test_master_script_uses_glue_pyspark_primitives() -> None:
    script_text = Path("src/glue/jobs/job_master_data.py").read_text(encoding="utf-8")

    assert "GlueContext" in script_text
    assert "Job(" in script_text
    assert "EvaluateDataQuality" in script_text


def test_master_column_validation_raises_clear_error() -> None:
    try:
        job_master_data.validate_required_columns(["customer_id", "customer_name"], ["customer_id", "risk_band"], "customers_frame")
    except RuntimeError as error:
        assert "risk_band" in str(error)
        assert "customers_frame" in str(error)
    else:
        raise AssertionError("Expected validate_required_columns to fail for missing master columns.")


def test_transaction_column_validation_raises_clear_error() -> None:
    try:
        job_transaction_data.validate_required_columns(["trade_id", "executed_price"], ["trade_id", "executed_quantity"], "trades_frame")
    except RuntimeError as error:
        assert "executed_quantity" in str(error)
        assert "trades_frame" in str(error)
    else:
        raise AssertionError("Expected validate_required_columns to fail for missing transaction columns.")


def test_generated_transaction_headers_match_job_expectations() -> None:
    base = Path("generated_data/raw/business_date=2026-04-19")
    expected_headers = {
        "orders.csv": [
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
        "trades.csv": [
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
        "fund_movements.csv": [
            "fund_movement_id",
            "business_date",
            "account_id",
            "movement_type",
            "amount",
            "currency_code",
        ],
        "settlements.csv": [
            "settlement_id",
            "trade_id",
            "business_date",
            "settlement_date",
            "settlement_status",
        ],
        "market_prices.csv": [
            "business_date",
            "instrument_id",
            "close_price",
        ],
        "holdings_snapshot.csv": [
            "account_id",
            "instrument_id",
            "holding_quantity",
            "average_cost",
            "market_price",
            "market_value",
        ],
    }

    for file_name, expected_header in expected_headers.items():
        with (base / file_name).open() as handle:
            actual_header = next(csv.reader(handle))
        assert actual_header == expected_header


def test_dq_publish_job_uses_glue_data_quality_rulesets() -> None:
    assert 'IsComplete "customer_id"' in job_publish_dq.build_customer_ruleset()
    assert 'IsUnique "account_id"' in job_publish_dq.build_account_ruleset()
    assert 'ColumnValues "gross_trade_amount" > 0' in job_publish_dq.build_trade_ruleset()


def test_redshift_load_script_supports_secret_arn_argument() -> None:
    script_text = Path("src/glue/jobs/job_redshift_load.py").read_text(encoding="utf-8")

    assert "secret_arn" in script_text
    assert "SecretArn" in script_text
