"""AWS Glue job that executes Redshift Serverless load statements.

This job builds a readable SQL bundle, saves it for audit/debugging, and then
submits the statements to Amazon Redshift Serverless through the Redshift Data
API as one transaction.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from urllib.parse import urlparse

MAX_BATCH_SQLS = 40


def join_path(root: str, *parts: str) -> str:
    """Join local paths and S3 URIs without adding double slashes."""

    cleaned_root = root.rstrip("/")
    cleaned_parts = [part.strip("/") for part in parts]
    return "/".join([cleaned_root] + cleaned_parts)


def resolve_args() -> dict[str, str]:
    """Resolve arguments from Glue runtime or local CLI execution."""

    try:
        from awsglue.utils import getResolvedOptions

        return getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "raw_root",
                "processed_root",
                "business_date",
                "iam_role_arn",
                "data_bucket",
                "workgroup_name",
                "database_name",
                "secret_arn",
            ],
        )
    except ImportError:
        parser = argparse.ArgumentParser()
        parser.add_argument("--JOB_NAME", default="local_redshift_load_job")
        parser.add_argument("--raw_root", "--raw-root", dest="raw_root", default="generated_data/raw")
        parser.add_argument("--processed_root", "--processed-root", dest="processed_root", default="generated_data/processed")
        parser.add_argument("--business_date", "--business-date", dest="business_date", required=True)
        parser.add_argument("--iam_role_arn", "--iam-role-arn", dest="iam_role_arn", default="arn:aws:iam::123456789012:role/demo-role")
        parser.add_argument("--data_bucket", "--data-bucket", dest="data_bucket", default="demo-curated-bucket")
        parser.add_argument("--workgroup_name", "--workgroup-name", dest="workgroup_name", default="demo-workgroup")
        parser.add_argument("--database_name", "--database-name", dest="database_name", default="brokerage")
        parser.add_argument("--secret_arn", "--secret-arn", dest="secret_arn", default="")
        return vars(parser.parse_args())


def derive_reference_root(raw_root: str) -> str:
    """Convert a raw root such as `s3://bucket/raw` into `s3://bucket/reference`."""

    if raw_root.endswith("/raw"):
        return raw_root[: -len("/raw")] + "/reference"
    if raw_root.endswith("raw"):
        return raw_root[: -len("raw")] + "reference"
    return join_path(raw_root, "reference")


def build_bootstrap_statements() -> list[str]:
    """Create all warehouse tables required by the daily load sequence.

    The Redshift load job must be able to initialize an empty warehouse on its
    own, so the DDL for dimensions and facts is included directly in the load
    path instead of assuming a one-time manual bootstrap already happened.
    """

    return [
        dedent(
            """
            create schema if not exists brokerage_dw
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_country (
                country_key bigint identity(1, 1),
                country_name varchar(100) not null,
                primary key (country_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_state (
                state_key bigint identity(1, 1),
                state_name varchar(100) not null,
                country_key bigint not null,
                primary key (state_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_city (
                city_key bigint identity(1, 1),
                city_name varchar(100) not null,
                state_key bigint not null,
                primary key (city_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_sector (
                sector_key bigint identity(1, 1),
                sector_name varchar(100) not null,
                primary key (sector_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_company (
                company_key bigint identity(1, 1),
                company_name varchar(200) not null,
                sector_key bigint not null,
                primary key (company_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_exchange (
                exchange_key bigint identity(1, 1),
                exchange_code varchar(20) not null,
                primary key (exchange_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_instrument (
                instrument_key bigint identity(1, 1),
                instrument_id varchar(30) not null,
                symbol varchar(30) not null,
                instrument_name varchar(200) not null,
                company_key bigint not null,
                exchange_key bigint not null,
                listing_country varchar(100) not null,
                primary key (instrument_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_advisor_scd2 (
                advisor_key bigint identity(1, 1),
                advisor_id varchar(30) not null,
                advisor_name varchar(200) not null,
                region varchar(50) not null,
                record_start_date date not null,
                record_end_date date not null,
                is_current char(1) not null,
                primary key (advisor_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_customer_scd2 (
                customer_key bigint identity(1, 1),
                customer_id varchar(30) not null,
                customer_name varchar(200) not null,
                email varchar(200) not null,
                mobile_number varchar(30) not null,
                city_name varchar(100) not null,
                state_name varchar(100) not null,
                country_name varchar(100) not null,
                risk_band varchar(20) not null,
                kyc_status varchar(20) not null,
                record_start_date date not null,
                record_end_date date not null,
                is_current char(1) not null,
                primary key (customer_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_account_scd2 (
                account_key bigint identity(1, 1),
                account_id varchar(30) not null,
                customer_id varchar(30) not null,
                advisor_id varchar(30) not null,
                account_segment varchar(50) not null,
                account_status varchar(20) not null,
                opened_date date not null,
                record_start_date date not null,
                record_end_date date not null,
                is_current char(1) not null,
                primary key (account_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.dim_date (
                date_key integer not null,
                business_date date not null,
                year_number integer not null,
                month_number integer not null,
                day_number integer not null,
                weekday_name varchar(20) not null,
                primary key (date_key)
            )
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.fact_orders (
                order_id varchar(40) not null,
                business_date date not null,
                account_id varchar(30) not null,
                customer_id varchar(30) not null,
                instrument_id varchar(30) not null,
                order_timestamp timestamp not null,
                order_side varchar(10) not null,
                order_type varchar(20) not null,
                ordered_quantity integer not null,
                limit_price numeric(18, 2) not null,
                order_status varchar(20) not null
            )
            diststyle auto
            sortkey (business_date, account_id)
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.fact_trades (
                trade_id varchar(40) not null,
                business_date date not null,
                order_id varchar(40) not null,
                account_id varchar(30) not null,
                customer_id varchar(30) not null,
                instrument_id varchar(30) not null,
                trade_side varchar(10) not null,
                trade_timestamp timestamp not null,
                executed_quantity integer not null,
                executed_price numeric(18, 2) not null,
                brokerage_fee numeric(18, 2) not null,
                tax_amount numeric(18, 2) not null,
                gross_trade_amount numeric(18, 2) not null,
                net_trade_amount numeric(18, 2) not null
            )
            diststyle auto
            sortkey (business_date, trade_timestamp)
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.fact_fund_movements (
                fund_movement_id varchar(40) not null,
                business_date date not null,
                account_id varchar(30) not null,
                movement_type varchar(30) not null,
                amount numeric(18, 2) not null,
                currency_code varchar(10) not null
            )
            diststyle auto
            sortkey (business_date, account_id)
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.fact_settlements (
                settlement_id varchar(40) not null,
                trade_id varchar(40) not null,
                business_date date not null,
                settlement_date date not null,
                settlement_status varchar(20) not null
            )
            diststyle auto
            sortkey (business_date, settlement_date)
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.fact_daily_holdings (
                business_date date not null,
                account_id varchar(30) not null,
                instrument_id varchar(30) not null,
                holding_quantity numeric(18, 2) not null,
                average_cost numeric(18, 2) not null,
                market_price numeric(18, 2) not null,
                market_value numeric(18, 2) not null
            )
            diststyle auto
            sortkey (business_date, account_id, instrument_id)
            """
        ).strip(),
        dedent(
            """
            create table if not exists brokerage_dw.audit_dq_results (
                audit_run_ts timestamp default current_timestamp,
                business_date date not null,
                dataset_name varchar(100) not null,
                pipeline_status varchar(20) not null,
                finding_count integer not null
            )
            """
        ).strip(),
    ]


def build_sql_statements(business_date: str, iam_role_arn: str, data_bucket: str, raw_root: str) -> list[str]:
    """Build the SQL statements used for Redshift Serverless loading.

    The statement list is intentionally explicit and sequential so the load flow
    is easy to review and debug:
    1. create staging tables when needed
    2. copy current snapshots/reference data into staging
    3. load non-SCD dimensions
    4. apply SCD2 logic for advisor, customer, and account dimensions
    5. refresh current-day facts
    """

    curated_prefix = f"s3://{data_bucket}/curated/business_date={business_date}"
    reference_prefix = derive_reference_root(raw_root)

    business_date_obj = datetime.strptime(business_date, "%Y-%m-%d").date()
    date_key = int(business_date_obj.strftime("%Y%m%d"))
    year_number = business_date_obj.year
    month_number = business_date_obj.month
    day_number = business_date_obj.day
    weekday_name = business_date_obj.strftime("%A")

    return build_bootstrap_statements() + [
        dedent(
            """
            -- Step 2a: Create a customer staging table that will drive geography loads
            -- and customer SCD2 processing for this business date.
            create table if not exists brokerage_dw.stg_customer_snapshot (
                customer_id varchar(30),
                customer_name varchar(200),
                email varchar(200),
                mobile_number varchar(30),
                city_name varchar(100),
                state_name varchar(100),
                country_name varchar(100),
                risk_band varchar(20),
                kyc_status varchar(20)
            )
            """
        ).strip(),
        dedent(
            """
            -- Step 2b: Create an account staging table that will drive account SCD2
            -- processing after the curated daily snapshot is copied from S3.
            create table if not exists brokerage_dw.stg_account_snapshot (
                account_id varchar(30),
                customer_id varchar(30),
                advisor_id varchar(30),
                account_segment varchar(50),
                account_status varchar(20),
                opened_date date
            )
            """
        ).strip(),
        dedent(
            """
            -- Step 2c: Create an advisor reference staging table used to maintain
            -- dim_advisor_scd2 from the reference landing files.
            create table if not exists brokerage_dw.stg_advisor_reference (
                advisor_id varchar(30),
                advisor_name varchar(200),
                region varchar(50)
            )
            """
        ).strip(),
        dedent(
            """
            -- Step 2d: Create an instrument reference staging table used to load
            -- dim_sector, dim_company, dim_exchange, and dim_instrument.
            create table if not exists brokerage_dw.stg_instrument_reference (
                instrument_id varchar(30),
                symbol varchar(30),
                instrument_name varchar(200),
                company_name varchar(200),
                sector_name varchar(100),
                exchange_code varchar(20),
                listing_country varchar(100)
            )
            """
        ).strip(),
        dedent(
            """
            -- Step 3a: Clear staging tables so the current run starts from a clean state.
            truncate table brokerage_dw.stg_customer_snapshot
            """
        ).strip(),
        dedent(
            """
            -- Step 3b: Clear staging tables so the current run starts from a clean state.
            truncate table brokerage_dw.stg_account_snapshot
            """
        ).strip(),
        dedent(
            """
            -- Step 3c: Clear staging tables so the current run starts from a clean state.
            truncate table brokerage_dw.stg_advisor_reference
            """
        ).strip(),
        dedent(
            """
            -- Step 3d: Clear staging tables so the current run starts from a clean state.
            truncate table brokerage_dw.stg_instrument_reference
            """
        ).strip(),
        dedent(
            f"""
            -- Step 4a: Copy the curated daily customer snapshot into staging.
            copy brokerage_dw.stg_customer_snapshot
            from '{curated_prefix}/master/customers_snapshot'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 4b: Copy the curated daily account snapshot into staging.
            copy brokerage_dw.stg_account_snapshot
            from '{curated_prefix}/master/accounts_snapshot'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 4c: Copy advisor reference data into staging. This feed drives
            -- dim_advisor_scd2 and the advisor relationship on account history.
            copy brokerage_dw.stg_advisor_reference
            from '{reference_prefix}/advisors.csv'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 4d: Copy instrument reference data into staging. This feed drives
            -- sector, company, exchange, and instrument dimensions.
            copy brokerage_dw.stg_instrument_reference
            from '{reference_prefix}/instruments.csv'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 5a: Insert the current business date into dim_date if it is not
            -- already present. Facts later point to this date grain analytically.
            insert into brokerage_dw.dim_date (
                date_key,
                business_date,
                year_number,
                month_number,
                day_number,
                weekday_name
            )
            select
                {date_key},
                '{business_date}',
                {year_number},
                {month_number},
                {day_number},
                '{weekday_name}'
            where not exists (
                select 1
                from brokerage_dw.dim_date
                where business_date = '{business_date}'
            )
            """
        ).strip(),
        dedent(
            """
            -- Step 5b: Load dim_country from the distinct country values found in
            -- the staged customer snapshot.
            insert into brokerage_dw.dim_country (country_name)
            select distinct stg.country_name
            from brokerage_dw.stg_customer_snapshot stg
            left join brokerage_dw.dim_country dim
                on dim.country_name = stg.country_name
            where dim.country_name is null
            """
        ).strip(),
        dedent(
            """
            -- Step 5c: Load dim_state and attach each state to the matching country.
            insert into brokerage_dw.dim_state (state_name, country_key)
            select distinct
                stg.state_name,
                country_dim.country_key
            from brokerage_dw.stg_customer_snapshot stg
            join brokerage_dw.dim_country country_dim
                on country_dim.country_name = stg.country_name
            left join brokerage_dw.dim_state state_dim
                on state_dim.state_name = stg.state_name
               and state_dim.country_key = country_dim.country_key
            where state_dim.state_name is null
            """
        ).strip(),
        dedent(
            """
            -- Step 5d: Load dim_city and attach each city to the matching state.
            insert into brokerage_dw.dim_city (city_name, state_key)
            select distinct
                stg.city_name,
                state_dim.state_key
            from brokerage_dw.stg_customer_snapshot stg
            join brokerage_dw.dim_country country_dim
                on country_dim.country_name = stg.country_name
            join brokerage_dw.dim_state state_dim
                on state_dim.state_name = stg.state_name
               and state_dim.country_key = country_dim.country_key
            left join brokerage_dw.dim_city city_dim
                on city_dim.city_name = stg.city_name
               and city_dim.state_key = state_dim.state_key
            where city_dim.city_name is null
            """
        ).strip(),
        dedent(
            """
            -- Step 5e: Load dim_sector from the staged instrument reference feed.
            insert into brokerage_dw.dim_sector (sector_name)
            select distinct stg.sector_name
            from brokerage_dw.stg_instrument_reference stg
            left join brokerage_dw.dim_sector dim
                on dim.sector_name = stg.sector_name
            where dim.sector_name is null
            """
        ).strip(),
        dedent(
            """
            -- Step 5f: Load dim_company and map each company to the correct sector.
            insert into brokerage_dw.dim_company (company_name, sector_key)
            select distinct
                stg.company_name,
                sector_dim.sector_key
            from brokerage_dw.stg_instrument_reference stg
            join brokerage_dw.dim_sector sector_dim
                on sector_dim.sector_name = stg.sector_name
            left join brokerage_dw.dim_company company_dim
                on company_dim.company_name = stg.company_name
               and company_dim.sector_key = sector_dim.sector_key
            where company_dim.company_name is null
            """
        ).strip(),
        dedent(
            """
            -- Step 5g: Load dim_exchange from the staged instrument reference feed.
            insert into brokerage_dw.dim_exchange (exchange_code)
            select distinct stg.exchange_code
            from brokerage_dw.stg_instrument_reference stg
            left join brokerage_dw.dim_exchange dim
                on dim.exchange_code = stg.exchange_code
            where dim.exchange_code is null
            """
        ).strip(),
        dedent(
            """
            -- Step 5h: Load dim_instrument and resolve the company and exchange
            -- surrogate keys from the previously loaded snowflake branches.
            insert into brokerage_dw.dim_instrument (
                instrument_id,
                symbol,
                instrument_name,
                company_key,
                exchange_key,
                listing_country
            )
            select distinct
                stg.instrument_id,
                stg.symbol,
                stg.instrument_name,
                company_dim.company_key,
                exchange_dim.exchange_key,
                stg.listing_country
            from brokerage_dw.stg_instrument_reference stg
            join brokerage_dw.dim_sector sector_dim
                on sector_dim.sector_name = stg.sector_name
            join brokerage_dw.dim_company company_dim
                on company_dim.company_name = stg.company_name
               and company_dim.sector_key = sector_dim.sector_key
            join brokerage_dw.dim_exchange exchange_dim
                on exchange_dim.exchange_code = stg.exchange_code
            left join brokerage_dw.dim_instrument instrument_dim
                on instrument_dim.instrument_id = stg.instrument_id
            where instrument_dim.instrument_id is null
            """
        ).strip(),
        dedent(
            f"""
            -- Step 6a: Close the current advisor row when tracked advisor attributes
            -- change in the latest reference file.
            update brokerage_dw.dim_advisor_scd2
            set record_end_date = '{business_date}',
                is_current = 'N'
            from brokerage_dw.stg_advisor_reference stg
            where brokerage_dw.dim_advisor_scd2.advisor_id = stg.advisor_id
              and brokerage_dw.dim_advisor_scd2.is_current = 'Y'
              and (
                  brokerage_dw.dim_advisor_scd2.advisor_name <> stg.advisor_name
                  or brokerage_dw.dim_advisor_scd2.region <> stg.region
              )
            """
        ).strip(),
        dedent(
            f"""
            -- Step 6b: Insert the new current advisor row for brand-new advisors
            -- or for advisors whose descriptive attributes changed.
            insert into brokerage_dw.dim_advisor_scd2 (
                advisor_id,
                advisor_name,
                region,
                record_start_date,
                record_end_date,
                is_current
            )
            select
                stg.advisor_id,
                stg.advisor_name,
                stg.region,
                '{business_date}',
                '9999-12-31',
                'Y'
            from brokerage_dw.stg_advisor_reference stg
            left join brokerage_dw.dim_advisor_scd2 dim
                on dim.advisor_id = stg.advisor_id
               and dim.is_current = 'Y'
            where dim.advisor_id is null
               or dim.advisor_name <> stg.advisor_name
               or dim.region <> stg.region
            """
        ).strip(),
        dedent(
            f"""
            -- Step 6c: Close the current customer row when tracked customer
            -- attributes change in the latest daily snapshot.
            update brokerage_dw.dim_customer_scd2
            set record_end_date = '{business_date}',
                is_current = 'N'
            from brokerage_dw.stg_customer_snapshot stg
            where brokerage_dw.dim_customer_scd2.customer_id = stg.customer_id
              and brokerage_dw.dim_customer_scd2.is_current = 'Y'
              and (
                  brokerage_dw.dim_customer_scd2.city_name <> stg.city_name
                  or brokerage_dw.dim_customer_scd2.state_name <> stg.state_name
                  or brokerage_dw.dim_customer_scd2.country_name <> stg.country_name
                  or brokerage_dw.dim_customer_scd2.risk_band <> stg.risk_band
                  or brokerage_dw.dim_customer_scd2.kyc_status <> stg.kyc_status
              )
            """
        ).strip(),
        dedent(
            f"""
            -- Step 6d: Insert the new current customer row for new customers and
            -- for customers whose tracked attributes changed.
            insert into brokerage_dw.dim_customer_scd2 (
                customer_id,
                customer_name,
                email,
                mobile_number,
                city_name,
                state_name,
                country_name,
                risk_band,
                kyc_status,
                record_start_date,
                record_end_date,
                is_current
            )
            select
                stg.customer_id,
                stg.customer_name,
                stg.email,
                stg.mobile_number,
                stg.city_name,
                stg.state_name,
                stg.country_name,
                stg.risk_band,
                stg.kyc_status,
                '{business_date}',
                '9999-12-31',
                'Y'
            from brokerage_dw.stg_customer_snapshot stg
            left join brokerage_dw.dim_customer_scd2 dim
                on dim.customer_id = stg.customer_id
               and dim.is_current = 'Y'
            where dim.customer_id is null
               or dim.city_name <> stg.city_name
               or dim.state_name <> stg.state_name
               or dim.country_name <> stg.country_name
               or dim.risk_band <> stg.risk_band
               or dim.kyc_status <> stg.kyc_status
            """
        ).strip(),
        dedent(
            f"""
            -- Step 6e: Close the current account row when advisor assignment,
            -- account segment, or account status changes.
            update brokerage_dw.dim_account_scd2
            set record_end_date = '{business_date}',
                is_current = 'N'
            from brokerage_dw.stg_account_snapshot stg
            where brokerage_dw.dim_account_scd2.account_id = stg.account_id
              and brokerage_dw.dim_account_scd2.is_current = 'Y'
              and (
                  brokerage_dw.dim_account_scd2.advisor_id <> stg.advisor_id
                  or brokerage_dw.dim_account_scd2.account_segment <> stg.account_segment
                  or brokerage_dw.dim_account_scd2.account_status <> stg.account_status
              )
            """
        ).strip(),
        dedent(
            f"""
            -- Step 6f: Insert the new current account row for new accounts and
            -- for accounts whose tracked attributes changed.
            insert into brokerage_dw.dim_account_scd2 (
                account_id,
                customer_id,
                advisor_id,
                account_segment,
                account_status,
                opened_date,
                record_start_date,
                record_end_date,
                is_current
            )
            select
                stg.account_id,
                stg.customer_id,
                stg.advisor_id,
                stg.account_segment,
                stg.account_status,
                stg.opened_date,
                '{business_date}',
                '9999-12-31',
                'Y'
            from brokerage_dw.stg_account_snapshot stg
            left join brokerage_dw.dim_account_scd2 dim
                on dim.account_id = stg.account_id
               and dim.is_current = 'Y'
            where dim.account_id is null
               or dim.advisor_id <> stg.advisor_id
               or dim.account_segment <> stg.account_segment
               or dim.account_status <> stg.account_status
            """
        ).strip(),
        dedent(
            f"""
            -- Step 7a: Clear the current business-date slice before reloading fact_orders.
            delete from brokerage_dw.fact_orders
            where business_date = '{business_date}'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 7b: Clear the current business-date slice before reloading fact_trades.
            delete from brokerage_dw.fact_trades
            where business_date = '{business_date}'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 7c: Clear the current business-date slice before reloading fact_fund_movements.
            delete from brokerage_dw.fact_fund_movements
            where business_date = '{business_date}'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 7d: Clear the current business-date slice before reloading fact_settlements.
            delete from brokerage_dw.fact_settlements
            where business_date = '{business_date}'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 7e: Clear the current business-date slice before reloading fact_daily_holdings.
            delete from brokerage_dw.fact_daily_holdings
            where business_date = '{business_date}'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 8a: Load fact_orders from the curated daily transaction output.
            copy brokerage_dw.fact_orders
            from '{curated_prefix}/transactions/orders'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 8b: Load fact_trades from the curated daily transaction output.
            copy brokerage_dw.fact_trades
            from '{curated_prefix}/transactions/trades'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 8c: Load fact_fund_movements from the curated daily transaction output.
            copy brokerage_dw.fact_fund_movements
            from '{curated_prefix}/transactions/fund_movements'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 8d: Load fact_settlements from the curated daily transaction output.
            copy brokerage_dw.fact_settlements
            from '{curated_prefix}/transactions/settlements'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
        dedent(
            f"""
            -- Step 8e: Load fact_daily_holdings from the curated daily transaction output.
            copy brokerage_dw.fact_daily_holdings
            from '{curated_prefix}/transactions/holdings_snapshot'
            iam_role '{iam_role_arn}'
            format as csv
            ignoreheader 1
            timeformat 'auto'
            dateformat 'auto'
            """
        ).strip(),
    ]


def build_sql_bundle(sql_statements: list[str]) -> str:
    """Render the statement list as one readable SQL script."""

    return ";\n\n".join(sql_statements) + ";"


def write_text(path: str, payload: str) -> None:
    """Write plain text to local storage or S3."""

    if path.startswith("s3://"):
        import boto3

        parsed = urlparse(path)
        boto3.client("s3").put_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"), Body=payload.encode("utf-8"))
        return

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(payload, encoding="utf-8")


def execute_redshift_batch(
    sql_statements: list[str],
    workgroup_name: str,
    database_name: str,
    secret_arn: str,
) -> str:
    """Submit SQL statements to Redshift Serverless as one transaction."""

    import boto3

    client = boto3.client("redshift-data")
    request = {
        "WorkgroupName": workgroup_name,
        "Database": database_name,
        "Sqls": sql_statements,
        "StatementName": "brokerage_daily_load",
    }
    if secret_arn:
        request["SecretArn"] = secret_arn

    response = client.batch_execute_statement(**request)
    return response["Id"]


def chunk_sql_statements(sql_statements: list[str], chunk_size: int = MAX_BATCH_SQLS) -> list[list[str]]:
    """Split SQL statements into Data API-compatible batch sizes."""

    return [sql_statements[index : index + chunk_size] for index in range(0, len(sql_statements), chunk_size)]


def wait_for_redshift_statement(statement_id: str) -> dict:
    """Poll the Redshift Data API until the submitted statement finishes."""

    import boto3

    client = boto3.client("redshift-data")
    while True:
        response = client.describe_statement(Id=statement_id)
        status = response["Status"]
        if status in {"FINISHED", "FAILED", "ABORTED"}:
            return response
        time.sleep(5)


def main() -> None:
    """Execute the Redshift SQL preparation and load job.

    The control flow is intentionally linear:
    1. resolve runtime arguments
    2. build readable SQL for all dimension/fact loads
    3. persist the SQL bundle to S3 for audit/debugging
    4. submit the SQL to Redshift Serverless through the Data API
    5. wait for completion and fail loudly if Redshift fails
    """

    args = resolve_args()

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Step 1: Build the SQL statements for all dimensions and facts.
    business_date = args["business_date"]
    sql_statements = build_sql_statements(
        business_date=business_date,
        iam_role_arn=args["iam_role_arn"],
        data_bucket=args["data_bucket"],
        raw_root=args["raw_root"],
    )

    # Step 2: Save the exact SQL bundle for debugging and audit visibility.
    sql_text = build_sql_bundle(sql_statements)
    output_path = join_path(
        args["processed_root"],
        "curated",
        f"business_date={business_date}",
        "sql",
        "redshift_load.sql",
    )

    print(f"Starting Redshift load Glue job for business_date={business_date}")
    write_text(output_path, sql_text)
    print(f"Saved SQL bundle to {output_path}")

    # Step 3: Submit the SQL to Redshift Serverless in API-compatible batches.
    sql_batches = chunk_sql_statements(sql_statements)
    for batch_index, sql_batch in enumerate(sql_batches, start=1):
        statement_id = execute_redshift_batch(
            sql_statements=sql_batch,
            workgroup_name=args["workgroup_name"],
            database_name=args["database_name"],
            secret_arn=args["secret_arn"],
        )
        print(f"Submitted Redshift Data API batch {batch_index}/{len(sql_batches)}: {statement_id}")

        # Step 4: Poll each batch until Redshift confirms success or returns an error.
        statement_result = wait_for_redshift_statement(statement_id)
        print(statement_result)
        if statement_result["Status"] != "FINISHED":
            raise RuntimeError(
                f"Redshift batch execution failed with status={statement_result['Status']} "
                f"error={statement_result.get('Error', 'unknown')}"
            )

    # Step 5: Mark the Glue job successful only after the warehouse load finishes.
    print("Redshift Serverless load completed successfully.")
    job.commit()


if __name__ == "__main__":
    main()
