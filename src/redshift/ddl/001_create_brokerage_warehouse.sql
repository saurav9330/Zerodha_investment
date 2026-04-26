-- Retail brokerage warehouse schema for Amazon Redshift Serverless.
-- The design intentionally uses a snowflake model instead of a single flat star
-- so learners practice production-style dimensional modeling.

create schema if not exists brokerage_dw;
set search_path to brokerage_dw;

create table if not exists dim_country (
    country_key bigint identity(1, 1),
    country_name varchar(100) not null,
    primary key (country_key)
);

create table if not exists dim_state (
    state_key bigint identity(1, 1),
    state_name varchar(100) not null,
    country_key bigint not null,
    primary key (state_key)
);

create table if not exists dim_city (
    city_key bigint identity(1, 1),
    city_name varchar(100) not null,
    state_key bigint not null,
    primary key (city_key)
);

create table if not exists dim_sector (
    sector_key bigint identity(1, 1),
    sector_name varchar(100) not null,
    primary key (sector_key)
);

create table if not exists dim_company (
    company_key bigint identity(1, 1),
    company_name varchar(200) not null,
    sector_key bigint not null,
    primary key (company_key)
);

create table if not exists dim_exchange (
    exchange_key bigint identity(1, 1),
    exchange_code varchar(20) not null,
    primary key (exchange_key)
);

create table if not exists dim_instrument (
    instrument_key bigint identity(1, 1),
    instrument_id varchar(30) not null,
    symbol varchar(30) not null,
    instrument_name varchar(200) not null,
    company_key bigint not null,
    exchange_key bigint not null,
    listing_country varchar(100) not null,
    primary key (instrument_key)
);

create table if not exists dim_advisor_scd2 (
    advisor_key bigint identity(1, 1),
    advisor_id varchar(30) not null,
    advisor_name varchar(200) not null,
    region varchar(50) not null,
    record_start_date date not null,
    record_end_date date not null,
    is_current char(1) not null,
    primary key (advisor_key)
);

create table if not exists dim_customer_scd2 (
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
);

create table if not exists dim_account_scd2 (
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
);

create table if not exists dim_date (
    date_key integer not null,
    business_date date not null,
    year_number integer not null,
    month_number integer not null,
    day_number integer not null,
    weekday_name varchar(20) not null,
    primary key (date_key)
);

create table if not exists fact_orders (
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
sortkey (business_date, account_id);

create table if not exists fact_trades (
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
sortkey (business_date, trade_timestamp);

create table if not exists fact_fund_movements (
    fund_movement_id varchar(40) not null,
    business_date date not null,
    account_id varchar(30) not null,
    movement_type varchar(30) not null,
    amount numeric(18, 2) not null,
    currency_code varchar(10) not null
)
diststyle auto
sortkey (business_date, account_id);

create table if not exists fact_settlements (
    settlement_id varchar(40) not null,
    trade_id varchar(40) not null,
    business_date date not null,
    settlement_date date not null,
    settlement_status varchar(20) not null
)
diststyle auto
sortkey (business_date, settlement_date);

create table if not exists fact_daily_holdings (
    business_date date not null,
    account_id varchar(30) not null,
    instrument_id varchar(30) not null,
    holding_quantity numeric(18, 2) not null,
    average_cost numeric(18, 2) not null,
    market_price numeric(18, 2) not null,
    market_value numeric(18, 2) not null
)
diststyle auto
sortkey (business_date, account_id, instrument_id);

create table if not exists audit_dq_results (
    audit_run_ts timestamp default current_timestamp,
    business_date date not null,
    dataset_name varchar(100) not null,
    pipeline_status varchar(20) not null,
    finding_count integer not null
);
