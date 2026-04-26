-- Example analytics queries for the brokerage warehouse.
-- These are useful both for learning and for validating that the pipeline
-- created business-facing value after ingestion.

set search_path to brokerage_dw;

-- 1. Daily trading volume and brokerage collected.
select
    business_date,
    count(distinct order_id) as order_count,
    count(distinct trade_id) as trade_count,
    sum(gross_trade_amount) as gross_volume,
    sum(brokerage_fee) as brokerage_revenue
from fact_trades
group by business_date
order by business_date;

-- 2. Top 10 traded instruments by gross volume.
select
    i.symbol,
    i.instrument_name,
    sum(t.gross_trade_amount) as gross_volume
from fact_trades t
join dim_instrument i
  on i.instrument_id = t.instrument_id
group by i.symbol, i.instrument_name
order by gross_volume desc
limit 10;

-- 3. Current active customer base by risk band.
select
    c.risk_band,
    count(distinct c.customer_id) as customer_count
from dim_customer_scd2 c
where c.is_current = 'Y'
group by c.risk_band
order by customer_count desc;

-- 4. Holdings market value by sector.
select
    h.business_date,
    s.sector_name,
    sum(h.market_value) as total_market_value
from fact_daily_holdings h
join dim_instrument i
  on i.instrument_id = h.instrument_id
join dim_company co
  on co.company_key = i.company_key
join dim_sector s
  on s.sector_key = co.sector_key
group by h.business_date, s.sector_name
order by h.business_date, total_market_value desc;

-- 5. Accounts that changed segment historically using SCD2.
select
    account_id,
    account_segment,
    record_start_date,
    record_end_date,
    is_current
from dim_account_scd2
where account_id in (
    select account_id
    from dim_account_scd2
    group by account_id
    having count(*) > 1
)
order by account_id, record_start_date;

