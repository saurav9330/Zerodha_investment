"""Generate realistic batch source data for a retail stock-broking platform.

The generator creates three categories of data:
1. Slowly changing master snapshots such as customers and accounts.
2. Transaction feeds such as orders, trades, fund movements, and settlements.
3. Market and holdings views that help build a richer warehouse model.

The code stays intentionally functional and well-commented so learners can
follow how the source domain is modeled.
"""

from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


CITY_STATE_COUNTRY = [
    ("Mumbai", "Maharashtra", "India"),
    ("Pune", "Maharashtra", "India"),
    ("Bengaluru", "Karnataka", "India"),
    ("Hyderabad", "Telangana", "India"),
    ("Delhi", "Delhi", "India"),
    ("Chennai", "Tamil Nadu", "India"),
    ("Ahmedabad", "Gujarat", "India"),
]

SECTORS = ["BANKING", "IT", "PHARMA", "AUTO", "FMCG", "ENERGY", "INSURANCE"]
EXCHANGES = ["NSE", "BSE"]
ORDER_TYPES = ["MARKET", "LIMIT"]
ORDER_STATUS = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", "REJECTED"]
SEGMENTS = ["EQUITY_DELIVERY", "INTRADAY", "FNO"]
RISK_BANDS = ["LOW", "MEDIUM", "HIGH"]
KYC_STATUS = ["PENDING", "VERIFIED"]
ACCOUNT_STATUS = ["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT"]


def current_ist_date() -> str:
    """Return the current date in Asia/Kolkata timezone.

    Using IST here keeps local sample generation aligned with the schedule
    configured in EventBridge Scheduler.
    """

    return datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()


def ensure_directory(path: str | Path) -> Path:
    """Create a directory if it does not already exist and return it."""

    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def write_csv_records(path: str | Path, rows: list[dict], fieldnames: list[str]) -> None:
    """Write dictionaries to CSV with a stable column order."""

    destination = Path(path)
    ensure_directory(destination.parent)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


@dataclass
class GeneratorConfig:
    """Configuration for how much data gets produced."""

    output_root: str
    start_date: str
    days: int
    customers: int
    accounts: int
    instruments: int
    orders_per_day: int
    seed: int


def _date_range(start_date: str, days: int) -> list[str]:
    """Return a list of YYYY-MM-DD business dates."""

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days)]


def _pick_city_bundle() -> tuple[str, str, str]:
    """Randomly pick a geography hierarchy."""

    return random.choice(CITY_STATE_COUNTRY)


def build_reference_data(config: GeneratorConfig) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Create reusable master data that later daily snapshots are derived from."""

    advisors = [
        {"advisor_id": f"ADV{index:03d}", "advisor_name": f"Advisor {index:03d}", "region": random.choice(["WEST", "SOUTH", "NORTH"])}
        for index in range(1, 16)
    ]

    instruments: list[dict] = []
    for index in range(1, config.instruments + 1):
        sector = random.choice(SECTORS)
        symbol = f"STK{index:04d}"
        instruments.append(
            {
                "instrument_id": f"INS{index:05d}",
                "symbol": symbol,
                "instrument_name": f"{symbol} Limited",
                "company_name": f"{symbol} Holdings",
                "sector_name": sector,
                "exchange_code": random.choice(EXCHANGES),
                "listing_country": "India",
            }
        )

    customers: list[dict] = []
    for index in range(1, config.customers + 1):
        city, state, country = _pick_city_bundle()
        customers.append(
            {
                "customer_id": f"CUST{index:06d}",
                "customer_name": f"Customer {index:06d}",
                "email": f"customer{index:06d}@example.com",
                "mobile_number": f"98{random.randint(10_000_000, 99_999_999)}",
                "city_name": city,
                "state_name": state,
                "country_name": country,
                "risk_band": random.choice(RISK_BANDS),
                "kyc_status": random.choices(KYC_STATUS, weights=[1, 9], k=1)[0],
            }
        )

    accounts: list[dict] = []
    for index in range(1, config.accounts + 1):
        customer = random.choice(customers)
        accounts.append(
            {
                "account_id": f"ACC{index:06d}",
                "customer_id": customer["customer_id"],
                "advisor_id": random.choice(advisors)["advisor_id"],
                "account_segment": random.choice(SEGMENTS),
                "account_status": random.choice(ACCOUNT_STATUS),
                "opened_date": (date(2024, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat(),
            }
        )

    return customers, accounts, instruments, advisors


def apply_daily_master_changes(
    base_customers: list[dict],
    base_accounts: list[dict],
    business_date_index: int,
) -> tuple[list[dict], list[dict]]:
    """Mutate a small slice of master data each day to make SCD2 meaningful."""

    customers = [dict(row) for row in base_customers]
    accounts = [dict(row) for row in base_accounts]

    if business_date_index >= 1:
        for row in customers[: max(1, len(customers) // 20)]:
            row["risk_band"] = "HIGH" if row["risk_band"] != "HIGH" else "MEDIUM"
            row["city_name"], row["state_name"], row["country_name"] = _pick_city_bundle()

    if business_date_index >= 2:
        for row in accounts[: max(1, len(accounts) // 15)]:
            row["account_segment"] = "FNO" if row["account_segment"] != "FNO" else "EQUITY_DELIVERY"
            row["account_status"] = "ACTIVE"
            advisor_number = random.randint(1, 15)
            row["advisor_id"] = f"ADV{advisor_number:03d}"

    return customers, accounts


def generate_orders_for_day(
    business_date: str,
    accounts: list[dict],
    instruments: list[dict],
    order_count: int,
) -> list[dict]:
    """Generate realistic order-level events for one business date."""

    orders: list[dict] = []
    base_timestamp = datetime.strptime(f"{business_date} 09:15:00", "%Y-%m-%d %H:%M:%S")
    active_accounts = [account for account in accounts if account["account_status"] == "ACTIVE"]

    for index in range(1, order_count + 1):
        account = random.choice(active_accounts)
        instrument = random.choice(instruments)
        quantity = random.choice([1, 2, 5, 10, 20, 25, 50, 100])
        limit_price = round(random.uniform(120, 2400), 2)
        order_side = random.choice(["BUY", "SELL"])
        order_status = random.choice(ORDER_STATUS)
        created_at = base_timestamp + timedelta(seconds=index * random.randint(1, 3))
        orders.append(
            {
                "order_id": f"ORD{business_date.replace('-', '')}{index:07d}",
                "business_date": business_date,
                "account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "instrument_id": instrument["instrument_id"],
                "order_timestamp": created_at.isoformat(),
                "order_side": order_side,
                "order_type": random.choice(ORDER_TYPES),
                "ordered_quantity": str(quantity),
                "limit_price": f"{limit_price:.2f}",
                "order_status": order_status,
            }
        )

    # Add one controlled bad record to make the DQ layer meaningful.
    if orders:
        bad_row = dict(orders[-1])
        bad_row["order_id"] = orders[0]["order_id"]
        bad_row["ordered_quantity"] = "0"
        orders.append(bad_row)

    return orders


def generate_trades_from_orders(orders: list[dict]) -> list[dict]:
    """Create trade executions only for completed orders."""

    trades: list[dict] = []
    trade_index = 1
    for order in orders:
        if order["order_status"] != "COMPLETED":
            continue

        ordered_quantity = int(float(order["ordered_quantity"]))
        if ordered_quantity <= 0:
            continue

        executed_price = round(float(order["limit_price"]) * random.uniform(0.985, 1.015), 2)
        trade_time = datetime.fromisoformat(order["order_timestamp"]) + timedelta(seconds=random.randint(2, 45))
        gross_amount = ordered_quantity * executed_price
        trades.append(
            {
                "trade_id": f"TRD{trade_index:08d}",
                "business_date": order["business_date"],
                "order_id": order["order_id"],
                "account_id": order["account_id"],
                "customer_id": order["customer_id"],
                "instrument_id": order["instrument_id"],
                "trade_side": order["order_side"],
                "trade_timestamp": trade_time.isoformat(),
                "executed_quantity": str(ordered_quantity),
                "executed_price": f"{executed_price:.2f}",
                "brokerage_fee": f"{max(5.0, gross_amount * 0.0003):.2f}",
                "tax_amount": f"{gross_amount * 0.00018:.2f}",
            }
        )
        trade_index += 1
    return trades


def generate_fund_movements(trades: list[dict]) -> list[dict]:
    """Create settlement-linked cash movements plus a small number of wallet top-ups."""

    fund_rows: list[dict] = []
    index = 1
    seen_accounts: set[str] = set()
    for trade in trades:
        gross_amount = int(float(trade["executed_quantity"])) * float(trade["executed_price"])
        signed_amount = gross_amount if trade["trade_side"] == "SELL" else -gross_amount
        fund_rows.append(
            {
                "fund_movement_id": f"FND{index:08d}",
                "business_date": trade["business_date"],
                "account_id": trade["account_id"],
                "movement_type": "TRADE_SETTLEMENT",
                "amount": f"{signed_amount:.2f}",
                "currency_code": "INR",
            }
        )
        index += 1

        if trade["account_id"] not in seen_accounts and random.random() < 0.20:
            fund_rows.append(
                {
                    "fund_movement_id": f"FND{index:08d}",
                    "business_date": trade["business_date"],
                    "account_id": trade["account_id"],
                    "movement_type": "DEPOSIT",
                    "amount": f"{random.choice([5000, 10000, 25000, 50000]):.2f}",
                    "currency_code": "INR",
                }
            )
            index += 1
            seen_accounts.add(trade["account_id"])

    return fund_rows


def generate_settlements(trades: list[dict]) -> list[dict]:
    """Generate T+1 settlement rows from trades."""

    settlement_rows: list[dict] = []
    for index, trade in enumerate(trades, start=1):
        settlement_rows.append(
            {
                "settlement_id": f"SET{index:08d}",
                "trade_id": trade["trade_id"],
                "business_date": trade["business_date"],
                "settlement_date": (
                    datetime.strptime(trade["business_date"], "%Y-%m-%d").date() + timedelta(days=1)
                ).isoformat(),
                "settlement_status": "PENDING" if index % 10 == 0 else "SETTLED",
            }
        )
    return settlement_rows


def generate_market_prices(business_date: str, instruments: list[dict]) -> list[dict]:
    """Generate a daily close price for every instrument."""

    prices: list[dict] = []
    for instrument in instruments:
        close_price = round(random.uniform(125, 2500), 2)
        prices.append(
            {
                "business_date": business_date,
                "instrument_id": instrument["instrument_id"],
                "close_price": f"{close_price:.2f}",
            }
        )
    return prices


def generate_holdings_snapshot(trades: list[dict], prices: list[dict]) -> list[dict]:
    """Build an end-of-day holdings snapshot from the trade feed."""

    price_lookup = {row["instrument_id"]: float(row["close_price"]) for row in prices}
    aggregate: dict[tuple[str, str], dict[str, float]] = {}

    for trade in trades:
        key = (trade["account_id"], trade["instrument_id"])
        aggregate.setdefault(key, {"quantity": 0.0, "cost": 0.0})
        quantity = float(trade["executed_quantity"])
        price = float(trade["executed_price"])
        signed_quantity = quantity if trade["trade_side"] == "BUY" else -quantity
        aggregate[key]["quantity"] += signed_quantity
        aggregate[key]["cost"] += signed_quantity * price

    snapshots: list[dict] = []
    for (account_id, instrument_id), measures in aggregate.items():
        if measures["quantity"] <= 0:
            continue
        market_price = price_lookup[instrument_id]
        market_value = measures["quantity"] * market_price
        avg_cost = measures["cost"] / measures["quantity"]
        snapshots.append(
            {
                "account_id": account_id,
                "instrument_id": instrument_id,
                "holding_quantity": f"{measures['quantity']:.0f}",
                "average_cost": f"{avg_cost:.2f}",
                "market_price": f"{market_price:.2f}",
                "market_value": f"{market_value:.2f}",
            }
        )
    return snapshots


def write_day_data(output_root: str, business_date: str, dataset_name: str, rows: list[dict]) -> None:
    """Persist one dataset under a partition-like folder structure."""

    if not rows:
        return
    day_directory = ensure_directory(Path(output_root) / "raw" / f"business_date={business_date}")
    fieldnames = list(rows[0].keys())
    write_csv_records(day_directory / f"{dataset_name}.csv", rows, fieldnames)


def generate_data(config: GeneratorConfig) -> None:
    """Generate the full three-day source landscape."""

    random.seed(config.seed)
    ensure_directory(config.output_root)
    business_dates = _date_range(config.start_date, config.days)
    customers, accounts, instruments, advisors = build_reference_data(config)

    # Advisors and instruments are treated as relatively static reference data.
    ensure_directory(Path(config.output_root) / "reference")
    write_csv_records(
        Path(config.output_root) / "reference" / "advisors.csv",
        advisors,
        list(advisors[0].keys()),
    )
    write_csv_records(
        Path(config.output_root) / "reference" / "instruments.csv",
        instruments,
        list(instruments[0].keys()),
    )

    for day_index, business_date in enumerate(business_dates):
        day_customers, day_accounts = apply_daily_master_changes(customers, accounts, day_index)
        orders = generate_orders_for_day(business_date, day_accounts, instruments, config.orders_per_day)
        trades = generate_trades_from_orders(orders)
        fund_movements = generate_fund_movements(trades)
        settlements = generate_settlements(trades)
        market_prices = generate_market_prices(business_date, instruments)
        holdings = generate_holdings_snapshot(trades, market_prices)

        # Daily snapshots make downstream SCD2 processing explicit.
        write_day_data(config.output_root, business_date, "customers_snapshot", day_customers)
        write_day_data(config.output_root, business_date, "accounts_snapshot", day_accounts)
        write_day_data(config.output_root, business_date, "orders", orders)
        write_day_data(config.output_root, business_date, "trades", trades)
        write_day_data(config.output_root, business_date, "fund_movements", fund_movements)
        write_day_data(config.output_root, business_date, "settlements", settlements)
        write_day_data(config.output_root, business_date, "market_prices", market_prices)
        write_day_data(config.output_root, business_date, "holdings_snapshot", holdings)


def parse_args() -> GeneratorConfig:
    """Parse CLI arguments for local execution or CodeBuild-driven generation."""

    parser = argparse.ArgumentParser(description="Generate sample brokerage data for three daily pipeline runs.")
    parser.add_argument("--output-root", default="generated_data", help="Base directory where raw data will be created.")
    parser.add_argument(
        "--start-date",
        default=current_ist_date(),
        help="First business date in YYYY-MM-DD format. Defaults to the current date in Asia/Kolkata timezone.",
    )
    parser.add_argument("--days", type=int, default=3, help="How many daily partitions to generate.")
    parser.add_argument("--customers", type=int, default=500, help="How many customers to create.")
    parser.add_argument("--accounts", type=int, default=800, help="How many accounts to create.")
    parser.add_argument("--instruments", type=int, default=120, help="How many tradable instruments to create.")
    parser.add_argument("--orders-per-day", type=int, default=3500, help="Approximate order count per day.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatable outputs.")
    args = parser.parse_args()
    return GeneratorConfig(
        output_root=args.output_root,
        start_date=args.start_date,
        days=args.days,
        customers=args.customers,
        accounts=args.accounts,
        instruments=args.instruments,
        orders_per_day=args.orders_per_day,
        seed=args.seed,
    )


if __name__ == "__main__":
    generate_data(parse_args())
