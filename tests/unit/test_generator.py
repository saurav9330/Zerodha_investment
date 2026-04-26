"""Unit tests for the brokerage data generator."""

import csv
from pathlib import Path

from src.data_generator.generate_brokerage_data import GeneratorConfig, generate_data


def test_generator_creates_expected_daily_files(tmp_path: Path) -> None:
    config = GeneratorConfig(
        output_root=str(tmp_path),
        start_date="2026-04-01",
        days=3,
        customers=20,
        accounts=30,
        instruments=10,
        orders_per_day=25,
        seed=7,
    )
    generate_data(config)

    day_path = tmp_path / "raw" / "business_date=2026-04-01"
    assert (day_path / "customers_snapshot.csv").exists()
    assert (day_path / "orders.csv").exists()

    with (day_path / "orders.csv").open("r", encoding="utf-8", newline="") as handle:
        orders = list(csv.DictReader(handle))
    assert len(orders) >= 25


def test_generator_creates_reference_data(tmp_path: Path) -> None:
    config = GeneratorConfig(
        output_root=str(tmp_path),
        start_date="2026-04-01",
        days=1,
        customers=10,
        accounts=15,
        instruments=5,
        orders_per_day=10,
        seed=11,
    )
    generate_data(config)
    assert (tmp_path / "reference" / "advisors.csv").exists()
    assert (tmp_path / "reference" / "instruments.csv").exists()
