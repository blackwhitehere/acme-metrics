"""Demo fixture setup: populate metrics store with sample computed metrics.

Usage:
    python -m acme_metrics.demo --setup
"""

from __future__ import annotations

import argparse
import math
import shutil
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from acme_metrics.store import MetricRecord, MetricsStore

DEMO_DIR = Path(__file__).parent / "_demo_data"
METRICS_DB = DEMO_DIR / "metrics.duckdb"


def generate_stock_returns_data() -> pd.DataFrame:
    """Generate daily return data for a set of stocks."""
    stocks = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]
    start = date(2024, 1, 1)
    n_days = 252

    rows: list[dict] = []
    for i, ticker in enumerate(stocks):
        price = 100.0 + i * 20
        seed = 42 + i * 7
        for d in range(n_days):
            seed = (seed * 1103515245 + 12345) & 0x7FFFFFFF
            noise = (seed / 0x7FFFFFFF - 0.5) * 6
            daily_return = noise + 0.02
            price *= 1 + daily_return / 100
            cum_return = (price / (100.0 + i * 20) - 1) * 100
            volatility = abs(math.sin(seed)) * 5 + 10
            rows.append(
                {
                    "date": start + timedelta(days=d),
                    "ticker": ticker,
                    "daily_return_pct": round(daily_return, 4),
                    "cumulative_return_pct": round(cum_return, 2),
                    "price": round(price, 2),
                    "volatility_30d": round(volatility, 2),
                }
            )

    return pd.DataFrame(rows)


def generate_data_quality_data() -> pd.DataFrame:
    """Generate data quality metrics for multiple datasets over time."""
    datasets = ["users", "orders", "products", "events"]
    start = date(2024, 1, 1)
    n_weeks = 52

    rows: list[dict] = []
    seed = 99
    for i, dataset_name in enumerate(datasets):
        base_completeness = 95.0 - i * 3
        base_rows = (i + 1) * 10000
        for w in range(n_weeks):
            seed = (seed * 1103515245 + 12345) & 0x7FFFFFFF
            jitter = (seed / 0x7FFFFFFF - 0.5) * 4
            completeness = max(80.0, min(100.0, base_completeness + jitter))
            seed = (seed * 1103515245 + 12345) & 0x7FFFFFFF
            freshness_hours = max(0.1, 2.0 + (seed / 0x7FFFFFFF - 0.5) * 3 + i * 0.5)
            row_count = int(base_rows + w * base_rows * 0.01)
            null_pct = round(100 - completeness, 2)
            rows.append(
                {
                    "date": start + timedelta(weeks=w),
                    "entity": dataset_name,
                    "completeness_pct": round(completeness, 2),
                    "null_pct": round(null_pct, 2),
                    "freshness_hours": round(freshness_hours, 2),
                    "row_count": row_count,
                }
            )

    return pd.DataFrame(rows)


def _populate_metrics_store(store: MetricsStore) -> int:
    """Compute and store summary metrics for demo datasets."""
    count = 0

    # Stock return summary metrics per ticker
    df = generate_stock_returns_data()
    for ticker, grp in df.groupby("ticker"):
        dataset_id = "data-prep:clean_prices"
        for metric_name, value in [
            (f"{ticker}_mean_return", grp["daily_return_pct"].mean()),
            (f"{ticker}_volatility", grp["volatility_30d"].mean()),
            (f"{ticker}_cumulative_return", grp["cumulative_return_pct"].iloc[-1]),
            (f"{ticker}_final_price", grp["price"].iloc[-1]),
        ]:
            store.record_metric(
                MetricRecord(dataset_id=dataset_id, metric_name=metric_name, metric_value=value)
            )
            count += 1

    # Data quality summary metrics per dataset
    dq = generate_data_quality_data()
    for entity, grp in dq.groupby("entity"):
        dataset_id = f"landing:{entity}"
        for metric_name, value in [
            ("avg_completeness", grp["completeness_pct"].mean()),
            ("avg_null_pct", grp["null_pct"].mean()),
            ("avg_freshness_hours", grp["freshness_hours"].mean()),
            ("latest_row_count", float(grp["row_count"].iloc[-1])),
        ]:
            store.record_metric(
                MetricRecord(dataset_id=dataset_id, metric_name=metric_name, metric_value=value)
            )
            count += 1

    return count


def setup() -> None:
    """Populate the demo metrics store with sample computed metrics."""
    if DEMO_DIR.exists():
        shutil.rmtree(DEMO_DIR)
    DEMO_DIR.mkdir(parents=True, exist_ok=True)

    store = MetricsStore(str(METRICS_DB))
    n = _populate_metrics_store(store)
    store.close()

    print(f"Demo metrics written to {METRICS_DB}")
    print(f"  Metrics recorded: {n}")


# Keep fixture registry for backward compatibility and timeseries exploration
FIXTURE_REGISTRY: dict[str, tuple] = {
    "Stock Returns": (
        generate_stock_returns_data,
        "Daily return metrics for 5 stocks over ~1 year",
    ),
    "Data Quality": (
        generate_data_quality_data,
        "Weekly data quality metrics for 4 datasets over 1 year",
    ),
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="acme-metrics demo setup")
    parser.add_argument("--setup", action="store_true", help="Populate fixture data")
    args = parser.parse_args()
    if args.setup:
        setup()
    else:
        parser.print_help()
