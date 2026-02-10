"""Fixture data generators for the metrics demo dashboard.

Generates sample DataFrames representing different metric schemas
to showcase the dashboard without requiring a data warehouse connection.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl


def generate_stock_returns_metrics() -> pl.DataFrame:
    """Generate daily return metrics for a set of stocks."""
    import math

    rng_seed = 42
    stocks = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA"]
    start = date(2024, 1, 1)
    n_days = 252

    rows: list[dict] = []
    for i, ticker in enumerate(stocks):
        price = 100.0 + i * 20
        # deterministic pseudo-random using simple LCG
        seed = rng_seed + i * 7
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
                    "entity": ticker,
                    "daily_return_pct": round(daily_return, 4),
                    "cumulative_return_pct": round(cum_return, 2),
                    "price": round(price, 2),
                    "volatility_30d": round(volatility, 2),
                }
            )

    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


def generate_data_quality_metrics() -> pl.DataFrame:
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

    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


def generate_model_performance_metrics() -> pl.DataFrame:
    """Generate ML model performance metrics across multiple models over time."""
    models = ["linear_v1", "xgboost_v2", "neural_v3"]
    n_months = 12

    rows: list[dict] = []
    seed = 77
    for i, model_name in enumerate(models):
        base_rmse = 0.15 - i * 0.02
        base_r2 = 0.70 + i * 0.08
        for m in range(n_months):
            seed = (seed * 1103515245 + 12345) & 0x7FFFFFFF
            jitter = (seed / 0x7FFFFFFF - 0.5) * 0.04
            rmse = max(0.01, base_rmse + jitter - m * 0.002)
            r2 = min(0.99, base_r2 - jitter + m * 0.005)
            seed = (seed * 1103515245 + 12345) & 0x7FFFFFFF
            sharpe = 0.5 + i * 0.3 + (seed / 0x7FFFFFFF - 0.5) * 0.4
            rows.append(
                {
                    "date": date(2024, m + 1, 1),
                    "entity": model_name,
                    "rmse": round(rmse, 4),
                    "r_squared": round(r2, 4),
                    "sharpe_ratio": round(sharpe, 2),
                }
            )

    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


# Registry of available fixture datasets: name -> (generator, description)
FIXTURE_REGISTRY: dict[str, tuple] = {
    "Stock Returns": (
        generate_stock_returns_metrics,
        "Daily return metrics for 5 stocks over ~1 year",
    ),
    "Data Quality": (
        generate_data_quality_metrics,
        "Weekly data quality metrics for 4 datasets over 1 year",
    ),
    "Model Performance": (
        generate_model_performance_metrics,
        "Monthly ML model performance metrics for 3 models over 1 year",
    ),
}
