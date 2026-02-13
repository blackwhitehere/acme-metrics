"""Shared fixture generation for acme-metrics.

Other apps (e.g. acme-data-catalog) can import and call these functions
to populate a metrics store and optionally register metrics in a catalog,
similar to how UI fragments are shared for embedding in other Streamlit apps.

Usage:
    from acme_metrics.fixtures import populate_metrics_store
    populate_metrics_store(metrics_db_path)

    from acme_metrics.fixtures import populate_catalog
    populate_catalog(store)
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from acme_metrics.store import MetricRecord, MetricsStore

if TYPE_CHECKING:
    from acme_data_catalog.store import CatalogStore


def populate_metrics_store(metrics_db_path: str) -> int:
    """Populate a metrics store with computed metrics for demo datasets.

    Args:
        metrics_db_path: Path to the DuckDB metrics database.

    Returns:
        Number of metrics recorded.
    """
    from acme_metrics.demo import (
        generate_data_quality_data,
        generate_stock_returns_data,
    )

    store = MetricsStore(metrics_db_path)
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
                MetricRecord(
                    dataset_id=dataset_id,
                    metric_name=metric_name,
                    metric_value=value,
                )
            )
            count += 1

    # Data quality summary metrics per entity
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
                MetricRecord(
                    dataset_id=dataset_id,
                    metric_name=metric_name,
                    metric_value=value,
                )
            )
            count += 1

    store.close()
    return count


def populate_catalog(store: CatalogStore) -> int:
    """Register metric summary records in the catalog for demo datasets.

    This records catalog-level metrics (null_count, duplicate_count) for
    datasets that landing and data-prep have registered.

    Args:
        store: An open CatalogStore instance to populate.

    Returns:
        Number of metrics recorded.
    """
    from acme_data_catalog.models import Metric

    now = datetime.now()
    count = 0

    for ds_id, nulls, dupes in [
        ("landing:prices", 42.0, 18.0),
        ("landing:fundamentals", 5.0, 0.0),
        ("data-prep:clean_prices", 0.0, 0.0),
        ("data-prep:clean_fundamentals", 0.0, 0.0),
    ]:
        store.record_metric(
            Metric(
                dataset_id=ds_id,
                metric_name="null_count",
                metric_value=nulls,
                computed_at=now,
            )
        )
        store.record_metric(
            Metric(
                dataset_id=ds_id,
                metric_name="duplicate_count",
                metric_value=dupes,
                computed_at=now,
            )
        )
        count += 2

    return count
