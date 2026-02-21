"""DuckDB target adapter for metrics persistence."""

from __future__ import annotations

import pandas as pd

from acme_metrics.core.base import BaseTarget
from acme_metrics.store import MetricRecord, MetricsStore


class DuckDBTarget(BaseTarget):
    """Stores metrics in the default MetricsStore DuckDB backend."""

    def __init__(self, target_id: str, db_path: str) -> None:
        self.target_id = target_id
        self._db_path = db_path

    def load_metrics(self, metric_id: str, source_id: str) -> pd.DataFrame:
        """Load existing metric rows for a metric/source pair."""
        dataset_id = self._dataset_id(metric_id, source_id)
        store = MetricsStore(self._db_path)
        try:
            rows = store.get_metrics_for(dataset_id)
        finally:
            store.close()

        if not rows:
            return pd.DataFrame(columns=["metric_name", "metric_value", "computed_at"])

        return pd.DataFrame(
            [
                {
                    "metric_name": row.metric_name,
                    "metric_value": row.metric_value,
                    "computed_at": row.computed_at,
                }
                for row in rows
            ]
        )

    def save_metrics(self, metric_id: str, source_id: str, metrics_df: pd.DataFrame) -> None:
        """Save metric rows for a metric/source pair."""
        dataset_id = self._dataset_id(metric_id, source_id)
        store = MetricsStore(self._db_path)
        try:
            for _, row in metrics_df.iterrows():
                store.record_metric(
                    MetricRecord(
                        dataset_id=dataset_id,
                        metric_name=str(row["metric_name"]),
                        metric_value=float(row["metric_value"]),
                        metadata={"metric_id": metric_id, "source_id": source_id},
                    )
                )
        finally:
            store.close()

    def _dataset_id(self, metric_id: str, source_id: str) -> str:
        """Build storage dataset ID for metric/source rows."""
        return f"{source_id}::{metric_id}"
