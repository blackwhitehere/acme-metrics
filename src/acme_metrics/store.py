"""MetricsStore — DuckDB-backed storage for computed dataset metrics."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime

import duckdb


@dataclass
class MetricRecord:
    """A single computed metric value."""

    dataset_id: str
    metric_name: str
    metric_value: float
    computed_at: datetime = field(default_factory=datetime.now)
    metadata: dict = field(default_factory=dict)


class MetricsStore:
    """DuckDB-backed store for computed metrics."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = duckdb.connect(db_path)
        self._init_tables()

    def _init_tables(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                dataset_id VARCHAR NOT NULL,
                metric_name VARCHAR NOT NULL,
                metric_value DOUBLE NOT NULL,
                computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata VARCHAR DEFAULT '{}'
            )
        """)

    def record_metric(self, metric: MetricRecord) -> None:
        """Record a computed metric."""
        self._conn.execute(
            """
            INSERT INTO metrics (dataset_id, metric_name, metric_value, computed_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                metric.dataset_id,
                metric.metric_name,
                metric.metric_value,
                metric.computed_at,
                json.dumps(metric.metadata),
            ],
        )

    def get_metrics_for(self, dataset_id: str) -> list[MetricRecord]:
        """Get all metrics for a dataset."""
        rows = self._conn.execute(
            "SELECT * FROM metrics WHERE dataset_id = ? ORDER BY computed_at DESC",
            [dataset_id],
        ).fetchall()
        return [self._row_to_metric(r) for r in rows]

    def list_dataset_ids(self) -> list[str]:
        """List all dataset IDs that have metrics."""
        rows = self._conn.execute(
            "SELECT DISTINCT dataset_id FROM metrics ORDER BY dataset_id"
        ).fetchall()
        return [r[0] for r in rows]

    def get_latest_metrics(self, dataset_id: str) -> dict[str, float]:
        """Get the latest value for each metric name for a dataset."""
        rows = self._conn.execute(
            """
            SELECT metric_name, metric_value
            FROM (
                SELECT metric_name, metric_value,
                       ROW_NUMBER() OVER (PARTITION BY metric_name ORDER BY computed_at DESC) AS rn
                FROM metrics WHERE dataset_id = ?
            ) WHERE rn = 1
            """,
            [dataset_id],
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    def _row_to_metric(self, row: tuple) -> MetricRecord:
        return MetricRecord(
            dataset_id=row[0],
            metric_name=row[1],
            metric_value=row[2],
            computed_at=row[3]
            if isinstance(row[3], datetime)
            else datetime.fromisoformat(str(row[3])),
            metadata=json.loads(row[4]) if row[4] else {},
        )

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
