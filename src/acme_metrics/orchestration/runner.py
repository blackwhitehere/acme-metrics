"""Runner for executing configured metric jobs."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from acme_metadeco import run as metadeco_run

from acme_metrics.config import MetricsConfig
from acme_metrics.core.base import MetricSpec


@dataclass(frozen=True)
class MetricsRunResult:
    """Result of a single metric run."""

    metric_id: str
    source_id: str
    rows_written: int


class MetricsRunner:
    """Executes metric runs using sources, specs, and targets."""

    def __init__(self, config: MetricsConfig) -> None:
        self._config = config

    def run(
        self,
        metric: MetricSpec,
        source_df: pd.DataFrame,
        existing_df: pd.DataFrame,
        target,
        run_name: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> MetricsRunResult:
        """Run a metric spec and persist results to target."""
        run_metadata: dict[str, object] = {
            "metric_id": metric.metric_id,
            "source_id": metric.source_id,
        }
        if metadata:
            run_metadata.update(metadata)

        with metadeco_run(
            run_name or f"metrics:{metric.metric_id}",
            db_path=self._config.metadeco_db_path,
            metadata=run_metadata,
        ):
            metrics_df = metric.compute(source_df, existing_df)
            self._validate_output(metric, metrics_df)
            target.save_metrics(metric.metric_id, metric.source_id, metrics_df)

        if self._config.catalog_auto_register:
            self._register_in_catalog(metric, metrics_df)

        return MetricsRunResult(
            metric_id=metric.metric_id,
            source_id=metric.source_id,
            rows_written=len(metrics_df.index),
        )

    def _validate_output(self, metric: MetricSpec, metrics_df: pd.DataFrame) -> None:
        """Validate metric output columns against declared schema."""
        missing = [column for column in metric.output_columns if column not in metrics_df.columns]
        if missing:
            raise ValueError(
                f"Metric '{metric.metric_id}' output missing columns: {', '.join(missing)}"
            )

    def _register_in_catalog(self, metric: MetricSpec, metrics_df: pd.DataFrame) -> None:
        """Register computed metric values in optional data catalog."""
        try:
            from acme_data_catalog import CatalogClient, Metric

            with CatalogClient.from_env() as client:
                for _, row in metrics_df.iterrows():
                    client.record_metric(
                        Metric(
                            dataset_id=metric.source_id,
                            metric_name=str(row["metric_name"]),
                            metric_value=float(row["metric_value"]),
                            metadata={"source": "acme-metrics", "metric_id": metric.metric_id},
                        )
                    )
        except Exception:
            return
