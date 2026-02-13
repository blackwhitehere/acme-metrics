"""The ``@metrics_job`` decorator — core API of acme-metrics.

Wraps a user-defined computation function with automatic data loading,
metric storage, and metadeco execution tracing.  The user function
receives a ``pd.DataFrame`` and returns a ``dict[str, float]`` of
computed metric values.

Example::

    from acme_metrics import metrics_job, compute

    @compute
    def calc_returns(df):
        returns = {}
        for col in df.select_dtypes("number").columns:
            returns[f"{col}_mean"] = df[col].mean()
            returns[f"{col}_std"] = df[col].std()
        return returns

    @metrics_job(name="daily-stats", dataset_id="prices:daily")
    def daily_stats(df):
        return calc_returns(df)

    # Execute — loads data, traces execution, stores metrics
    result = daily_stats()
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from acme_metadeco import run as metadeco_run

from acme_metrics.config import MetricsConfig, get_config
from acme_metrics.decorators import load, save
from acme_metrics.store import MetricRecord, MetricsStore

logger = logging.getLogger(__name__)


@load
def _load_data(df: pd.DataFrame) -> pd.DataFrame:
    """Pass through — data is provided by the caller."""
    return df


@save
def _save_metrics(
    store: MetricsStore,
    dataset_id: str,
    metrics: dict[str, float],
) -> None:
    """Persist computed metrics to the metrics store."""
    for name, value in metrics.items():
        store.record_metric(
            MetricRecord(
                dataset_id=dataset_id,
                metric_name=name,
                metric_value=value,
            )
        )


def _register_in_catalog(
    config: MetricsConfig,
    job_name: str,
    dataset_id: str,
    metrics: dict[str, float],
) -> None:
    """Record in the catalog that this dataset has computed metrics."""
    try:
        from acme_data_catalog import CatalogStore, Metric

        cat = CatalogStore(config.catalog_db_path)
        for metric_name, metric_value in metrics.items():
            cat.record_metric(
                Metric(
                    dataset_id=dataset_id,
                    metric_name=metric_name,
                    metric_value=metric_value,
                    metadata={"source": "acme-metrics", "job": job_name},
                )
            )
        cat.close()
    except Exception:
        logger.warning("Failed to register metrics in catalog", exc_info=True)


def metrics_job(
    name: str,
    dataset_id: str,
    metadata: dict[str, Any] | None = None,
) -> Callable:
    """Decorator that turns a metric computation function into a traced job.

    The decorated function should accept a ``pd.DataFrame`` and return
    a ``dict[str, float]`` mapping metric names to values.  When called
    with a DataFrame, the framework:

    1. Resolves ``MetricsConfig`` from the environment.
    2. Opens a metadeco tracing context.
    3. Passes the DataFrame through a ``@load``-decorated stage.
    4. Calls the user function to compute metrics.
    5. Persists metrics to the ``MetricsStore`` via a ``@save`` stage.
    6. Optionally registers metrics in the catalog.
    7. Returns the computed metrics dict.

    Args:
        name: Job name (used as metadeco app_name and for logging).
        dataset_id: Identifier for the dataset being measured.
        metadata: Extra metadata dict attached to the metadeco run.
    """

    def decorator(
        fn: Callable[[pd.DataFrame], dict[str, float]],
    ) -> Callable[..., dict[str, float]]:
        @functools.wraps(fn)
        def wrapper(df: pd.DataFrame, **config_overrides: Any) -> dict[str, float]:
            config = get_config(**config_overrides)
            store = MetricsStore(config.store_db_path)

            with metadeco_run(
                name,
                db_path=config.metadeco_db_path,
                metadata=metadata or {},
            ):
                loaded = _load_data(df)
                metrics = fn(loaded)
                _save_metrics(store, dataset_id, metrics)

            if config.catalog_db_path:
                _register_in_catalog(config, name, dataset_id, metrics)

            store.close()
            return metrics

        return wrapper

    return decorator
