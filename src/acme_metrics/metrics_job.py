"""The ``@metrics_job`` decorator for programmatic metric execution.

Wraps a user-defined computation function and executes it through the
same runner pipeline used by project-mode metrics execution.

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
from collections.abc import Callable
from typing import Any

import pandas as pd

from acme_metrics.config import get_config
from acme_metrics.core import MetricSpec
from acme_metrics.orchestration import MetricsRunner
from acme_metrics.targets.duckdb import DuckDBTarget


def _to_metrics_df(metrics: dict[str, float]) -> pd.DataFrame:
    """Convert metric dict into standard metric row dataframe."""
    return pd.DataFrame(
        [
            {
                "metric_name": metric_name,
                "metric_value": float(metric_value),
            }
            for metric_name, metric_value in metrics.items()
        ]
    )


def metrics_job(
    name: str,
    dataset_id: str,
    metadata: dict[str, Any] | None = None,
) -> Callable:
    """Decorator that turns a metric computation function into a traced job.

    The decorated function accepts a source ``pd.DataFrame`` and returns
    ``dict[str, float]``. The decorator bridges this into the project-mode
    metric pipeline and persists results with the default DuckDB target.

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
            computed_metrics: dict[str, float] = {}

            def _compute(source_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
                del existing_df
                nonlocal computed_metrics
                computed_metrics = fn(source_df)
                return _to_metrics_df(computed_metrics)

            metric_spec = MetricSpec(
                metric_id=name,
                source_id=dataset_id,
                compute_fn=_compute,
            )
            target = DuckDBTarget(target_id="default", db_path=config.store_db_path)
            runner = MetricsRunner(config)

            existing_df = target.load_metrics(metric_spec.metric_id, metric_spec.source_id)
            runner.run(
                metric=metric_spec,
                source_df=df,
                existing_df=existing_df,
                target=target,
                run_name=name,
                metadata=metadata,
            )
            return computed_metrics

        return wrapper

    return decorator
