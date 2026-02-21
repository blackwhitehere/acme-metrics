from __future__ import annotations

from pathlib import Path

import pandas as pd

from acme_metrics import metrics_job
from acme_metrics.config import reset_config
from acme_metrics.store import MetricsStore


def test_metrics_job_persists_metrics_via_runner(tmp_path: Path) -> None:
    db_path = tmp_path / "metrics.duckdb"
    traces_path = tmp_path / "traces.duckdb"

    reset_config()

    @metrics_job(name="daily-stats", dataset_id="prices:daily")
    def _compute(df: pd.DataFrame) -> dict[str, float]:
        return {
            "mean_price": float(df["price"].mean()),
            "max_price": float(df["price"].max()),
        }

    input_df = pd.DataFrame([{"price": 10.0}, {"price": 20.0}, {"price": 30.0}])
    result = _compute(
        input_df,
        store_db_path=str(db_path),
        metadeco_db_path=str(traces_path),
        catalog_auto_register=False,
    )

    assert result["mean_price"] == 20.0
    assert result["max_price"] == 30.0

    store = MetricsStore(str(db_path))
    try:
        records = store.get_metrics_for("prices:daily::daily-stats")
    finally:
        store.close()

    metric_names = {record.metric_name for record in records}
    assert metric_names == {"mean_price", "max_price"}
