from __future__ import annotations

from pathlib import Path

from acme_metrics.config import get_config, reset_config
from acme_metrics.core import ConfigDiscovery
from acme_metrics.orchestration import MetricsRunner


def _write_project_files(project_root: Path, db_path: Path) -> None:
    (project_root / "sources").mkdir(parents=True)
    (project_root / "metrics").mkdir(parents=True)
    (project_root / "targets").mkdir(parents=True)

    for package_dir in ("sources", "metrics", "targets"):
        (project_root / package_dir / "__init__.py").write_text("\n", encoding="utf-8")

    (project_root / "sources" / "sample_source.py").write_text(
        "from __future__ import annotations\n\n"
        "import pandas as pd\n"
        "from acme_metrics.core import BaseSource\n\n"
        "class SampleSource(BaseSource):\n"
        '    source_id = "prices"\n\n'
        "    def load(self) -> pd.DataFrame:\n"
        '        return pd.DataFrame([{"close": 10.0}, {"close": 20.0}])\n\n'
        "prices = SampleSource()\n",
        encoding="utf-8",
    )

    (project_root / "metrics" / "sample_metric.py").write_text(
        "from __future__ import annotations\n\n"
        "import pandas as pd\n"
        "from acme_metrics.core import MetricSpec\n\n"
        "def _compute(source_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:\n"
        "    return pd.DataFrame([\n"
        "        {\n"
        '            "metric_name": "close_mean",\n'
        "            \"metric_value\": float(source_df['close'].mean()),\n"
        "        },\n"
        "    ])\n\n"
        "price_stats = MetricSpec(\n"
        '    metric_id="price-stats",\n'
        '    source_id="prices",\n'
        "    compute_fn=_compute,\n"
        ")\n",
        encoding="utf-8",
    )

    (project_root / "targets" / "sample_target.py").write_text(
        "from __future__ import annotations\n\n"
        "from acme_metrics.targets.duckdb import DuckDBTarget\n\n"
        f'local = DuckDBTarget(target_id="local", db_path="{db_path}")\n',
        encoding="utf-8",
    )


def test_discovery_and_runner_execute_metric(tmp_path: Path) -> None:
    project_root = tmp_path / "metrics-project"
    db_path = tmp_path / "metrics.duckdb"
    _write_project_files(project_root, db_path)

    reset_config()
    config = get_config(config_root=str(project_root), store_db_path=str(db_path))

    discovery = ConfigDiscovery(project_root)
    discovery.load()

    metric = discovery.get_metric("price-stats")
    source = discovery.get_source(metric.source_id)
    target = discovery.get_target("local")

    runner = MetricsRunner(config)
    result = runner.run(
        metric=metric,
        source_df=source.load(),
        existing_df=target.load_metrics(metric.metric_id, metric.source_id),
        target=target,
    )

    assert result.metric_id == "price-stats"
    assert result.rows_written == 1

    stored = target.load_metrics("price-stats", "prices")
    assert len(stored.index) == 1
    assert stored.iloc[0]["metric_name"] == "close_mean"
