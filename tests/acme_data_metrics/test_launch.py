"""Tests for metrics UI launch context."""

from __future__ import annotations

from pathlib import Path

import pytest

from acme_metrics.launch import (
    LaunchContextError,
    LaunchMode,
    create_injected_launch_context,
    create_launch_context_from_env,
)


def _write_project_files(project_root: Path) -> None:
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
        "    del existing_df\n"
        "    return pd.DataFrame([\n"
        "        {\n"
        '            "metric_name": "close_mean",\n'
        "            \"metric_value\": float(source_df['close'].mean()),\n"
        "        }\n"
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
        'local = DuckDBTarget(target_id="local", db_path="metrics.duckdb")\n',
        encoding="utf-8",
    )


def test_create_injected_launch_context_happy_path(tmp_path: Path) -> None:
    metrics_db = tmp_path / "metrics.duckdb"
    metrics_db.touch()
    project_root = tmp_path / "project"
    _write_project_files(project_root)

    context = create_injected_launch_context(
        metrics_db_path=str(metrics_db),
        config_root=str(project_root),
        title="Fund Metrics",
        icon="📈",
    )

    assert context.mode is LaunchMode.DEPLOYMENT
    assert context.metrics_db_path == str(metrics_db)
    assert context.config_root == str(project_root)
    assert context.source_ids == ("prices",)
    assert context.metric_bindings == (("price-stats", "prices"),)
    assert context.target_ids == ("local",)
    assert context.title == "Fund Metrics"
    assert context.icon == "📈"


def test_create_injected_launch_context_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(LaunchContextError, match="Metrics DB path does not exist"):
        create_injected_launch_context(metrics_db_path=str(tmp_path / "missing.duckdb"))


def test_create_injected_launch_context_invalid_config_root(tmp_path: Path) -> None:
    metrics_db = tmp_path / "metrics.duckdb"
    metrics_db.touch()

    with pytest.raises(LaunchContextError, match="Config root path does not exist"):
        create_injected_launch_context(
            metrics_db_path=str(metrics_db),
            config_root=str(tmp_path / "missing-project"),
        )


def test_create_launch_context_from_env_uses_injected_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    metrics_db = tmp_path / "metrics.duckdb"
    metrics_db.touch()
    project_root = tmp_path / "project"
    _write_project_files(project_root)

    monkeypatch.setenv("ACME_METRICS_DB_PATH", str(metrics_db))
    monkeypatch.setenv("ACME_METRICS_CONFIG_ROOT", str(project_root))
    monkeypatch.setenv("ACME_METRICS_TITLE", "Metrics UI")
    monkeypatch.setenv("ACME_METRICS_ICON", "🧪")

    context = create_launch_context_from_env()

    assert context.mode is LaunchMode.DEPLOYMENT
    assert context.metrics_db_path == str(metrics_db)
    assert context.config_root == str(project_root)
    assert context.source_ids == ("prices",)
    assert context.title == "Metrics UI"
    assert context.icon == "🧪"
