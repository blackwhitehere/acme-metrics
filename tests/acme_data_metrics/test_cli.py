from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from acme_metrics.cli.main import _cmd_serve, main


def _run_cli(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "acme_metrics._main", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )


def test_init_creates_project_scaffold(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    result = _run_cli(["init", "--path", str(project_root)], cwd=tmp_path)

    assert result.returncode == 0
    assert (project_root / "sources" / "sample_source.py").exists()
    assert (project_root / "metrics" / "sample_metric.py").exists()
    assert (project_root / "targets" / "sample_target.py").exists()
    assert (project_root / "config.py").exists()
    assert (project_root / "env.manifest").exists()


def test_inspect_and_metrics_run(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    db_path = tmp_path / "metrics.duckdb"

    init_result = _run_cli(["init", "--path", str(project_root)], cwd=tmp_path)
    assert init_result.returncode == 0

    inspect_result = _run_cli(["--config-root", str(project_root), "inspect"], cwd=tmp_path)
    assert inspect_result.returncode == 0
    assert "sample-source" in inspect_result.stdout
    assert "sample-metric" in inspect_result.stdout
    assert "local" in inspect_result.stdout

    run_result = _run_cli(
        [
            "--config-root",
            str(project_root),
            "metrics",
            "run",
            "--metric-id",
            "sample-metric",
            "--target",
            "local",
        ],
        cwd=tmp_path,
    )
    assert run_result.returncode == 0
    assert "Completed metric run" in run_result.stdout
    assert "metric: sample-metric" in run_result.stdout
    assert db_path.exists()


def test_inspect_verbose_type_metrics(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    init_result = _run_cli(["init", "--path", str(project_root)], cwd=tmp_path)
    assert init_result.returncode == 0

    inspect_result = _run_cli(
        ["--config-root", str(project_root), "inspect", "--type", "metrics", "--verbose"],
        cwd=tmp_path,
    )
    assert inspect_result.returncode == 0
    assert "sample-metric" in inspect_result.stdout
    assert "columns:" in inspect_result.stdout
    assert "Sources:" not in inspect_result.stdout


def test_metrics_run_all_executes_multiple_metrics(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    init_result = _run_cli(["init", "--path", str(project_root)], cwd=tmp_path)
    assert init_result.returncode == 0

    extra_metric = project_root / "metrics" / "extra_metric.py"
    extra_metric.write_text(
        "from __future__ import annotations\n\n"
        "import pandas as pd\n"
        "from acme_metrics.core import MetricSpec\n\n"
        "def _compute(source_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:\n"
        "    del existing_df\n"
        "    return pd.DataFrame([\n"
        "        {\n"
        '            "metric_name": "value_sum",\n'
        "            \"metric_value\": float(source_df['value'].sum()),\n"
        "        }\n"
        "    ])\n\n"
        "extra_metric = MetricSpec(\n"
        '    metric_id="extra-metric",\n'
        '    source_id="sample-source",\n'
        "    compute_fn=_compute,\n"
        ")\n",
        encoding="utf-8",
    )

    run_result = _run_cli(
        ["--config-root", str(project_root), "metrics", "run", "--all", "--target", "local"],
        cwd=tmp_path,
    )
    assert run_result.returncode == 0
    assert "metric: sample-metric" in run_result.stdout
    assert "metric: extra-metric" in run_result.stdout


def test_cmd_serve_uses_injected_env(monkeypatch) -> None:
    class DummyConfig:
        store_db_path = "metrics.duckdb"
        config_root = "project-root"

    class DummyArgs:
        host = "0.0.0.0"
        port = 8600
        metrics_db_path = "custom.duckdb"
        title = "Metrics UI"
        icon = "🧪"

    captured = {}

    def _fake_run(command, env, check):
        captured["command"] = command
        captured["env"] = env
        captured["check"] = check

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr("acme_metrics.cli.main.subprocess.run", _fake_run)

    exit_code = _cmd_serve(DummyConfig(), DummyArgs())

    assert exit_code == 0
    assert captured["command"][0] == "streamlit"
    assert captured["env"]["ACME_METRICS_DB_PATH"] == "custom.duckdb"
    assert captured["env"]["ACME_METRICS_CONFIG_ROOT"] == "project-root"
    assert captured["env"]["ACME_METRICS_TITLE"] == "Metrics UI"
    assert captured["env"]["ACME_METRICS_ICON"] == "🧪"


def test_serve_command_invokes_streamlit_via_main(monkeypatch) -> None:
    captured = {}

    def _fake_run(command, env, check):
        captured["command"] = command
        captured["env"] = env
        captured["check"] = check

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr("acme_metrics.cli.main.subprocess.run", _fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "adm",
            "serve",
            "--metrics-db-path",
            "serve.duckdb",
            "--host",
            "127.0.0.1",
            "--port",
            "8601",
            "--title",
            "Serve Title",
            "--icon",
            "✅",
        ],
    )

    main()

    assert captured["command"][:2] == ["streamlit", "run"]
    assert "demo_app.py" in captured["command"][2]
    assert captured["env"]["ACME_METRICS_DB_PATH"] == "serve.duckdb"
    assert captured["env"]["ACME_METRICS_CONFIG_ROOT"] == "."
    assert captured["env"]["ACME_METRICS_TITLE"] == "Serve Title"
    assert captured["env"]["ACME_METRICS_ICON"] == "✅"
