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


def test_create_injected_launch_context_happy_path(tmp_path: Path) -> None:
    metrics_db = tmp_path / "metrics.duckdb"
    metrics_db.touch()

    context = create_injected_launch_context(
        metrics_db_path=str(metrics_db),
        title="Fund Metrics",
        icon="📈",
    )

    assert context.mode is LaunchMode.DEPLOYMENT
    assert context.metrics_db_path == str(metrics_db)
    assert context.title == "Fund Metrics"
    assert context.icon == "📈"


def test_create_injected_launch_context_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(LaunchContextError, match="Metrics DB path does not exist"):
        create_injected_launch_context(metrics_db_path=str(tmp_path / "missing.duckdb"))


def test_create_launch_context_from_env_uses_injected_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    metrics_db = tmp_path / "metrics.duckdb"
    metrics_db.touch()

    monkeypatch.setenv("ACME_METRICS_DB_PATH", str(metrics_db))
    monkeypatch.setenv("ACME_METRICS_TITLE", "Metrics UI")
    monkeypatch.setenv("ACME_METRICS_ICON", "🧪")

    context = create_launch_context_from_env()

    assert context.mode is LaunchMode.DEPLOYMENT
    assert context.metrics_db_path == str(metrics_db)
    assert context.title == "Metrics UI"
    assert context.icon == "🧪"
