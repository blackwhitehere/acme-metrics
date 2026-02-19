"""Launch contract for acme-metrics UI."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from acme_streamlit.navigation import Navigation

from acme_metrics.demo import METRICS_DB
from acme_metrics.store import MetricsStore
from acme_metrics.ui.fragments import MetricsBrowserFragment


class LaunchMode(StrEnum):
    """Supported UI launch modes."""

    DEMO = "demo"
    DEPLOYMENT = "deployment"


class LaunchContextError(ValueError):
    """Raised when launch context cannot be resolved from provided inputs."""


@dataclass(frozen=True)
class LaunchContext:
    """Typed launch contract for constructing the metrics UI."""

    mode: LaunchMode
    metrics_db_path: str
    title: str = "acme-metrics"
    icon: str = "📊"


def create_demo_launch_context() -> LaunchContext:
    """Build launch context for local demo execution."""
    if not METRICS_DB.exists():
        raise LaunchContextError(
            "Demo data not found. Run `just demo` or `python -m acme_metrics.demo --setup` first."
        )

    return LaunchContext(
        mode=LaunchMode.DEMO,
        metrics_db_path=str(METRICS_DB),
        title="acme-metrics Demo",
    )


def create_injected_launch_context(
    *, metrics_db_path: str, title: str = "acme-metrics", icon: str = "📊"
) -> LaunchContext:
    """Build launch context from externally injected service paths."""
    if not Path(metrics_db_path).exists():
        raise LaunchContextError(f"Metrics DB path does not exist: {metrics_db_path}")

    return LaunchContext(
        mode=LaunchMode.DEPLOYMENT,
        metrics_db_path=metrics_db_path,
        title=title,
        icon=icon,
    )


def create_launch_context_from_env() -> LaunchContext:
    """Resolve launch context from environment, falling back to demo mode."""
    import os

    metrics_db_path = os.getenv("ACME_METRICS_DB_PATH")
    if metrics_db_path:
        return create_injected_launch_context(
            metrics_db_path=metrics_db_path,
            title=os.getenv("ACME_METRICS_TITLE", "acme-metrics"),
            icon=os.getenv("ACME_METRICS_ICON", "📊"),
        )

    return create_demo_launch_context()


def build_navigation(context: LaunchContext) -> Navigation:
    """Create metrics navigation from launch context."""
    store = MetricsStore(context.metrics_db_path)
    nav = Navigation(context.title, icon=context.icon)
    nav.page(MetricsBrowserFragment(store))
    return nav
