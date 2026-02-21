"""Launch contract for acme-metrics UI."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from acme_streamlit.navigation import Navigation

from acme_metrics.core import ConfigDiscovery
from acme_metrics.demo import METRICS_DB
from acme_metrics.store import MetricsStore
from acme_metrics.ui.fragments import MetricsBrowserFragment, MetricsProjectOverviewFragment


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
    config_root: str | None = None
    source_ids: tuple[str, ...] = ()
    metric_bindings: tuple[tuple[str, str], ...] = ()
    target_ids: tuple[str, ...] = ()
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
    *,
    metrics_db_path: str,
    title: str = "acme-metrics",
    icon: str = "📊",
    config_root: str | None = None,
) -> LaunchContext:
    """Build launch context from externally injected service paths."""
    if not Path(metrics_db_path).exists():
        raise LaunchContextError(f"Metrics DB path does not exist: {metrics_db_path}")

    source_ids: tuple[str, ...] = ()
    metric_bindings: tuple[tuple[str, str], ...] = ()
    target_ids: tuple[str, ...] = ()

    if config_root:
        config_path = Path(config_root)
        if not config_path.exists():
            raise LaunchContextError(f"Config root path does not exist: {config_root}")

        discovery = ConfigDiscovery(config_path)
        discovery.load()
        source_ids = tuple(sorted(discovery.sources))
        metric_bindings = tuple(
            sorted((metric_id, metric.source_id) for metric_id, metric in discovery.metrics.items())
        )
        target_ids = tuple(sorted(discovery.targets))

    return LaunchContext(
        mode=LaunchMode.DEPLOYMENT,
        metrics_db_path=metrics_db_path,
        config_root=config_root,
        source_ids=source_ids,
        metric_bindings=metric_bindings,
        target_ids=target_ids,
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
            config_root=os.getenv("ACME_METRICS_CONFIG_ROOT"),
        )

    return create_demo_launch_context()


def build_navigation(context: LaunchContext) -> Navigation:
    """Create metrics navigation from launch context."""
    store = MetricsStore(context.metrics_db_path)
    nav = Navigation(context.title, icon=context.icon)
    if context.mode is LaunchMode.DEPLOYMENT:
        nav.page(MetricsProjectOverviewFragment(context))
    nav.page(MetricsBrowserFragment(store))
    return nav
