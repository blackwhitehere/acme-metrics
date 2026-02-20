"""Configuration for acme-metrics.

Composes metadeco and metrics store settings resolved from
environment variables, .env files, or explicit overrides.
"""

from __future__ import annotations

from acme_config import AppConfig, ConfigField, resolve_config


class MetricsConfig(AppConfig):
    """Configuration for the metrics computation framework.

    Env vars prefixed with ``METRICS_``. Example .env::

        METRICS_STORE_DB_PATH=metrics.duckdb
        METRICS_METADECO_DB_PATH=traces.duckdb
    """

    model_config = {"env_prefix": "METRICS_"}

    store_db_path: str = ConfigField(
        default="metrics.duckdb",
        description="Path to the DuckDB file for computed metrics",
    )

    metadeco_db_path: str = ConfigField(
        default="traces.duckdb",
        description="Path to the DuckDB file for execution traces",
    )


_config: MetricsConfig | None = None


def get_config(**overrides: object) -> MetricsConfig:
    """Return the resolved config, creating it on first call."""
    global _config
    if _config is None or overrides:
        _config = resolve_config(MetricsConfig, overrides=overrides or None)
    return _config


def reset_config() -> None:
    """Reset cached config (useful in tests and demo setup)."""
    global _config
    _config = None
