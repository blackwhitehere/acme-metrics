"""Core base types for metrics workflows."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd


class BaseSource(ABC):
    """Base class for loading source datasets."""

    source_id: str

    @abstractmethod
    def load(self) -> pd.DataFrame:
        """Load source data for metric computation."""


class BaseTarget(ABC):
    """Base class for loading and saving metrics."""

    target_id: str

    @abstractmethod
    def load_metrics(self, metric_id: str, source_id: str) -> pd.DataFrame:
        """Load existing metric rows for a metric job."""

    @abstractmethod
    def save_metrics(self, metric_id: str, source_id: str, metrics_df: pd.DataFrame) -> None:
        """Persist computed metric rows for a metric job."""


@dataclass(frozen=True)
class MetricSpec:
    """Defines a metric job from source binding and compute function."""

    metric_id: str
    source_id: str
    compute_fn: Callable[[pd.DataFrame, pd.DataFrame], pd.DataFrame]
    output_columns: tuple[str, ...] = ("metric_name", "metric_value")
    description: str = ""

    def compute(self, source_df: pd.DataFrame, existing_metrics_df: pd.DataFrame) -> pd.DataFrame:
        """Compute metric rows from source and existing data."""
        return self.compute_fn(source_df, existing_metrics_df)
