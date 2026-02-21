"""Discovery for metrics project configuration modules."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from acme_metrics.core.base import BaseSource, BaseTarget, MetricSpec


@dataclass
class ConfigDiscovery:
    """Discovers sources, metrics, and targets from project modules."""

    config_path: Path
    sources: dict[str, BaseSource] = field(default_factory=dict)
    metrics: dict[str, MetricSpec] = field(default_factory=dict)
    targets: dict[str, BaseTarget] = field(default_factory=dict)

    def load(self) -> None:
        """Load all modules in configured project directories."""
        self.config_path = Path(self.config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config path not found: {self.config_path}")

        self._load_sources()
        self._load_metrics()
        self._load_targets()

    def _load_sources(self) -> None:
        """Load source definitions from sources directory."""
        self._load_from_dir(
            subdir="sources",
            module_prefix="sources",
            collector=self._collect_source,
        )

    def _load_metrics(self) -> None:
        """Load metric definitions from metrics directory."""
        self._load_from_dir(
            subdir="metrics",
            module_prefix="metrics",
            collector=self._collect_metric,
        )

    def _load_targets(self) -> None:
        """Load target definitions from targets directory."""
        self._load_from_dir(
            subdir="targets",
            module_prefix="targets",
            collector=self._collect_target,
        )

    def _load_from_dir(self, subdir: str, module_prefix: str, collector: Any) -> None:
        """Load Python modules from a directory and collect objects."""
        module_dir = self.config_path / subdir
        if not module_dir.exists():
            return

        for py_file in module_dir.glob("*.py"):
            if py_file.name.startswith("_"):
                continue
            module = self._import_module(f"{module_prefix}.{py_file.stem}", py_file)
            for name in dir(module):
                obj = getattr(module, name)
                collector(obj)

    def _collect_source(self, obj: object) -> None:
        """Collect source objects from a loaded module."""
        if isinstance(obj, BaseSource):
            self.sources[obj.source_id] = obj

    def _collect_metric(self, obj: object) -> None:
        """Collect metric objects from a loaded module."""
        if isinstance(obj, MetricSpec):
            self.metrics[obj.metric_id] = obj
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, MetricSpec):
                    self.metrics[item.metric_id] = item

    def _collect_target(self, obj: object) -> None:
        """Collect target objects from a loaded module."""
        if isinstance(obj, BaseTarget):
            self.targets[obj.target_id] = obj

    def _import_module(self, name: str, path: Path) -> Any:
        """Import module from path."""
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load module from {path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    def get_source(self, source_id: str) -> BaseSource:
        """Return source by ID."""
        if source_id not in self.sources:
            raise KeyError(f"Source not found: {source_id}")
        return self.sources[source_id]

    def get_metric(self, metric_id: str) -> MetricSpec:
        """Return metric by ID."""
        if metric_id not in self.metrics:
            raise KeyError(f"Metric not found: {metric_id}")
        return self.metrics[metric_id]

    def get_target(self, target_id: str) -> BaseTarget:
        """Return target by ID."""
        if target_id not in self.targets:
            raise KeyError(f"Target not found: {target_id}")
        return self.targets[target_id]
