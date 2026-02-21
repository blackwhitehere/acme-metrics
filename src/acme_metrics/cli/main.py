"""Main CLI for acme-metrics project workflows."""

from __future__ import annotations

import argparse
import fnmatch
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from acme_metrics.config import MetricsConfig, get_config
from acme_metrics.core import ConfigDiscovery, MetricSpec
from acme_metrics.orchestration import MetricsRunner


@dataclass
class CLIContext:
    """Carries shared CLI state between commands."""

    config: MetricsConfig
    discovery: ConfigDiscovery | None = None

    def load_discovery(self) -> ConfigDiscovery:
        """Load project module discovery once."""
        if self.discovery is None:
            self.discovery = ConfigDiscovery(Path(self.config.config_root))
            self.discovery.load()
        return self.discovery


def _build_parser() -> argparse.ArgumentParser:
    """Create CLI parser with subcommands."""
    parser = argparse.ArgumentParser(prog="adm", description="acme-metrics metrics workflow CLI")
    parser.add_argument("--config-root", type=str, help="Path with sources/metrics/targets")

    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser(
        "init",
        help="Scaffold sources/metrics/targets project structure",
    )
    init_parser.add_argument("--path", type=str, default=".", help="Directory to scaffold")
    init_parser.add_argument("--force", action="store_true", help="Overwrite starter files")

    inspect_parser = subparsers.add_parser("inspect", help="Inspect discovered project objects")
    inspect_parser.add_argument(
        "--type",
        choices=("all", "sources", "metrics", "targets"),
        default="all",
        help="Object type to inspect",
    )
    inspect_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed object information",
    )

    sources_parser = subparsers.add_parser("sources", help="Source commands")
    sources_sub = sources_parser.add_subparsers(dest="sources_command")
    sources_sub.add_parser("list", help="List discovered sources")

    metrics_parser = subparsers.add_parser("metrics", help="Metric commands")
    metrics_sub = metrics_parser.add_subparsers(dest="metrics_command")
    metrics_sub.add_parser("list", help="List discovered metrics")
    metrics_run = metrics_sub.add_parser("run", help="Run a metric job")
    metrics_run.add_argument("metric_ids", nargs="*", help="Metric IDs or glob patterns")
    metrics_run.add_argument("--metric-id", help="Single metric ID (compat option)")
    metrics_run.add_argument("--all", action="store_true", help="Run all discovered metrics")
    metrics_run.add_argument("--target", required=True, help="Target ID to persist metric rows")

    targets_parser = subparsers.add_parser("targets", help="Target commands")
    targets_sub = targets_parser.add_subparsers(dest="targets_command")
    targets_sub.add_parser("list", help="List discovered targets")

    serve_parser = subparsers.add_parser("serve", help="Launch Streamlit UI")
    serve_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind Streamlit server",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=8501,
        help="Port to bind Streamlit server",
    )
    serve_parser.add_argument(
        "--metrics-db-path",
        type=str,
        help="Override metrics DB path used by the UI",
    )
    serve_parser.add_argument("--title", type=str, help="UI title")
    serve_parser.add_argument("--icon", type=str, help="UI icon")

    return parser


def _print_sources(discovery: ConfigDiscovery, verbose: bool) -> None:
    """Print source object summary."""
    print("Sources:")
    if not discovery.sources:
        print("  (none)")
    for source_id, source in sorted(discovery.sources.items()):
        if verbose:
            print(f"  - {source_id} ({source.__class__.__name__})")
        else:
            print(f"  - {source_id}")


def _print_metrics(discovery: ConfigDiscovery, verbose: bool) -> None:
    """Print metric object summary."""
    print("Metrics:")
    if not discovery.metrics:
        print("  (none)")
    for metric_id, metric in sorted(discovery.metrics.items()):
        if verbose:
            print(
                "  - "
                f"{metric_id} (source: {metric.source_id}, columns: {list(metric.output_columns)})"
            )
            if metric.description:
                print(f"      description: {metric.description}")
        else:
            print(f"  - {metric_id} (source: {metric.source_id})")


def _print_targets(discovery: ConfigDiscovery, verbose: bool) -> None:
    """Print target object summary."""
    print("Targets:")
    if not discovery.targets:
        print("  (none)")
    for target_id, target in sorted(discovery.targets.items()):
        if verbose:
            print(f"  - {target_id} ({target.__class__.__name__})")
        else:
            print(f"  - {target_id}")


def _print_discovery(discovery: ConfigDiscovery, inspect_type: str, verbose: bool) -> None:
    """Print discovery summary for selected object kinds."""
    if inspect_type in ("all", "sources"):
        _print_sources(discovery, verbose)
    if inspect_type in ("all", "metrics"):
        _print_metrics(discovery, verbose)
    if inspect_type in ("all", "targets"):
        _print_targets(discovery, verbose)


def _resolve_metric_selection(
    discovery: ConfigDiscovery,
    args: argparse.Namespace,
) -> list[MetricSpec]:
    """Resolve metric specs to run from explicit IDs, patterns, or --all."""
    if args.all:
        selected_ids = sorted(discovery.metrics)
    else:
        requested = []
        if args.metric_id:
            requested.append(args.metric_id)
        requested.extend(args.metric_ids)

        if not requested:
            raise ValueError("Specify metric IDs/patterns or pass --all")

        selected_ids = []
        for pattern in requested:
            if "*" in pattern or "?" in pattern:
                matched = sorted(
                    metric_id
                    for metric_id in discovery.metrics
                    if fnmatch.fnmatch(metric_id, pattern)
                )
                if not matched:
                    print(f"Warning: pattern '{pattern}' matched no metrics")
                selected_ids.extend(matched)
            else:
                if pattern not in discovery.metrics:
                    raise ValueError(f"Metric not found: {pattern}")
                selected_ids.append(pattern)

        selected_ids = list(dict.fromkeys(selected_ids))

    return [discovery.get_metric(metric_id) for metric_id in selected_ids]


def _cmd_serve(config: MetricsConfig, args: argparse.Namespace) -> int:
    """Launch Streamlit app for metrics browsing."""
    app_path = Path(__file__).resolve().parent.parent / "demo_app.py"

    env = os.environ.copy()
    env["ACME_METRICS_DB_PATH"] = args.metrics_db_path or config.store_db_path
    env["ACME_METRICS_CONFIG_ROOT"] = config.config_root
    if args.title:
        env["ACME_METRICS_TITLE"] = args.title
    if args.icon:
        env["ACME_METRICS_ICON"] = args.icon

    command = [
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        args.host,
        "--server.port",
        str(args.port),
    ]

    print(f"Launching acme-metrics UI on http://{args.host}:{args.port}")
    result = subprocess.run(command, env=env, check=False)
    return result.returncode


def _cmd_init(path: Path, force: bool) -> None:
    """Initialize a metrics project scaffold."""
    path.mkdir(parents=True, exist_ok=True)
    for directory in ("sources", "metrics", "targets"):
        (path / directory).mkdir(exist_ok=True)
        init_file = path / directory / "__init__.py"
        if not init_file.exists() or force:
            init_file.write_text("\n", encoding="utf-8")

    source_file = path / "sources" / "sample_source.py"
    metric_file = path / "metrics" / "sample_metric.py"
    target_file = path / "targets" / "sample_target.py"
    config_file = path / "config.py"
    manifest_file = path / "env.manifest"

    if force or not source_file.exists():
        source_file.write_text(
            "from __future__ import annotations\n\n"
            "import pandas as pd\n"
            "from acme_metrics.core import BaseSource\n\n"
            "class SampleSource(BaseSource):\n"
            '    source_id = "sample-source"\n\n'
            "    def load(self) -> pd.DataFrame:\n"
            "        return pd.DataFrame([\n"
            '            {"value": 1.0},\n'
            '            {"value": 2.0},\n'
            '            {"value": 3.0},\n'
            "        ])\n\n"
            "sample_source = SampleSource()\n",
            encoding="utf-8",
        )

    if force or not metric_file.exists():
        metric_file.write_text(
            "from __future__ import annotations\n\n"
            "import pandas as pd\n"
            "from acme_metrics.core import MetricSpec\n\n"
            "def _compute(source_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:\n"
            "    return pd.DataFrame([\n"
            "        {\n"
            '            "metric_name": "row_count",\n'
            '            "metric_value": float(len(source_df.index)),\n'
            "        },\n"
            "        {\n"
            '            "metric_name": "value_mean",\n'
            "            \"metric_value\": float(source_df['value'].mean()),\n"
            "        },\n"
            "    ])\n\n"
            "sample_metric = MetricSpec(\n"
            '    metric_id="sample-metric",\n'
            '    source_id="sample-source",\n'
            "    compute_fn=_compute,\n"
            ")\n",
            encoding="utf-8",
        )

    if force or not target_file.exists():
        target_file.write_text(
            "from __future__ import annotations\n\n"
            "from acme_metrics.config import get_config\n"
            "from acme_metrics.targets.duckdb import DuckDBTarget\n\n"
            "sample_target = DuckDBTarget(\n"
            '    target_id="local",\n'
            "    db_path=get_config().store_db_path,\n"
            ")\n",
            encoding="utf-8",
        )

    if force or not config_file.exists():
        config_file.write_text(
            "from __future__ import annotations\n\n"
            "from dataclasses import dataclass\n"
            "from pathlib import Path\n\n"
            "@dataclass(frozen=True)\n"
            "class MetricsProjectConfig:\n"
            "    store_db_path: str\n"
            "    metadeco_db_path: str\n"
            "    enable_catalog_registration: bool = False\n"
            "    catalog_db_path: str | None = None\n"
            '    secrets_backend: str = "env"\n'
            '    connection_backend: str = "acme-conn"\n'
            "    ui_enabled: bool = True\n\n"
            "project_config = MetricsProjectConfig(\n"
            '    store_db_path=str(Path("metrics.duckdb")),\n'
            '    metadeco_db_path=str(Path("traces.duckdb")),\n'
            ")\n",
            encoding="utf-8",
        )

    if force or not manifest_file.exists():
        manifest_file.write_text(
            "# Metrics workflow runtime\n"
            "ACME_METRICS_CONFIG_ROOT=.\n"
            "ACME_METRICS_STORE_DB_PATH=metrics.duckdb\n"
            "ACME_METRICS_METADECO_DB_PATH=traces.duckdb\n"
            "ACME_METRICS_CATALOG_AUTO_REGISTER=false\n"
            "\n"
            "# Optional data catalog integration\n"
            "ACME_DATA_CATALOG_DB_PATH=\n"
            "\n"
            "# Optional UI launch contract\n"
            "ACME_METRICS_DB_PATH=metrics.duckdb\n"
            "ACME_METRICS_TITLE=acme-metrics\n"
            "ACME_METRICS_ICON=📊\n",
            encoding="utf-8",
        )

    print(f"Scaffold created at {path}")


def main() -> None:
    """Execute the metrics CLI."""
    parser = _build_parser()
    args = parser.parse_args()

    overrides = {}
    if args.config_root:
        overrides["config_root"] = args.config_root
    config = get_config(**overrides)
    ctx = CLIContext(config=config)

    if args.command == "init":
        _cmd_init(Path(args.path), args.force)
        return

    if args.command is None:
        parser.print_help()
        return

    if args.command == "serve":
        exit_code = _cmd_serve(config, args)
        if exit_code != 0:
            sys.exit(exit_code)
        return

    try:
        discovery = ctx.load_discovery()
    except Exception as exc:
        print(f"Error loading config: {exc}")
        sys.exit(1)

    if args.command == "inspect":
        _print_discovery(discovery, args.type, args.verbose)
        return

    if args.command == "sources" and args.sources_command == "list":
        if not discovery.sources:
            print("No sources found")
            return
        for source_id in sorted(discovery.sources):
            print(source_id)
        return

    if args.command == "targets" and args.targets_command == "list":
        if not discovery.targets:
            print("No targets found")
            return
        for target_id in sorted(discovery.targets):
            print(target_id)
        return

    if args.command == "metrics" and args.metrics_command == "list":
        if not discovery.metrics:
            print("No metrics found")
            return
        for metric_id, metric in sorted(discovery.metrics.items()):
            print(f"{metric_id} (source: {metric.source_id})")
        return

    if args.command == "metrics" and args.metrics_command == "run":
        try:
            metrics = _resolve_metric_selection(discovery, args)
            target = discovery.get_target(args.target)
        except (KeyError, ValueError) as exc:
            print(f"Error: {exc}")
            sys.exit(1)

        runner = MetricsRunner(config)
        failures = 0
        for metric in metrics:
            source = discovery.get_source(metric.source_id)
            source_df = source.load()
            existing_df = target.load_metrics(metric.metric_id, metric.source_id)
            try:
                result = runner.run(metric, source_df, existing_df, target)
            except Exception as exc:
                failures += 1
                print(f"Failed metric run '{metric.metric_id}': {exc}")
                continue

            print("Completed metric run")
            print(f"  metric: {result.metric_id}")
            print(f"  source: {result.source_id}")
            print(f"  rows written: {result.rows_written}")

        if failures:
            print(f"Completed with {failures} failed metric run(s)")
            sys.exit(1)
        return

    parser.print_help()
