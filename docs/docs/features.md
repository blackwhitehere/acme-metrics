# Features

`acme-metrics` standardizes implementation of metric calculations workflows.

## Core capabilities

- Organizes definitions in `sources/`, `metrics/`, and `targets/` Python modules
- CLI commands for scaffolding, inspection, execution, and UI launch
- Model for metric calculation functions and incremental metric publishing via scheduled jobs
- Metric calculation jobs tracked with `acme-metadeco`
- Optional metric registration in `acme-data-catalog`
- Streamlit navigation for metrics browsing and project overview

## Project-object model

The framework treats metric jobs as a composition of three runtime objects:

- **Source**: loads tabular data (`BaseSource.load`) for a dataset context
- **Metric**: defines compute intent and output schema (`MetricSpec`)
- **Target**: persists and reads metric rows (`BaseTarget`)

This keeps metric computation logic independent from storage and source-connection details.

## Workflow model

Each metric run follows the same sequence:

1. load source dataset rows
2. load existing metrics for metric/source pair
3. compute new metric rows via `MetricSpec.compute_fn`
4. validate output schema columns
5. persist rows to the selected target
6. trace execution via `acme-metadeco`

## Execution and orchestration

- **Single-run orchestration** via `MetricsRunner`
- **Batch execution** from CLI with explicit metric IDs, glob patterns, or `--all`
- **Consistent validation** of required output columns before persistence
- **Failure visibility** in batch mode where failed metric runs are reported individually

## Storage and compatibility

- **Default adapter**: `DuckDBTarget` backed by `MetricsStore`
- **Target interface** allows projects to add custom persistence backends
- **Programmatic compatibility**: `@metrics_job` routes through the same runner/target pipeline used by CLI runs
- **Existing metrics input** is part of compute contract (`compute_fn(source_df, existing_df)`)

## Configuration and environment

- Runtime settings resolve through `acme-config` (`MetricsConfig`)
- Standard environment variables:
	- `ACME_METRICS_CONFIG_ROOT`
	- `ACME_METRICS_STORE_DB_PATH`
	- `ACME_METRICS_METADECO_DB_PATH`
	- `ACME_METRICS_CATALOG_AUTO_REGISTER`
- Generated `env.manifest` from `adm init` provides a baseline deployment contract

## Observability and lineage hooks

- **Run tracing** through `acme-metadeco` for each metric execution
- **Stage-level trace context** carried by runner metadata (metric/source bindings)
- **Optional catalog registration** to `acme-data-catalog` for dataset-metric visibility

## UI capabilities

- Streamlit UI launch from CLI (`adm serve`)
- Deployment-aware project overview (discovered sources, metrics, targets)
- Metrics browser for stored metric values and dataset comparisons

## Developer experience

- `adm init` scaffolds project layout and starter modules
- `adm inspect` provides object-level visibility with optional verbose details
- Docs and CLI share the same object vocabulary (`source`, `metric`, `target`)
- Python-based project modules keep workflows testable with regular unit tests

## Integrations

| Library | Purpose |
|---------|---------|
| `acme-config` | Resolve runtime settings and CLI overrides |
| `acme-secrets` | Provide secret wiring in project adapters |
| `acme-conn` | Provide source/target connection abstractions in adapters |
| `acme-metadeco` | Trace metric execution runs |
| `acme-streamlit` | Shared navigation and page patterns |
| `acme-data-catalog` (optional) | Register computed metrics by dataset |
