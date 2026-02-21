# acme-metrics

`acme-metrics` is a template-style framework for building dataset metrics workflows.

## Project convention

User projects define metrics workflows with three folders:

- `sources/`: data loaders for landed datasets
- `metrics/`: metric specs (`MetricSpec`) binding source + compute function + output schema
- `targets/`: persistence targets for computed metrics

This mirrors the pattern used by `acme-landing`, adapted for metrics jobs.

## Quick start

Create a starter project structure:

```bash
adm init --path ./my-metrics-project
```

Inspect discovered objects:

```bash
adm --config-root ./my-metrics-project inspect --verbose
```

Run one metric:

```bash
adm --config-root ./my-metrics-project metrics run --metric-id sample-metric --target local
```

Run all metrics:

```bash
adm --config-root ./my-metrics-project metrics run --all --target local
```

Launch the UI:

```bash
adm serve --metrics-db-path ./metrics.duckdb --port 8501
```

## Integrations

- `acme-config`: runtime settings and CLI configuration
- `acme-secrets`: secret management in project-level config wiring
- `acme-conn`: source/target connection abstractions in project adapters
- `acme-metadeco`: execution tracing for metric runs
- `acme-streamlit`: standard navigation and page component contract
- `acme-data-catalog` (optional): metric registration when enabled

## Runtime environment

Scaffolded projects include `env.manifest` with defaults for:

- `ACME_METRICS_CONFIG_ROOT`
- `ACME_METRICS_STORE_DB_PATH`
- `ACME_METRICS_METADECO_DB_PATH`
- `ACME_METRICS_CATALOG_AUTO_REGISTER`

## Validation

```bash
uv run ruff check src/acme_metrics tests/acme_data_metrics
uv run pytest tests/acme_data_metrics -q
cd docs && uv run mkdocs build
```