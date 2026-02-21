# Cheat Sheet

## Setup

```bash
uv sync
```

## Scaffold

```bash
adm init --path ./my-metrics-project
```

## Inspect

```bash
adm --config-root ./my-metrics-project inspect --verbose
adm --config-root ./my-metrics-project sources list
adm --config-root ./my-metrics-project metrics list
adm --config-root ./my-metrics-project targets list
```

## Run metrics

```bash
adm --config-root ./my-metrics-project metrics run --metric-id sample-metric --target local
adm --config-root ./my-metrics-project metrics run --all --target local
adm --config-root ./my-metrics-project metrics run "daily-*" --target local
```

## Serve UI

```bash
adm --config-root ./my-metrics-project serve --metrics-db-path ./metrics.duckdb
```

## Verify project

```bash
uv run ruff check src/acme_metrics tests/acme_data_metrics
uv run pytest tests/acme_data_metrics -q
cd docs && uv run mkdocs build
```
