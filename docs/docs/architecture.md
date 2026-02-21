# Architecture

## Overview

`acme-metrics` is organized around three runtime object types:

- **Sources**: load dataset rows (`BaseSource`)
- **Metrics**: compute rows (`MetricSpec`)
- **Targets**: load/save metric rows (`BaseTarget`)

The CLI discovers these objects from project files and executes metric runs through `MetricsRunner`.

## Project structure (application side)

```text
my-metrics-project/
├── config.py
├── env.manifest
├── sources/
├── metrics/
└── targets/
```

## Core runtime components

### Configuration layer

- `MetricsConfig` (`acme_metrics.config`) resolves runtime settings from env/defaults/overrides
- `ConfigDiscovery` (`acme_metrics.core.config`) imports project modules and builds registries:
  - `sources[source_id]`
  - `metrics[metric_id]`
  - `targets[target_id]`

### Source layer

- Base abstraction: `BaseSource`
- Contract: `load() -> pandas.DataFrame`

### Metric layer

- Core model: `MetricSpec`
- Each spec defines:
  - `metric_id`
  - `source_id`
  - `compute_fn(source_df, existing_df) -> metrics_df`
  - output schema columns

### Target layer

- Base abstraction: `BaseTarget`
- Contract:
  - `load_metrics(metric_id, source_id) -> DataFrame`
  - `save_metrics(metric_id, source_id, metrics_df) -> None`
- Default implementation: `DuckDBTarget`

### Orchestration layer

- `MetricsRunner` executes one metric run:
  1. compute rows from source + existing metrics
  2. validate expected output columns
  3. persist rows to target
  4. record run trace via metadeco
  5. optionally register rows in catalog

## UI launch architecture

- `LaunchContext` resolves deployment or demo mode
- Deployment mode can include discovered project metadata from `ACME_METRICS_CONFIG_ROOT`
- Streamlit navigation shows:
  - project overview (deployment mode)
  - metrics browser
