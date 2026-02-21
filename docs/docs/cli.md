# CLI Reference

Complete command-line interface reference for `acme-metrics`.

## Global options

```bash
adm --help
adm --config-root PATH
```

## init

Initialize a new metrics project scaffold.

```bash
adm init [OPTIONS]
```

Options:

- `--path PATH`: target directory (default: current directory)
- `--force`: overwrite starter files

## inspect

Inspect discovered project objects.

```bash
adm --config-root PATH inspect [OPTIONS]
```

Options:

- `--type {all,sources,metrics,targets}`
- `--verbose`

## sources list

List discovered sources.

```bash
adm --config-root PATH sources list
```

## metrics list

List discovered metrics and their source bindings.

```bash
adm --config-root PATH metrics list
```

## metrics run

Run one or more metrics against a selected target.

```bash
adm --config-root PATH metrics run [METRIC_IDS_OR_PATTERNS...] --target TARGET_ID [OPTIONS]
```

Options:

- `--metric-id METRIC_ID` (single metric compatibility option)
- `--all` (run all discovered metrics)
- `--target TARGET_ID` (required)

Examples:

```bash
# Run one metric
adm --config-root . metrics run --metric-id price-summary --target local

# Run by pattern
adm --config-root . metrics run "daily-*" --target local

# Run all
adm --config-root . metrics run --all --target local
```

## targets list

List discovered targets.

```bash
adm --config-root PATH targets list
```

## serve

Launch Streamlit UI.

```bash
adm --config-root PATH serve [OPTIONS]
```

Options:

- `--host HOST` (default: `127.0.0.1`)
- `--port PORT` (default: `8501`)
- `--metrics-db-path PATH`
- `--title TITLE`
- `--icon ICON`
