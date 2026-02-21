# Installation

```bash
uv sync
```

Run quick validation:

```bash
uv run ruff check src/acme_metrics tests/acme_data_metrics
uv run pytest tests/acme_data_metrics -q
```