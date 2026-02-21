# User Guides

## Create a new metrics project

```bash
adm init --path ./my-metrics-project
cd ./my-metrics-project
```

Generated structure:

```text
my-metrics-project/
├── config.py
├── env.manifest
├── sources/
├── metrics/
└── targets/
```

## Define a source

```python
from __future__ import annotations

import pandas as pd
from acme_metrics.core import BaseSource


class PricesSource(BaseSource):
    source_id = "prices"

    def load(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"symbol": "AAPL", "close": 180.0},
            {"symbol": "MSFT", "close": 410.0},
        ])


prices = PricesSource()
```

## Define a metric

```python
from __future__ import annotations

import pandas as pd
from acme_metrics.core import MetricSpec


def _compute(source_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    del existing_df # Required by the compute_fn signature intentionally unused in this example.
    return pd.DataFrame(
        [
            {"metric_name": "row_count", "metric_value": float(len(source_df.index))},
            {"metric_name": "close_mean", "metric_value": float(source_df["close"].mean())},
        ]
    )


price_summary = MetricSpec(
    metric_id="price-summary",
    source_id="prices",
    compute_fn=_compute,
)
```

## Define a target

```python
from __future__ import annotations

from acme_metrics.config import get_config
from acme_metrics.targets.duckdb import DuckDBTarget


local = DuckDBTarget(target_id="local", db_path=get_config().store_db_path)
```

## Run metrics

```bash
# Inspect discovered objects
adm --config-root . inspect --verbose

# Run one metric
adm --config-root . metrics run --metric-id price-summary --target local

# Run all metrics
adm --config-root . metrics run --all --target local
```

## Launch the UI

```bash
adm --config-root . serve --metrics-db-path ./metrics.duckdb --port 8501
```
