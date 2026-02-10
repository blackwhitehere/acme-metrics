"""Streamlit demo dashboard for acme-metrics.

Displays timeseries metrics for multiple entities with support for
any metric schema. Users select a dataset, pick metric columns, and
filter by entity to explore the data interactively.
"""

from __future__ import annotations

import polars as pl
import streamlit as st

from acme_metrics.demo import FIXTURE_REGISTRY

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="acme-metrics Demo", page_icon="📊", layout="wide")
st.title("📊 acme-metrics Demo Dashboard")
st.caption("Explore timeseries metrics for multiple entities across different schemas")

# ---------------------------------------------------------------------------
# Dataset selector
# ---------------------------------------------------------------------------
dataset_names = list(FIXTURE_REGISTRY.keys())
selected_name = st.sidebar.selectbox("Dataset", dataset_names)
generator_fn, description = FIXTURE_REGISTRY[selected_name]
st.sidebar.caption(description)

df: pl.DataFrame = generator_fn()

# ---------------------------------------------------------------------------
# Schema detection — infer date, entity, and metric columns automatically
# ---------------------------------------------------------------------------
date_cols = [c for c in df.columns if df[c].dtype in (pl.Date, pl.Datetime)]
string_cols = [c for c in df.columns if df[c].dtype in (pl.Utf8, pl.Categorical)]
numeric_cols = [
    c
    for c in df.columns
    if df[c].dtype
    in (
        pl.Float32,
        pl.Float64,
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    )
]

if not date_cols:
    st.error("Selected dataset has no date column — cannot plot timeseries.")
    st.stop()

date_col = st.sidebar.selectbox("Date column", date_cols, index=0)

entity_col: str | None = None
if string_cols:
    entity_col = st.sidebar.selectbox("Entity column", string_cols, index=0)

if not numeric_cols:
    st.error("Selected dataset has no numeric columns to display.")
    st.stop()

selected_metrics = st.sidebar.multiselect(
    "Metrics to plot",
    numeric_cols,
    default=numeric_cols[:2],
)

# ---------------------------------------------------------------------------
# Entity filter
# ---------------------------------------------------------------------------
entities: list[str] = []
if entity_col:
    all_entities = sorted(df[entity_col].unique().to_list())
    entities = st.sidebar.multiselect(
        "Filter entities", all_entities, default=all_entities
    )
    if entities:
        df = df.filter(pl.col(entity_col).is_in(entities))

# ---------------------------------------------------------------------------
# Date range filter
# ---------------------------------------------------------------------------
min_date = df[date_col].min()
max_date = df[date_col].max()
if min_date and max_date:
    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start, end = date_range
        df = df.filter(
            (pl.col(date_col) >= pl.lit(start).cast(pl.Date))
            & (pl.col(date_col) <= pl.lit(end).cast(pl.Date))
        )

# ---------------------------------------------------------------------------
# Summary metrics row
# ---------------------------------------------------------------------------
if selected_metrics and entity_col:
    st.subheader("Summary")
    summary_cols = st.columns(len(selected_metrics))
    for i, metric_name in enumerate(selected_metrics):
        latest = df.sort(date_col).group_by(entity_col).last()
        mean_val = latest[metric_name].mean()
        summary_cols[i].metric(
            label=metric_name,
            value=f"{mean_val:.2f}" if mean_val is not None else "N/A",
        )

# ---------------------------------------------------------------------------
# Timeseries charts
# ---------------------------------------------------------------------------
st.subheader("Timeseries")

if not selected_metrics:
    st.info("Select at least one metric to plot.")
else:
    pdf = df.to_pandas()

    for metric_name in selected_metrics:
        st.markdown(f"**{metric_name}**")

        if entity_col:
            # Pivot to get one column per entity for the line chart
            pivot = pdf.pivot_table(
                index=date_col,
                columns=entity_col,
                values=metric_name,
                aggfunc="mean",
            )
            st.line_chart(pivot)
        else:
            chart_df = pdf.set_index(date_col)[[metric_name]]
            st.line_chart(chart_df)

# ---------------------------------------------------------------------------
# Cross-entity comparison (latest snapshot)
# ---------------------------------------------------------------------------
if entity_col and selected_metrics:
    st.subheader("Entity Comparison (latest)")
    latest_df = df.sort(date_col).group_by(entity_col).last()
    latest_pdf = latest_df.select([entity_col] + selected_metrics).to_pandas()
    latest_pdf = latest_pdf.set_index(entity_col)
    st.bar_chart(latest_pdf)

# ---------------------------------------------------------------------------
# Raw data table
# ---------------------------------------------------------------------------
with st.expander("Raw Data", expanded=False):
    st.dataframe(df.to_pandas(), width="stretch")

# ---------------------------------------------------------------------------
# Schema info
# ---------------------------------------------------------------------------
with st.expander("Schema Info", expanded=False):
    schema_rows = [
        {"column": name, "dtype": str(dtype)} for name, dtype in df.schema.items()
    ]
    st.table(pl.DataFrame(schema_rows).to_pandas())
