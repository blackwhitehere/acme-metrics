"""PageFragment implementations for acme-metrics.

These fragments can be embedded in any Streamlit app (e.g. acme-data-catalog).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st
from acme_streamlit import format_duration, metrics_row, status_badge

from acme_metrics.store import MetricsStore

if TYPE_CHECKING:
    from acme_metadeco.storage.query import QueryInterface

    from acme_metrics.launch import LaunchContext


class MetricsJobsFragment:
    """Shows metrics job execution history from metadeco traces."""

    name = "Metrics Jobs"
    description = "View metrics computation job history"
    icon = "🔬"

    def __init__(self, store: QueryInterface) -> None:
        self._store = store

    def render(self) -> None:
        runs = self._store.list_runs()

        if not runs:
            st.info("No metrics job runs recorded yet.")
            return

        metrics_row(
            [
                ("Total Runs", len(runs)),
                ("Jobs", len({r.app_name for r in runs})),
            ]
        )

        run_rows = []
        for r in runs:
            run_rows.append(
                {
                    "Run ID": r.run_id[:12] + "...",
                    "Job": r.app_name,
                    "Status": r.status.value,
                    "Duration": format_duration(r.duration_seconds)
                    if r.duration_seconds
                    else "N/A",
                    "Spans": r.total_spans,
                    "Start": str(r.start_timestamp)[:19],
                }
            )

        st.dataframe(pd.DataFrame(run_rows), use_container_width=True)

        # Detail view for selected run
        selected_run = st.selectbox(
            "Select run for details",
            runs,
            format_func=lambda r: f"{r.app_name} ({r.run_id[:12]}...)",
            key="metrics_fragment_run_select",
        )

        if selected_run:
            st.subheader(f"Run: {selected_run.app_name}")
            st.markdown(f"**Status:** {status_badge(selected_run.status.value)}")

            if selected_run.duration_seconds:
                st.markdown(f"**Duration:** {format_duration(selected_run.duration_seconds)}")

            spans = self._store.get_spans_for_run(selected_run.run_id)
            if spans:
                st.subheader("Execution Spans")
                span_rows = []
                for s in spans:
                    span_rows.append(
                        {
                            "Function": s.function_name,
                            "Stage": s.decorator_type,
                            "Status": s.status.value,
                            "Duration": format_duration(s.duration_seconds)
                            if s.duration_seconds
                            else "N/A",
                            "Module": s.module_name,
                        }
                    )
                st.dataframe(pd.DataFrame(span_rows), use_container_width=True)


class MetricsBrowserFragment:
    """Browse computed metrics by dataset. Embeddable in portal/catalog."""

    name = "Metrics Browser"
    description = "Browse computed metrics stored in acme-metrics"
    icon = "📈"

    def __init__(self, store: MetricsStore) -> None:
        self._store = store

    def render(self) -> None:
        dataset_ids = self._store.list_dataset_ids()
        if not dataset_ids:
            st.info("No metrics computed yet.")
            return

        selected_id = st.selectbox(
            "Select dataset",
            dataset_ids,
            key="metrics_browser_dataset_select",
        )

        if not selected_id:
            return

        latest = self._store.get_latest_metrics(selected_id)

        # Summary cards for latest values
        if latest:
            card_items = [(name, f"{value:.4f}") for name, value in list(latest.items())[:6]]
            metrics_row(card_items)

        # Full metric history table
        records = self._store.get_metrics_for(selected_id)
        if records:
            rows = [
                {
                    "Metric": r.metric_name,
                    "Value": r.metric_value,
                    "Computed": str(r.computed_at)[:19],
                }
                for r in records
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

        # Cross-dataset comparison
        with st.expander("Compare Across Datasets"):
            all_rows = []
            for ds_id in dataset_ids:
                for r in self._store.get_metrics_for(ds_id):
                    all_rows.append(
                        {
                            "Dataset": ds_id,
                            "Metric": r.metric_name,
                            "Value": r.metric_value,
                            "Computed": str(r.computed_at)[:19],
                        }
                    )
            if all_rows:
                st.dataframe(pd.DataFrame(all_rows), use_container_width=True)
            else:
                st.info("No metrics recorded across any dataset.")


class MetricsProjectOverviewFragment:
    """Shows project-level sources/metrics/targets discovered for deployment mode."""

    name = "Project Overview"
    description = "Inspect discovered sources, metrics, and targets"
    icon = "🧭"

    def __init__(self, context: LaunchContext) -> None:
        self._context = context

    def render(self) -> None:
        st.markdown("## Metrics Project Overview")

        if self._context.config_root:
            st.caption(f"Config root: {self._context.config_root}")

        metrics_row(
            [
                ("Sources", len(self._context.source_ids)),
                ("Metrics", len(self._context.metric_bindings)),
                ("Targets", len(self._context.target_ids)),
            ]
        )

        if self._context.source_ids:
            st.markdown("### Sources")
            st.dataframe(
                pd.DataFrame([{"Source ID": source_id} for source_id in self._context.source_ids]),
                use_container_width=True,
            )

        if self._context.metric_bindings:
            st.markdown("### Metrics")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "Metric ID": metric_id,
                            "Source ID": source_id,
                        }
                        for metric_id, source_id in self._context.metric_bindings
                    ]
                ),
                use_container_width=True,
            )

        if self._context.target_ids:
            st.markdown("### Targets")
            st.dataframe(
                pd.DataFrame([{"Target ID": target_id} for target_id in self._context.target_ids]),
                use_container_width=True,
            )
