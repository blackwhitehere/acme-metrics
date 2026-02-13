"""Streamlit demo app for acme-metrics.

Uses sidebar navigation via acme_streamlit.Navigation.
Pages: Metrics Browser.
"""

from __future__ import annotations

import streamlit as st
from acme_streamlit.navigation import Navigation

from acme_metrics.demo import METRICS_DB
from acme_metrics.store import MetricsStore
from acme_metrics.ui.fragments import MetricsBrowserFragment

if not METRICS_DB.exists():
    st.set_page_config(page_title="acme-metrics Demo", page_icon="📊", layout="wide")
    st.error("Demo data not found. Run `just demo` or `python -m acme_metrics.demo --setup` first.")
    st.stop()

store = MetricsStore(str(METRICS_DB))

nav = Navigation("acme-metrics", icon="📊")

nav.page(MetricsBrowserFragment(store))

nav.render()
