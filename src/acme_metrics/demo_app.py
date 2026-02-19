"""Streamlit demo app for acme-metrics.

Uses sidebar navigation via acme_streamlit.Navigation.
Pages: Metrics Browser.
"""

from __future__ import annotations

import streamlit as st

from acme_metrics.launch import (
    LaunchContextError,
    build_navigation,
    create_launch_context_from_env,
)

try:
    launch_context = create_launch_context_from_env()
except LaunchContextError as exc:
    st.set_page_config(page_title="acme-metrics", page_icon="📊", layout="wide")
    st.error(str(exc))
    st.stop()

st.set_page_config(page_title=launch_context.title, page_icon=launch_context.icon, layout="wide")
build_navigation(launch_context).render()
