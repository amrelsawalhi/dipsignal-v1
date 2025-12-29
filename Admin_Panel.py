"""
Home Dashboard - System Overview
"""
import streamlit as st
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from admin_utils.db_utils import (
    get_table_row_count,
    get_latest_timestamp,
    get_data_freshness,
    get_asset_count_by_class,
    get_recent_news_count,
    get_asset_coverage_today,
    test_connection
)
from admin_utils.components import metric_card, status_badge, format_time_ago, section_header
from admin_utils.chart_utils import create_asset_distribution_pie, create_coverage_chart
from admin_utils.styles import apply_custom_styles
from admin_utils.auth import require_authentication, show_logout_button
import pandas as pd

st.set_page_config(page_title="DipSignal Admin - Home", page_icon="📊", layout="wide")

# Require authentication - this will show login page if not authenticated
require_authentication()

# Apply custom styles
apply_custom_styles()

# Show logout button in sidebar
show_logout_button()

# Header
st.title("DipSignal Admin Panel")

# System Status Checks - Compact inline version
col1, col2, col3, col4 = st.columns(4)

with col1:
    db_status, db_message = test_connection()
    if db_status:
        st.metric("Database", "✅ Connected", delta=None, help="PostgreSQL connection active")
    else:
        st.metric("Database", "❌ Error", delta=None, help=db_message)

with col2:
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', 3000))
        sock.close()
        if result == 0:
            st.metric("Dagster", "✅ Running", delta=None, help="Port 3000")
            st.markdown("[Open Dagster UI ↗](http://localhost:3000)")
        else:
            st.metric("Dagster", "⚠️ Stopped", delta=None, help="Run 'dagster dev' to start")
    except:
        st.metric("Dagster", "❌ Error", delta=None, help="Cannot check status")

with col3:
    try:
        total_assets = get_table_row_count("dim_assets")
        st.metric("Assets", total_assets)
    except:
        st.metric("Assets", "Error")

with col4:
    try:
        news_count = get_recent_news_count(days=7)
        st.metric("News (7d)", news_count)
    except:
        st.metric("News (7d)", "Error")

st.markdown("---")

# Data Freshness - Compact table
st.markdown("### Data Freshness")

tables_to_check = [
    ("fact_asset_prices", "timestamp", "Prices"),
    ("fact_macro_indicators", "date", "Macro"),
    ("fact_sentiment_index", "date", "FGI"),
    ("fact_news_articles", "date", "News"),
    ("fact_ai_analysis", "date", "AI"),
]

freshness_data = []
for table, column, display_name in tables_to_check:
    try:
        hours_old, latest_time, status = get_data_freshness(table, column)
        freshness_data.append({
            "Table": display_name,
            "Last Update": latest_time.strftime("%m-%d %H:%M") if latest_time else "Never",
            "Status": "🟢" if status == 'fresh' else "🟡" if status == 'stale' else "🔴"
        })
    except Exception as e:
        freshness_data.append({"Table": display_name, "Last Update": "Error", "Status": "🔴"})

st.dataframe(pd.DataFrame(freshness_data), use_container_width=True, hide_index=True)

st.markdown("---")

# Asset Distribution - Side by side charts
st.markdown("### Asset Overview")

col1, col2 = st.columns(2)

with col1:
    try:
        asset_dist = get_asset_count_by_class()
        if not asset_dist.empty:
            # Rename columns for chart
            asset_dist.columns = ['asset_class', 'count']
            fig = create_asset_distribution_pie(asset_dist)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No asset data available")
    except Exception as e:
        st.error(f"Error loading asset distribution: {e}")

with col2:
    try:
        coverage = get_asset_coverage_today()
        if not coverage.empty:
            # Rename columns for chart
            coverage.columns = ['asset_class', 'total_assets', 'assets_with_data_today']
            fig = create_coverage_chart(coverage)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No coverage data available")
    except Exception as e:
        st.error(f"Error loading coverage data: {e}")


