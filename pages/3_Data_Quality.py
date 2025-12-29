"""
Data Quality Dashboard - Monitor data completeness and freshness
"""
import streamlit as st
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from admin_utils.db_utils import (
    get_data_freshness,
    execute_query,
    get_asset_coverage_today
)
from admin_utils.styles import apply_custom_styles
import pandas as pd

st.set_page_config(page_title="Data Quality", layout="wide")

# Authentication
from admin_utils.auth import require_authentication, show_logout_button
require_authentication()
show_logout_button()
apply_custom_styles()

st.title("Data Quality")

# Freshness Metrics
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
    except:
        freshness_data.append({"Table": display_name, "Last Update": "Error", "Status": "🔴"})

df_freshness = pd.DataFrame(freshness_data)

# Display metrics
col1, col2, col3 = st.columns(3)

fresh_count = len(df_freshness[df_freshness['Status'] == '🟢'])
stale_count = len(df_freshness[df_freshness['Status'] == '🟡'])
critical_count = len(df_freshness[df_freshness['Status'] == '🔴'])

with col1:
    st.metric("Fresh", fresh_count, help="< 24 hours")

with col2:
    st.metric("Stale", stale_count, help="24-72 hours")

with col3:
    st.metric("Critical", critical_count, help="> 72 hours")

# Freshness table
st.dataframe(df_freshness, use_container_width=True, hide_index=True)

st.markdown("---")

# Asset Coverage
st.markdown("### Asset Coverage Today")

try:
    coverage = get_asset_coverage_today()
    if not coverage.empty:
        coverage.columns = ['Class', 'Total', 'Today']
        coverage['%'] = (coverage['Today'] / coverage['Total'] * 100).round(1)
        st.dataframe(coverage, use_container_width=True, hide_index=True)
    else:
        st.info("No coverage data")
except Exception as e:
    st.error(f"Error: {e}")

st.markdown("---")

# Null Value Checks
st.markdown("### Null Values (Last 7 Days)")

try:
    null_query = """
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN price_open IS NULL THEN 1 ELSE 0 END) as null_open,
            SUM(CASE WHEN price_close IS NULL THEN 1 ELSE 0 END) as null_close,
            SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume
        FROM dipsignal.fact_asset_prices
        WHERE DATE(timestamp) >= CURRENT_DATE - INTERVAL '7 days'
    """
    
    df_nulls, error = execute_query(null_query)
    
    if error:
        st.error(f"Error: {error}")
    elif df_nulls is not None and not df_nulls.empty:
        total = df_nulls['total'].iloc[0]
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            null_open = df_nulls['null_open'].iloc[0]
            pct = (null_open / total * 100) if total > 0 else 0
            st.metric("Null Open", f"{null_open} ({pct:.1f}%)")
        
        with col2:
            null_close = df_nulls['null_close'].iloc[0]
            pct = (null_close / total * 100) if total > 0 else 0
            st.metric("Null Close", f"{null_close} ({pct:.1f}%)")
        
        with col3:
            null_volume = df_nulls['null_volume'].iloc[0]
            pct = (null_volume / total * 100) if total > 0 else 0
            st.metric("Null Volume", f"{null_volume} ({pct:.1f}%)")
        
        if null_open > 0 or null_close > 0:
            st.warning("⚠️ Null values detected - may indicate API issues")
    else:
        st.info("No data")
        
except Exception as e:
    st.error(f"Error: {e}")

st.markdown("---")

if st.button("🔄 Refresh"):
    st.rerun()


