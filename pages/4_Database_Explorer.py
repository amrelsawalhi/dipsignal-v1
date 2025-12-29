"""
Database Explorer - Interactive SQL query interface
"""
import streamlit as st
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from admin_utils.db_utils import (
    get_all_tables,
    get_table_schema,
    get_table_row_count,
    execute_query
)
from admin_utils.styles import apply_custom_styles
import pandas as pd

st.set_page_config(page_title="Database Explorer", layout="wide")

# Authentication
from admin_utils.auth import require_authentication, show_logout_button
require_authentication()
show_logout_button()
apply_custom_styles()

st.title("Database Explorer")

# Table Browser
st.markdown("### Tables")

try:
    tables = get_all_tables()
    
    if tables:
        selected_table = st.selectbox("Select table:", tables)
        
        if selected_table:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                try:
                    row_count = get_table_row_count(selected_table)
                    st.metric("Rows", f"{row_count:,}")
                except:
                    st.metric("Rows", "Error")
            
            with col2:
                if st.button("🔍 Preview"):
                    st.session_state['preview_table'] = selected_table
            
            with col3:
                if st.button("📐 Schema"):
                    st.session_state['show_schema'] = selected_table
            
            # Show schema
            if st.session_state.get('show_schema') == selected_table:
                try:
                    schema_df = get_table_schema(selected_table)
                    st.dataframe(schema_df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Error: {e}")
            
            # Show preview
            if st.session_state.get('preview_table') == selected_table:
                try:
                    query = f"SELECT * FROM dipsignal.{selected_table} LIMIT 100"
                    df, error = execute_query(query)
                    
                    if error:
                        st.error(f"Error: {error}")
                    elif df is not None:
                        st.dataframe(df, use_container_width=True)
                        csv = df.to_csv(index=False)
                        st.download_button("📥 Download CSV", csv, f"{selected_table}.csv", "text/csv")
                    else:
                        st.info("No data")
                except Exception as e:
                    st.error(f"Error: {e}")
    else:
        st.warning("No tables found")
        
except Exception as e:
    st.error(f"Error: {e}")

st.markdown("---")

# Custom SQL Query
st.markdown("### SQL Query")

# Query templates
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📊 Latest Prices"):
        st.session_state['sql_query'] = """SELECT da.symbol, da.asset_class, fap.price_close, fap.timestamp
FROM dipsignal.fact_asset_prices fap
JOIN dipsignal.dim_assets da ON fap.asset_id = da.asset_id
WHERE DATE(fap.timestamp) = CURRENT_DATE
ORDER BY fap.timestamp DESC LIMIT 50"""

with col2:
    if st.button("📰 Recent News"):
        st.session_state['sql_query'] = """SELECT date, source, title, url
FROM dipsignal.fact_news_articles
ORDER BY date DESC LIMIT 20"""

with col3:
    if st.button("🤖 AI Analysis"):
        st.session_state['sql_query'] = """SELECT da.symbol, faa.trend_signal, faa.date
FROM dipsignal.fact_ai_analysis faa
JOIN dipsignal.dim_assets da ON faa.asset_id = da.asset_id
ORDER BY faa.date DESC LIMIT 10"""

# SQL editor
sql_query = st.text_area(
    "Query:",
    value=st.session_state.get('sql_query', 'SELECT * FROM dipsignal.dim_assets LIMIT 10'),
    height=150
)

col1, col2 = st.columns([1, 4])

with col1:
    execute_button = st.button("▶️ Execute", type="primary")

with col2:
    if st.button("🗑️ Clear"):
        st.session_state['sql_query'] = ''
        st.rerun()

# Execute query
if execute_button and sql_query.strip():
    try:
        df, error = execute_query(sql_query)
        
        if error:
            st.error(f"Error: {error}")
        elif df is not None:
            st.success(f"✅ {len(df)} rows returned")
            st.dataframe(df, use_container_width=True)
            csv = df.to_csv(index=False)
            st.download_button("📥 Download CSV", csv, "results.csv", "text/csv")
        else:
            st.success("✅ Query executed")
            
    except Exception as e:
        st.error(f"Error: {e}")


