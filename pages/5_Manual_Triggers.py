"""
Manual Triggers - Run individual collectors on demand
"""
import streamlit as st
import sys
import os
import subprocess
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from admin_utils.styles import apply_custom_styles

st.set_page_config(page_title="Manual Triggers", layout="wide")

# Authentication
from admin_utils.auth import require_authentication, show_logout_button
require_authentication()
show_logout_button()
apply_custom_styles()

st.title("Manual Triggers")

# Custom button styling
st.markdown("""
<style>
    .stButton > button {
        border: 1px solid rgba(128, 128, 128, 0.3) !important;
        border-radius: 6px !important;
    }
    .stButton > button:hover {
        border-color: rgba(128, 128, 128, 0.5) !important;
    }
</style>
""", unsafe_allow_html=True)

# Job mapping
job_scripts = {
    'binance': ('src/collectors/fetch_binance.py', 'Binance'),
    'stocks': ('src/collectors/fetch_stocks.py', 'Stocks'),
    'commodities': ('src/collectors/fetch_commodities.py', 'Commodities'),
    'macro': ('src/collectors/fetch_macro.py', 'Macro'),
    'fgi': ('src/collectors/fetch_fgi.py', 'FGI'),
    'crypto_news': ('src/collectors/fetch_crypto_news.py', 'Crypto News'),
    'stock_news': ('src/collectors/fetch_yfinance_news.py', 'Stock News'),
    'macro_summary': ('src/collectors/generate_macro_summary.py', 'Macro Summary'),
    'asset_analysis': ('src/collectors/generate_asset_analysis.py', 'Asset Analysis'),
    'news_summaries': ('src/collectors/generate_daily_news_summaries.py', 'News Summaries'),
    'portfolio': ('src/collectors/generate_weekly_portfolio_recommendation.py', 'Portfolio'),
}

# Data Collection
st.markdown("### Data Collection")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    if st.button("Binance\n~45s", key="binance", use_container_width=True):
        st.session_state['running_job'] = 'binance'

with col2:
    if st.button("Stocks\n~8 min", key="stocks", use_container_width=True):
        st.session_state['running_job'] = 'stocks'

with col3:
    if st.button("Commodities\n~25s", key="commodities", use_container_width=True):
        st.session_state['running_job'] = 'commodities'

with col4:
    if st.button("Macro\n~30s", key="macro", use_container_width=True):
        st.session_state['running_job'] = 'macro'

with col5:
    if st.button("FGI\n~5s", key="fgi", use_container_width=True):
        st.session_state['running_job'] = 'fgi'

st.markdown("---")

# News Collection
st.markdown("### News Collection")

col1, col2 = st.columns(2)

with col1:
    if st.button("Crypto News\n~15s", key="crypto_news", use_container_width=True):
        st.session_state['running_job'] = 'crypto_news'

with col2:
    if st.button("Stock News\n~3 min", key="stock_news", use_container_width=True):
        st.session_state['running_job'] = 'stock_news'

st.markdown("---")

# AI Analysis
st.markdown("### AI Analysis")

col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("Macro Summary\n~45s", key="macro_summary", use_container_width=True):
        st.session_state['running_job'] = 'macro_summary'

with col2:
    if st.button("Asset Analysis\n~60 min", key="asset_analysis", use_container_width=True):
        st.session_state['running_job'] = 'asset_analysis'

with col3:
    if st.button("News Summaries\n~2 min", key="news_summaries", use_container_width=True):
        st.session_state['running_job'] = 'news_summaries'

with col4:
    if st.button("Portfolio\n~1 min", key="portfolio", use_container_width=True):
        st.session_state['running_job'] = 'portfolio'

st.markdown("---")

# Execution Status
if 'running_job' in st.session_state and st.session_state['running_job']:
    job = st.session_state['running_job']
    
    if job in job_scripts:
        script_path, job_name = job_scripts[job]
        
        st.markdown(f"### Execute: {job_name}")
        
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        full_path = os.path.join(project_root, script_path)
        
        st.code(f"python {script_path}", language="bash")
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            if st.button("▶️ Run", type="primary"):
                with st.spinner(f"Running {job_name}..."):
                    try:
                        env = os.environ.copy()
                        env['PYTHONPATH'] = project_root
                        
                        result = subprocess.run(
                            [sys.executable, full_path],
                            cwd=project_root,
                            capture_output=True,
                            text=True,
                            timeout=300,
                            env=env
                        )
                        
                        if result.returncode == 0:
                            st.success(f"✅ {job_name} completed")
                            if result.stdout:
                                with st.expander("View Output"):
                                    st.code(result.stdout, language="log")
                        else:
                            st.error(f"❌ {job_name} failed")
                            if result.stderr:
                                st.code(result.stderr, language="log")
                            
                    except subprocess.TimeoutExpired:
                        st.error("❌ Timeout (5 min)")
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
                
                st.session_state['running_job'] = None
        
        with col2:
            if st.button("❌ Cancel"):
                st.session_state['running_job'] = None
                st.rerun()


