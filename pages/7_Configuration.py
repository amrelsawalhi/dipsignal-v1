"""
Configuration - View system settings and environment variables
"""
import streamlit as st
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from admin_utils.asset_utils import get_all_assets
from admin_utils.styles import apply_custom_styles
import pandas as pd

st.set_page_config(page_title="Configuration", layout="wide")

# Authentication
from admin_utils.auth import require_authentication, show_logout_button
require_authentication()
show_logout_button()
apply_custom_styles()

st.title("Configuration")

# Environment Variables
st.markdown("### Environment")

try:
    from dotenv import load_dotenv
    load_dotenv()
    
    db_url = os.getenv("DATABASE_URL", "Not set")
    
    # Mask password
    if db_url and "Not set" not in db_url:
        parts = db_url.split("@")
        if len(parts) == 2:
            user_pass = parts[0].split("//")[1]
            if ":" in user_pass:
                user = user_pass.split(":")[0]
                masked_url = f"postgresql://{user}:****@{parts[1]}"
            else:
                masked_url = db_url
        else:
            masked_url = "****"
    else:
        masked_url = "Not configured"
    
    st.code(masked_url, language="text")
    
    # API Keys
    api_keys = {
        "FRED_API_KEY": os.getenv("FRED_API_KEY", "Not set"),
        "GEMINI_API_KEY": os.getenv("GEMINI_API_KEY", "Not set"),
    }
    
    for key, value in api_keys.items():
        if value and "Not set" not in value:
            masked = value[:4] + "*" * (len(value) - 8) + value[-4:] if len(value) > 8 else "****"
        else:
            masked = "Not configured"
        st.text(f"{key}: {masked}")
    
except Exception as e:
    st.error(f"Error: {e}")

st.markdown("---")

# AI Models
st.markdown("### AI Models")

models = {
    "Macro Summary": "gemma-3-27b-it",
    "Asset Analysis": "gemma-3-27b-it",
    "News Summaries": "gemma-3-12b-it",
    "Portfolio Recommendation": "gemini-2.5-flash",
}

col1, col2 = st.columns(2)
items = list(models.items())
with col1:
    for task, model in items[:2]:
        st.text(f"{task}: {model}")
with col2:
    for task, model in items[2:]:
        st.text(f"{task}: {model}")

st.markdown("---")

# Asset Configuration
st.markdown("### Assets")

try:
    assets = get_all_assets()
    
    tab1, tab2, tab3, tab4 = st.tabs(["💰 Crypto", "📈 Stocks", "🏆 Commodities", "📰 RSS"])
    
    with tab1:
        st.json(assets['crypto'])
    
    with tab2:
        st.json(assets['stocks'])
    
    with tab3:
        st.json(assets['commodities'])
    
    with tab4:
        st.json(assets['rss_feeds'])
        
except Exception as e:
    st.error(f"Error: {e}")

st.markdown("---")

# Database Schema
st.markdown("### Database Schema")

tables = [
    {"Table": "dim_assets", "Type": "Dimension", "Description": "Asset master data"},
    {"Table": "dim_date", "Type": "Dimension", "Description": "Date dimension"},
    {"Table": "fact_asset_prices", "Type": "Fact", "Description": "OHLCV price data"},
    {"Table": "fact_macro_indicators", "Type": "Fact", "Description": "Macro indicators"},
    {"Table": "fact_sentiment_index", "Type": "Fact", "Description": "Fear & Greed Index"},
    {"Table": "fact_news_articles", "Type": "Fact", "Description": "News articles"},
    {"Table": "fact_ai_analysis", "Type": "Fact", "Description": "AI asset analysis"},
    {"Table": "fact_macro_summary", "Type": "Fact", "Description": "AI macro summaries"},
]

st.dataframe(pd.DataFrame(tables), use_container_width=True, hide_index=True)

st.markdown("---")

if st.button("🔄 Refresh"):
    st.rerun()


