"""
Asset Management - Add/remove assets and RSS feeds
"""
import streamlit as st
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from admin_utils.asset_utils import (
    add_crypto_asset,
    add_stock_asset,
    add_commodity_asset,
    add_rss_feed,
    remove_asset,
    remove_rss_feed,
    get_all_assets,
    get_asset_stats
)
from admin_utils.components import section_header
from admin_utils.styles import apply_custom_styles
import pandas as pd

st.set_page_config(page_title="Asset Management", layout="wide")

# Authentication
from admin_utils.auth import require_authentication, show_logout_button
require_authentication()
show_logout_button()
apply_custom_styles()

st.title("Asset Management")
st.markdown("Add or remove assets and RSS feeds from the pipeline")

# Asset Statistics
section_header("Current Assets", "")

try:
    stats = get_asset_stats()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Crypto Assets", stats['crypto_count'])
    
    with col2:
        st.metric("Stocks", stats['stock_count'])
    
    with col3:
        st.metric("Commodities", stats['commodity_count'])
    
    with col4:
        st.metric("RSS Feeds", stats['rss_feed_count'])
    
except Exception as e:
    st.error(f"Error loading asset statistics: {e}")

st.markdown("---")

# View Current Assets
section_header("View Current Assets", "")

try:
    assets = get_all_assets()
    
    tab1, tab2, tab3, tab4 = st.tabs(["Crypto", "Stocks", "Commodities", "RSS Feeds"])
    
    with tab1:
        st.markdown("### Crypto Assets (Binance)")
        if assets['crypto']:
            crypto_df = pd.DataFrame([
                {"Binance Pair": pair, "Symbol": symbol}
                for pair, symbol in assets['crypto'].items()
            ])
            st.dataframe(crypto_df, use_container_width=True, hide_index=True)
        else:
            st.info("No crypto assets configured")
    
    with tab2:
        st.markdown("### Stock Assets (yfinance)")
        if assets['stocks']:
            stock_df = pd.DataFrame({"Ticker": assets['stocks']})
            st.dataframe(stock_df, use_container_width=True, hide_index=True)
        else:
            st.info("No stock assets configured")
    
    with tab3:
        st.markdown("### Commodity Assets (yfinance)")
        if assets['commodities']:
            commodity_df = pd.DataFrame({"Symbol": assets['commodities']})
            st.dataframe(commodity_df, use_container_width=True, hide_index=True)
        else:
            st.info("No commodity assets configured")
    
    with tab4:
        st.markdown("### RSS News Feeds")
        if assets['rss_feeds']:
            rss_df = pd.DataFrame([
                {"Source": source, "URL": url}
                for source, url in assets['rss_feeds'].items()
            ])
            st.dataframe(rss_df, use_container_width=True, hide_index=True)
        else:
            st.info("No RSS feeds configured")
            
except Exception as e:
    st.error(f"Error loading assets: {e}")

st.markdown("---")

# Add New Assets
section_header("Add New Asset", "")

asset_type = st.selectbox(
    "Select asset type to add:",
    ["Crypto (Binance)", "Stock (yfinance)", "Commodity (yfinance)", "RSS Feed"]
)

if asset_type == "Crypto (Binance)":
    st.markdown("### Add Crypto Asset")
    st.caption("Example: Binance Pair = DOGEUSDT, Symbol = DOGE")
    
    col1, col2 = st.columns(2)
    
    with col1:
        binance_pair = st.text_input("Binance Trading Pair", placeholder="DOGEUSDT")
    
    with col2:
        symbol = st.text_input("Symbol", placeholder="DOGE")
    
    if st.button("Add Crypto Asset", type="primary"):
        if binance_pair and symbol:
            success, message = add_crypto_asset(binance_pair.upper(), symbol.upper())
            if success:
                st.success(f"✅ {message}")
                st.balloons()
            else:
                st.error(f"❌ {message}")
        else:
            st.warning("⚠️ Please fill in both fields")

elif asset_type == "Stock (yfinance)":
    st.markdown("### Add Stock Asset")
    st.caption("Example: TSLA, NVDA, AAPL")
    
    ticker = st.text_input("Stock Ticker", placeholder="TSLA")
    
    if st.button("Add Stock", type="primary"):
        if ticker:
            success, message = add_stock_asset(ticker.upper())
            if success:
                st.success(f"✅ {message}")
                st.balloons()
            else:
                st.error(f"❌ {message}")
        else:
            st.warning("⚠️ Please enter a ticker symbol")

elif asset_type == "Commodity (yfinance)":
    st.markdown("### Add Commodity Asset")
    st.caption("Example: GC=F (Gold), SI=F (Silver), CL=F (Crude Oil)")
    
    commodity_symbol = st.text_input("Commodity Symbol", placeholder="GC=F")
    
    if st.button("Add Commodity", type="primary"):
        if commodity_symbol:
            success, message = add_commodity_asset(commodity_symbol.upper())
            if success:
                st.success(f"✅ {message}")
                st.balloons()
            else:
                st.error(f"❌ {message}")
        else:
            st.warning("⚠️ Please enter a commodity symbol")

elif asset_type == "RSS Feed":
    st.markdown("### Add RSS Feed")
    st.caption("Example: Source = Bloomberg, URL = https://www.bloomberg.com/feed")
    
    col1, col2 = st.columns(2)
    
    with col1:
        source_name = st.text_input("Source Name", placeholder="Bloomberg")
    
    with col2:
        feed_url = st.text_input("RSS Feed URL", placeholder="https://www.bloomberg.com/feed")
    
    if st.button("Add RSS Feed", type="primary"):
        if source_name and feed_url:
            success, message = add_rss_feed(source_name, feed_url)
            if success:
                st.success(f"✅ {message}")
                st.balloons()
            else:
                st.error(f"❌ {message}")
        else:
            st.warning("⚠️ Please fill in both fields")

st.markdown("---")

# Remove Assets
section_header("Remove Asset", "")

st.warning("⚠️ **Warning**: Removing an asset will delete it from config files but NOT from the database (to preserve historical data).")

remove_type = st.selectbox(
    "Select asset type to remove:",
    ["Crypto", "Stock", "Commodity", "RSS Feed"],
    key="remove_type"
)

try:
    assets = get_all_assets()
    
    if remove_type == "Crypto":
        if assets['crypto']:
            crypto_symbols = list(assets['crypto'].values())
            selected_crypto = st.selectbox("Select crypto to remove:", crypto_symbols)
            
            if st.button("Remove Crypto", type="secondary"):
                success, message = remove_asset(selected_crypto, "crypto")
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.error(f"❌ {message}")
        else:
            st.info("No crypto assets to remove")
    
    elif remove_type == "Stock":
        if assets['stocks']:
            selected_stock = st.selectbox("Select stock to remove:", assets['stocks'])
            
            if st.button("Remove Stock", type="secondary"):
                success, message = remove_asset(selected_stock, "stock")
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.error(f"❌ {message}")
        else:
            st.info("No stocks to remove")
    
    elif remove_type == "Commodity":
        if assets['commodities']:
            selected_commodity = st.selectbox("Select commodity to remove:", assets['commodities'])
            
            if st.button("Remove Commodity", type="secondary"):
                success, message = remove_asset(selected_commodity, "commodity")
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.error(f"❌ {message}")
        else:
            st.info("No commodities to remove")
    
    elif remove_type == "RSS Feed":
        if assets['rss_feeds']:
            feed_sources = list(assets['rss_feeds'].keys())
            selected_feed = st.selectbox("Select RSS feed to remove:", feed_sources)
            
            if st.button("Remove RSS Feed", type="secondary"):
                success, message = remove_rss_feed(selected_feed)
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.error(f"❌ {message}")
        else:
            st.info("No RSS feeds to remove")
            
except Exception as e:
    st.error(f"Error loading assets for removal: {e}")

st.markdown("---")

# Refresh button
if st.button("Refresh Asset List"):
    st.rerun()

