import streamlit as st
import pandas as pd
import altair as alt
from sqlalchemy import text
import sys
import google.generativeai as genai
import os
from dotenv import load_dotenv

# Load ENV
load_dotenv()

# Add project root to sys.path to ensure 'src' module is found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.core.db_manager import DBManager
import json

# Page Config
st.set_page_config(
    page_title="DipSignal Super Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Data Fetching ---
@st.cache_resource
def get_db_engine():
    return DBManager.get_engine()

def get_asset_classes():
    engine = get_db_engine()
    query = text("SELECT DISTINCT asset_class FROM dipsignal.dim_assets ORDER BY asset_class")
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(query).fetchall()]

def get_asset_list(asset_class=None):
    engine = get_db_engine()
    # Filter by asset class AND ensure they have at least one price record (EXISTS clause)
    # This matches the user request: "only include assets that are actually in the fact table"
    base_query = """
        SELECT da.symbol, da.name, da.asset_class 
        FROM dipsignal.dim_assets da
        WHERE EXISTS (
            SELECT 1 FROM dipsignal.fact_asset_prices fap 
            WHERE fap.asset_id = da.asset_id
        )
    """
    params = {}
    if asset_class:
        base_query += " AND da.asset_class = :asset_class"
        params['asset_class'] = asset_class
        
    base_query += " ORDER BY da.symbol"
    
    with engine.connect() as conn:
        return pd.read_sql(text(base_query), conn, params=params)

def get_price_history(symbol, days=365):
    engine = get_db_engine()
    query = text("""
        SELECT 
            f.timestamp, 
            f.price_close, 
            f.price_open, 
            f.price_high, 
            f.price_low, 
            f.volume,
            f.dynamic_metadata
        FROM dipsignal.fact_asset_prices f
        JOIN dipsignal.dim_assets d ON f.asset_id = d.asset_id
        WHERE d.symbol = :symbol
        ORDER BY f.timestamp DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol, "limit": days})
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df = df.sort_values('timestamp', ascending=True) # Sort back for chart
        return df


def get_latest_fgi():
    engine = get_db_engine()
    query = text("""
        SELECT fgi_value, date 
        FROM dipsignal.fact_sentiment_index 
        ORDER BY date DESC 
        LIMIT 1
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        return result if result else (None, None)

def get_top_movers():
    engine = get_db_engine()
    query = text("""
        SELECT 
            d.symbol, 
            d.name, 
            d.asset_class,
            (f.dynamic_metadata->>'pct_change')::numeric as pct_change,
            f.price_close
        FROM dipsignal.fact_asset_prices f
        JOIN dipsignal.dim_assets d ON f.asset_id = d.asset_id
        WHERE f.timestamp = (SELECT MAX(timestamp) FROM dipsignal.fact_asset_prices)
        ORDER BY pct_change DESC NULLS LAST
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

def get_latest_news(limit=20):
    engine = get_db_engine()
    query = text("""
        SELECT date, source, title, summary, url
        FROM dipsignal.fact_news_articles
        ORDER BY date DESC, article_id DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"limit": limit})



# Configure Gemini
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)

def generate_ai_analysis(symbol, df, asset_class):
    """
    Uses Gemini Flash to analyze the asset.
    """
    if not api_key:
        return "⚠️ No GEMINI_API_KEY found in .env"

    model = genai.GenerativeModel('gemini-2.5-flash-lite')
    
    # Prepare Context
    latest_data = df.tail(30).to_string() # Last 30 days
    prompt = f"""
    You are a Senior Financial Analyst. Analyze the following data for {symbol} ({asset_class}).
    
    PRICE HISTORY (Last 30 Days):
    {latest_data}
    
    INSTRUCTIONS:
    1. Identify the short-term trend (Bullish/Bearish/Neutral).
    2. Highlight key support/resistance levels derived from the data.
    3. Note any significant volume anomalies.
    4. Provide a 1-paragraph executive summary of the price action.
    
    Format output in Markdown. Be concise specific.
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error generating analysis: {e}"

# --- UI Layout ---

page = st.sidebar.radio("Navigation", ["Market Overview", "Asset Explorer", "News Feed"])

import streamlit.components.v1 as components

# TradingView Ticker Tape Widget
components.html("""
<!-- TradingView Widget BEGIN -->
<div class="tradingview-widget-container">
  <div class="tradingview-widget-container__widget"></div>
  <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js" async>
  {
  "symbols": [
    {
      "proName": "FOREXCOM:SPXUSD",
      "title": "S&P 500"
    },
    {
      "proName": "FOREXCOM:NSXUSD",
      "title": "US 100"
    },
    {
      "proName": "FX_IDC:EURUSD",
      "title": "EUR/USD"
    },
    {
      "proName": "BITSTAMP:BTCUSD",
      "title": "Bitcoin"
    },
    {
      "proName": "BITSTAMP:ETHUSD",
      "title": "Ethereum"
    },
    {
        "proName": "FX_IDC:XAUUSD",
        "title": "Gold"
    },
    {
        "proName": "TVC:USOIL",
        "title": "Crude Oil"
    }
  ],
  "showSymbolLogo": true,
  "colorTheme": "light",
  "isTransparent": false,
  "displayMode": "adaptive",
  "locale": "en"
}
  </script>
</div>
<!-- TradingView Widget END -->
""", height=70)

if page == "Market Overview":
    st.title("🌍 Market Overview")
    
    # 1. Fear and Greed Index
    fgi_val, fgi_date = get_latest_fgi()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if fgi_val is not None:
            st.metric(label="Fear & Greed Index", value=int(fgi_val), delta=None)
            st.caption(f"Last updated: {fgi_date}")
            
            if fgi_val < 25:
                st.error("Extreme Fear 😱")
            elif fgi_val < 45:
                st.warning("Fear 😨")
            elif fgi_val > 75:
                st.error("Extreme Greed 🤑") 
            elif fgi_val > 55:
                st.success("Greed 💰")
            else:
                st.info("Neutral 😐")
        else:
            st.write("No FGI Data Available")

    # 2. Top Movers
    st.subheader("🔥 Top Movers (Last 24h)")
    movers = get_top_movers()
    
    if not movers.empty:
        col_gain, col_loss = st.columns(2)
        
        with col_gain:
            st.write("**Top Gainers**")
            # Strictly positive
            gainers = movers[movers['pct_change'] > 0].head(5)[['symbol', 'pct_change', 'price_close']]
            if not gainers.empty:
                st.dataframe(gainers.style.format({'pct_change': "{:.2%}", 'price_close': "${:.2f}"}), use_container_width=True)
            else:
                st.info("No gainers today.")
            
        with col_loss:
            st.write("**Top Losers**")
            # Strictly negative
            losers = movers[movers['pct_change'] < 0].tail(5).sort_values(by='pct_change', ascending=True)[['symbol', 'pct_change', 'price_close']]
            if not losers.empty:
                st.dataframe(losers.style.format({'pct_change': "{:.2%}", 'price_close': "${:.2f}"}), use_container_width=True)
            else:
                st.info("No losers today.")
    else:
        st.info("No price data available for Top Movers.")

elif page == "Asset Explorer":
    st.title("🔎 Asset Explorer")
    
    # Filter: Asset Class
    all_classes = get_asset_classes()
    selected_class = st.selectbox("Filter by Asset Class", ["All"] + all_classes)
    
    filter_class = None if selected_class == "All" else selected_class
    assets = get_asset_list(filter_class)
    
    if not assets.empty:
        col_sel, col_info = st.columns([1, 2])
        
        with col_sel:
            selected_symbol = st.selectbox("Select Asset", assets['symbol'].unique())
        
        if selected_symbol:
            asset_info = assets[assets['symbol'] == selected_symbol].iloc[0]
            
            with col_info:
                st.subheader(f"{asset_info['name']} ({asset_info['symbol']})")
                st.caption(f"Class: {asset_info['asset_class']}")
            
            # Fetch Data
            df = get_price_history(selected_symbol)
            
            if not df.empty:
                latest = df.iloc[-1]
                prev = df.iloc[-2] if len(df) > 1 else latest
                change = latest['price_close'] - prev['price_close']
                pct_change = (change / prev['price_close']) if prev['price_close'] != 0 else 0
                
                # Metrics Row
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Price", f"${latest['price_close']:.2f}", f"{pct_change:.2%}")
                m2.metric("Volume", f"{int(latest['volume']):,}")
                m3.metric("High", f"${latest['price_high']:.2f}")
                m4.metric("Low", f"${latest['price_low']:.2f}")

                # Chart
                st.subheader("Price Chart 📈")
                
                chart = alt.Chart(df).mark_line().encode(
                    x='timestamp:T',
                    y=alt.Y('price_close:Q', scale=alt.Scale(zero=False), title='Price ($)'),
                    tooltip=['timestamp', 'price_close', 'volume']
                ).properties(
                    width=800, 
                    height=400
                ).interactive()
                
                st.altair_chart(chart, use_container_width=True)
                
                # AI Analysis Section
                st.divider()
                st.subheader("🤖 AI Analyst")
                if st.button(f"Generate Analysis for {selected_symbol}"):
                    with st.spinner("Analyzing market structure..."):
                        analysis = generate_ai_analysis(selected_symbol, df, asset_info['asset_class'])
                        st.markdown(analysis)
                
            else:
                st.warning("No price history found for this asset.")
                
    else:
        st.error("No assets found matching criteria.")

elif page == "News Feed":
    st.title("📰 Latest News")
    
    news_df = get_latest_news()
    
    if not news_df.empty:
        for _, row in news_df.iterrows():
            with st.container():
                st.subheader(row['title'])
                st.caption(f"{row['source']} • {row['date']}")
                st.write(row['summary'])
                st.markdown(f"[Read more]({row['url']})")
                st.divider()
    else:
        st.info("No news articles found in the database.")

# Footer
st.sidebar.markdown("---")
st.sidebar.caption("Powered by DipSignal 1.0")
