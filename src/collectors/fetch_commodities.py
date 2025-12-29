import pandas as pd
import yfinance as yf
import json
import time
import random
import os
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table, text
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger

log = get_logger("FETCH_COMMODITIES")

def fetch_commodity_assets(engine):
    """Fetch specific assets from commodities.json, resolving their IDs from dim_assets."""
    
    # 1. Load the Commodities list
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'commodities.json')
    try:
        with open(config_path, 'r') as f:
            target_symbols = json.load(f)
            # Ensure they are uppercase
            target_symbols = [s.upper() for s in target_symbols]
    except FileNotFoundError:
        log.error(f"Config file not found: {config_path}")
        return []
    except json.JSONDecodeError:
        log.error(f"Invalid JSON in: {config_path}")
        return []

    if not target_symbols:
        log.warning("Commodities list is empty.")
        return []

    # 2. Query DB for these specific symbols to get their IDs
    # Filter by asset_class = 'COMMODITY' as well
    query = text("SELECT asset_id, symbol FROM dipsignal.dim_assets WHERE asset_class = 'COMMODITY' AND symbol IN :symbols")
    
    with engine.connect() as conn:
        return conn.execute(query, {"symbols": tuple(target_symbols)}).fetchall()

def calculate_technicals(df):
    """Calculate moving averages and other technicals."""
    if len(df) < 200:
        return df
        
    df['sma_50'] = df['Close'].rolling(window=50).mean()
    df['sma_200'] = df['Close'].rolling(window=200).mean()
    # Simple Daily Return
    df['daily_return'] = df['Close'].pct_change()
    return df

def fetch_history_with_retries(ticker, period="5y"):
    """Fetch history with retry logic."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            hist = ticker.history(period=period)
            if hist is None or hist.empty:
                raise ValueError("Empty or None history returned")
            return hist
        except Exception as e:
            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) + random.random()
                log.warning(f"Attempt {attempt + 1} failed for {ticker.ticker}: {e}. Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
            else:
                log.error(f"Failed to fetch history for {ticker.ticker} after {max_retries} attempts: {e}")
                return None
    return None

def fetch_commodity_data(symbol, period="5y"):
    """Fetch history for a single commodity symbol."""
    ticker = yf.Ticker(symbol)
    
    # 1. Fetch History
    hist = fetch_history_with_retries(ticker, period)
    
    if hist is None or hist.empty:
        return None
        
    # 2. Process Data
    hist = hist.reset_index()
    hist['Date'] = pd.to_datetime(hist['Date']).dt.tz_localize(None) 
    
    if hist['Date'].dt.tz is None:
        hist['Date'] = hist['Date'].dt.tz_localize('UTC')
    else:
        hist['Date'] = hist['Date'].dt.tz_convert('UTC')
        
    hist = calculate_technicals(hist)
    
    return hist

def is_trading_day():
    """Check if today is a trading day (Mon-Fri, not weekend)"""
    from datetime import datetime
    now = datetime.now()
    return now.weekday() < 5

def main():
    # Check if it's a trading day
    if not is_trading_day():
        log.info("Weekend/Holiday detected - skipping commodities (markets closed)")
        return
    
    log.info("Trading day detected - fetching commodity data")
    
    engine = DBManager.get_engine()
    assets = fetch_commodity_assets(engine)
    
    if not assets:
        log.warning("No commodity assets found in dim_assets. Ensure you have seeded them with the correct SQL.")
        return
        
    log.info(f"Found {len(assets)} commodity assets to process.")
    
    metadata_obj = MetaData()
    fact_table = Table('fact_asset_prices', metadata_obj, autoload_with=engine, schema='dipsignal')
    
    for asset_id, symbol in assets:
        log.info(f"Fetching data for {symbol}...")
        
        df = fetch_commodity_data(symbol)
        
        if df is None or df.empty:
            log.warning(f"No data for {symbol}")
            continue
            
        records = []
        for _, row in df.iterrows():
            # Construct Technical Metadata
            # For commodities, we focus on price action and technicals
            meta = {
                "sma_50": row.get('sma_50'),
                "sma_200": row.get('sma_200'),
                "pct_change": row.get('daily_return'),
                # Using lower() keys or standard? keeping consistent with stocks
                "volume": row.get('Volume') 
            }
            # Clean NaNs
            meta = {k: (None if pd.isna(v) else v) for k, v in meta.items()}

            records.append({
                "asset_id": asset_id,
                "timestamp": row['Date'],
                "interval": "1d",
                "price_open": row['Open'],
                "price_high": row['High'],
                "price_low": row['Low'],
                "price_close": row['Close'],
                "volume": row['Volume'],
                "dynamic_metadata": meta 
            })
            
        # Bulk Insert per Asset
        if records:
            stmt = insert(fact_table).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['asset_id', 'timestamp', 'interval']
            )
            
            with engine.begin() as conn:
                result = conn.execute(stmt)
                log.info(f"Inserted {result.rowcount} rows for {symbol}.")
                
        # Be nice to the API
        time.sleep(1.0) 

if __name__ == "__main__":
    main()
