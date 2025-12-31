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

log = get_logger("FETCH_STOCKS")

def fetch_equity_assets(engine):

    

    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'top_50.json')
    try:
        with open(config_path, 'r') as f:
            target_symbols = json.load(f)
            # Ensure they are uppercase as likely stored in DB
            target_symbols = [s.upper() for s in target_symbols]
    except FileNotFoundError:
        log.error(f"Config file not found: {config_path}")
        return []
    except json.JSONDecodeError:
        log.error(f"Invalid JSON in: {config_path}")
        return []

    if not target_symbols:
        log.warning("Top 50 list is empty.")
        return []


    query = text("SELECT asset_id, symbol FROM dipsignal.dim_assets WHERE asset_class = 'EQUITY' AND symbol IN :symbols")
    
    with engine.connect() as conn:
        # Pass the list as a tuple for the bindparams
        return conn.execute(query, {"symbols": tuple(target_symbols)}).fetchall()

def calculate_technicals(df):

    if len(df) < 200:
        return df
        
    df['sma_50'] = df['Close'].rolling(window=50).mean()
    df['sma_200'] = df['Close'].rolling(window=200).mean()
    # Simple Daily Return
    df['daily_return'] = df['Close'].pct_change()
    return df

def fetch_stock_history_and_metadata(symbol, period="5y"):

    ticker = yf.Ticker(symbol)
    

    max_retries = 3
    for attempt in range(max_retries):
        try:
            hist = ticker.history(period=period)
            if hist is None or hist.empty:
                raise ValueError("Empty or None history returned")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) + random.random()
                log.warning(f"Attempt {attempt + 1} failed for {symbol}: {e}. Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
            else:
                log.error(f"Failed to fetch history for {symbol} after {max_retries} attempts: {e}")
                return None, None
        
    if hist.empty:
        return None, None
        

    try:
        info = ticker.info
        fundamentals = {
            "market_cap": info.get("marketCap"),
            "pe_ratio": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "dividend_yield": info.get("dividendYield"),
            "sector": info.get("sector")
        }
    except Exception:
        fundamentals = {}


    # Reset index to get Date column
    hist = hist.reset_index()
    hist['Date'] = pd.to_datetime(hist['Date']).dt.tz_localize(None) 

    if hist['Date'].dt.tz is None:
        hist['Date'] = hist['Date'].dt.tz_localize('UTC')
    else:
        hist['Date'] = hist['Date'].dt.tz_convert('UTC')
        
    hist = calculate_technicals(hist)
    
    return hist, fundamentals

def is_trading_day():

    from datetime import datetime
    now = datetime.now()
    return now.weekday() < 5

def main():

    if not is_trading_day():
        log.info("Weekend/Holiday detected - skipping stocks (markets closed)")
        return
    
    log.info("Trading day detected - fetching stock data")
    
    engine = DBManager.get_engine()
    assets = fetch_equity_assets(engine)
    
    if not assets:
        log.warning("No equity assets found in dim_assets. Run fetch_sp500.py first (and ensure it used 'EQUITY').")
        return
        
    log.info(f"Found {len(assets)} equity assets to process.")
    
    metadata_obj = MetaData()
    fact_table = Table('fact_asset_prices', metadata_obj, autoload_with=engine, schema='dipsignal')
    
    for asset_id, symbol in assets:
        log.info(f"Fetching data for {symbol}...")
        
        # Yahoo Finance often prefers '-' over '.'
        yf_symbol = symbol.replace('.', '-')
        
        df, fundamentals = fetch_stock_history_and_metadata(yf_symbol)
        
        if df is None or df.empty:
            log.warning(f"No data for {symbol}")
            continue
            
        records = []
        for _, row in df.iterrows():

            meta = {
                "sma_50": row.get('sma_50'),
                "sma_200": row.get('sma_200'),
                "pct_change": row.get('daily_return'),
                "dividends": row.get('Dividends'),
                "stock_splits": row.get('Stock Splits'),
                # Snapshot data (same for all rows in this batch, essentially)
                "market_cap": fundamentals.get("market_cap"),
                "pe_ratio": fundamentals.get("pe_ratio")
            }

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
                "dynamic_metadata": meta # SQLAlchemy handles dict serialization for JSONB
            })
            

        if records:
            stmt = insert(fact_table).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['asset_id', 'timestamp', 'interval']
            )
            
            with engine.begin() as conn:
                result = conn.execute(stmt)
                log.info(f"Inserted {result.rowcount} rows for {symbol}.")
                

        time.sleep(2.0) 

if __name__ == "__main__":
    main()
