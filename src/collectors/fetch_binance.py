import pandas as pd
import pandas_ta as ta
import requests
import time
import json
from src.core.db_manager import DBManager
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, MetaData, Table
from src.core.logger_manager import get_logger
import os

log = get_logger("BINANCE")

def fetch_ohlcv_binance_full(symbol="BTCUSDT", interval="1d", start_date="2018-02-01", days=4000):
    url = "https://api.binance.com/api/v3/klines"
    all_data = []
    limit = 1000
    start_ts = int(pd.to_datetime(start_date).timestamp() * 1000)
    one_day_ms = 24 * 60 * 60 * 1000
    total_batches = (days // limit) + (1 if days % limit else 0)

    for _ in range(total_batches):
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": start_ts,
            "limit": min(limit, days)
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break

        all_data.extend(batch)
        start_ts = batch[-1][0] + one_day_ms
        days -= limit
        time.sleep(0.5) 

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms").dt.date
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
    return df[["timestamp", "open", "high", "low", "close", "volume"]]

def fetch_ohlcv_binance(symbol: str, interval="1d", limit=250):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        log.info(f"Error: {e}")
        exit(1)

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
    df["symbol"] = symbol
    return df[["timestamp", "symbol", "open", "high", "low", "close", "volume"]]

def fetch_market_cap_coingecko(coingecko_id: str, symbol: str) -> float:
    """Fetch current market cap from CoinGecko API with exponential backoff retry logic"""
    if not coingecko_id:
        log.warning(f"No CoinGecko ID provided for {symbol}")
        return None
    
    url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}"
    params = {
        'localization': 'false',
        'tickers': 'false',
        'market_data': 'true',
        'community_data': 'false',
        'developer_data': 'false',
        'sparkline': 'false'
    }
    
    max_retries = 5
    base_delay = 5  # Start with 5 seconds
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            market_cap = data.get('market_data', {}).get('market_cap', {}).get('usd')
            return market_cap
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit error
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff: 5s, 10s, 20s, 40s, 80s
                    log.warning(f"Rate limited for {symbol}. Retrying in {delay}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    log.error(f"Failed to fetch market cap for {symbol} after {max_retries} attempts: Rate limit exceeded")
                    return None
            else:
                log.warning(f"HTTP error for {symbol}: {e}")
                return None
        except Exception as e:
            log.warning(f"Failed to fetch market cap for {symbol}: {e}")
            return None
    
    return None


def calculate_indicators(df: pd.DataFrame, market_cap: float = None) -> pd.DataFrame:
    df = df.copy()

    # Calculate indicators
    sma_20 = ta.sma(df['close'], length=20).round(2)
    sma_50 = ta.sma(df['close'], length=50).round(2)
    sma_200 = ta.sma(df['close'], length=200).round(2)
    rsi = ta.rsi(df['close'], length=14).round(2)
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    
    # Pack into dynamic_metadata
    temp = pd.DataFrame({
        'rsi': rsi,
        'sma_20': sma_20,
        'sma_50': sma_50,
        'sma_200': sma_200,        
        'macd': macd['MACD_12_26_9'].round(2),
        'pct_change': df['close'].pct_change().round(4),
        'market_cap': market_cap
    })
    
    # Fill NaN with None so JSON handles it as null
    temp = temp.where(pd.notnull(temp), None)

    # Pass dict objects directly; SQLAlchemy/Psycopg2 will adapt them to JSONB
    # IMPORTANT: Convert to standard Python types (float) because default JSON encoder crashes on numpy types
    df['dynamic_metadata'] = temp.apply(lambda row: json.loads(row.to_json()), axis=1)

    return df

def get_asset_id(symbol, engine):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT asset_id FROM dipsignal.dim_assets WHERE symbol = :symbol"),
            {"symbol": symbol}
        ).fetchone()
        
        if result:
            return result[0]
        else:
            log.info(f"Asset {symbol} not found. Creating...")
            result = conn.execute(
                text("""
                    INSERT INTO dipsignal.dim_assets (symbol, asset_class, name) 
                    VALUES (:symbol, 'CRYPTO', :symbol) 
                    RETURNING asset_id
                """),
                {"symbol": symbol}
            ).fetchone()
            conn.commit()
            return result[0]

def main():
    # Load symbol config
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/crypto_assets.json')
    try:
        with open(CONFIG_PATH, 'r') as f:
            content = f.read().strip()
            if not content:
                crypto_config = {}
                log.warning(f"Config file at {CONFIG_PATH} is empty.")
            else:
                crypto_config = json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.error(f"Config file error at {CONFIG_PATH}: {e}")
        crypto_config = {}

    engine = DBManager.get_engine()
    interval = "1d"

    if not crypto_config:
        log.warning("No symbols to process. Check configuration.")
        return

    for binance_pair, config in crypto_config.items():
        symbol = config.get('symbol')
        coingecko_id = config.get('coingecko_id')
        
        log.info(f"Processing {symbol}...")
        

        market_cap = fetch_market_cap_coingecko(coingecko_id, symbol)
        if market_cap:
            log.info(f"{symbol} market cap: ${market_cap:,.0f}")
        
        # Delay to avoid CoinGecko rate limits (free tier: ~10 calls/min)
        time.sleep(6)
        

        df = fetch_ohlcv_binance(symbol=binance_pair, interval=interval, limit=200)
        
        if df.empty:
            log.info(f"No data for {symbol}")
            continue

        df = calculate_indicators(df, market_cap=market_cap)
        

        asset_id = get_asset_id(symbol, engine)
        df['asset_id'] = asset_id
        df['interval'] = interval
        
        df = df.rename(columns={
            'open': 'price_open',
            'high': 'price_high',
            'low': 'price_low',
            'close': 'price_close'
        })
        

        target_cols = ['asset_id', 'timestamp', 'interval', 'price_open', 'price_high', 'price_low', 'price_close', 'volume', 'dynamic_metadata']
        df_final = df[target_cols]

        try:
            # Direct SQLAlchemy approach matches DB types better than pandas.to_sql
            metadata = MetaData()

            table = Table('fact_asset_prices', metadata, autoload_with=engine, schema='dipsignal')
            

            records = df_final.to_dict(orient='records')
            

            stmt = insert(table).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['asset_id', 'timestamp', 'interval']
            )
            
            with engine.begin() as conn:
                result = conn.execute(stmt)
                log.info(f"{symbol} sync complete. {result.rowcount} rows inserted.")
                
        except Exception as e:
            log.info(f"{symbol} sync failed: {e}")

if __name__ == "__main__":
    main()
