"""
Asset management utilities for adding/removing assets and RSS feeds
"""
import json
import os
from pathlib import Path
from sqlalchemy import text
from src.core.db_manager import DBManager


# Config file paths
CONFIG_DIR = Path(__file__).parent.parent / "src" / "config"
CRYPTO_CONFIG = CONFIG_DIR / "crypto_assets.json"
STOCKS_CONFIG = CONFIG_DIR / "top_50.json"
COMMODITIES_CONFIG = CONFIG_DIR / "commodities.json"
FEEDS_CONFIG = CONFIG_DIR / "feeds.json"


def load_json_config(filepath):
    """Load a JSON config file"""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {} if filepath.name == "crypto_assets.json" or filepath.name == "feeds.json" else []
    except json.JSONDecodeError:
        return {} if filepath.name == "crypto_assets.json" or filepath.name == "feeds.json" else []


def save_json_config(filepath, data):
    """Save data to a JSON config file"""
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)


def insert_to_dim_assets(symbol, asset_class, name=None):
    """Insert a new asset into dim_assets table"""
    engine = DBManager.get_engine()
    
    query = text("""
        INSERT INTO dipsignal.dim_assets (symbol, asset_class, name)
        VALUES (:symbol, :asset_class, :name)
        ON CONFLICT (symbol) DO NOTHING
        RETURNING asset_id
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {
            "symbol": symbol,
            "asset_class": asset_class,
            "name": name or symbol
        })
        conn.commit()
        row = result.fetchone()
        return row[0] if row else None


def add_crypto_asset(binance_pair, symbol):
    """
    Add a crypto asset to crypto_assets.json and dim_assets
    binance_pair: e.g., "DOGEUSDT"
    symbol: e.g., "DOGE"
    """
    # Load current config
    config = load_json_config(CRYPTO_CONFIG)
    
    # Check if already exists
    if binance_pair in config:
        return False, f"Crypto pair {binance_pair} already exists"
    
    # Add to config
    config[binance_pair] = symbol
    save_json_config(CRYPTO_CONFIG, config)
    
    # Add to database
    asset_id = insert_to_dim_assets(symbol, "CRYPTO", f"{symbol} (Cryptocurrency)")
    
    if asset_id:
        return True, f"Successfully added {symbol} (pair: {binance_pair})"
    else:
        return True, f"Added to config. Asset {symbol} may already exist in database."


def add_stock_asset(ticker):
    """
    Add a stock to top_50.json and dim_assets
    ticker: e.g., "TSLA"
    """
    # Load current config
    config = load_json_config(STOCKS_CONFIG)
    
    # Check if already exists
    if ticker in config:
        return False, f"Stock {ticker} already exists"
    
    # Add to config
    config.append(ticker)
    save_json_config(STOCKS_CONFIG, config)
    
    # Add to database
    asset_id = insert_to_dim_assets(ticker, "STOCK", f"{ticker} Stock")
    
    if asset_id:
        return True, f"Successfully added {ticker}"
    else:
        return True, f"Added to config. Stock {ticker} may already exist in database."


def add_commodity_asset(symbol):
    """
    Add a commodity to commodities.json and dim_assets
    symbol: e.g., "GC=F" for Gold
    """
    # Load current config
    config = load_json_config(COMMODITIES_CONFIG)
    
    # Check if already exists
    if symbol in config:
        return False, f"Commodity {symbol} already exists"
    
    # Add to config
    config.append(symbol)
    save_json_config(COMMODITIES_CONFIG, config)
    
    # Add to database
    asset_id = insert_to_dim_assets(symbol, "COMMODITY", f"{symbol} Commodity")
    
    if asset_id:
        return True, f"Successfully added {symbol}"
    else:
        return True, f"Added to config. Commodity {symbol} may already exist in database."


def add_rss_feed(source_name, url):
    """
    Add an RSS feed to feeds.json
    source_name: e.g., "Bloomberg"
    url: e.g., "https://www.bloomberg.com/feed"
    """
    # Load current config
    config = load_json_config(FEEDS_CONFIG)
    
    # Check if already exists
    if source_name in config:
        return False, f"RSS feed {source_name} already exists"
    
    # Add to config
    config[source_name] = url
    save_json_config(FEEDS_CONFIG, config)
    
    return True, f"Successfully added RSS feed: {source_name}"


def remove_asset(symbol, asset_class):
    """
    Remove an asset from config file
    Note: Does NOT remove from dim_assets to preserve historical data
    """
    if asset_class == "crypto":
        config = load_json_config(CRYPTO_CONFIG)
        # Find and remove the pair with this symbol
        pairs_to_remove = [pair for pair, sym in config.items() if sym == symbol]
        for pair in pairs_to_remove:
            del config[pair]
        save_json_config(CRYPTO_CONFIG, config)
        return True, f"Removed {symbol} from crypto config"
    
    elif asset_class == "stock":
        config = load_json_config(STOCKS_CONFIG)
        if symbol in config:
            config.remove(symbol)
            save_json_config(STOCKS_CONFIG, config)
            return True, f"Removed {symbol} from stocks config"
        return False, f"Stock {symbol} not found"
    
    elif asset_class == "commodity":
        config = load_json_config(COMMODITIES_CONFIG)
        if symbol in config:
            config.remove(symbol)
            save_json_config(COMMODITIES_CONFIG, config)
            return True, f"Removed {symbol} from commodities config"
        return False, f"Commodity {symbol} not found"
    
    return False, "Invalid asset class"


def remove_rss_feed(source_name):
    """Remove an RSS feed from feeds.json"""
    config = load_json_config(FEEDS_CONFIG)
    
    if source_name in config:
        del config[source_name]
        save_json_config(FEEDS_CONFIG, config)
        return True, f"Removed RSS feed: {source_name}"
    
    return False, f"RSS feed {source_name} not found"


def get_all_assets():
    """Load all assets from config files"""
    return {
        "crypto": load_json_config(CRYPTO_CONFIG),
        "stocks": load_json_config(STOCKS_CONFIG),
        "commodities": load_json_config(COMMODITIES_CONFIG),
        "rss_feeds": load_json_config(FEEDS_CONFIG)
    }


def validate_symbol(symbol, asset_class):
    """Check if a symbol already exists in configs"""
    assets = get_all_assets()
    
    if asset_class == "crypto":
        # Check if symbol already exists as a value in crypto config
        return symbol not in assets["crypto"].values()
    elif asset_class == "stock":
        return symbol not in assets["stocks"]
    elif asset_class == "commodity":
        return symbol not in assets["commodities"]
    
    return True


def get_asset_stats():
    """Get statistics about configured assets"""
    assets = get_all_assets()
    
    return {
        "crypto_count": len(assets["crypto"]),
        "stock_count": len(assets["stocks"]),
        "commodity_count": len(assets["commodities"]),
        "rss_feed_count": len(assets["rss_feeds"]),
        "total_assets": len(assets["crypto"]) + len(assets["stocks"]) + len(assets["commodities"])
    }
