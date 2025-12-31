import pandas as pd
from fredapi import Fred
import yfinance as yf
import os
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table
import json

log = get_logger("MACRO")

def fetch_macro_data(start_date="2018-02-1", end_date=None):
    FRED_API_KEY = os.getenv("FRED_API_KEY")
    fred = Fred(api_key=FRED_API_KEY)



    CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/macro_indicators.json')
    try:
        with open(CONFIG_PATH, 'r') as f:
            content = f.read().strip()
            if not content:
                fred_series = {}
                log.warning(f"Config file at {CONFIG_PATH} is empty.")
            else:
                fred_series = json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.error(f"Config file error at {CONFIG_PATH}: {e}")
        fred_series = {}

    if not fred_series:
        log.warning("No FRED series configured. Skipping FRED fetch.")

    df_list = []
    for name, series_id in fred_series.items():
        s = fred.get_series(series_id, start_date, end_date)
        s.name = name
        df_list.append(s)

    fred_df = pd.concat(df_list, axis=1)


    try:
        log.info("Fetching DXY...")
        dxy_data = yf.download("DX-Y.NYB", start=start_date, end=end_date, interval="1d", auto_adjust=False, progress=False)
        log.info(f"DXY data shape: {dxy_data.shape}")
        # Handle MultiIndex columns
        if isinstance(dxy_data.columns, pd.MultiIndex):
            dxy = dxy_data['Close'].iloc[:, 0] if 'Close' in dxy_data.columns.get_level_values(0) else pd.Series(dtype=float)
        else:
            dxy = dxy_data["Close"] if "Close" in dxy_data.columns else pd.Series(dtype=float)
        dxy.name = "dxy"
    except Exception as e:
        log.error(f"Failed to fetch DXY: {e}")
        dxy = pd.Series(dtype=float, name="dxy")
    
    try:
        log.info("Fetching VIX...")
        vix_data = yf.download("^VIX", start=start_date, end=end_date, interval="1d", auto_adjust=False, progress=False)
        log.info(f"VIX data shape: {vix_data.shape}")
        # Handle MultiIndex columns
        if isinstance(vix_data.columns, pd.MultiIndex):
            vix = vix_data['Close'].iloc[:, 0] if 'Close' in vix_data.columns.get_level_values(0) else pd.Series(dtype=float)
        else:
            vix = vix_data["Close"] if "Close" in vix_data.columns else pd.Series(dtype=float)
        vix.name = "vix"
    except Exception as e:
        log.error(f"Failed to fetch VIX: {e}")
        vix = pd.Series(dtype=float, name="vix")
    
    try:
        log.info("Fetching 10Y Treasury...")
        treasury_data = yf.download("^TNX", start=start_date, end=end_date, interval="1d", auto_adjust=False, progress=False)
        log.info(f"Treasury data shape: {treasury_data.shape}")
        # Handle MultiIndex columns
        if isinstance(treasury_data.columns, pd.MultiIndex):
            treasury_10y = treasury_data['Close'].iloc[:, 0] if 'Close' in treasury_data.columns.get_level_values(0) else pd.Series(dtype=float)
        else:
            treasury_10y = treasury_data["Close"] if "Close" in treasury_data.columns else pd.Series(dtype=float)
        treasury_10y.name = "treasury_10y"
    except Exception as e:
        log.error(f"Failed to fetch 10Y Treasury: {e}")
        treasury_10y = pd.Series(dtype=float, name="treasury_10y")


    macro_df = pd.concat([fred_df, dxy, vix, treasury_10y], axis=1)
    full_index = pd.date_range(start=macro_df.index.min(), end=macro_df.index.max(), freq="D")
    macro_df = macro_df.reindex(full_index)
    macro_df.index.name = "date"


    check_cols = [col for col in ["cpi", "interest_rate", "sp500", "dxy", "vix", "treasury_10y"] if col in macro_df.columns]
    macro_df["market_closed"] = macro_df[check_cols].isna().all(axis=1)


    macro_df = macro_df.ffill().round(2).reset_index()


    available_cols = ["date"]
    for col in ["dxy", "sp500", "cpi", "interest_rate", "vix", "treasury_10y", "unemployment_rate", "gdp"]:
        if col in macro_df.columns:
            available_cols.append(col)
    available_cols.append("market_closed")
    
    macro_df = macro_df[available_cols]
    return macro_df

def main():
    df_new = fetch_macro_data()
    engine = DBManager.get_engine()
    
    try:

        metadata = MetaData()
        table = Table('fact_macro_indicators', metadata, autoload_with=engine, schema='dipsignal')
        


        records = df_new.to_dict(orient='records')
        
        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['date'])
        
        with engine.begin() as conn:
            result = conn.execute(stmt)
            log.info(f"Macro fetch complete. {result.rowcount} new macro indicators inserted safely.")
            
    except Exception as e:
        log.info(f"Macro fetch failed: {e}")

if __name__ == "__main__":
    main()