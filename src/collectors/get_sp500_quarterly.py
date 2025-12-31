import pandas as pd
import requests
import io
import json
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger

log = get_logger("SP500_INIT")

def fetch_and_store_sp500():
    log.info("Fetching S&P 500 constituents from Wikipedia...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    table = pd.read_html(io.StringIO(response.text))[0]
    

    records = []
    for _, row in table.iterrows():
        # Fix symbol format (BRK.B -> BRK-B)
        symbol = row['Symbol'].replace('.', '-')
        

        metadata = {
            "gics_sector": row.get('GICS Sector'),
            "gics_sub_industry": row.get('GICS Sub-Industry'),
            "headquarters": row.get('Headquarters Location'),
            "date_added": str(row.get('Date added')),
            "cik": str(row.get('CIK')),
            "founded": str(row.get('Founded'))
        }
        
        records.append({
            "symbol": symbol,
            "asset_class": "EQUITY",
            "name": row['Security'],
            "static_metadata": metadata
        })
        
    log.info(f"Processed {len(records)} equity assets.")
    

    engine = DBManager.get_engine()
    metadata = MetaData()
    try:
        dim_assets = Table('dim_assets', metadata, autoload_with=engine, schema='dipsignal')
    except Exception as e:
        log.error(f"Failed to reflect table dim_assets: {e}")
        return

    stmt = insert(dim_assets).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=['symbol'],
        set_={
            "name": stmt.excluded.name,
            "static_metadata": stmt.excluded.static_metadata,
            "asset_class": stmt.excluded.asset_class 
        }
    )
    
    with engine.begin() as conn:
        result = conn.execute(stmt)
        log.info(f"Inserted/Updated {result.rowcount} rows in dim_assets.")

if __name__ == "__main__":
    fetch_and_store_sp500()