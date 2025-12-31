import requests
import pandas as pd
from src.core.db_manager import DBManager
from sqlalchemy import MetaData, Table, Column, Integer, String, Date
from sqlalchemy.dialects.postgresql import insert
from src.core.logger_manager import get_logger

log = get_logger("CRYPTO-FGI")

def fetch_fgi(url='https://api.alternative.me/fng/?limit=100', timeout=10):
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    data = response.json()
    data_list = data.get("data", [])
    if not data_list:
        return None

    rows = []
    for item in data_list:
        ts = pd.to_datetime(pd.to_numeric(item['timestamp'], errors='coerce'), unit='s', utc=True)
        value = int(item['value'])
        classification = item['value_classification']
        rows.append({
            "date": ts.date(), 
            "fgi_value": value, 
            "classification": classification
        })

    df = pd.DataFrame(rows).sort_values(by="date", ascending=True)
    
    return df

def main():
    df = fetch_fgi()
    if df is None or df.empty:
        log.info("No FGI data fetched.")
        return

    engine = DBManager.get_engine()
    
    try:

        metadata = MetaData()
        table = Table('fact_sentiment_index', metadata,
            autoload_with=engine,
            schema='dipsignal'
        )
        

        records = df.to_dict(orient='records')
        

        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['date'])
        
        with engine.begin() as conn:
            result = conn.execute(stmt)
            log.info(f"FGI fetch complete. {result.rowcount} new FGI records inserted safely.")
            
    except Exception as e:
        log.info(f"FGI fetch failed: {e}")

if __name__ == "__main__":
    main()