import pandas as pd
import pandas_market_calendars as mcal
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table

log = get_logger("SEED_DIM_DATE")

def seed_dim_date(start_year=2015, end_year=2030):
    log.info(f"Generating dim_date from {start_year} to {end_year}...")
    
    # 1. Generate full date range
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # 2. Get NYSE Market Schedule
    nyse = mcal.get_calendar('NYSE')
    # Use schedule() to find valid trading days
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    trading_days = set(schedule.index.date) # Set of valid dates

    # 3. Build DataFrame
    records = []
    for d in dates:
        date_obj = d.date()
        date_id = int(d.strftime('%Y%m%d')) # 20250101
        
        is_weekend = d.weekday() >= 5 # 5=Sat, 6=Sun
        is_us_market_open = (date_obj in trading_days)
        
        records.append({
            "date_id": date_id,
            "full_date": date_obj,
            "year": d.year,
            "quarter": d.quarter,
            "month": d.month,
            "week": d.isocalendar().week,
            "day_of_week": d.weekday(),
            "day_name": d.strftime('%A'),
            "is_weekend": is_weekend,
            "is_us_market_open": is_us_market_open,
            "is_crypto_open": True # Crypto never sleeps
        })
        
    df = pd.DataFrame(records)
    
    # 4. Insert into Database
    engine = DBManager.get_engine()
    metadata = MetaData()
    # Reflect tables
    try:
        dim_date_table = Table('dim_date', metadata, autoload_with=engine, schema='dipsignal')
    except Exception as e:
        log.error(f"Could not reflect table 'dim_date'. Did you run the SQL to create it? Error: {e}")
        return

    # Bulk insert with ON CONFLICT DO UPDATE (to allow re-running script safely)
    # We update 'is_us_market_open' in case calendar logic changes
    stmt = insert(dim_date_table).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=['date_id'],
        set_={
            "is_us_market_open": stmt.excluded.is_us_market_open,
            "is_weekend": stmt.excluded.is_weekend
        }
    )

    with engine.begin() as conn:
        result = conn.execute(stmt)
        log.info(f"Seeded dim_date with {len(records)} days. Rows affected: {result.rowcount}")

if __name__ == "__main__":
    seed_dim_date()
