"""
Database utility functions for the admin panel
"""
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from src.core.db_manager import DBManager


def get_table_row_count(table_name, schema="dipsignal"):
    """Get the total number of rows in a table"""
    engine = DBManager.get_engine()
    query = text(f"SELECT COUNT(*) as count FROM {schema}.{table_name}")
    
    with engine.connect() as conn:
        result = conn.execute(query)
        return result.fetchone()[0]


def get_latest_timestamp(table_name, column_name, schema="dipsignal"):
    """Get the most recent timestamp from a table"""
    engine = DBManager.get_engine()
    query = text(f"SELECT MAX({column_name}) as latest FROM {schema}.{table_name}")
    
    with engine.connect() as conn:
        result = conn.execute(query)
        latest = result.fetchone()[0]
        return latest


def get_table_schema(table_name, schema="dipsignal"):
    """Get column information for a table"""
    engine = DBManager.get_engine()
    query = text(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema, "table": table_name})
        return pd.DataFrame(result.fetchall(), columns=["Column", "Type", "Nullable"])


def execute_query(sql, params=None):
    """Execute a custom SQL query and return results as DataFrame"""
    engine = DBManager.get_engine()
    
    try:
        with engine.connect() as conn:
            if params:
                result = conn.execute(text(sql), params)
            else:
                result = conn.execute(text(sql))
            
            # Check if query returns results
            if result.returns_rows:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df, None
            else:
                return None, "Query executed successfully (no results returned)"
    except Exception as e:
        return None, str(e)


def get_data_freshness(table_name, timestamp_column, schema="dipsignal"):
    """
    Calculate how old the latest data is in hours
    Returns: (hours_old, latest_timestamp, status)
    status: 'fresh' (<24h), 'stale' (24-72h), 'critical' (>72h)
    """
    latest = get_latest_timestamp(table_name, timestamp_column, schema)
    
    if latest is None:
        return None, None, 'critical'
    
    # Convert to datetime if it's a date object
    if isinstance(latest, datetime):
        # It's already a datetime
        if latest.tzinfo is None:
            # Make it timezone-aware
            latest = latest.replace(tzinfo=datetime.now().astimezone().tzinfo)
    else:
        # It's a date object, convert to datetime at midnight
        from datetime import date
        latest = datetime.combine(latest, datetime.min.time())
        latest = latest.replace(tzinfo=datetime.now().astimezone().tzinfo)
    
    now = datetime.now().astimezone()
    delta = now - latest
    hours_old = delta.total_seconds() / 3600
    
    if hours_old < 24:
        status = 'fresh'
    elif hours_old < 72:
        status = 'stale'
    else:
        status = 'critical'
    
    return hours_old, latest, status


def get_all_tables(schema="dipsignal"):
    """Get list of all tables in the schema"""
    engine = DBManager.get_engine()
    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        ORDER BY table_name
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema})
        return [row[0] for row in result.fetchall()]


def get_asset_count_by_class():
    """Get count of assets by asset class"""
    engine = DBManager.get_engine()
    query = text("""
        SELECT asset_class, COUNT(*) as count
        FROM dipsignal.dim_assets
        GROUP BY asset_class
        ORDER BY asset_class
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        return pd.DataFrame(result.fetchall(), columns=["Asset Class", "Count"])


def get_recent_news_count(days=7):
    """Get count of news articles in the last N days"""
    engine = DBManager.get_engine()
    query = text("""
        SELECT COUNT(*) as count
        FROM dipsignal.fact_news_articles
        WHERE date >= CURRENT_DATE - :days
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"days": days})
        return result.fetchone()[0]


def get_asset_coverage_today():
    """Check which assets have price data for today"""
    engine = DBManager.get_engine()
    query = text("""
        SELECT 
            da.asset_class,
            COUNT(DISTINCT da.asset_id) as total_assets,
            COUNT(DISTINCT CASE 
                WHEN DATE(fap.timestamp) = CURRENT_DATE 
                THEN fap.asset_id 
            END) as assets_with_data_today
        FROM dipsignal.dim_assets da
        LEFT JOIN dipsignal.fact_asset_prices fap ON da.asset_id = fap.asset_id
        GROUP BY da.asset_class
        ORDER BY da.asset_class
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        return pd.DataFrame(result.fetchall(), columns=["Asset Class", "Total Assets", "Assets with Data Today"])


def test_connection():
    """Test database connection"""
    try:
        engine = DBManager.get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)
