import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc
import polars as pl

load_dotenv()

class DBManager:
    _engine = None

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            url = os.getenv("DATABASE_URL")
            if not url:
                print("❌ CRITICAL: DATABASE_URL not found.")
                sys.exit(1)
            
            try:
                cls._engine = create_engine(
                    url, 
                    pool_size=10,        # Increased for multi-asset fetching
                    max_overflow=20, 
                    pool_recycle=1800,   # Shorter recycle for high-frequency daily tasks
                    connect_args={'connect_timeout': 10}
                )
                with cls._engine.connect() as conn:
                    pass 
            except exc.OperationalError as e:
                cls._engine = None
                print(f"❌ DATABASE CONNECTION ERROR: {e.orig if hasattr(e, 'orig') else e}")
                sys.exit(1)
        return cls._engine

    @classmethod
    def write_df(cls, df, table_name, schema="dipsignal"):
        """
        UPGRADED: Uses Polars for high-speed writes if a Polars DF is passed.
        Falls back to Pandas for compatibility with your existing scripts.
        """
        engine = cls.get_engine()
        
        # If it's a Polars DataFrame, use the high-speed engine
        if isinstance(df, pl.DataFrame):
            df.write_database(
                table_name=f"{schema}.{table_name}",
                connection=engine,
                if_table_exists="append",
                engine="sqlalchemy" # Use 'adbc' later if you install it for even more speed
            )
        else:
            # Fallback for your current Pandas scripts
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=2000 # Increased chunksize for Postgres 18 efficiency
            )
        print(f"✅ Data pushed to {schema}.{table_name}")

    @classmethod
    def dispose(cls):
        """Cleanly close all connections in the pool."""
        if cls._engine:
            cls._engine.dispose()
            cls._engine = None