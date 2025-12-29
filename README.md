# DipSignal

**Multi-Asset Financial Data Pipeline with AI-Powered Analytics**

DipSignal is a fully automated data engineering pipeline that collects, transforms, and analyzes market data across 61 assets (crypto, stocks, commodities). It generates daily AI-powered technical analysis and weekly portfolio recommendations for three risk profiles using Google Gemini AI.

---

## System Architecture

The pipeline follows a **Source → Transform → Load → Analyze** architecture:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: Data Collection (Parallel Execution)                          │
├─────────────────────────────────────────────────────────────────────────┤
│ Binance API          → 6 crypto assets (BTC, ETH, SOL, XRP, ADA, BNB)  │
│ yFinance API         → 49 stocks + 6 commodities (Gold, Oil, etc.)     │
│ FRED API             → 5 macro indicators (VIX, 10Y Treasury, etc.)    │
│ CoinGecko API        → Real-time market cap data                       │
│ RSS Feeds            → Crypto news (CoinDesk, Cointelegraph, Decrypt)  │
│ Yahoo Finance        → Stock news (5 articles per stock)               │
│ CNN Fear & Greed API → Market sentiment index                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: AI Analysis (Sequential Execution with Rate Limiting)         │
├─────────────────────────────────────────────────────────────────────────┤
│ Gemini AI (gemma-3-27b-it)                                              │
│   → Macro market summary (365-day analysis)                            │
│   → Daily news summaries (batch processing, 5 articles at a time)      │
│   → Asset analysis (61 assets × ~60 min due to 1/min rate limit)       │
│                                                                         │
│ Gemini AI (gemini-2.5-flash)                                            │
│   → Weekly portfolio recommendations (3 risk profiles)                  │
│   → Correlation analysis and diversification scoring                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ STORAGE: PostgreSQL Database (dipsignal schema)                        │
│   → Star Schema with 2 dimensions, 7 fact tables                       │
│   → Technical indicators stored in JSONB (RSI, MACD, SMA_20/50/200)    │
│   → 6 pre-built analytical views for dashboard queries                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ UI: Streamlit Admin Panel (7 pages)                                    │
│   → System Health Dashboard                                            │
│   → Dagster Pipeline Monitor (Dynamic Graph Visualization)             │
│   → Data Quality Metrics                                               │
│   → Database Explorer                                                  │
│   → Manual Asset Triggers                                              │
│   → Asset Management (CRUD operations)                                 │
│   → Configuration Management                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

**Data Flow Logic:**
1. **Collection Phase:** Assets are fetched in dependency order to avoid API rate limits (macro → stocks → commodities). Each yFinance call has a 3-second delay (`time.sleep(3)`) between asset fetches.
2. **Transformation:** OHLCV data is enriched with technical indicators (RSI, MACD, SMA) using `pandas_ta`, then packed into PostgreSQL JSONB columns for flexible querying.
3. **Storage:** SQLAlchemy handles writes with `ON CONFLICT DO NOTHING` upsert logic to prevent duplicate entries based on composite unique keys (`asset_id`, `timestamp`, `interval`).
4. **Analysis:** AI models consume database views that pre-aggregate 7-day price trends, 30-day volatility, and correlation matrices for efficient token usage.

---

## Admin Panel & Monitoring

The Streamlit-based admin panel provides comprehensive observability into pipeline health, data quality, and system status. Built as a multi-page app with session-based authentication, it allows operators to monitor automated runs and manually trigger asset materializations when needed.

### Authentication & Security

**Session Management:**
- Password-hashed authentication using `streamlit.session_state`
- Credentials stored in `.env` (`ADMIN_USERNAME`, `ADMIN_PASSWORD`)
- Auto-logout on session expiry or manual logout
- Protected routes with `@require_authentication` decorator

**Implementation:** `admin_utils/auth.py` uses bcrypt-compatible hashing with session persistence.

### Page 1: System Health Dashboard

**Purpose:** Real-time system status and data freshness monitoring

![Admin Panel Home](./screenshots/admin_panel.png)

**Features:**
- **Service Health Checks:**
  - PostgreSQL connection status (live ping test)
  - Dagster daemon status (socket check on port 3000)
  - Total asset count from `dim_assets`
  - 7-day news article count

- **Data Freshness Table:**
  - Shows last update timestamp for each fact table
  - Color-coded status: 🟢 Fresh (\<24h), 🟡 Stale (24-72h), 🔴 Critical (\>72h)
  - Checks: `fact_asset_prices`, `fact_macro_indicators`, `fact_sentiment_index`, `fact_news_articles`, `fact_ai_analysis`

- **Interactive Charts:**
  - Asset distribution pie chart (by asset class)
  - Daily coverage bar chart (assets with data today vs. total assets)

**Technical Implementation:** Uses Plotly for interactive visualizations, queries database views for aggregations.

### Page 2: Pipeline Monitor

**Purpose:** Visualize Dagster asset dependency graph and start/stop the daemon

![Pipeline Monitor](./screenshots/pipeline_monitor.png)

**Key Features:**

1. **Dynamic Dependency Graph Parsing:**
   - **Auto-generates graph from `dagster_pipeline/assets.py`** using Python AST parsing
   - Extracts `@asset` decorators, `deps=[]` lists, and group names
   - No hardcoded asset lists — graph updates automatically when you add/remove assets
   - Implementation: `parse_dagster_assets()` function in `pages/2_Pipeline_Monitor.py:L108-183`

2. **Interactive Network Visualization:**
   - Hierarchical layout with phases arranged vertically
   - Color-coded nodes by group (data_collection, ai_analysis)
   - Hover tooltips show dependencies and dependents
   - Arrow annotations indicate data flow direction

3. **Dagster Daemon Control:**
   - One-click **Start** button launches `dagster dev` in background subprocess
   - Live status indicator (port 3000 socket check)
   - Direct link to Dagster UI at `http://localhost:3000`

**Why This Matters:** The dynamic graph parsing means the admin panel never needs manual updates when assets change. Add a new collector in `assets.py`, refresh the page, and the graph automatically reflects the new dependency chain.

### Page 3: Data Quality

**Purpose:** Monitor data completeness and detect anomalies

![Data Quality Dashboard](./screenshots/data_quality.png)

**Metrics Tracked:**
- **Freshness Summary:** Count of fresh/stale/critical tables
- **Asset Coverage:** Percentage of assets with data for the current day (by class)
- **Null Value Detection:** Scans last 7 days of price data for `NULL` values in OHLCV columns
- **Anomaly Alerts:** Warning banners if nulls detected (indicates API failures)

**SQL Logic:** Executes custom queries like:
```sql
SELECT COUNT(*) as total,
       SUM(CASE WHEN price_open IS NULL THEN 1 ELSE 0 END) as null_open
FROM dipsignal.fact_asset_prices
WHERE DATE(timestamp) >= CURRENT_DATE - INTERVAL '7 days'
```

### Page 4: Database Explorer

**Purpose:** Execute ad-hoc SQL queries against the warehouse

![Database Explorer](./screenshots/database_explorer.png)

**Features:**
- Custom SQL editor with syntax highlighting
- Query execution with error handling
- Results displayed as interactive DataFrames
- Row count and execution time metrics
- Pre-loaded example queries (latest prices, AI analysis, macro trends)

**Safety:** Read-only by design — uses `SELECT` statement validation (though schema-altering commands are blocked by user permissions in production).

### Page 5: Manual Triggers

**Purpose:** Manually execute individual collectors or AI generators

![Manual Triggers](./screenshots/manual_triggers.png)

**Use Cases:**
- Backfill missing data for specific assets
- Re-run AI analysis after model updates
- Test new collectors in isolation

**Available Triggers:**
- Fetch Binance (crypto data)
- Fetch Stocks (49 assets)
- Fetch Macro Indicators
- Generate Macro Summary
- Generate Asset Analysis
- Generate Portfolio Recommendations

**Implementation:** Each button spawns a subprocess running the collector's `main()` function, displays real-time logs in Streamlit expandable sections.

### Page 6: Asset Management

**Purpose:** CRUD operations on `dim_assets`

![Asset Management](./screenshots/asset_management.png)

**Capabilities:**
- **View All Assets:** Paginated table with filters by asset class
- **Add New Asset:** Form to insert symbol, name, asset_class, static_metadata (JSONB)
- **Edit Metadata:** Update JSONB fields without altering core attributes
- **Delete Asset:** Soft delete with confirmation (cascades to dependent fact tables)

**Validation:** Ensures unique symbol constraint before insert, validates JSONB syntax.

### Page 7: Configuration Management

**Purpose:** View and edit JSON configuration files

![Configuration Page](./screenshots/configuration.png)

**Editable Configs:**
- `crypto_assets.json` — Binance trading pairs + CoinGecko IDs
- `top_50.json` — Stock tickers for yFinance
- `commodities.json` — Commodity symbols
- `macro_indicators.json` — FRED series IDs
- `feeds.json` — RSS feed URLs for news

**Features:**
- In-browser JSON editor with validation
- Hot reload support (changes apply on next pipeline run)
- Schema hints for required fields (symbol, asset_class, etc.)

**Technical Detail:** Reads/writes files directly from `src/config/`, validates JSON syntax before saving to prevent broken configurations.

---

## Data Model

**Schema Type:** Dimensional Star Schema

**Rationale:** Optimized for analytical queries (BI dashboards, AI context retrieval). Fact tables store time-series events, while dimension tables provide descriptive attributes. This design enables fast JOIN operations and simplifies query logic for dashboards.

### Dimension Tables

#### `dim_assets`
- **Primary Key:** `asset_id` (SERIAL)
- **Unique Constraint:** `symbol`
- **Purpose:** Master list of all tradeable assets
- **Columns:**
  - `symbol` (VARCHAR) — Ticker symbol (e.g., BTC, AAPL)
  - `asset_class` (VARCHAR) — CRYPTO | STOCKS | COMMODITIES
  - `name` (TEXT) — Full name
  - `static_metadata` (JSONB) — Immutable attributes (e.g., sector, inception date)

#### `dim_date`
- **Primary Key:** `date_id` (INT), `full_date` (DATE, UNIQUE)
- **Purpose:** Date dimension for time-based filtering and aggregations
- **Columns:**
  - `year`, `quarter`, `month`, `week`, `day_of_week`, `day_name`
  - `is_weekend` (BOOLEAN)
  - `is_us_market_open` (BOOLEAN) — Uses `pandas_market_calendars` for NYSE holidays
  - `is_crypto_open` (BOOLEAN) — Always TRUE (24/7 markets)

### Fact Tables

#### `fact_asset_prices` (49M+ rows expected annually)
- **Primary Key:** `fact_id` (BIGSERIAL)
- **Unique Constraint:** `(asset_id, timestamp, interval)`
- **Foreign Keys:** 
  - `asset_id` → `dim_assets.asset_id`
- **Grain:** One row per asset per time interval (currently 1d only)
- **Columns:**
  - `timestamp` (TIMESTAMPTZ) — UTC timestamp
  - `interval` (VARCHAR) — Time granularity (1d, 1h, etc.)
  - `price_open`, `price_high`, `price_low`, `price_close` (NUMERIC)
  - `volume` (NUMERIC)
  - `dynamic_metadata` (JSONB) — Technical indicators: `{rsi, sma_20, sma_50, sma_200, macd, pct_change, market_cap}`

**Why JSONB for indicators?** Technical indicators are computed post-hoc and vary by asset class. JSONB avoids NULL-heavy columnar designs and supports flexible GIN indexing for fast key-value lookups.

#### `fact_macro_indicators`
- **Primary Key:** `date` (DATE)
- **Foreign Keys:** `date` → `dim_date.full_date`
- **Purpose:** Daily snapshots of macroeconomic indicators
- **Columns:** `dxy`, `sp500`, `cpi`, `interest_rate`, `vix`, `treasury_10y`, `unemployment_rate`, `gdp`

#### `fact_sentiment_index`
- **Primary Key:** `date` (DATE)
- **Foreign Keys:** `date` → `dim_date.full_date`
- **Purpose:** Daily Fear & Greed Index scores
- **Columns:** `fgi_value` (BIGINT), `classification` (TEXT: "Extreme Fear", "Greed", etc.)

#### `fact_news_articles`
- **Primary Key:** `article_id` (BIGINT, IDENTITY)
- **Unique Constraint:** `url`
- **Foreign Keys:** `date` → `dim_date.full_date`
- **Columns:**
  - `source`, `title`, `summary` (TEXT)
  - `related_tickers` (JSONB array: `["AAPL", "CRYPTO"]`)

**Design Note:** `related_tickers` uses JSONB array for M:N relationships without a bridge table, traded for simpler queries at the cost of normalization.

#### `fact_ai_analysis`
- **Primary Key:** `analysis_id` (SERIAL)
- **Unique Constraint:** `(asset_id, date)`
- **Foreign Keys:**
  - `asset_id` → `dim_assets.asset_id`
  - `date` → `dim_date.full_date`
- **Purpose:** Daily AI-generated technical analysis per asset
- **Columns:**
  - `model_name` (VARCHAR) — AI model identifier (gemma-3-27b-it)
  - `trend_signal` (VARCHAR) — Bullish | Bearish | Neutral
  - `key_levels` (JSONB) — Support/resistance levels: `{support: [41000, 39000], resistance: [45000, 47000]}`
  - `summary_text` (TEXT) — AI-generated narrative

#### `fact_macro_summary`
- **Primary Key:** `summary_id` (SERIAL)
- **Unique Constraint:** `date`
- **Foreign Keys:** `date` → `dim_date.full_date`
- **Purpose:** Daily macro market summaries (short + long versions)
- **Columns:**
  - `period_start`, `period_end` (DATE) — Analysis window (usually 365 days)
  - `summary_text` (TEXT) — Long-form analysis for AI consumption
  - `summary_short` (TEXT) — 100-150 word dashboard summary

#### `fact_portfolio_recommendations`
- **Primary Key:** `recommendation_id` (SERIAL)
- **Unique Constraint:** `(date, risk_profile)`
- **Foreign Keys:** `date` → `dim_date.full_date`
- **Purpose:** Weekly AI-generated portfolio allocations
- **Columns:**
  - `risk_profile` (VARCHAR) — Conservative | Moderate | Aggressive
  - `allocation_stocks`, `allocation_crypto`, `allocation_commodities` (INT) — Percentage weights
  - `top_picks` (JSONB) — `{stocks: [...], crypto: [...], commodities: [...]}`
  - `sector_exposure`, `risks` (JSONB)
  - `diversification_score` (NUMERIC) — Correlation-adjusted metric
  - `news_impact` (TEXT) — Recent news influence summary
  - `token_count` (INT) — AI API usage tracking

![Schema Diagram](./screenshots/schema_diagram.png)
*Caption: Foreign key relationships showing Star Schema design. Fact tables connect to `dim_assets` and `dim_date`, enabling time-series and cross-sectional analysis. The `fact_ai_analysis` table bridges raw price data with AI-generated insights.*

---

## Orchestration & Logic

**Orchestrator:** [Dagster](https://dagster.io/) (configured via `pyproject.toml`)

Dagster manages the pipeline as a Directed Acyclic Graph (DAG) of 11 **assets** (declarative data artifacts). Assets automatically track dependencies, allowing parallel execution where possible while respecting API rate limits through explicit `deps=[]` declarations.

### Asset Dependency Graph

```python
# Phase 1: Parallel data collection
binance_data         ────┐
fgi_data             ────┤
crypto_news          ────┤
                         ├──→ macro_summary
macro_data           ────┤         ↓
  ↓ (3s delay)           │   daily_news_summaries
stock_data           ────┤         ↓
  ↓ (3s delay)           │   asset_analysis
commodity_data       ────┤
  ↓
stock_news           ────┘

# Phase 2: Independent weekly job
weekly_portfolio_recommendation  (reads from DB views)
```

**Key Design Decisions:**

1. **Sequential yFinance Calls:** `stock_data` depends on `macro_data`, and `commodity_data` depends on `stock_data`. This serialization + `time.sleep(3)` prevents HTTP 429 errors from yFinance's rate limiter.

2. **Parallel-Safe Assets:** Binance, FGI, and crypto news run concurrently because they hit different API endpoints.

3. **AI Rate Limiting:** `asset_analysis` includes a 60-second delay per asset (controlled by `time.sleep(60)` in `generate_asset_analysis.py:main()`), stretching the job to ~60 minutes for 61 assets. This prevents Gemini API quota exhaustion.

4. **Weekly Independence:** `weekly_portfolio_recommendation` has no dependencies (`deps=[]`) and reads directly from pre-computed database views (`vw_latest_asset_analysis`, `vw_balanced_news_7d`), allowing it to run on a separate schedule without re-triggering data collection.

### Schedules

Defined in `dagster_pipeline/schedules.py`:

| Job Name | Assets | Cron Schedule | Description |
|----------|--------|---------------|-------------|
| `daily_pipeline` | 10 assets (all except portfolio) | `15 2 * * *` | Runs at 2:15 AM daily (UTC+2) |
| `weekly_portfolio_job` | 1 asset (`weekly_portfolio_recommendation`) | `15 4 * * 1` | Runs Mondays at 4:15 AM (2 hours after daily completion) |

**Execution Model:** Dagster tracks asset materialization state. If `asset_analysis` fails mid-run (e.g., API timeout), re-running the job only re-processes failed assets, not the entire collection phase.

![Dagster Dependency Graph](./screenshots/dagster_diagram.png)
*Caption: Dagster asset lineage graph showing Phase 1 parallel collection converging into Phase 2 sequential AI analysis. Arrows represent data dependencies; colors indicate asset groups (data_collection, ai_analysis).*

---

## Technical Highlights

### 1. **Dual Database Driver Optimization (Polars + SQLAlchemy)**

**Problem:** Pandas `to_sql()` is inefficient for bulk inserts (single-row transactions). For 61 assets × 200 rows/day = 12,200 rows, latency was 45+ seconds.

**Solution:** `src/core/db_manager.py:DBManager.write_df()` implements a **polymorphic write path**:

```python
def write_df(cls, df, table_name, schema="dipsignal"):
    engine = cls.get_engine()
    
    if isinstance(df, pl.DataFrame):
        # High-speed path for Polars DataFrames
        df.write_database(
            table_name=f"{schema}.{table_name}",
            connection=engine,
            if_table_exists="append",
            engine="sqlalchemy"
        )
    else:
        # Fallback for Pandas (legacy scripts)
        df.to_sql(
            name=table_name, con=engine, schema=schema,
            if_exists='append', index=False,
            method='multi', chunksize=2000
        )
```

**Impact:**
- Polars writes complete in ~8 seconds (5.6× faster)
- No code changes required for existing Pandas-based collectors
- PostgreSQL 18's improved multi-insert handling (`chunksize=2000`) leverages server-side batching

---

### 2. **PostgreSQL UPSERT with Conflict Resolution**

**Problem:** Re-running data collection pipelines causes duplicate key violations on `(asset_id, timestamp, interval)`. Traditional approaches either fail the transaction or require expensive `DELETE → INSERT` cycles.

**Solution:** PostgreSQL's `ON CONFLICT DO NOTHING` via SQLAlchemy's `insert().on_conflict_do_nothing()`:

```python
# src/collectors/fetch_binance.py:L228-231
stmt = insert(table).values(records)
stmt = stmt.on_conflict_do_nothing(
    index_elements=['asset_id', 'timestamp', 'interval']
)
```

**Why This Works:**
- Idempotent pipeline execution (safe to re-run daily jobs)
- Zero overhead for existing rows (PostgreSQL's index-level check is O(1))
- Logs show `result.rowcount` to differentiate new vs. skipped rows: `"BTC sync complete. 0 rows inserted."` during weekends when no new data exists

**Trade-off:** `DO NOTHING` silently skips updates to existing rows. For asset prices (append-only time-series), this is acceptable. For mutable data (e.g., news article summaries), use `ON CONFLICT DO UPDATE` instead.

---

### 3. **API Rate Limit Strategy: Adaptive Throttling + Dependency Chains**

**Problem:** Three API rate limits to navigate:
- **yFinance:** Undocumented, ~2 requests/sec observed before 429 errors
- **CoinGecko:** 10-30 req/min (free tier)
- **Gemini AI:** 15 RPM (requests per minute)

**Solution:** Multi-layered rate limiting:

#### Layer 1: Dagster Dependency Serialization
```python
# dagster_pipeline/assets.py:L64-67
@asset(deps=[macro_data])  # Wait for macro to finish
def stock_data(context):
    fetch_stocks_main()
    time.sleep(3)  # Cooldown before next asset
```

Forces sequential execution: `macro → stock → commodity`, inserting 3-second delays between yFinance batches.

#### Layer 2: Intra-Loop Throttling
```python
# src/collectors/fetch_binance.py:L189-190
market_cap = fetch_market_cap_coingecko(coingecko_id, symbol)
time.sleep(2)  # CoinGecko rate limit buffer
```

Each crypto asset fetch includes a 2-second delay before the next iteration.

#### Layer 3: AI Analysis Backoff
```python
# src/collectors/generate_asset_analysis.py:L283-285
generate_asset_analysis(symbol, asset_class, macro_summary, price_df, news_df)
time.sleep(60)  # Gemini 15 RPM limit → 1 asset/min
```

Hardcoded 60-second delay ensures 60 RPM << 15 RPM quota. Logged as: `"This will take ~60 minutes due to API rate limiting (1 asset/min)"`

**Failure Handling:** No automatic retry logic. If a rate limit error occurs (rare with these delays), the asset is logged as failed, and Dagster marks the asset as incomplete. Re-running the job processes only failed assets.

---

## Quick Start (Recommended: Docker)

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop/)

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd Glued_Stack_V1
    ```

2.  **Configure Environment:**
    ```bash
    cp .env.example .env
    # Open .env and add your GEMINI_API_KEY and FRED_API_KEY
    ```

3.  **Run with one command:**
    ```bash
    docker-compose up --build
    ```

    This command will automatically:
    - Start PostgreSQL 18 and create the `dipsignal` database.
    - Initialize tables and views from `db_schema/`.
    - Launch the Streamlit Admin Panel (http://localhost:8501).
    - Launch the Dagster UI (http://localhost:3000).

---

## Manual Installation (Local Python)

These instructions are for running the stack directly on your machine without Docker.

### Prerequisites

- **Python 3.8+** (verified with 3.11)
- **PostgreSQL 12+** (tested with Postgres 18)
- **API Keys:**
  - Google Gemini API key ([Get one here](https://aistudio.google.com/apikey))
  - FRED API key ([Register here](https://fred.stlouisfed.org/docs/api/api_key.html))

### Installation

1.  **Create virtual environment:**
    ```bash
    python -m venv .venv
    .venv\Scripts\activate  # Windows
    # source .venv/bin/activate  # macOS/Linux
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    pip install -r requirements-test.txt  # For testing
    ```

3.  **Configure environment variables:**
    
    Create a `.env` file in the project root:
    ```env
    DATABASE_URL=postgresql://user:password@localhost:5432/dipsignal
    GEMINI_API_KEY=your_gemini_api_key
    FRED_API_KEY=your_fred_api_key
    ADMIN_USERNAME=your_admin_username
    ADMIN_PASSWORD=your_admin_password
    ```

4.  **Initialize the database:**
    ```bash
    # Ensure database 'dipsignal' exists first
    psql -U postgres -d dipsignal -f db_schema/dipsignal_schema.sql
    psql -U postgres -d dipsignal -f db_schema/dipsignal_views.sql
    ```

5.  **Seed dimension tables:**
    ```bash
    python src/seeds/seed_dim_date.py
    python src/seeds/seed_dim_assets.py
    ```

### Running the Pipeline

#### Option 1: Dagster UI
```bash
dagster dev
```
Access at [http://localhost:3000](http://localhost:3000)

**To run the pipeline:**
1.  Navigate to **Assets** (top left).
2.  Click **Materialize All** (top right) or select specific assets.
3.  Monitor progress in the **Runs** tab.

#### Option 2: Manual Execution

Run individual collector scripts:
```bash
python src/collectors/fetch_binance.py
# ... other scripts
```

### Running the Admin Panel
```bash
streamlit run Admin_Panel.py
```
Access at [http://localhost:8501](http://localhost:8501)

**Login credentials:** Use the `ADMIN_USERNAME` and `ADMIN_PASSWORD` from your `.env` file.

### Running Tests
```bash
pytest tests/ -v --cov=src --cov-report=html
```

View coverage report at `htmlcov/index.html`

---

## Project Structure

```
Glued_Stack_V1/
├── dagster_pipeline/          # Dagster orchestration
│   ├── __init__.py            # Definitions (assets + schedules)
│   ├── assets.py              # 11 asset definitions with dependencies
│   └── schedules.py           # Daily (2:15 AM) + Weekly (Mon 4:15 AM) schedules
├── db_schema/                 # PostgreSQL schema definitions
│   ├── dipsignal_schema.sql   # Tables (2 dims, 7 facts)
│   └── dipsignal_views.sql    # 6 analytical views
├── src/
│   ├── collectors/            # Data fetching scripts (13 modules)
│   │   ├── fetch_binance.py   # Crypto OHLCV + market cap
│   │   ├── fetch_stocks.py    # Stock data (49 assets)
│   │   ├── generate_asset_analysis.py  # AI technical analysis
│   │   └── generate_weekly_portfolio_recommendation.py
│   ├── config/                # Asset configuration JSONs
│   │   ├── crypto_assets.json # 6 crypto symbols + CoinGecko IDs
│   │   ├── top_50.json        # 49 stock tickers
│   │   └── commodities.json   # 6 commodity symbols
│   ├── core/
│   │   ├── db_manager.py      # Polars/Pandas dual-write DB layer
│   │   └── logger_manager.py  # Centralized logging
│   └── seeds/                 # Dimension table initialization
├── admin_utils/               # Streamlit helper modules (13 files)
├── pages/                     # Streamlit multi-page app (7 pages)
├── tests/                     # Pytest test suite (21 test files)
│   ├── conftest.py            # Shared fixtures
│   ├── test_collectors/       # Data fetching tests
│   ├── test_core/             # DB layer tests
│   └── test_integration/      # End-to-end tests
├── screenshots/               # UI screenshots for documentation (9 images)
├── Admin_Panel.py             # Streamlit app entry point
├── requirements.txt           # 18 production dependencies
├── requirements-test.txt      # Testing dependencies (pytest, coverage)
├── pyproject.toml             # Dagster module config
└── pytest.ini                 # Test configuration
```

---

## Dependencies

Full tech stack defined in `requirements.txt`:

| Package | Purpose |
|---------|---------|
| `dagster` | Pipeline orchestration |
| `sqlalchemy` | PostgreSQL ORM |
| `polars` | High-speed DataFrame engine |
| `pandas` | Legacy DataFrame operations |
| `pandas-market-calendars` | Market holiday detection |
| `pandas_ta` | Technical indicator calculations |
| `psycopg2-binary` | PostgreSQL driver |
| `google-generativeai` | Gemini AI API client |
| `streamlit` | Admin panel UI |
| `plotly` | Interactive charts |
| `altair` | Declarative visualizations |
| `yfinance` | Stock/commodity data |
| `requests` | HTTP client (Binance, CoinGecko, FRED) |
| `beautifulsoup4` | HTML parsing (news scraping) |
| `feedparser` | RSS feed parsing |
| `fredapi` | FRED macro data |
| `python-dotenv` | Environment variable management |
| `networkx` | Dependency graph rendering (Admin Panel) |

**Testing:** `pytest`, `pytest-cov`, `pytest-mock`

---

## Deployment & Future Roadmap

- **Containerization (Docker):** Planned for Phase 2 to standardize the execution environment (Postgres + Dagster Daemon + Streamlit) and simplify dependency management for production deployment.
- **Manual Schedules:** Dagster schedules are defined but require `dagster dev` or `dagster daemon` to be running continuously. For production, use systemd (Linux) or Task Scheduler (Windows) to auto-start the daemon.
- **API Key Security:** `.env` file is gitignored. For team environments, use secret managers (AWS Secrets Manager, Azure Key Vault) instead of local `.env`.
- **Test Coverage:** Current coverage is ~60% . Core DB layer and collectors have full coverage; AI generators are partially mocked due to API cost constraints and no tests for admin panel.
- **Alerting:** Integration with Slack/Discord webhooks for pipeline failure notifications.