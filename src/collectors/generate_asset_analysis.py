"""
Generate AI-powered analysis for all assets using Gemini
"""
import os
import json
import time
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from dotenv import load_dotenv
from google import genai
from google.genai import types
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger

load_dotenv()
log = get_logger("ASSET_ANALYSIS")

# Configure Gemini
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    client = genai.Client(api_key=api_key)
else:
    log.error("GEMINI_API_KEY not found")
    exit(1)

def get_macro_summary():
    """Get the latest macro summary"""
    engine = DBManager.get_engine()
    query = text("""
        SELECT summary_text 
        FROM dipsignal.fact_macro_summary 
        ORDER BY date DESC LIMIT 1
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        return result[0] if result else None

def get_all_assets():
    """Get all assets from dim_assets"""
    engine = DBManager.get_engine()
    query = text("""
        SELECT asset_id, symbol, asset_class
        FROM dipsignal.dim_assets
        ORDER BY symbol
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        return df

def get_asset_data(symbol, asset_class):
    """Get 200 days of price data and 5 recent news for an asset"""
    engine = DBManager.get_engine()
    
    # Get 200 days of price data
    price_query = text("""
        SELECT 
            f.timestamp::date as date,
            f.price_open, f.price_high, f.price_low, f.price_close, f.volume,
            f.dynamic_metadata
        FROM dipsignal.fact_asset_prices f
        JOIN dipsignal.dim_assets d ON f.asset_id = d.asset_id
        WHERE d.symbol = :symbol
        ORDER BY f.timestamp DESC
        LIMIT 200
    """)
    
    # Get 5 recent news - filter based on asset class
    if asset_class == 'CRYPTO':
        news_query = text("""
            SELECT title, date
            FROM dipsignal.fact_news_articles
            WHERE related_tickers @> '["CRYPTO"]'
            ORDER BY date DESC
            LIMIT 5
        """)
        news_params = {}
    else:
        news_query = text("""
            SELECT title, date
            FROM dipsignal.fact_news_articles
            WHERE related_tickers @> :ticker_json
            ORDER BY date DESC
            LIMIT 5
        """)
        news_params = {"ticker_json": json.dumps([symbol])}
    
    with engine.connect() as conn:
        price_df = pd.read_sql(price_query, conn, params={"symbol": symbol})
        news_df = pd.read_sql(news_query, conn, params=news_params)
    
    return price_df, news_df

def check_existing_analysis(asset_id):
    """Check if analysis already exists for this asset today"""
    engine = DBManager.get_engine()
    
    query = text("""
        SELECT analysis_id
        FROM dipsignal.fact_ai_analysis
        WHERE asset_id = :asset_id
          AND DATE(created_at) = CURRENT_DATE
        LIMIT 1
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"asset_id": asset_id}).fetchone()
        return result is not None

def clean_ai_response(text):
    """Remove unwanted artifacts from AI response"""
    if not text:
        return text
    
    # Remove lines starting with @[
    lines = text.split('\n')
    cleaned_lines = [line for line in lines if not line.strip().startswith('@[')]
    
    return '\n'.join(cleaned_lines).strip()

def parse_json_response(text):
    """Parse JSON from AI response, handling markdown code blocks"""
    try:
        # Remove markdown code blocks if present
        text = text.strip()
        if text.startswith("```"):
            # Extract content between ```json and ```
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:].strip()
        
        parsed = json.loads(text)
        return parsed
    except json.JSONDecodeError as e:
        log.error(f"JSON parse error: {e}")
        return None

def generate_asset_analysis(symbol, asset_class, macro_summary, price_df, news_df, retry=False):
    """Generate AI analysis for a single asset"""
    
    # Prepare data strings
    price_data = price_df.to_string(index=False)
    news_headlines = "\n".join([f"- {row['title']} ({row['date']})" for _, row in news_df.iterrows()])
    
    if not news_headlines:
        news_headlines = "No recent news available"
    
    prompt = f"""
You are a Senior Financial Analyst. Analyze {symbol} ({asset_class}) and provide a structured assessment.

MACRO CONTEXT:
{macro_summary}

PRICE DATA (Last 200 Days):
{price_data}

RECENT NEWS:
{news_headlines}

INSTRUCTIONS:
Provide your analysis in the following JSON format (respond with ONLY valid JSON, no markdown):

{{
  "trend_signal": "Bullish" | "Bearish" | "Neutral",
  "key_levels": {{
    "support": [price1, price2],
    "resistance": [price1, price2]
  }},
  "summary_text": "2-3 sentence executive summary with specific price targets and catalysts"
}}

Rules:
- trend_signal must be exactly one of: Bullish, Bearish, Neutral
- support/resistance should be 2 realistic price levels based on recent data
- summary_text should be concise and actionable
"""
    
    try:
        response = client.models.generate_content(
            model='gemma-3-27b-it',
            contents=prompt
        )
        
        cleaned_text = clean_ai_response(response.text)
        parsed = parse_json_response(cleaned_text)
        
        if not parsed:
            raise ValueError("Failed to parse JSON response")
        
        # Validate structure
        if "trend_signal" not in parsed or parsed["trend_signal"] not in ["Bullish", "Bearish", "Neutral"]:
            raise ValueError(f"Invalid trend_signal: {parsed.get('trend_signal')}")
        if "key_levels" not in parsed or "support" not in parsed["key_levels"] or "resistance" not in parsed["key_levels"]:
            raise ValueError("Missing key_levels structure")
        if "summary_text" not in parsed:
            raise ValueError("Missing summary_text")
        
        return parsed
        
    except Exception as e:
        log.error(f"{symbol}: Analysis failed - {e}")
        if not retry:
            log.info(f"{symbol}: Retrying in 60 seconds...")
            time.sleep(60)
            return generate_asset_analysis(symbol, asset_class, macro_summary, price_df, news_df, retry=True)
        return None

def save_analysis(asset_id, analysis):
    """Save analysis to database"""
    engine = DBManager.get_engine()
    
    insert_query = text("""
        INSERT INTO dipsignal.fact_ai_analysis 
        (asset_id, date, model_name, trend_signal, key_levels, summary_text)
        VALUES (:asset_id, :date, :model_name, :trend_signal, :key_levels, :summary_text)
        ON CONFLICT (asset_id, date) DO UPDATE SET
            model_name = EXCLUDED.model_name,
            trend_signal = EXCLUDED.trend_signal,
            key_levels = EXCLUDED.key_levels,
            summary_text = EXCLUDED.summary_text
    """)
    
    with engine.begin() as conn:
        conn.execute(insert_query, {
            "asset_id": asset_id,
            "date": datetime.now().date(),
            "model_name": "gemma-3-27b-it",
            "trend_signal": analysis["trend_signal"],
            "key_levels": json.dumps(analysis["key_levels"]),
            "summary_text": analysis["summary_text"]
        })

def is_trading_day():
    """Check if today is a trading day (Mon-Fri, not weekend)"""
    from datetime import datetime
    now = datetime.now()
    # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday
    return now.weekday() < 5

def main():
    log.info("Starting asset analysis generation...")
    
    # Check if it's a trading day
    trading_day = is_trading_day()
    if trading_day:
        log.info("Trading day detected - analyzing all assets")
    else:
        log.info("Weekend/Holiday detected - analyzing CRYPTO only")
    
    # Get macro summary
    log.info("Fetching macro summary...")
    macro_summary = get_macro_summary()
    if not macro_summary:
        log.error("No macro summary found. Run generate_macro_summary.py first.")
        return
    
    # Get all assets
    log.info("Fetching assets...")
    assets_df = get_all_assets()
    
    # Filter assets based on trading day
    if not trading_day:
        # Weekend/Holiday: Only analyze CRYPTO
        assets_df = assets_df[assets_df['asset_class'] == 'CRYPTO']
        log.info(f"Filtered to {len(assets_df)} crypto assets (markets closed)")
    
    total_assets = len(assets_df)
    log.info(f"Found {total_assets} assets to analyze")
    
    success_count = 0
    failed_assets = []
    
    for idx, row in assets_df.iterrows():
        asset_id = row['asset_id']
        symbol = row['symbol']
        asset_class = row['asset_class']
        
        log.info(f"[{idx+1}/{total_assets}] Analyzing {symbol} ({asset_class})...")
        
        # Check if analysis already exists for today
        if check_existing_analysis(asset_id):
            log.info(f"{symbol}: Analysis already exists for today, skipping")
            success_count += 1  # Count as success since we have the data
            continue
        
        # Get data
        price_df, news_df = get_asset_data(symbol, asset_class)
        
        if price_df.empty:
            log.warning(f"{symbol}: No price data found, skipping")
            failed_assets.append(symbol)
            continue
        
        # Generate analysis
        analysis = generate_asset_analysis(symbol, asset_class, macro_summary, price_df, news_df)
        
        if analysis:
            save_analysis(asset_id, analysis)
            log.info(f"{symbol}: SUCCESS {analysis['trend_signal']} - {analysis['summary_text'][:50]}...")
            success_count += 1
        else:
            log.error(f"{symbol}: FAILED after retry")
            failed_assets.append(symbol)
        
        # Rate limiting: 1 asset per minute (60s delay)
        if idx < total_assets - 1:  # Don't sleep after last asset
            log.info("Waiting 60 seconds (rate limit)...")
            time.sleep(60)
    
    log.info(f"\n{'='*80}")
    log.info(f"Asset analysis complete!")
    log.info(f"Success: {success_count}/{total_assets}")
    if failed_assets:
        log.warning(f"Failed: {', '.join(failed_assets)}")
    log.info(f"{'='*80}\n")

if __name__ == "__main__":
    main()
