import pandas as pd
import os
import time
from datetime import datetime, timedelta
from sqlalchemy import text
from google import genai
from google.genai import types
from dotenv import load_dotenv
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger

load_dotenv()
log = get_logger("MACRO_SUMMARY")

# Configure Gemini
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    client = genai.Client(api_key=api_key)
else:
    log.error("GEMINI_API_KEY not found in .env")
    exit(1)

def get_macro_data(days=365):
    """Fetch the last N days of macro data"""
    engine = DBManager.get_engine()
    
    query = text("""
        SELECT 
            date, dxy, sp500, cpi, interest_rate, 
            vix, treasury_10y, unemployment_rate, gdp
        FROM dipsignal.fact_macro_indicators
        WHERE date >= CURRENT_DATE - INTERVAL ':days days'
        ORDER BY date DESC
        LIMIT :days
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"days": days})
        return df.sort_values('date', ascending=True)  # Sort chronologically for AI

def check_existing_summary(date):
    """Check if a summary already exists for today"""
    engine = DBManager.get_engine()
    
    query = text("""
        SELECT summary_text 
        FROM dipsignal.fact_macro_summary 
        WHERE date = :date
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"date": date}).fetchone()
        return result[0] if result else None

def clean_ai_response(text):
    """Remove unwanted artifacts from AI response"""
    if not text:
        return text
    
    # Remove lines starting with @[
    lines = text.split('\n')
    cleaned_lines = [line for line in lines if not line.strip().startswith('@[')]
    
    return '\n'.join(cleaned_lines).strip()

def generate_macro_summary(macro_df):
    """Use Gemini to generate both long and short macro summaries"""
    
    # Prepare the data summary
    data_summary = macro_df.to_string(index=False)
    
    # Generate LONG summary (for AI analysis)
    long_prompt = f"""
You are a Senior Macroeconomic Analyst. Analyze the following 365 days of macro data and provide a comprehensive market summary.

MACRO DATA (Last 365 Days):
{data_summary}

INSTRUCTIONS:
1. Identify the overall macro trend (Risk-On, Risk-Off, Transitioning)
2. Highlight key turning points in the data (e.g., Fed pivot, inflation peak)
3. Assess current market regime (Bull, Bear, Sideways)
4. Note correlations between indicators (e.g., VIX vs SP500, rates vs dollar)
5. Provide a 2-3 paragraph executive summary

Format your response as a cohesive narrative. Be specific with numbers and dates. Focus on actionable insights for asset analysis.
"""
    
    try:
        log.info("Generating LONG macro summary...")
        response = client.models.generate_content(
            model='gemma-3-27b-it',
            contents=long_prompt
        )
        summary_long = clean_ai_response(response.text)
    except Exception as e:
        log.error(f"Failed to generate long summary: {e}")
        return None, None
    
    # Wait to avoid rate limit
    time.sleep(2)
    
    # Generate SHORT summary (for dashboard display)
    short_prompt = f"""
Condense the following comprehensive macro analysis into a 100-150 word executive summary suitable for a dashboard.

FULL ANALYSIS:
{summary_long}

Create a concise summary that includes:
1. Current market regime (1 sentence)
2. Key macro drivers (1-2 sentences)  
3. Primary risk/opportunity (1 sentence)

Be direct and actionable. No fluff.
"""
    
    try:
        log.info("Generating SHORT macro summary...")
        response = client.models.generate_content(
            model='gemma-3-27b-it',
            contents=short_prompt
        )
        summary_short = clean_ai_response(response.text)
    except Exception as e:
        log.error(f"Failed to generate short summary: {e}")
        summary_short = summary_long[:150] + "..."  # Fallback to truncation
    
    return summary_long, summary_short

def save_macro_summary(summary_long, summary_short, period_start, period_end):
    """Save both macro summaries to the database"""
    engine = DBManager.get_engine()
    
    insert_query = text("""
        INSERT INTO dipsignal.fact_macro_summary 
        (date, period_start, period_end, model_name, summary_text, summary_short)
        VALUES (:date, :period_start, :period_end, :model_name, :summary_text, :summary_short)
        ON CONFLICT (date) DO NOTHING
    """)
    
    with engine.begin() as conn:
        result = conn.execute(insert_query, {
            "date": datetime.now().date(),
            "period_start": period_start,
            "period_end": period_end,
            "model_name": "gemma-3-27b-it",
            "summary_text": summary_long,
            "summary_short": summary_short
        })
        return result.rowcount

def is_trading_day():
    """Check if today is a trading day (Mon-Fri, not weekend)"""
    now = datetime.now()
    return now.weekday() < 5

def main():
    log.info("Starting macro summary generation...")
    
    # Check if it's a trading day
    if not is_trading_day():
        log.info("Weekend/Holiday detected - skipping macro summary (markets closed)")
        return  # Return normally instead of exit
    
    log.info("Trading day detected - generating macro summary")
    
    today = datetime.now().date()
    
    # Check if summary already exists
    existing = check_existing_summary(today)
    if existing:
        log.info(f"Macro summary already exists for {today}")
        print(f"\n{'='*80}\nEXISTING MACRO SUMMARY:\n{'='*80}\n{existing}\n{'='*80}\n")
        return
    
    # Fetch macro data
    log.info("Fetching 365 days of macro data...")
    macro_df = get_macro_data(days=365)
    
    if macro_df.empty:
        log.error("No macro data found")
        return
    
    period_start = macro_df['date'].min()
    period_end = macro_df['date'].max()
    
    log.info(f"Analyzing period: {period_start} to {period_end}")
    
    # Generate both summaries
    summary_long, summary_short = generate_macro_summary(macro_df)
    
    if not summary_long or not summary_short:
        log.error("Failed to generate summaries")
        return
    
    # Save to database
    rows_inserted = save_macro_summary(summary_long, summary_short, period_start, period_end)
    
    if rows_inserted > 0:
        log.info("Macro summaries saved successfully")
        print(f"\n{'='*80}\nSHORT SUMMARY (Dashboard):\n{'='*80}\n{summary_short}\n")
        print(f"\n{'='*80}\nLONG SUMMARY (AI Context):\n{'='*80}\n{summary_long}\n{'='*80}\n")
    else:
        log.warning("Summary already exists (conflict)")
    
    log.info("Macro summary generation complete")

if __name__ == "__main__":
    main()
