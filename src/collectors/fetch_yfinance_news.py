import yfinance as yf
import pandas as pd
import time
import json
import re
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger

log = get_logger("YFINANCE_NEWS")

def clean_html(text):

    if not text:
        return ""

    clean = re.compile('<.*?>')
    return re.sub(clean, '', text).strip()

def get_equity_symbols():

    engine = DBManager.get_engine()
    query = text("""
        SELECT symbol 
        FROM dipsignal.dim_assets 
        WHERE asset_class = 'EQUITY'
        ORDER BY symbol
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [row[0] for row in result]

def fetch_news_for_ticker(symbol, max_articles=5):

    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        
        if not news:
            log.info(f"No news found for {symbol}")
            return []
        
        articles = []
        for i, article in enumerate(news[:max_articles]):
            try:
                # New yfinance structure: data is nested under 'content'
                content = article.get('content', {})
                

                title = content.get('title', '').strip()
                description = content.get('description', '')
                summary = clean_html(description) if description else None
                

                canonical_url = content.get('canonicalUrl', {})
                click_url = content.get('clickThroughUrl', {})
                url = canonical_url.get('url') or click_url.get('url', '')
                

                provider = content.get('provider', {})
                publisher = provider.get('displayName', 'Unknown')
                

                pub_date_str = content.get('pubDate')
                if pub_date_str:
                    try:
                        # Parse ISO format: 2025-12-26T19:10:03Z
                        date = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00')).date()
                    except:
                        date = datetime.now().date()
                else:
                    date = datetime.now().date()
                

                related_tickers = content.get('finance', {}).get('relatedTickers', [symbol])
                if not related_tickers:
                    related_tickers = [symbol]
                

                if not title or not url:
                    log.warning(f"{symbol}: Skipping article - missing title or URL")
                    continue
                
                articles.append({
                    'date': date,
                    'source': publisher,
                    'title': title,
                    'summary': summary,
                    'url': url,
                    'related_tickers': json.dumps(related_tickers)
                })
                
            except Exception as e:
                log.warning(f"Error parsing article for {symbol}: {e}")
                continue
        
        log.info(f"{symbol}: Collected {len(articles)} articles")
        return articles
        
    except Exception as e:
        log.error(f"Failed to fetch news for {symbol}: {e}")
        return []

def insert_on_conflict_nothing(table, conn, keys, data_iter):

    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=['url'])
    result = conn.execute(stmt)
    return result.rowcount

def main():
    log.info("Starting yfinance news collection...")
    

    symbols = get_equity_symbols()
    
    if not symbols:
        log.warning("No equity symbols found in dim_assets")
        return
    
    log.info(f"Fetching news for {len(symbols)} stocks...")
    
    all_articles = []
    
    for i, symbol in enumerate(symbols, 1):
        log.info(f"[{i}/{len(symbols)}] Fetching news for {symbol}...")
        
        articles = fetch_news_for_ticker(symbol, max_articles=5)
        all_articles.extend(articles)
        
        # Rate limiting: 0.5s between requests
        if i < len(symbols):  # Don't sleep after last ticker
            time.sleep(0.5)
    

    if all_articles:
        df = pd.DataFrame(all_articles)
        
        try:
            # Use pandas to_sql with on_conflict handling
            engine = DBManager.get_engine()
            with engine.begin() as conn:
                # Insert with ON CONFLICT DO NOTHING
                df.to_sql(
                    name='fact_news_articles',
                    con=conn,
                    schema='dipsignal',
                    if_exists='append',
                    index=False,
                    method=insert_on_conflict_nothing
                )
            log.info(f"Successfully inserted {len(all_articles)} news articles")
        except Exception as e:
            log.error(f"Failed to insert news articles: {e}")
    else:
        log.info("No news articles to insert")
    
    log.info("yfinance news collection complete")

if __name__ == "__main__":
    main()
