import feedparser
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import re
from src.core.db_manager import DBManager
from sqlalchemy.dialects.postgresql import insert
from src.core.logger_manager import get_logger
import json


log = get_logger("NEWS")


# Load RSS Feed URLs from config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/feeds.json')
try:
    with open(CONFIG_PATH, 'r') as f:
        # Check if file is empty before loading, or catch JSONDecodeError
        content = f.read().strip()
        if not content:
            FEEDS = {}
            log.warning(f"Config file at {CONFIG_PATH} is empty.")
        else:
            FEEDS = json.loads(content)
            if not FEEDS:
                 log.warning(f"Config file at {CONFIG_PATH} contains no feeds.")
except (FileNotFoundError, json.JSONDecodeError) as e:
    log.error(f"Config file error at {CONFIG_PATH}: {e}")
    FEEDS = {}

def clean_html(text):
    return BeautifulSoup(text, "html.parser").get_text().strip()

def parse_date(entry):
    if "published_parsed" in entry and entry.published_parsed:
        return datetime(*entry.published_parsed[:3]).strftime("%Y-%m-%d")
    return None

def fetch_feed(source, url):
    parsed = feedparser.parse(url)
    articles = []
    for entry in parsed.entries:
        # Safely get title and summary, handling potential missing keys
        title = entry.get("title", "").strip()
        summary = clean_html(entry.get("summary", entry.get("description", "")))
        link = entry.get("link", "")
        
        # Only append if essential data is present
        if title and link:
            articles.append({
                "date": parse_date(entry),
                "source": source,
                "title": title,
                "summary": summary,
                "url": link,
                "related_tickers": json.dumps(["CRYPTO"]) # Tag all crypto news
            })
        else:
            log.warning(f"Skipping entry from {source} due to missing title or link: {entry.get('link', 'N/A')}")
    return articles

def fetch_all_news():
    all_data = []
    for source, url in FEEDS.items():
        all_data.extend(fetch_feed(source, url))
    df = pd.DataFrame(all_data)
    df.dropna(subset=["date", "title", "url"], inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df[
    ~df['summary'].str.contains(r'Degenz Live|DEGENZ|http[s]?://', flags=re.IGNORECASE, na=False)]

    return df

def insert_on_conflict_nothing(table, conn, keys, data_iter):
    """
    Custom insertion method for Postgres to handle unique constraint conflicts.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=['url'])
    result = conn.execute(stmt)
    return result.rowcount

def main():
    df_news = fetch_all_news()
    
    # Use direct engine access for custom insertion method
    engine = DBManager.get_engine()
    
    try:
        rows_inserted = df_news.to_sql(
            'fact_news_articles', 
            engine, 
            schema='dipsignal', 
            if_exists='append', 
            index=False, 
            method=insert_on_conflict_nothing
        )
        log.info(f"News fetch complete. {rows_inserted} new articles inserted.")
    except Exception as e:
        log.info(f"News fetch failed: {e}")

if __name__ == "__main__":
    main()