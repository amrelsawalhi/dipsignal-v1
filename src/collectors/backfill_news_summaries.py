
import os
import time
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai
from sqlalchemy import text
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger
import concurrent.futures
import json

load_dotenv()
log = get_logger("BACKFILL_SUMMARIES")

def fetch_article_content(url):

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        article_body = soup.find('div', class_='body yf-5ef8bf')
        if article_body:
            paragraphs = article_body.find_all('p')
            article_text = '\n'.join([p.get_text() for p in paragraphs])
        else:

            paragraphs = soup.find_all('p')
            article_text = '\n'.join([p.get_text() for p in paragraphs])
        
        return article_text[:3000]
    except Exception as e:
        return None


def scrape_batch(articles):

    scraped = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_article = {
            executor.submit(fetch_article_content, article[1]): article 
            for article in articles
        }
        
        for future in concurrent.futures.as_completed(future_to_article):
            article = future_to_article[future]
            article_id, url, title = article
            
            try:
                content = future.result()
                if content:
                    scraped.append({
                        'article_id': article_id,
                        'url': url,
                        'title': title,
                        'content': content
                    })
            except Exception as e:
                log.warning(f"Failed to scrape: {title[:50]}...")
    
    return scraped


def generate_batch_summaries(articles):

    
    client = genai.Client(api_key=os.getenv('GEMINI_API_KEY'))
    
    # Build batch prompt
    articles_text = ""
    for i, article in enumerate(articles, 1):
        articles_text += f"""
Article {i}:
Title: {article['title']}
Content: {article['content']}

---
"""
    
    prompt = f"""
Summarize each of these {len(articles)} financial news articles in 2-3 concise sentences.
Focus on key facts, market impact, and actionable insights.

{articles_text}

Return ONLY a JSON array with summaries in order, like this:
[
  "Summary for article 1...",
  "Summary for article 2...",
  ...
]

No extra formatting, just the JSON array.
"""
    
    try:
        response = client.models.generate_content(
            model='gemma-3-12b-it',
            contents=prompt
        )
        
        # Parse JSON response
        response_text = response.text.strip()
        
        # Clean markdown code blocks if present
        if response_text.startswith('```'):
            response_text = response_text.split('```')[1]
            if response_text.startswith('json'):
                response_text = response_text[4:]
        
        summaries = json.loads(response_text)
        
        # Match summaries to articles
        results = []
        for i, article in enumerate(articles):
            if i < len(summaries):
                results.append({
                    'article_id': article['article_id'],
                    'summary': summaries[i]
                })
        
        return results
        
    except Exception as e:
        log.error(f"Batch AI generation failed: {e}")
        return []


def get_articles_without_summaries(engine, batch_size=100):

    query = text("""
        SELECT article_id, url, title 
        FROM dipsignal.fact_news_articles 
        WHERE summary IS NULL OR LENGTH(summary) < 20
        ORDER BY date DESC
        LIMIT :batch_size
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"batch_size": batch_size})
        return result.fetchall()


def update_article_summaries(engine, summaries):

    query = text("""
        UPDATE dipsignal.fact_news_articles 
        SET summary = :summary 
        WHERE article_id = :article_id
    """)
    
    with engine.begin() as conn:
        for item in summaries:
            conn.execute(query, item)


def main():
    log.info("Starting OPTIMIZED news summary backfill...")
    
    engine = DBManager.get_engine()
    
    # Get total count
    count_query = text("""
        SELECT COUNT(*) 
        FROM dipsignal.fact_news_articles 
        WHERE summary IS NULL OR LENGTH(summary) < 20
    """)
    with engine.connect() as conn:
        total_count = conn.execute(count_query).scalar()
    
    log.info(f"Found {total_count} articles without summaries")
    
    if total_count == 0:
        log.info("No articles to process!")
        return
    
    # Process in batches
    batch_size = 100
    ai_batch_size = 5  # Process 5 articles per AI call (reduced from 10 to avoid quota)
    processed = 0
    success_count = 0
    failed_count = 0
    
    while processed < total_count:
        # Fetch batch
        articles = get_articles_without_summaries(engine, batch_size)
        
        if not articles:
            break
        
        log.info(f"Processing batch of {len(articles)} articles...")
        
        # Process in AI batches of 10
        for i in range(0, len(articles), ai_batch_size):
            ai_batch = articles[i:i+ai_batch_size]
            

            scraped = scrape_batch(ai_batch)
            
            if not scraped:
                failed_count += len(ai_batch)
                processed += len(ai_batch)
                continue
            

            summaries = generate_batch_summaries(scraped)
            
            if summaries:

                update_article_summaries(engine, summaries)
                success_count += len(summaries)
                log.info(f"[{processed+len(summaries)}/{total_count}] OK Batch of {len(summaries)} articles")
            
            failed_count += len(scraped) - len(summaries)
            processed += len(ai_batch)
            
            # Rate limiting: 30 RPM with 5-article batches
            # Wait 4 seconds to ensure we stay under 15K TPM
            time.sleep(4)
        
        # Progress update
        log.info(f"Progress: {processed}/{total_count} ({processed/total_count*100:.1f}%)")
        log.info(f"Success: {success_count}, Failed: {failed_count}")
    
    log.info("=" * 80)
    log.info("Backfill complete!")
    log.info(f"Total processed: {processed}")
    log.info(f"Success: {success_count}")
    log.info(f"Failed: {failed_count}")
    log.info("=" * 80)


if __name__ == "__main__":
    main()
