
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google import genai
from sqlalchemy import text
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger

load_dotenv()
log = get_logger("TOP_5_NEWS")

model_name = "gemini-2.5-flash-lite"

def get_last_24h_news(engine):

    query = text("""
        SELECT 
            article_id,
            title,
            summary,
            url,
            source,
            date
        FROM dipsignal.fact_news_articles
        WHERE date >= CURRENT_DATE - INTERVAL '1 day'
          AND summary IS NOT NULL
          AND LENGTH(summary) > 20
        ORDER BY date DESC
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        return result.fetchall()


def check_existing_top_news_today(engine):

    query = text("""
        SELECT COUNT(*) as count
        FROM dipsignal.fact_top_news
        WHERE date = CURRENT_DATE
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        return result[0] > 0 if result else False


def generate_top_5_news(news_articles):

    client = genai.Client(api_key=os.getenv('GEMINI_API_KEY'))
    

    articles_text = ""
    article_map = {}  # Map index to article_id
    
    for idx, article in enumerate(news_articles, 1):
        article_id, title, summary, url, source, date = article
        article_map[idx] = article_id
        
        articles_text += f"""
Article {idx}:
Title: {title}
Summary: {summary}
Source: {source}
Date: {date}

---
"""
    
    prompt = f"""
System/Context:

You are the Chief Investment Officer (CIO) of a Global Macro Hedge Fund. You monitor Equities, Commodities, and Crypto. Your job is to identify the 5 most critical narratives driving global liquidity and risk sentiment today.

The Instruction:

Analyze the provided list of news titles and summaries from the last 24 hours. Select the Top 5 events based on their ability to impact global markets or signal a trend shift.

Global Priority Hierarchy (Use this to weigh importance):

1. Systemic Macro: Federal Reserve/Central Bank policies, Inflation (CPI/PPI), Geopolitical conflicts affecting supply chains (e.g., Oil/Energy) and Global Systemic Events (e.g., pandemics, natural disasters, wars). (Highest Priority)

2. Market Leaders: Earnings or news regarding "Magnificent 7" stocks (NVDA, MSFT, AAPL, GOOGL, AMZN, META, TSLA) or Bitcoin/Ethereum specific infrastructure.

3. Commodity Shocks: Significant supply disruptions in Oil, Gold, or critical metals (Copper/Lithium).

4. Sector Specific: Regulatory changes or massive M&A activity.

Selection Rules:

- Do not let one asset class dominate unless the event is systemic (e.g., if Crypto crashes 20%, it can take 3 spots. If it's a slow day, ensure diversity).
- You MUST select exactly 5 articles, ranked from most important (1) to least important (5).

NEWS ARTICLES (Last 24 Hours):

{articles_text}

Output Format: 

Return ONLY a valid JSON object with the key "top_news". Each object must contain the article number (from the list above) and an importance score (1-10, where 10 is highest impact).

Example format:
{{
  "top_news": [
    {{"article_number": 3, "importance_score": 10}},
    {{"article_number": 7, "importance_score": 9}},
    {{"article_number": 12, "importance_score": 7}},
    {{"article_number": 5, "importance_score": 5}},
    {{"article_number": 15, "importance_score": 3}}
  ]
}}

No extra formatting, just the JSON object. Ensure you return EXACTLY 5 articles.
"""
    
    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt
        )
        

        response_text = response.text.strip()
        

        if response_text.startswith('```'):
            response_text = response_text.split('```')[1]
            if response_text.startswith('json'):
                response_text = response_text[4:]
        
        parsed = json.loads(response_text)
        

        if "top_news" not in parsed:
            raise ValueError("Missing 'top_news' key in response")
        
        if len(parsed["top_news"]) != 5:
            raise ValueError(f"Expected exactly 5 articles, got {len(parsed['top_news'])}")
        
        # Convert article numbers to article IDs
        results = []
        for rank, item in enumerate(parsed["top_news"], 1):
            article_number = item["article_number"]
            importance_score = item["importance_score"]
            
            if article_number not in article_map:
                log.warning(f"Invalid article number: {article_number}, skipping")
                continue
            
            # Validate importance score
            if not (1 <= importance_score <= 10):
                log.warning(f"Invalid importance score: {importance_score}, capping to range")
                importance_score = max(1, min(10, importance_score))
            
            results.append({
                "article_id": article_map[article_number],
                "importance_score": importance_score,
                "rank_position": rank
            })
        
        if len(results) != 5:
            log.error(f"Failed to extract exactly 5 valid articles, got {len(results)}")
            return None
        
        return results
        
    except json.JSONDecodeError as e:
        log.error(f"JSON parse error: {e}")
        log.error(f"Response text: {response_text}")
        return None
    except Exception as e:
        log.error(f"AI generation failed: {e}")
        return None


def save_top_news(engine, top_news):

    

    delete_query = text("""
        DELETE FROM dipsignal.fact_top_news
        WHERE date = CURRENT_DATE
    """)
    
    insert_query = text("""
        INSERT INTO dipsignal.fact_top_news 
        (date, article_id, importance_score, rank_position, model_name)
        VALUES (:date, :article_id, :importance_score, :rank_position, :model_name)
    """)
    
    with engine.begin() as conn:

        conn.execute(delete_query)
        

        for item in top_news:
            conn.execute(insert_query, {
                "date": datetime.now().date(),
                "article_id": item["article_id"],
                "importance_score": item["importance_score"],
                "rank_position": item["rank_position"],
                "model_name": model_name
            })


def main():
    log.info("Starting top 5 news generation...")
    
    engine = DBManager.get_engine()
    

    if check_existing_top_news_today(engine):
        log.info("Top 5 news already generated for today - skipping")
        return
    

    news_articles = get_last_24h_news(engine)
    
    if not news_articles:
        log.warning("No news articles found in last 24 hours with summaries")
        return
    
    if len(news_articles) < 5:
        log.warning(f"Only {len(news_articles)} articles found - need at least 5 for selection")
        return
    
    log.info(f"Found {len(news_articles)} news articles from last 24 hours")
    

    log.info("Sending to Gemini AI for CIO analysis...")
    top_news = generate_top_5_news(news_articles)
    
    if not top_news:
        log.error("Failed to generate top 5 news")
        return
    

    save_top_news(engine, top_news)
    
    log.info("=" * 80)
    log.info("Top 5 Critical News Selection Complete!")
    log.info("=" * 80)
    
    # Log the results
    for item in top_news:
        article_id = item["article_id"]
        score = item["importance_score"]
        rank = item["rank_position"]
        
        # Get article title for logging
        query = text("SELECT title FROM dipsignal.fact_news_articles WHERE article_id = :id")
        with engine.connect() as conn:
            result = conn.execute(query, {"id": article_id}).fetchone()
            title = result[0] if result else "Unknown"
        
        log.info(f"#{rank} (Score: {score}/10) - {title}")
    
    log.info("=" * 80)


if __name__ == "__main__":
    main()
