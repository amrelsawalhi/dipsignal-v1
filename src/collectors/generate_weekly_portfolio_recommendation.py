

import os
import json
from datetime import datetime, timedelta
from sqlalchemy import text
from src.core.db_manager import DBManager
from src.core.logger_manager import get_logger
from google import genai

log = get_logger("WEEKLY_PORTFOLIO")

def fetch_all_data():

    engine = DBManager.get_engine()
    
    with engine.connect() as conn:

        macro_query = text("""
            SELECT summary_text 
            FROM dipsignal.fact_macro_summary 
            ORDER BY date DESC 
            LIMIT 1
        """)
        macro_result = conn.execute(macro_query).fetchone()
        macro_summary = macro_result[0] if macro_result else "No macro data available"
        

        asset_query = text("""
            SELECT * FROM dipsignal.vw_latest_asset_analysis
        """)
        assets = conn.execute(asset_query).fetchall()
        

        news_query = text("""
            SELECT title, summary, url, date, related_tickers
            FROM dipsignal.vw_balanced_news_7d
        """)
        news = conn.execute(news_query).fetchall()
        

        fgi_query = text("""
            SELECT date, fgi_value, classification
            FROM dipsignal.fact_sentiment_index
            ORDER BY date DESC
            LIMIT 7
        """)
        fgi_results = conn.execute(fgi_query).fetchall()
        fgi = fgi_results if fgi_results else []
        

        sector_query = text("""
            SELECT * FROM dipsignal.vw_sector_summary
        """)
        sectors = conn.execute(sector_query).fetchall()
        

        technical_query = text("""
            SELECT * FROM dipsignal.vw_asset_technical_indicators
        """)
        technicals = conn.execute(technical_query).fetchall()
        

        correlation_query = text("""
            SELECT * FROM dipsignal.vw_asset_correlations
        """)
        correlations = conn.execute(correlation_query).fetchall()
    
    return {
        'macro_summary': macro_summary,
        'assets': assets,
        'news': news,
        'fgi': fgi,
        'sectors': sectors,
        'technicals': technicals,
        'correlations': correlations
    }

def build_prompt(data, risk_profile):

    

    macro_context = f"**Macroeconomic Summary:**\n{data['macro_summary']}\n\n"
    

    sector_overview = "**Sector Overview:**\n"
    for sector in data['sectors']:
        sector_overview += f"- {sector[0]}: {sector[1]} assets ({sector[2]} bullish, {sector[3]} bearish, {sector[4]} neutral)\n"
    sector_overview += "\n"
    

    asset_analyses = "**Detailed Asset Analyses:**\n\n"
    

    tech_dict = {t[0]: t for t in data['technicals']}
    
    for asset in data['assets']:
        # View columns: symbol, asset_class, trend_signal, summary_text, date, current_price, change_7d, change_30d
        symbol = asset[0]
        asset_class = asset[1]
        trend = asset[2]
        analysis = asset[3]
        current_price = asset[5] if asset[5] is not None else 0
        change_7d = asset[6] if asset[6] is not None else 0
        change_30d = asset[7] if asset[7] is not None else 0
        
        asset_analyses += f"**{symbol}** ({asset_class}):\n"
        asset_analyses += f"- Trend: {trend}\n"
        asset_analyses += f"- Analysis: {analysis}\n"
        asset_analyses += f"- Price: ${current_price:.2f}, 7d: {change_7d:.2f}%, 30d: {change_30d:.2f}%\n"
        

        if symbol in tech_dict:
            t = tech_dict[symbol]
            vol = t[1] if t[1] is not None else 0
            vol_ratio = t[3] if t[3] is not None else 0
            asset_analyses += f"- Volatility (30d): {vol:.2f}%, Volume: {vol_ratio:.2f}x avg\n"
        
        asset_analyses += "\n"
    

    correlation_text = "**Asset Correlations (Top 50 pairs):**\n"
    for corr in data['correlations']:
        correlation_text += f"- {corr[0]}-{corr[1]}: {corr[2]:.2f}\n"
    correlation_text += "\n"
    

    news_text = f"**Recent News (Last 7 Days - {len(data['news'])} articles):**\n\n"
    for article in data['news'][:50]:  # Limit to 50 for prompt
        news_text += f"- **{article[0]}**\n  {article[1]}\n\n"
    

    fgi_text = "**Crypto Market Psychology (Fear & Greed Index - Last 7 Days):**\n"
    if data['fgi']:
        for fgi_row in data['fgi']:
            fgi_text += f"- {fgi_row[0]}: {fgi_row[1]} ({fgi_row[2]})\n"
    else:
        fgi_text += "- No FGI data available\n"
    fgi_text += "\n"
    

    prompt = f"""You are a professional portfolio manager. Generate a comprehensive portfolio recommendation for a **{risk_profile}** risk profile.

{macro_context}
{sector_overview}
{asset_analyses}
{correlation_text}
{news_text}
{fgi_text}

**Instructions:**
1. Recommend allocation percentages across stocks, crypto, and commodities
2. Select 10-15 top picks with specific weights (must sum to 100%)
3. For each pick, provide:
   - Symbol, weight (%), rationale, sector
   - Correlation notes (avoid assets with >0.8 correlation)
   - Price momentum, volatility, volume status
4. Calculate sector exposure percentages
5. Provide diversification score (0-10)
6. Explain correlation analysis and risk management
7. List 5-7 key risks
8. Recommend rebalance frequency

**Risk Profile Guidelines:**
- Conservative: 50% stocks, 40% commodities, 10% crypto, focus on low volatility
- Moderate: 70% stocks, 20% commodities, 10% crypto, balanced growth/stability
- Aggressive: 80% stocks, 10% commodities, 10% crypto, maximize growth

Return ONLY valid JSON in this exact format:
{{
    "allocation": {{"stocks": 70, "crypto": 10, "commodities": 20}},
    "top_picks": [
        {{
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "weight": 10,
            "rationale": "...",
            "sector": "Technology",
            "correlation_notes": "...",
            "price_momentum": "Strong Bullish",
            "volatility": "Low (0.8%)",
            "volume_status": "Normal"
        }}
    ],
    "sector_exposure": {{"Technology": 30, "Healthcare": 20}},
    "diversification_score": 8.5,
    "correlation_analysis": "...",
    "overall_rationale": "...",
    "news_impact": "...",
    "risks": ["risk 1", "risk 2"],
    "rebalance_frequency": "Quarterly"
}}
"""
    
    return prompt

def generate_recommendation(risk_profile):
    """Generate portfolio recommendation for a specific risk profile"""
    log.info(f"Generating {risk_profile} portfolio recommendation...")
    

    data = fetch_all_data()
    log.info(f"  - Fetched {len(data['assets'])} assets, {len(data['news'])} news articles")
    

    prompt = build_prompt(data, risk_profile)
    prompt_tokens = len(prompt) // 4  # Rough estimate
    log.info(f"  - Prompt size: ~{prompt_tokens:,} tokens")
    

    client = genai.Client(api_key=os.getenv('GEMINI_API_KEY'))
    
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt
    )
    

    response_text = response.text.strip()
    
    # Handle markdown code blocks
    if '```json' in response_text:
        start = response_text.find('```json') + 7
        end = response_text.find('```', start)
        response_text = response_text[start:end].strip()
    elif '```' in response_text:
        parts = response_text.split('```')
        if len(parts) >= 3:
            response_text = parts[1].strip()
            if response_text.startswith('json'):
                response_text = response_text[4:].strip()
    
    recommendation = json.loads(response_text)
    

    recommendation['_metadata'] = {
        'token_count': prompt_tokens,
        'data_sources': {
            'assets_count': len(data['assets']),
            'news_count': len(data['news']),
            'correlations_count': len(data['correlations']),
            'technicals_count': len(data['technicals'])
        }
    }
    
    log.info(f"  - [OK] {risk_profile} recommendation generated successfully")
    return recommendation

def save_recommendation(recommendation, risk_profile):
    """Save recommendation to database"""
    engine = DBManager.get_engine()
    
    insert_query = text("""
        INSERT INTO dipsignal.fact_portfolio_recommendations (
            date, risk_profile, model_name,
            allocation_stocks, allocation_crypto, allocation_commodities,
            top_picks, sector_exposure,
            diversification_score, correlation_analysis, overall_rationale, news_impact,
            risks, rebalance_frequency,
            token_count, data_sources
        ) VALUES (
            CURRENT_DATE, :risk_profile, 'gemini-2.5-flash',
            :allocation_stocks, :allocation_crypto, :allocation_commodities,
            :top_picks, :sector_exposure,
            :diversification_score, :correlation_analysis, :overall_rationale, :news_impact,
            :risks, :rebalance_frequency,
            :token_count, :data_sources
        )
        ON CONFLICT (date, risk_profile) 
        DO UPDATE SET
            allocation_stocks = EXCLUDED.allocation_stocks,
            allocation_crypto = EXCLUDED.allocation_crypto,
            allocation_commodities = EXCLUDED.allocation_commodities,
            top_picks = EXCLUDED.top_picks,
            sector_exposure = EXCLUDED.sector_exposure,
            diversification_score = EXCLUDED.diversification_score,
            correlation_analysis = EXCLUDED.correlation_analysis,
            overall_rationale = EXCLUDED.overall_rationale,
            news_impact = EXCLUDED.news_impact,
            risks = EXCLUDED.risks,
            rebalance_frequency = EXCLUDED.rebalance_frequency,
            token_count = EXCLUDED.token_count,
            data_sources = EXCLUDED.data_sources,
            created_at = CURRENT_TIMESTAMP
    """)
    
    with engine.begin() as conn:
        conn.execute(insert_query, {
            'risk_profile': risk_profile,
            'allocation_stocks': recommendation['allocation']['stocks'],
            'allocation_crypto': recommendation['allocation']['crypto'],
            'allocation_commodities': recommendation['allocation']['commodities'],
            'top_picks': json.dumps(recommendation['top_picks']),
            'sector_exposure': json.dumps(recommendation['sector_exposure']),
            'diversification_score': recommendation['diversification_score'],
            'correlation_analysis': recommendation['correlation_analysis'],
            'overall_rationale': recommendation['overall_rationale'],
            'news_impact': recommendation['news_impact'],
            'risks': json.dumps(recommendation['risks']),
            'rebalance_frequency': recommendation['rebalance_frequency'],
            'token_count': recommendation['_metadata']['token_count'],
            'data_sources': json.dumps(recommendation['_metadata']['data_sources'])
        })
    
    log.info(f"  - [OK] {risk_profile} recommendation saved to database")

def main():
    """Generate weekly portfolio recommendations for all risk profiles"""
    log.info("=" * 80)
    log.info("WEEKLY PORTFOLIO RECOMMENDATION GENERATOR")
    log.info("=" * 80)
    
    risk_profiles = ['Conservative', 'Moderate', 'Aggressive']
    
    for risk_profile in risk_profiles:
        try:
            recommendation = generate_recommendation(risk_profile)
            save_recommendation(recommendation, risk_profile)
        except Exception as e:
            log.error(f"Failed to generate {risk_profile} recommendation: {e}")
            continue
    
    log.info("=" * 80)
    log.info("[OK] Weekly portfolio recommendations complete!")
    log.info("=" * 80)

if __name__ == "__main__":
    main()
