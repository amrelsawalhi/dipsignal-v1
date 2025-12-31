"""
Dagster assets for DipSignal data pipeline
"""
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import sys
import os
import time

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.collectors.fetch_binance import main as fetch_binance_main
from src.collectors.fetch_stocks import main as fetch_stocks_main
from src.collectors.fetch_commodities import main as fetch_commodities_main
from src.collectors.fetch_macro import main as fetch_macro_main
from src.collectors.fetch_fgi import main as fetch_fgi_main
from src.collectors.fetch_crypto_news import main as fetch_crypto_news_main
from src.collectors.fetch_yfinance_news import main as fetch_yfinance_news_main
from src.collectors.generate_macro_summary import main as generate_macro_summary_main
from src.collectors.generate_asset_analysis import main as generate_asset_analysis_main
from src.collectors.generate_daily_news_summaries import main as generate_daily_news_summaries_main
from src.collectors.generate_top_5_news import main as generate_top_5_news_main
from src.collectors.generate_weekly_portfolio_recommendation import main as generate_weekly_portfolio_main


# ============================================================================
# PHASE 1: DATA COLLECTION (Run in parallel)
# ============================================================================

@asset(group_name="data_collection")
def binance_data(context: AssetExecutionContext) -> Output[None]:
    """Fetch crypto OHLCV data from Binance (6 assets)"""
    context.log.info("Fetching Binance crypto data...")
    fetch_binance_main()
    
    return Output(
        None,
        metadata={
            "assets": MetadataValue.int(6),
            "source": MetadataValue.text("Binance API"),
            "description": MetadataValue.text("BTC, ETH, SOL, ADA, XRP, BNB"),
        }
    )


@asset(group_name="data_collection")
def macro_data(context: AssetExecutionContext) -> Output[None]:
    """Fetch macroeconomic indicators from FRED and yfinance"""
    context.log.info("Fetching macro data...")
    fetch_macro_main()
    
    # Wait 3 seconds before next yfinance asset to avoid rate limiting
    time.sleep(3)
    
    return Output(
        None,
        metadata={
            "indicators": MetadataValue.int(5),
            "source": MetadataValue.text("FRED + yfinance"),
            "description": MetadataValue.text("VIX, 10Y Treasury, Fed Funds, Unemployment, GDP"),
        }
    )


@asset(
    group_name="data_collection",
    deps=[macro_data]  # Wait for macro to finish before fetching stocks
)
def stock_data(context: AssetExecutionContext) -> Output[None]:
    """Fetch stock OHLCV data from yfinance (49 assets)"""
    context.log.info("Fetching stock data...")
    fetch_stocks_main()
    
    # Wait 3 seconds before next yfinance asset to avoid rate limiting
    time.sleep(3)
    
    return Output(
        None,
        metadata={
            "assets": MetadataValue.int(49),
            "source": MetadataValue.text("yfinance API"),
            "description": MetadataValue.text("Top 49 stocks by market cap"),
        }
    )


@asset(
    group_name="data_collection",
    deps=[stock_data]  # Wait for stocks to finish before fetching commodities
)
def commodity_data(context: AssetExecutionContext) -> Output[None]:
    """Fetch commodity OHLCV data from yfinance (6 assets)"""
    context.log.info("Fetching commodity data...")
    fetch_commodities_main()
    
    return Output(
        None,
        metadata={
            "assets": MetadataValue.int(6),
            "source": MetadataValue.text("yfinance API"),
            "description": MetadataValue.text("Gold, Silver, Oil, Copper, Natural Gas, Brent"),
        }
    )



@asset(group_name="data_collection")
def fgi_data(context: AssetExecutionContext) -> Output[None]:
    """Fetch Fear & Greed Index"""
    context.log.info("Fetching Fear & Greed Index...")
    fetch_fgi_main()
    
    return Output(
        None,
        metadata={
            "source": MetadataValue.text("CNN Fear & Greed API"),
            "description": MetadataValue.text("Market sentiment indicator"),
        }
    )


# ============================================================================
# PHASE 2: AI ANALYSIS (Sequential execution to avoid API rate limits)
# ============================================================================

@asset(group_name="data_collection")
def crypto_news(context: AssetExecutionContext) -> Output[None]:
    """Fetch crypto news from RSS feeds"""
    context.log.info("Fetching crypto news...")
    fetch_crypto_news_main()
    
    return Output(
        None,
        metadata={
            "sources": MetadataValue.int(3),
            "description": MetadataValue.text("CoinDesk, Cointelegraph, Decrypt"),
        }
    )


@asset(
    group_name="data_collection",
    deps=[commodity_data]  # Wait for commodities to finish
)
def stock_news(context: AssetExecutionContext) -> Output[None]:
    """Fetch stock news from yfinance (49 stocks)"""
    context.log.info("Fetching stock news...")
    fetch_yfinance_news_main()
    
    return Output(
        None,
        metadata={
            "stocks": MetadataValue.int(49),
            "articles_per_stock": MetadataValue.int(5),
            "source": MetadataValue.text("Yahoo Finance"),
        }
    )


@asset(
    group_name="ai_analysis",
    deps=["macro_summary"]  # Depends on macro_summary which has all data
)
def daily_news_summaries(context: AssetExecutionContext) -> Output[None]:
    """Generate AI summaries for today's news articles"""
    context.log.info("Generating AI summaries for today's news...")
    generate_daily_news_summaries_main()
    
    return Output(
        None,
        metadata={
            "model": MetadataValue.text("gemma-3-12b-it"),
            "batch_size": MetadataValue.int(5),
            "description": MetadataValue.text("AI summaries for new articles"),
        }
    )


@asset(
    group_name="ai_analysis",
    deps=["daily_news_summaries"]  # Wait for news summaries to be ready
)
def top_5_news(context: AssetExecutionContext) -> Output[None]:
    """Generate top 5 critical news from CIO perspective"""
    context.log.info("Generating top 5 critical news...")
    generate_top_5_news_main()
    
    return Output(
        None,
        metadata={
            "model": MetadataValue.text("gemini-2.5-flash-lite"),
            "description": MetadataValue.text("Top 5 news ranking from Global Macro CIO perspective"),
        }
    )


@asset(
    group_name="ai_analysis",
    deps=[
        stock_news,      # End of yfinance chain
        binance_data,    # Parallel crypto data
        crypto_news,     # Parallel crypto news
        fgi_data,        # Parallel sentiment data
    ]
)
def macro_summary(context: AssetExecutionContext) -> Output[None]:
    """Generate AI-powered macro market summary"""
    context.log.info("Generating macro summary with Gemini AI...")
    generate_macro_summary_main()
    
    return Output(
        None,
        metadata={
            "model": MetadataValue.text("gemma-3-27b-it"),
            "summaries": MetadataValue.int(2),
            "description": MetadataValue.text("Long (AI context) + Short (dashboard)"),
        }
    )


@asset(
    group_name="ai_analysis",
    deps=["daily_news_summaries"]  # All other data is transitively available
)
def asset_analysis(context: AssetExecutionContext) -> Output[None]:
    """Generate AI-powered analysis for all 61 assets"""
    context.log.info("Generating asset analysis with Gemini AI (61 assets)...")
    context.log.warning("This will take ~60 minutes due to API rate limiting (1 asset/min)")
    
    generate_asset_analysis_main()
    
    return Output(
        None,
        metadata={
            "assets_analyzed": MetadataValue.int(61),
            "model": MetadataValue.text("gemma-3-27b-it"),
            "duration_estimate": MetadataValue.text("~60 minutes"),
            "rate_limit": MetadataValue.text("1 asset per minute"),
        }
    )


@asset(
    group_name="ai_analysis",
    deps=[]  # No dependencies - runs independently, reads from database
)
def weekly_portfolio_recommendation(context: AssetExecutionContext) -> Output[None]:
    """Generate weekly portfolio recommendations for 3 risk profiles"""
    context.log.info("Generating weekly portfolio recommendations...")
    context.log.info("  - Conservative (50% stocks, 40% commodities, 10% crypto)")
    context.log.info("  - Moderate (70% stocks, 20% commodities, 10% crypto)")
    context.log.info("  - Aggressive (80% stocks, 10% commodities, 10% crypto)")
    
    generate_weekly_portfolio_main()
    
    return Output(
        None,
        metadata={
            "risk_profiles": MetadataValue.int(3),
            "model": MetadataValue.text("gemini-2.5-flash"),
            "assets_analyzed": MetadataValue.int(61),
            "news_articles": MetadataValue.int(300),
            "token_usage": MetadataValue.text("~130K tokens total"),
            "description": MetadataValue.text("Comprehensive portfolio recommendations with correlation analysis"),
        }
    )

