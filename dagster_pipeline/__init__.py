"""
Dagster pipeline for DipSignal data orchestration
"""
from dagster import Definitions
from .assets import (
    binance_data,
    stock_data,
    commodity_data,
    macro_data,
    fgi_data,
    crypto_news,
    stock_news,
    daily_news_summaries,
    top_5_news,
    macro_summary,
    asset_analysis,
    weekly_portfolio_recommendation,
)
from .schedules import daily_pipeline_schedule, weekly_portfolio_schedule

defs = Definitions(
    assets=[
        # Phase 1: Data Collection
        binance_data,
        stock_data,
        commodity_data,
        macro_data,
        fgi_data,
        # Phase 2: News Collection
        crypto_news,
        stock_news,
        daily_news_summaries,  # NEW: AI summaries for today's news
        top_5_news,            # NEW: Top 5 critical news from CIO perspective
        # Phase 3: AI Analysis
        macro_summary,
        asset_analysis,
        weekly_portfolio_recommendation,  # NEW: Weekly portfolio recommendations (3 risk profiles)
    ],
    schedules=[daily_pipeline_schedule, weekly_portfolio_schedule],
)
