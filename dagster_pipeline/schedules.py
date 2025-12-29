"""
Dagster schedules for DipSignal pipeline
"""
from dagster import ScheduleDefinition, define_asset_job

# Daily job - runs all assets except weekly portfolio
daily_pipeline_job = define_asset_job(
    name="daily_pipeline",
    description="Daily data collection, news fetching, and AI analysis",
    selection=[
        "binance_data",
        "macro_data",
        "stock_data",
        "commodity_data",
        "fgi_data",
        "crypto_news",
        "stock_news",
        "macro_summary",
        "daily_news_summaries",
        "asset_analysis",
    ]
)

# Weekly job - runs only portfolio recommendations
weekly_portfolio_job = define_asset_job(
    name="weekly_portfolio_job",
    description="Weekly portfolio recommendations for 3 risk profiles",
    selection=["weekly_portfolio_recommendation"]
)

# Daily schedule (2:15 AM every day)
daily_pipeline_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="15 2 * * *",  # 2:15 AM every day
    description="Run daily pipeline at 2:15 AM (UTC+2)",
)

# Weekly schedule (4:15 AM every Monday - 2 hours after daily pipeline)
weekly_portfolio_schedule = ScheduleDefinition(
    job=weekly_portfolio_job,
    cron_schedule="15 4 * * 1",  # 4:15 AM on Mondays (1 = Monday)
    description="Generate weekly portfolio recommendations on Mondays at 4:15 AM",
)
