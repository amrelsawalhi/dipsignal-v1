-- ============================================================================
-- View 1: Latest Asset Analysis with Price Trends
-- ============================================================================
CREATE OR REPLACE VIEW dipsignal.vw_latest_asset_analysis AS
WITH latest_prices AS (
    SELECT 
        asset_id,
        price_close as current_price,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY timestamp DESC) as rn
    FROM dipsignal.fact_asset_prices
    WHERE interval = '1d'
),
price_7d AS (
    SELECT DISTINCT ON (asset_id)
        asset_id,
        price_close as price_7d_ago
    FROM dipsignal.fact_asset_prices
    WHERE interval = '1d'
      AND timestamp >= CURRENT_DATE - INTERVAL '7 days'
      AND timestamp < CURRENT_DATE - INTERVAL '6 days'
    ORDER BY asset_id, timestamp DESC
),
price_30d AS (
    SELECT DISTINCT ON (asset_id)
        asset_id,
        price_close as price_30d_ago
    FROM dipsignal.fact_asset_prices
    WHERE interval = '1d'
      AND timestamp >= CURRENT_DATE - INTERVAL '30 days'
      AND timestamp < CURRENT_DATE - INTERVAL '29 days'
    ORDER BY asset_id, timestamp DESC
)
SELECT DISTINCT
    a.symbol,
    a.asset_class,
    ai.trend_signal,
    ai.summary_text,
    ai.date,
    lp.current_price,
    ROUND(((lp.current_price - p7.price_7d_ago) / p7.price_7d_ago * 100)::numeric, 2) as change_7d,
    ROUND(((lp.current_price - p30.price_30d_ago) / p30.price_30d_ago * 100)::numeric, 2) as change_30d
FROM dipsignal.fact_ai_analysis ai
JOIN dipsignal.dim_assets a ON ai.asset_id = a.asset_id
JOIN latest_prices lp ON a.asset_id = lp.asset_id AND lp.rn = 1
LEFT JOIN price_7d p7 ON a.asset_id = p7.asset_id
LEFT JOIN price_30d p30 ON a.asset_id = p30.asset_id
WHERE ai.date >= CURRENT_DATE
ORDER BY a.symbol;

COMMENT ON VIEW dipsignal.vw_latest_asset_analysis IS 'Latest AI analysis for all assets with 7d and 30d price trends';


-- ============================================================================
-- View 2: Asset Technical Indicators (30-day volatility and volume)
-- ============================================================================
CREATE OR REPLACE VIEW dipsignal.vw_asset_technical_indicators AS
WITH daily_returns AS (
    SELECT 
        asset_id,
        timestamp,
        price_close,
        volume,
        (price_close - LAG(price_close) OVER (PARTITION BY asset_id ORDER BY timestamp)) 
        / LAG(price_close) OVER (PARTITION BY asset_id ORDER BY timestamp) * 100 as daily_return
    FROM dipsignal.fact_asset_prices
    WHERE interval = '1d'
      AND timestamp >= CURRENT_DATE - INTERVAL '30 days'
),
latest_volume AS (
    SELECT 
        asset_id,
        volume as current_volume,
        ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY timestamp DESC) as rn
    FROM daily_returns
)
SELECT 
    a.symbol,
    ROUND(STDDEV(dr.daily_return)::numeric, 2) as volatility_30d,
    ROUND(AVG(dr.volume)::numeric, 0) as avg_volume_30d,
    ROUND((lv.current_volume / AVG(dr.volume))::numeric, 2) as volume_ratio
FROM dipsignal.dim_assets a
JOIN daily_returns dr ON a.asset_id = dr.asset_id
JOIN latest_volume lv ON a.asset_id = lv.asset_id AND lv.rn = 1
WHERE dr.daily_return IS NOT NULL
  AND a.asset_id IN (
      SELECT DISTINCT asset_id 
      FROM dipsignal.fact_ai_analysis 
      WHERE date >= CURRENT_DATE
  )
GROUP BY a.symbol, lv.current_volume
ORDER BY a.symbol;

COMMENT ON VIEW dipsignal.vw_asset_technical_indicators IS '30-day volatility, average volume, and current volume ratio for all assets';


-- ============================================================================
-- View 3: Asset Correlations (Top 50 pairs with correlation > 0.3)
-- ============================================================================
CREATE OR REPLACE VIEW dipsignal.vw_asset_correlations AS
WITH price_data AS (
    SELECT 
        a.symbol,
        ap.timestamp::date as date,
        ap.price_close
    FROM dipsignal.fact_asset_prices ap
    JOIN dipsignal.dim_assets a ON ap.asset_id = a.asset_id
    WHERE ap.interval = '1d'
      AND ap.timestamp >= CURRENT_DATE - INTERVAL '30 days'
      AND a.asset_id IN (
          SELECT DISTINCT asset_id 
          FROM dipsignal.fact_ai_analysis 
          WHERE date >= CURRENT_DATE
      )
),
correlations AS (
    SELECT 
        p1.symbol as symbol1,
        p2.symbol as symbol2,
        ROUND(CORR(p1.price_close, p2.price_close)::numeric, 2) as correlation
    FROM price_data p1
    JOIN price_data p2 ON p1.date = p2.date AND p1.symbol < p2.symbol
    GROUP BY p1.symbol, p2.symbol
    HAVING COUNT(*) >= 20  -- At least 20 data points for statistical significance
)
SELECT symbol1, symbol2, correlation
FROM correlations
WHERE ABS(correlation) > 0.3
ORDER BY ABS(correlation) DESC
LIMIT 50;

COMMENT ON VIEW dipsignal.vw_asset_correlations IS 'Top 50 asset correlation pairs (30-day window, min 20 data points)';


-- ============================================================================
-- View 4: Balanced News (7 days, 300 articles with 30% crypto cap)
-- ============================================================================
CREATE OR REPLACE VIEW dipsignal.vw_balanced_news_7d AS
(
    -- Crypto news (max 30% = 90 articles)
    SELECT 
        title, 
        summary, 
        url, 
        date, 
        related_tickers,
        'crypto' as news_category
    FROM dipsignal.fact_news_articles
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
      AND summary IS NOT NULL
      AND related_tickers @> '["CRYPTO"]'::jsonb
    ORDER BY date DESC
    LIMIT 90
)
UNION ALL
(
    SELECT 
        title, 
        summary, 
        url, 
        date, 
        related_tickers,
        'non_crypto' as news_category
    FROM dipsignal.fact_news_articles
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
      AND summary IS NOT NULL
      AND NOT (related_tickers @> '["CRYPTO"]'::jsonb)
    ORDER BY date DESC
    LIMIT 210
)
ORDER BY date DESC;

COMMENT ON VIEW dipsignal.vw_balanced_news_7d IS 'Last 7 days of news (300 articles max, 30% crypto cap to reduce noise)';


-- ============================================================================
-- View 5: Sector Summary (Asset counts and trend breakdown)
-- ============================================================================
CREATE OR REPLACE VIEW dipsignal.vw_sector_summary AS
SELECT 
    a.asset_class,
    COUNT(*) as asset_count,
    SUM(CASE WHEN ai.trend_signal = 'Bullish' THEN 1 ELSE 0 END) as bullish_count,
    SUM(CASE WHEN ai.trend_signal = 'Bearish' THEN 1 ELSE 0 END) as bearish_count,
    SUM(CASE WHEN ai.trend_signal = 'Neutral' THEN 1 ELSE 0 END) as neutral_count
FROM dipsignal.dim_assets a
JOIN dipsignal.fact_ai_analysis ai ON a.asset_id = ai.asset_id
WHERE ai.date >= CURRENT_DATE
GROUP BY a.asset_class
ORDER BY a.asset_class;

COMMENT ON VIEW dipsignal.vw_sector_summary IS 'Asset count and trend breakdown by sector/asset class';


-- ============================================================================
-- View 6: Market Cap and Latest Price (All Assets)
-- ============================================================================
CREATE OR REPLACE VIEW dipsignal.vw_market_cap_latest_price AS
SELECT a.symbol,
    a.name,
    a.asset_class,
    ((p.dynamic_metadata ->> 'market_cap'::text)::numeric)::bigint AS market_cap,
    p.price_close AS latest_price,
    p."timestamp"
FROM dipsignal.dim_assets a
JOIN LATERAL ( SELECT fact_asset_prices.price_close,
            fact_asset_prices."timestamp",
            fact_asset_prices.dynamic_metadata
           FROM dipsignal.fact_asset_prices
          WHERE fact_asset_prices.asset_id = a.asset_id AND (fact_asset_prices.dynamic_metadata ->> 'market_cap'::text) IS NOT NULL
          ORDER BY fact_asset_prices."timestamp" DESC
         LIMIT 1) p ON true
ORDER BY ((p.dynamic_metadata ->> 'market_cap'::text)::numeric) DESC;

COMMENT ON VIEW dipsignal.vw_market_cap_latest_price IS 'Market cap and latest price for all assets';