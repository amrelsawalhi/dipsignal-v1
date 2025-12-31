-- ============================================================================
-- View: vw_top_5_news_details
-- Purpose: Join top news with full article details for easy querying
-- ============================================================================

CREATE OR REPLACE VIEW dipsignal.vw_top_5_news_details AS
SELECT 
    tn.top_news_id,
    tn.date,
    tn.rank_position,
    tn.importance_score,
    tn.model_name,
    tn.created_at,
    -- Article details
    na.article_id,
    na.title,
    na.summary,
    na.url,
    na.source,
    na.related_tickers
FROM 
    dipsignal.fact_top_news tn
    INNER JOIN dipsignal.fact_news_articles na 
        ON tn.article_id = na.article_id
ORDER BY 
    tn.date DESC, 
    tn.rank_position ASC;

COMMENT ON VIEW dipsignal.vw_top_5_news_details 
IS 'Complete view of top 5 news with full article details, ranked by importance. One-stop query for dashboard display.';
