-- ============================================================================
-- New Fact Table: fact_top_news
-- Purpose: Store AI-selected top 5 critical news per day
-- CIO Perspective: Global Macro Hedge Fund investment priorities
-- ============================================================================

CREATE TABLE IF NOT EXISTS dipsignal.fact_top_news
(
    top_news_id serial NOT NULL,
    date date NOT NULL,
    article_id bigint NOT NULL,
    importance_score integer NOT NULL,
    rank_position integer NOT NULL,
    model_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key
    CONSTRAINT fact_top_news_pkey PRIMARY KEY (top_news_id),
    
    -- Unique constraints
    CONSTRAINT fact_top_news_date_article_key UNIQUE (date, article_id),
    CONSTRAINT fact_top_news_date_rank_key UNIQUE (date, rank_position),
    
    -- Foreign keys
    CONSTRAINT fact_top_news_article_id_fkey FOREIGN KEY (article_id)
        REFERENCES dipsignal.fact_news_articles (article_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    
    CONSTRAINT fact_top_news_date_fkey FOREIGN KEY (date)
        REFERENCES dipsignal.dim_date (full_date) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    
    -- Validation constraints
    CONSTRAINT valid_importance_score CHECK (importance_score >= 1 AND importance_score <= 10),
    CONSTRAINT valid_rank_position CHECK (rank_position >= 1 AND rank_position <= 5)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_top_news_date
    ON dipsignal.fact_top_news(date);

CREATE INDEX IF NOT EXISTS idx_top_news_rank
    ON dipsignal.fact_top_news(date, rank_position);

-- Table comments
COMMENT ON TABLE dipsignal.fact_top_news 
IS 'AI-curated top 5 critical news per day based on Global Macro Hedge Fund CIO perspective. Uses Gemini AI to analyze market impact and trend signals across Equities, Commodities, and Crypto.';

COMMENT ON COLUMN dipsignal.fact_top_news.importance_score 
IS 'AI-assigned importance score from 1-10 based on market impact potential and systemic risk';

COMMENT ON COLUMN dipsignal.fact_top_news.rank_position 
IS 'Ranking position from 1 (most important) to 5 (least important of top 5)';

COMMENT ON COLUMN dipsignal.fact_top_news.article_id 
IS 'Foreign key to fact_news_articles - links to full article details (title, summary, url, source)';
