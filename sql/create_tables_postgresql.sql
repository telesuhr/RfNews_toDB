-- PostgreSQL用 Refinitiv ニュース記事テーブル
CREATE TABLE IF NOT EXISTS news_articles (
    id BIGSERIAL PRIMARY KEY,
    story_id VARCHAR(100) UNIQUE NOT NULL,
    headline TEXT NOT NULL,
    summary TEXT,
    body_text TEXT,
    source VARCHAR(100),
    published_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    language VARCHAR(10) DEFAULT 'en',
    category VARCHAR(50),
    urgency_level INTEGER DEFAULT 3,
    priority_score INTEGER DEFAULT 0
);

-- インデックス作成
CREATE INDEX IF NOT EXISTS idx_news_articles_story_id ON news_articles(story_id);
CREATE INDEX IF NOT EXISTS idx_news_articles_published_at ON news_articles(published_at);
CREATE INDEX IF NOT EXISTS idx_news_articles_source ON news_articles(source);
CREATE INDEX IF NOT EXISTS idx_news_articles_category ON news_articles(category);
CREATE INDEX IF NOT EXISTS idx_news_articles_created_at ON news_articles(created_at);
CREATE INDEX IF NOT EXISTS idx_news_articles_priority_score ON news_articles(priority_score);

-- コメント追加
COMMENT ON TABLE news_articles IS 'Refinitivニュース記事マスタ';
COMMENT ON COLUMN news_articles.story_id IS 'Refinitiv記事ID';
COMMENT ON COLUMN news_articles.headline IS 'ニュースヘッドライン';
COMMENT ON COLUMN news_articles.summary IS 'ニュース要約';
COMMENT ON COLUMN news_articles.body_text IS '記事本文';
COMMENT ON COLUMN news_articles.source IS 'ニュースソース（Reuters, Bloomberg等）';
COMMENT ON COLUMN news_articles.published_at IS '発行日時（UTC）';
COMMENT ON COLUMN news_articles.language IS '言語コード';
COMMENT ON COLUMN news_articles.category IS 'ニュースカテゴリ';
COMMENT ON COLUMN news_articles.urgency_level IS '緊急度レベル（1-5）';
COMMENT ON COLUMN news_articles.priority_score IS '優先度スコア（キーワードベース）';

-- ニュース記事関連銘柄リンクテーブル
CREATE TABLE IF NOT EXISTS news_rics (
    id BIGSERIAL PRIMARY KEY,
    news_id BIGINT NOT NULL,
    ric_code VARCHAR(50) NOT NULL,
    relevance_score DECIMAL(3,2) DEFAULT 1.00,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (news_id) REFERENCES news_articles(id) ON DELETE CASCADE,
    UNIQUE(news_id, ric_code)
);

-- インデックス作成
CREATE INDEX IF NOT EXISTS idx_news_rics_ric_code ON news_rics(ric_code);
CREATE INDEX IF NOT EXISTS idx_news_rics_relevance_score ON news_rics(relevance_score);

-- コメント追加
COMMENT ON TABLE news_rics IS 'ニュース記事関連銘柄リンク';
COMMENT ON COLUMN news_rics.news_id IS 'ニュース記事ID';
COMMENT ON COLUMN news_rics.ric_code IS 'RICコード';
COMMENT ON COLUMN news_rics.relevance_score IS '関連度スコア（0.00-1.00）';

-- ニュース取得ログテーブル
CREATE TYPE fetch_status AS ENUM ('running', 'completed', 'failed');

CREATE TABLE IF NOT EXISTS news_fetch_log (
    id BIGSERIAL PRIMARY KEY,
    fetch_start TIMESTAMPTZ NOT NULL,
    fetch_end TIMESTAMPTZ,
    articles_fetched INTEGER DEFAULT 0,
    articles_inserted INTEGER DEFAULT 0,
    articles_updated INTEGER DEFAULT 0,
    status fetch_status DEFAULT 'running',
    error_message TEXT,
    api_calls INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- インデックス作成
CREATE INDEX IF NOT EXISTS idx_news_fetch_log_start ON news_fetch_log(fetch_start);
CREATE INDEX IF NOT EXISTS idx_news_fetch_log_status ON news_fetch_log(status);

-- コメント追加
COMMENT ON TABLE news_fetch_log IS 'ニュース取得ログ';
COMMENT ON COLUMN news_fetch_log.fetch_start IS '取得開始日時';
COMMENT ON COLUMN news_fetch_log.fetch_end IS '取得終了日時';
COMMENT ON COLUMN news_fetch_log.articles_fetched IS '取得記事数';
COMMENT ON COLUMN news_fetch_log.articles_inserted IS '新規挿入記事数';
COMMENT ON COLUMN news_fetch_log.articles_updated IS '更新記事数';
COMMENT ON COLUMN news_fetch_log.status IS '取得ステータス';
COMMENT ON COLUMN news_fetch_log.error_message IS 'エラーメッセージ';
COMMENT ON COLUMN news_fetch_log.api_calls IS 'API呼び出し回数';

-- データ品質チェック用ビュー
CREATE OR REPLACE VIEW news_quality_check AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_articles,
    COUNT(CASE WHEN headline IS NULL OR headline = '' THEN 1 END) as missing_headline,
    COUNT(CASE WHEN published_at IS NULL THEN 1 END) as missing_published_at,
    COUNT(CASE WHEN source IS NULL OR source = '' THEN 1 END) as missing_source,
    AVG(LENGTH(headline)) as avg_headline_length,
    AVG(LENGTH(body_text)) as avg_body_length
FROM news_articles 
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- 記事数統計ビュー
CREATE OR REPLACE VIEW news_statistics AS
SELECT 
    source,
    language,
    category,
    COUNT(*) as article_count,
    MIN(published_at) as earliest_article,
    MAX(published_at) as latest_article,
    AVG(LENGTH(body_text)) as avg_body_length
FROM news_articles 
GROUP BY source, language, category
ORDER BY article_count DESC;