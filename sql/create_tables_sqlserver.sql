-- SQL Server用 Refinitiv ニュース記事テーブル
CREATE TABLE news_articles (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    story_id NVARCHAR(100) UNIQUE NOT NULL,
    headline NVARCHAR(MAX) NOT NULL,
    summary NVARCHAR(MAX),
    body_text NVARCHAR(MAX),
    source NVARCHAR(100),
    published_at DATETIMEOFFSET NOT NULL,
    created_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET(),
    updated_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET(),
    language NVARCHAR(10) DEFAULT 'en',
    category NVARCHAR(50),
    urgency_level INT DEFAULT 3,
    priority_score INT DEFAULT 0
);

-- インデックス作成
CREATE INDEX idx_news_articles_story_id ON news_articles(story_id);
CREATE INDEX idx_news_articles_published_at ON news_articles(published_at);
CREATE INDEX idx_news_articles_source ON news_articles(source);
CREATE INDEX idx_news_articles_category ON news_articles(category);
CREATE INDEX idx_news_articles_created_at ON news_articles(created_at);
CREATE INDEX idx_news_articles_priority_score ON news_articles(priority_score);

-- ニュース記事関連銘柄リンクテーブル
CREATE TABLE news_rics (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    news_id BIGINT NOT NULL,
    ric_code NVARCHAR(50) NOT NULL,
    relevance_score DECIMAL(3,2) DEFAULT 1.00,
    created_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET(),
    FOREIGN KEY (news_id) REFERENCES news_articles(id) ON DELETE CASCADE,
    UNIQUE(news_id, ric_code)
);

-- インデックス作成
CREATE INDEX idx_news_rics_ric_code ON news_rics(ric_code);
CREATE INDEX idx_news_rics_relevance_score ON news_rics(relevance_score);

-- ニュース取得ログテーブル
CREATE TABLE news_fetch_log (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    fetch_start DATETIMEOFFSET NOT NULL,
    fetch_end DATETIMEOFFSET,
    articles_fetched INT DEFAULT 0,
    articles_inserted INT DEFAULT 0,
    articles_updated INT DEFAULT 0,
    status NVARCHAR(20) DEFAULT 'running',
    error_message NVARCHAR(MAX),
    api_calls INT DEFAULT 0,
    created_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET()
);

-- インデックス作成
CREATE INDEX idx_news_fetch_log_start ON news_fetch_log(fetch_start);
CREATE INDEX idx_news_fetch_log_status ON news_fetch_log(status);

-- データ品質チェック用ビュー
CREATE VIEW news_quality_check AS
SELECT 
    CAST(created_at AS DATE) as date,
    COUNT(*) as total_articles,
    COUNT(CASE WHEN headline IS NULL OR headline = '' THEN 1 END) as missing_headline,
    COUNT(CASE WHEN published_at IS NULL THEN 1 END) as missing_published_at,
    COUNT(CASE WHEN source IS NULL OR source = '' THEN 1 END) as missing_source,
    AVG(LEN(headline)) as avg_headline_length,
    AVG(LEN(body_text)) as avg_body_length
FROM news_articles 
GROUP BY CAST(created_at AS DATE);

-- 記事数統計ビュー
CREATE VIEW news_statistics AS
SELECT 
    source,
    language,
    category,
    COUNT(*) as article_count,
    MIN(published_at) as earliest_article,
    MAX(published_at) as latest_article,
    AVG(LEN(body_text)) as avg_body_length
FROM news_articles 
GROUP BY source, language, category;