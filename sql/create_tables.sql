-- Refinitiv ニュース記事テーブル
CREATE TABLE IF NOT EXISTS news_articles (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    story_id VARCHAR(100) UNIQUE NOT NULL COMMENT 'Refinitiv記事ID',
    headline TEXT NOT NULL COMMENT 'ニュースヘッドライン',
    summary TEXT COMMENT 'ニュース要約',
    body_text LONGTEXT COMMENT '記事本文',
    source VARCHAR(100) COMMENT 'ニュースソース（Reuters, Bloomberg等）',
    published_at DATETIME NOT NULL COMMENT '発行日時（UTC）',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'レコード作成日時',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'レコード更新日時',
    language VARCHAR(10) DEFAULT 'en' COMMENT '言語コード',
    category VARCHAR(50) COMMENT 'ニュースカテゴリ',
    urgency_level TINYINT DEFAULT 3 COMMENT '緊急度レベル（1-5）',
    
    INDEX idx_published_at (published_at),
    INDEX idx_source (source),
    INDEX idx_category (category),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Refinitivニュース記事マスタ';

-- ニュース記事関連銘柄リンクテーブル
CREATE TABLE IF NOT EXISTS news_rics (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    news_id BIGINT NOT NULL COMMENT 'ニュース記事ID',
    ric_code VARCHAR(50) NOT NULL COMMENT 'RICコード',
    relevance_score DECIMAL(3,2) DEFAULT 1.00 COMMENT '関連度スコア（0.00-1.00）',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (news_id) REFERENCES news_articles(id) ON DELETE CASCADE,
    INDEX idx_ric_code (ric_code),
    INDEX idx_relevance_score (relevance_score),
    UNIQUE KEY unique_news_ric (news_id, ric_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='ニュース記事関連銘柄リンク';

-- ニュース取得ログテーブル
CREATE TABLE IF NOT EXISTS news_fetch_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    fetch_start DATETIME NOT NULL COMMENT '取得開始日時',
    fetch_end DATETIME COMMENT '取得終了日時',
    articles_fetched INT DEFAULT 0 COMMENT '取得記事数',
    articles_inserted INT DEFAULT 0 COMMENT '新規挿入記事数',
    articles_updated INT DEFAULT 0 COMMENT '更新記事数',
    status ENUM('running', 'completed', 'failed') DEFAULT 'running' COMMENT '取得ステータス',
    error_message TEXT COMMENT 'エラーメッセージ',
    api_calls INT DEFAULT 0 COMMENT 'API呼び出し回数',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_fetch_start (fetch_start),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='ニュース取得ログ';

-- データ品質チェック用ビュー
CREATE OR REPLACE VIEW news_quality_check AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_articles,
    COUNT(CASE WHEN headline IS NULL OR headline = '' THEN 1 END) as missing_headline,
    COUNT(CASE WHEN published_at IS NULL THEN 1 END) as missing_published_at,
    COUNT(CASE WHEN source IS NULL OR source = '' THEN 1 END) as missing_source,
    AVG(CHAR_LENGTH(headline)) as avg_headline_length,
    AVG(CHAR_LENGTH(body_text)) as avg_body_length
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
    AVG(CHAR_LENGTH(body_text)) as avg_body_length
FROM news_articles 
GROUP BY source, language, category
ORDER BY article_count DESC;