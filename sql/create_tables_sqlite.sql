-- SQLite用 Refinitiv ニュース記事テーブル
CREATE TABLE IF NOT EXISTS news_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    story_id TEXT UNIQUE NOT NULL,
    headline TEXT NOT NULL,
    summary TEXT,
    body_text TEXT,
    source TEXT,
    published_at DATETIME NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    language TEXT DEFAULT 'en',
    category TEXT,
    urgency_level INTEGER DEFAULT 3
);

-- インデックス作成
CREATE INDEX IF NOT EXISTS idx_story_id ON news_articles(story_id);
CREATE INDEX IF NOT EXISTS idx_published_at ON news_articles(published_at);
CREATE INDEX IF NOT EXISTS idx_source ON news_articles(source);
CREATE INDEX IF NOT EXISTS idx_category ON news_articles(category);
CREATE INDEX IF NOT EXISTS idx_created_at ON news_articles(created_at);

-- ニュース記事関連銘柄リンクテーブル
CREATE TABLE IF NOT EXISTS news_rics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id INTEGER NOT NULL,
    ric_code TEXT NOT NULL,
    relevance_score REAL DEFAULT 1.00,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (news_id) REFERENCES news_articles(id) ON DELETE CASCADE
);

-- インデックス作成
CREATE INDEX IF NOT EXISTS idx_news_rics_ric_code ON news_rics(ric_code);
CREATE INDEX IF NOT EXISTS idx_news_rics_relevance_score ON news_rics(relevance_score);
CREATE UNIQUE INDEX IF NOT EXISTS unique_news_ric ON news_rics(news_id, ric_code);

-- ニュース取得ログテーブル
CREATE TABLE IF NOT EXISTS news_fetch_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fetch_start DATETIME NOT NULL,
    fetch_end DATETIME,
    articles_fetched INTEGER DEFAULT 0,
    articles_inserted INTEGER DEFAULT 0,
    articles_updated INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    error_message TEXT,
    api_calls INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- インデックス作成
CREATE INDEX IF NOT EXISTS idx_fetch_log_start ON news_fetch_log(fetch_start);
CREATE INDEX IF NOT EXISTS idx_fetch_log_status ON news_fetch_log(status);