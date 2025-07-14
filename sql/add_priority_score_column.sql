-- 既存データベースにpriority_scoreカラムを追加するマイグレーションスクリプト

-- PostgreSQL版
-- 注: PostgreSQL環境で実行してください

-- 1. priority_scoreカラムを追加
ALTER TABLE news_articles 
ADD COLUMN IF NOT EXISTS priority_score INTEGER DEFAULT 0;

-- 2. インデックスを作成
CREATE INDEX IF NOT EXISTS idx_news_articles_priority_score 
ON news_articles(priority_score);

-- 3. カラムコメントを追加
COMMENT ON COLUMN news_articles.priority_score IS '優先度スコア（キーワードベース）';

-- 4. 既存データのpriority_scoreを0に設定（既にデフォルト値で設定されているはずですが念のため）
UPDATE news_articles 
SET priority_score = 0 
WHERE priority_score IS NULL;


-- SQL Server版
-- 注: SQL Server環境で実行してください（上記PostgreSQL版とは別に実行）
/*
-- カラムが存在しない場合のみ追加
IF NOT EXISTS (
    SELECT * FROM sys.columns 
    WHERE object_id = OBJECT_ID(N'news_articles') 
    AND name = 'priority_score'
)
BEGIN
    ALTER TABLE news_articles 
    ADD priority_score INT DEFAULT 0;
END

-- インデックスが存在しない場合のみ作成
IF NOT EXISTS (
    SELECT * FROM sys.indexes 
    WHERE name = 'idx_news_articles_priority_score' 
    AND object_id = OBJECT_ID(N'news_articles')
)
BEGIN
    CREATE INDEX idx_news_articles_priority_score 
    ON news_articles(priority_score);
END

-- 既存データのpriority_scoreを0に設定
UPDATE news_articles 
SET priority_score = 0 
WHERE priority_score IS NULL;
*/