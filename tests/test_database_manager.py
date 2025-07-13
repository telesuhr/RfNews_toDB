"""
データベース操作のテスト（TDD）
実装前にテストを定義して期待される動作を明確化
"""
import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# テスト用のパス設定
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))

class TestDatabaseManager:
    """データベース管理クラスのテスト"""
    
    def test_database_manager_initialization(self):
        """データベース管理クラスの初期化テスト"""
        from database_manager import DatabaseManager
        
        # 設定ファイルパスを指定して初期化
        db_manager = DatabaseManager(config_path="config/config.json")
        
        # 基本属性の存在確認
        assert hasattr(db_manager, 'config')
        assert hasattr(db_manager, 'logger')
        assert hasattr(db_manager, 'db_config')
        
        # 初期状態の確認
        assert db_manager.config is not None
        assert db_manager.is_connected == False
    
    def test_database_connection(self):
        """データベース接続テスト"""
        from database_manager import DatabaseManager
        
        with patch.object(DatabaseManager, '_load_config', return_value={}), \
             patch('database_manager.DatabaseConfig') as mock_db_config_class:
            
            # DatabaseConfigインスタンスのモック
            mock_db_config = MagicMock()
            mock_db_config.test_connection.return_value = True
            mock_db_config_class.return_value = mock_db_config
            
            db_manager = DatabaseManager()
            result = db_manager.connect()
            
            assert result == True
            assert db_manager.is_connected == True
            mock_db_config.test_connection.assert_called_once()
    
    def test_database_connection_failure(self):
        """データベース接続失敗テスト"""
        from database_manager import DatabaseManager
        
        with patch.object(DatabaseManager, '_load_config', return_value={}), \
             patch('database_manager.DatabaseConfig') as mock_db_config_class:
            
            mock_db_config = MagicMock()
            mock_db_config.test_connection.return_value = False
            mock_db_config_class.return_value = mock_db_config
            
            db_manager = DatabaseManager()
            result = db_manager.connect()
            
            assert result == False
            assert db_manager.is_connected == False
    
    def test_create_tables(self):
        """テーブル作成テスト"""
        from database_manager import DatabaseManager
        
        with patch('config.database.DatabaseConfig.execute_sql_file') as mock_execute:
            mock_execute.return_value = True
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.create_tables()
            
            assert result == True
            mock_execute.assert_called_once_with('sql/create_tables.sql')
    
    def test_insert_news_article_success(self):
        """ニュース記事挿入成功テスト"""
        from database_manager import DatabaseManager
        from news_fetcher import NewsArticle
        
        # テスト用記事データ
        article = NewsArticle(
            story_id="TEST001",
            headline="Test Headline",
            summary="Test Summary",
            published_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        )
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.insert_news_article(article)
            
            assert result == True
            # データベースセッションが使われることを確認
            mock_db_session.add.assert_called_once()
            mock_db_session.commit.assert_called_once()
    
    def test_insert_news_article_duplicate(self):
        """重複記事挿入テスト（既存のstory_idの場合）"""
        from database_manager import DatabaseManager
        from news_fetcher import NewsArticle
        from sqlalchemy.exc import IntegrityError
        
        article = NewsArticle(
            story_id="DUPLICATE001",
            headline="Duplicate Headline",
            published_at=datetime.now(timezone.utc)
        )
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            # IntegrityError（重複エラー）をシミュレート
            mock_db_session.commit.side_effect = IntegrityError("duplicate", None, None)
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.insert_news_article(article)
            
            # 重複の場合はFalseを返すがエラーにはならない
            assert result == False
            mock_db_session.rollback.assert_called_once()
    
    def test_bulk_insert_news_articles(self):
        """ニュース記事一括挿入テスト"""
        from database_manager import DatabaseManager
        from news_fetcher import NewsArticle
        
        # 複数の記事データ
        articles = [
            NewsArticle(
                story_id=f"TEST{i:03d}",
                headline=f"Test Headline {i}",
                published_at=datetime.now(timezone.utc)
            )
            for i in range(1, 6)
        ]
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.bulk_insert_news_articles(articles)
            
            assert result['success'] == True
            assert result['inserted_count'] == 5
            assert result['failed_count'] == 0
            
            # 5回add（各記事ごと）、1回commit（トランザクション）
            assert mock_db_session.add.call_count == 5
            mock_db_session.commit.assert_called_once()
    
    def test_bulk_insert_with_partial_failure(self):
        """一括挿入での部分失敗テスト"""
        from database_manager import DatabaseManager
        from news_fetcher import NewsArticle
        
        articles = [
            NewsArticle(story_id="SUCCESS001", headline="Success", published_at=datetime.now(timezone.utc)),
            NewsArticle(story_id="FAIL001", headline="Fail", published_at=datetime.now(timezone.utc)),
            NewsArticle(story_id="SUCCESS002", headline="Success", published_at=datetime.now(timezone.utc))
        ]
        
        with patch.object(DatabaseManager, 'insert_news_article') as mock_insert:
            # 2番目の記事だけ失敗
            mock_insert.side_effect = [True, False, True]
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.bulk_insert_news_articles(articles)
            
            assert result['success'] == True  # 部分成功
            assert result['inserted_count'] == 2
            assert result['failed_count'] == 1
            assert mock_insert.call_count == 3
    
    def test_get_news_by_story_id(self):
        """ストーリーIDによるニュース記事取得テスト"""
        from database_manager import DatabaseManager
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            # モック結果を設定
            mock_article = MagicMock()
            mock_article.story_id = "TEST001"
            mock_article.headline = "Test Headline"
            mock_db_session.query.return_value.filter.return_value.first.return_value = mock_article
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.get_news_by_story_id("TEST001")
            
            assert result is not None
            assert result.story_id == "TEST001"
            assert result.headline == "Test Headline"
    
    def test_get_news_by_date_range(self):
        """日付範囲によるニュース記事取得テスト"""
        from database_manager import DatabaseManager
        
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            # モック結果リストを設定
            mock_articles = [MagicMock() for _ in range(3)]
            for i, article in enumerate(mock_articles):
                article.story_id = f"TEST{i:03d}"
                article.headline = f"Test Headline {i}"
            
            mock_db_session.query.return_value.filter.return_value.all.return_value = mock_articles
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.get_news_by_date_range(start_date, end_date)
            
            assert len(result) == 3
            assert all(hasattr(article, 'story_id') for article in result)
    
    def test_update_news_article(self):
        """ニュース記事更新テスト"""
        from database_manager import DatabaseManager
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            # 既存記事のモック
            mock_article = MagicMock()
            mock_article.story_id = "TEST001"
            mock_article.headline = "Old Headline"
            mock_db_session.query.return_value.filter.return_value.first.return_value = mock_article
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            # 更新データ
            update_data = {
                'headline': 'Updated Headline',
                'summary': 'Updated Summary'
            }
            
            result = db_manager.update_news_article("TEST001", update_data)
            
            assert result == True
            # 属性が更新されることを確認
            assert mock_article.headline == 'Updated Headline'
            assert mock_article.summary == 'Updated Summary'
            mock_db_session.commit.assert_called_once()
    
    def test_delete_news_article(self):
        """ニュース記事削除テスト"""
        from database_manager import DatabaseManager
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            # 削除対象記事のモック
            mock_article = MagicMock()
            mock_db_session.query.return_value.filter.return_value.first.return_value = mock_article
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.delete_news_article("TEST001")
            
            assert result == True
            mock_db_session.delete.assert_called_once_with(mock_article)
            mock_db_session.commit.assert_called_once()
    
    def test_get_news_statistics(self):
        """ニュース統計情報取得テスト"""
        from database_manager import DatabaseManager
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            # 統計クエリの結果をモック
            mock_stats = MagicMock()
            mock_stats.total_articles = 100
            mock_stats.sources_count = 5
            mock_stats.latest_article = datetime.now(timezone.utc)
            mock_db_session.execute.return_value.fetchone.return_value = mock_stats
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.get_news_statistics()
            
            assert result is not None
            assert 'total_articles' in result
            assert 'sources_count' in result
            assert 'latest_article' in result
    
    def test_cleanup_old_articles(self):
        """古い記事のクリーンアップテスト"""
        from database_manager import DatabaseManager
        
        cutoff_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            # 削除された行数をモック
            mock_db_session.query.return_value.filter.return_value.delete.return_value = 10
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.cleanup_old_articles(cutoff_date)
            
            assert result == 10  # 削除された記事数
            mock_db_session.commit.assert_called_once()

class TestNewsArticleORM:
    """ニュース記事ORMモデルのテスト"""
    
    def test_news_article_orm_creation(self):
        """ニュース記事ORMモデル作成テスト"""
        from database_manager import NewsArticleORM
        
        # ORMオブジェクトの作成
        article_orm = NewsArticleORM(
            story_id="TEST001",
            headline="Test Headline",
            summary="Test Summary",
            source="Reuters",
            published_at=datetime.now(timezone.utc),
            language="en"
        )
        
        # 属性の確認
        assert article_orm.story_id == "TEST001"
        assert article_orm.headline == "Test Headline"
        assert article_orm.source == "Reuters"
        assert article_orm.language == "en"
    
    def test_news_article_orm_to_dict(self):
        """ニュース記事ORM辞書変換テスト"""
        from database_manager import NewsArticleORM
        
        article_orm = NewsArticleORM(
            story_id="TEST001",
            headline="Test Headline",
            published_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        )
        
        article_dict = article_orm.to_dict()
        
        # 辞書形式への変換確認
        assert isinstance(article_dict, dict)
        assert article_dict['story_id'] == "TEST001"
        assert article_dict['headline'] == "Test Headline"
        assert 'published_at' in article_dict
    
    def test_news_article_orm_validation(self):
        """ニュース記事ORM検証テスト"""
        from database_manager import NewsArticleORM
        
        # 有効なデータでの作成
        valid_article = NewsArticleORM(
            story_id="VALID001",
            headline="Valid Headline",
            published_at=datetime.now(timezone.utc)
        )
        
        # 検証メソッドの実行（例外が発生しないことを確認）
        try:
            valid_article.validate()
            validation_passed = True
        except ValueError:
            validation_passed = False
        
        assert validation_passed == True

class TestFetchLogManager:
    """取得ログ管理のテスト"""
    
    def test_start_fetch_log(self):
        """取得ログ開始テスト"""
        from database_manager import DatabaseManager
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            log_id = db_manager.start_fetch_log()
            
            assert log_id is not None
            mock_db_session.add.assert_called_once()
            mock_db_session.commit.assert_called_once()
    
    def test_complete_fetch_log(self):
        """取得ログ完了テスト"""
        from database_manager import DatabaseManager
        
        with patch('config.database.DatabaseConfig.get_session') as mock_session:
            mock_db_session = MagicMock()
            mock_session.return_value.__enter__.return_value = mock_db_session
            
            # 既存ログのモック
            mock_log = MagicMock()
            mock_db_session.query.return_value.filter.return_value.first.return_value = mock_log
            
            db_manager = DatabaseManager()
            db_manager.is_connected = True
            
            result = db_manager.complete_fetch_log(
                log_id=1,
                articles_fetched=100,
                articles_inserted=95,
                articles_updated=5
            )
            
            assert result == True
            # ログ情報が更新されることを確認
            assert mock_log.status == 'completed'
            assert mock_log.articles_fetched == 100
            assert mock_log.articles_inserted == 95
            mock_db_session.commit.assert_called_once()