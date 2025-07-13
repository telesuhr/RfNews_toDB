"""
ニュース取得機能のテスト（TDD）
実装前にテストを定義して期待される動作を明確化
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# テスト用のパス設定
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

class TestNewsFetcher:
    """ニュース取得クラスのテスト"""
    
    def test_fetcher_initialization(self):
        """ニュース取得クラスの初期化テスト"""
        from news_fetcher import NewsFetcher
        
        # 設定ファイルパスを指定して初期化
        fetcher = NewsFetcher(config_path="config/config.json")
        
        # 基本属性の存在確認
        assert hasattr(fetcher, 'config')
        assert hasattr(fetcher, 'logger')
        assert hasattr(fetcher, 'api_logger')
        
        # 初期状態の確認
        assert fetcher.config is not None
        assert fetcher.is_connected == False
    
    def test_eikon_connection(self):
        """Refinitiv EIKON接続テスト"""
        from news_fetcher import NewsFetcher
        
        with patch('eikon.set_app_key') as mock_set_key, \
             patch.dict('os.environ', {'REFINITIV_API_KEY': 'test_key'}):
            fetcher = NewsFetcher()
            
            # 接続成功ケース
            mock_set_key.return_value = None
            result = fetcher.connect()
            
            assert result == True
            assert fetcher.is_connected == True
            mock_set_key.assert_called_once_with('test_key')
    
    def test_eikon_connection_failure(self):
        """EIKON接続失敗テスト"""
        from news_fetcher import NewsFetcher
        
        with patch('eikon.set_app_key') as mock_set_key:
            mock_set_key.side_effect = Exception("API key invalid")
            
            fetcher = NewsFetcher()
            result = fetcher.connect()
            
            assert result == False
            assert fetcher.is_connected == False
    
    def test_fetch_news_headlines_success(self):
        """ニュースヘッドライン取得成功テスト"""
        from news_fetcher import NewsFetcher
        
        # モックデータの準備
        mock_news_data = pd.DataFrame({
            'storyId': ['001', '002', '003'],
            'headline': ['Market Update', 'Economic News', 'Company Report'],
            'summary': ['Summary 1', 'Summary 2', 'Summary 3'],
            'sourceCode': ['Reuters', 'Bloomberg', 'Reuters'],
            'publishedAt': ['2024-01-01T10:00:00Z', '2024-01-01T11:00:00Z', '2024-01-01T12:00:00Z'],
            'language': ['en', 'en', 'en']
        })
        
        with patch('eikon.get_news_headlines') as mock_get_news:
            mock_get_news.return_value = mock_news_data
            
            fetcher = NewsFetcher()
            fetcher.is_connected = True
            
            result = fetcher.fetch_headlines(count=10)
            
            # 結果の検証
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert 'storyId' in result.columns
            assert 'headline' in result.columns
            assert 'publishedAt' in result.columns
            
            # API呼び出しの確認
            mock_get_news.assert_called_once_with(count=10)
    
    def test_fetch_news_headlines_failure(self):
        """ニュースヘッドライン取得失敗テスト"""
        from news_fetcher import NewsFetcher
        
        with patch('eikon.get_news_headlines') as mock_get_news:
            mock_get_news.side_effect = Exception("API error")
            
            fetcher = NewsFetcher()
            fetcher.is_connected = True
            
            result = fetcher.fetch_headlines(count=10)
            
            # エラー時はNoneまたは空のDataFrameを返す
            assert result is None or result.empty
    
    def test_fetch_news_with_filters(self):
        """フィルタ付きニュース取得テスト"""
        from news_fetcher import NewsFetcher
        
        mock_news_data = pd.DataFrame({
            'storyId': ['001', '002'],
            'headline': ['Market Update', 'Economic News'],
            'category': ['EQUITY', 'FOREX'],
            'language': ['en', 'en']
        })
        
        with patch('eikon.get_news_headlines') as mock_get_news:
            mock_get_news.return_value = mock_news_data
            
            fetcher = NewsFetcher()
            fetcher.is_connected = True
            
            # カテゴリフィルタテスト
            result = fetcher.fetch_headlines(
                count=50,
                category='EQUITY',
                language='en'
            )
            
            assert isinstance(result, pd.DataFrame)
            mock_get_news.assert_called_once()
    
    def test_rate_limiting(self):
        """レート制限テスト"""
        from news_fetcher import NewsFetcher
        
        with patch('time.sleep') as mock_sleep:
            fetcher = NewsFetcher()
            
            # レート制限適用
            fetcher._apply_rate_limit()
            
            # sleep が呼ばれることを確認
            mock_sleep.assert_called_once()
            # デフォルトの遅延時間（1秒）を確認
            args = mock_sleep.call_args[0]
            assert args[0] >= 0.5  # 最低0.5秒の遅延
    
    def test_retry_mechanism(self):
        """リトライ機能テスト"""
        from news_fetcher import NewsFetcher
        
        with patch('eikon.get_news_headlines') as mock_get_news:
            # 最初の2回は失敗、3回目は成功
            mock_get_news.side_effect = [
                Exception("Network error"),
                Exception("Timeout"),
                pd.DataFrame({'storyId': ['001'], 'headline': ['Success']})
            ]
            
            with patch('time.sleep'):  # リトライ待機をスキップ
                fetcher = NewsFetcher()
                fetcher.is_connected = True
                
                result = fetcher.fetch_headlines_with_retry(count=10, max_retries=3)
                
                # 最終的に成功すること
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 1
                assert result.iloc[0]['headline'] == 'Success'
                
                # 3回呼び出されることを確認
                assert mock_get_news.call_count == 3
    
    def test_retry_exhausted(self):
        """リトライ回数上限テスト"""
        from news_fetcher import NewsFetcher
        
        with patch('eikon.get_news_headlines') as mock_get_news:
            # 常に失敗
            mock_get_news.side_effect = Exception("Persistent error")
            
            with patch('time.sleep'):
                fetcher = NewsFetcher()
                fetcher.is_connected = True
                
                result = fetcher.fetch_headlines_with_retry(count=10, max_retries=2)
                
                # すべてのリトライが失敗した場合
                assert result is None
                assert mock_get_news.call_count == 2
    
    def test_data_validation(self):
        """取得データの検証テスト"""
        from news_fetcher import NewsFetcher
        
        # 不正なデータを含むモック
        invalid_data = pd.DataFrame({
            'storyId': ['001', None, '003'],  # None値を含む
            'headline': ['Valid', '', 'Another Valid'],  # 空文字を含む
            'publishedAt': ['2024-01-01T10:00:00Z', 'invalid-date', '2024-01-01T12:00:00Z']
        })
        
        fetcher = NewsFetcher()
        
        # データ検証メソッドのテスト
        cleaned_data = fetcher._validate_and_clean_data(invalid_data)
        
        # 無効なレコードが除外されること
        assert len(cleaned_data) < len(invalid_data)
        # 有効なデータのみ残ること
        assert all(cleaned_data['storyId'].notna())
        assert all(cleaned_data['headline'] != '')
    
    def test_date_filtering(self):
        """日付フィルタリングテスト"""
        from news_fetcher import NewsFetcher
        import pytz
        
        # 異なる日付のニュースデータ
        date_data = pd.DataFrame({
            'storyId': ['001', '002', '003'],
            'headline': ['Old News', 'Recent News', 'Future News'],
            'publishedAt': [
                '2024-01-01T10:00:00Z',
                '2024-01-15T10:00:00Z', 
                '2024-02-01T10:00:00Z'
            ]
        })
        
        fetcher = NewsFetcher()
        
        # 特定期間のフィルタリング（UTC タイムゾーン付き）
        start_date = datetime(2024, 1, 10, tzinfo=pytz.UTC)
        end_date = datetime(2024, 1, 20, tzinfo=pytz.UTC)
        
        filtered_data = fetcher._filter_by_date(date_data, start_date, end_date)
        
        # 期間内のデータのみ残ること
        assert len(filtered_data) == 1
        assert filtered_data.iloc[0]['headline'] == 'Recent News'
    
    def test_duplicate_detection(self):
        """重複記事検出テスト"""
        from news_fetcher import NewsFetcher
        
        # 重複を含むデータ
        duplicate_data = pd.DataFrame({
            'storyId': ['001', '002', '001'],  # storyId重複
            'headline': ['News A', 'News B', 'News A'],
            'summary': ['Summary A', 'Summary B', 'Summary A']
        })
        
        fetcher = NewsFetcher()
        
        # 重複除去
        unique_data = fetcher._remove_duplicates(duplicate_data)
        
        # 重複が除去されること
        assert len(unique_data) == 2
        assert len(unique_data['storyId'].unique()) == 2

class TestNewsDataModel:
    """ニュースデータモデルのテスト"""
    
    def test_news_article_creation(self):
        """ニュース記事データモデル作成テスト"""
        from news_fetcher import NewsArticle
        
        # 記事データの作成
        article = NewsArticle(
            story_id="TEST001",
            headline="Test Headline",
            summary="Test Summary",
            body_text="Test Body",
            source="Reuters",
            published_at=datetime.now(),
            language="en",
            category="EQUITY"
        )
        
        # 属性の確認
        assert article.story_id == "TEST001"
        assert article.headline == "Test Headline"
        assert article.source == "Reuters"
        assert article.language == "en"
    
    def test_news_article_validation(self):
        """ニュース記事データ検証テスト"""
        from news_fetcher import NewsArticle
        
        # 必須フィールドが欠如している場合のテスト
        with pytest.raises(ValueError):
            NewsArticle(
                story_id="",  # 空のstory_id
                headline="Test",
                published_at=datetime.now()
            )
        
        with pytest.raises(ValueError):
            NewsArticle(
                story_id="TEST001",
                headline="",  # 空のheadline
                published_at=datetime.now()
            )
    
    def test_news_article_to_dict(self):
        """ニュース記事辞書変換テスト"""
        from news_fetcher import NewsArticle
        
        article = NewsArticle(
            story_id="TEST001",
            headline="Test Headline",
            published_at=datetime(2024, 1, 1, 10, 0, 0)
        )
        
        article_dict = article.to_dict()
        
        # 辞書形式への変換確認
        assert isinstance(article_dict, dict)
        assert article_dict['story_id'] == "TEST001"
        assert article_dict['headline'] == "Test Headline"
        assert 'published_at' in article_dict