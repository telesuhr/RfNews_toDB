"""
Refinitiv EIKON ニュース取得モジュール
Refinitiv EIKON APIガイドの標準パターンに基づく実装
"""
import os
import json
import time
import warnings
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import pandas as pd
from dataclasses import dataclass, asdict

# Refinitiv警告を抑制
warnings.filterwarnings('ignore')

try:
    import eikon as ek
except ImportError:
    print("Warning: eikon module not found. Some features may not work.")
    ek = None

from logger import setup_logger, APICallLogger

@dataclass
class NewsArticle:
    """ニュース記事データモデル"""
    story_id: str
    headline: str
    published_at: datetime
    summary: Optional[str] = None
    body_text: Optional[str] = None
    source: Optional[str] = None
    language: str = "en"
    category: Optional[str] = None
    urgency_level: int = 3
    
    def __post_init__(self):
        """データ検証"""
        if not self.story_id or self.story_id.strip() == "":
            raise ValueError("story_id is required")
        if not self.headline or self.headline.strip() == "":
            raise ValueError("headline is required")
        if not isinstance(self.published_at, datetime):
            raise ValueError("published_at must be datetime object")
    
    def to_dict(self) -> Dict[str, Any]:
        """辞書形式に変換"""
        return asdict(self)

class NewsFetcher:
    """Refinitiv EIKON ニュース取得クラス"""
    
    def __init__(self, config_path: str = "config/config.json"):
        """
        初期化
        
        Args:
            config_path: 設定ファイルパス
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = setup_logger('news_fetcher', config_path)
        self.api_logger = APICallLogger(self.logger)
        self.is_connected = False
        
        # 設定値の取得
        self.eikon_config = self.config.get('eikon', {})
        self.fetch_config = self.config.get('news_fetch', {})
        self.text_config = self.config.get('text_processing', {})
    
    def _load_config(self) -> Dict[str, Any]:
        """設定ファイル読み込み"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # デフォルト設定
                return {
                    'eikon': {
                        'api_key': os.getenv('REFINITIV_API_KEY', ''),
                        'timeout_seconds': 30,
                        'max_retries': 3,
                        'retry_delay_seconds': 2,
                        'rate_limit_delay': 1.0
                    },
                    'news_fetch': {
                        'default_count': 100,
                        'max_count': 500,
                        'languages': ['en', 'ja'],
                        'categories': ['TOP', 'EQUITY', 'FOREX', 'COMMODITY']
                    },
                    'text_processing': {
                        'max_headline_length': 500,
                        'duplicate_similarity_threshold': 0.85
                    }
                }
        except Exception as e:
            self.logger.error(f"設定ファイル読み込みエラー: {e}")
            raise
    
    def connect(self) -> bool:
        """
        Refinitiv EIKON APIに接続
        
        Returns:
            接続成功可否
        """
        try:
            if not ek:
                raise ImportError("eikon module not available")
            
            api_key = self.eikon_config.get('api_key') or os.getenv('REFINITIV_API_KEY')
            if not api_key:
                raise ValueError("API key not found")
            
            ek.set_app_key(api_key)
            self.is_connected = True
            self.logger.info("Refinitiv EIKON接続成功")
            return True
            
        except Exception as e:
            self.logger.error(f"Refinitiv EIKON接続エラー: {e}")
            self.is_connected = False
            return False
    
    def fetch_headlines(self, count: int = None, category: str = None, 
                       language: str = None, start_date: datetime = None,
                       end_date: datetime = None) -> Optional[pd.DataFrame]:
        """
        ニュースヘッドライン取得
        
        Args:
            count: 取得件数
            category: カテゴリフィルタ
            language: 言語フィルタ
            start_date: 開始日時
            end_date: 終了日時
        
        Returns:
            ニュースデータフレーム
        """
        if not self.is_connected:
            self.logger.error("EIKON未接続")
            return None
        
        try:
            # パラメータ設定
            params = {}
            count_val = count or self.fetch_config.get('default_count', 100)
            count_val = min(count_val, self.fetch_config.get('max_count', 500))
            
            # Refinitiv APIは基本的にcountのみをサポート
            # その他のフィルタは取得後に適用
            
            # レート制限適用
            self._apply_rate_limit()
            
            # API呼び出し
            start_time = time.time()
            news_data = ek.get_news_headlines(count=count_val)
            response_time = time.time() - start_time
            
            # ログ記録
            self.api_logger.log_api_call(
                method='get_news_headlines',
                params={'count': count_val},
                success=True,
                response_time=response_time
            )
            
            if news_data is not None and not news_data.empty:
                # データ検証・クリーニング
                cleaned_data = self._validate_and_clean_data(news_data)
                self.logger.info(f"ニュース取得成功: {len(cleaned_data)}件")
                return cleaned_data
            else:
                self.logger.warning("取得されたニュースデータが空です")
                return pd.DataFrame()
                
        except Exception as e:
            self.api_logger.log_api_call(
                method='get_news_headlines',
                params=params,
                success=False
            )
            self.logger.error(f"ニュース取得エラー: {e}")
            return None
    
    def fetch_headlines_with_retry(self, max_retries: int = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        リトライ機能付きニュースヘッドライン取得
        
        Args:
            max_retries: 最大リトライ回数
            **kwargs: fetch_headlines引数
        
        Returns:
            ニュースデータフレーム
        """
        max_retries = max_retries or self.eikon_config.get('max_retries', 3)
        retry_delay = self.eikon_config.get('retry_delay_seconds', 2)
        
        for attempt in range(max_retries):
            try:
                result = self.fetch_headlines(**kwargs)
                if result is not None:
                    return result
                    
            except Exception as e:
                self.api_logger.log_retry(attempt + 1, max_retries, str(e))
                
                if attempt < max_retries - 1:
                    # 指数バックオフ
                    delay = retry_delay * (2 ** attempt)
                    time.sleep(delay)
                else:
                    self.logger.error(f"リトライ回数上限に達しました: {max_retries}")
                    return None
        
        return None
    
    def _apply_rate_limit(self):
        """レート制限適用"""
        delay = self.eikon_config.get('rate_limit_delay', 1.0)
        self.api_logger.log_rate_limit(delay)
        time.sleep(delay)
    
    def _validate_and_clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        データ検証・クリーニング
        
        Args:
            data: 生データ
        
        Returns:
            クリーニング済みデータ
        """
        if data.empty:
            return data
        
        original_count = len(data)
        
        # 必須フィールドの確認
        required_fields = ['storyId', 'headline']
        for field in required_fields:
            if field in data.columns:
                data = data[data[field].notna()]
                data = data[data[field] != '']
        
        # 重複除去
        if 'storyId' in data.columns:
            data = data.drop_duplicates(subset=['storyId'])
        
        # テキスト長制限
        max_headline_length = self.text_config.get('max_headline_length', 500)
        if 'headline' in data.columns:
            data['headline'] = data['headline'].str.slice(0, max_headline_length)
        
        # 日時フォーマット統一
        if 'publishedAt' in data.columns:
            data = self._standardize_datetime(data, 'publishedAt')
        
        cleaned_count = len(data)
        if cleaned_count < original_count:
            self.logger.info(f"データクリーニング: {original_count} -> {cleaned_count}件")
        
        return data
    
    def _standardize_datetime(self, data: pd.DataFrame, datetime_column: str) -> pd.DataFrame:
        """日時フォーマット統一"""
        try:
            data[datetime_column] = pd.to_datetime(data[datetime_column], utc=True)
            # 無効な日時を除外
            data = data[data[datetime_column].notna()]
        except Exception as e:
            self.logger.warning(f"日時フォーマット変換エラー: {e}")
        
        return data
    
    def _filter_by_date(self, data: pd.DataFrame, start_date: datetime, 
                       end_date: datetime, date_column: str = 'publishedAt') -> pd.DataFrame:
        """日付範囲フィルタリング"""
        if date_column not in data.columns:
            return data
        
        # 日時列をdatetimeに変換
        data = self._standardize_datetime(data, date_column)
        
        # タイムゾーンを統一（比較対象もUTCに変換）
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=pd.Timestamp.now().tz_localize('UTC').tzinfo)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=pd.Timestamp.now().tz_localize('UTC').tzinfo)
        
        # 日付範囲フィルタ
        mask = (data[date_column] >= start_date) & (data[date_column] <= end_date)
        filtered_data = data[mask]
        
        self.logger.info(f"日付フィルタリング: {len(data)} -> {len(filtered_data)}件")
        return filtered_data
    
    def _remove_duplicates(self, data: pd.DataFrame, 
                          id_column: str = 'storyId') -> pd.DataFrame:
        """重複除去"""
        if id_column not in data.columns:
            return data
        
        original_count = len(data)
        unique_data = data.drop_duplicates(subset=[id_column])
        
        if len(unique_data) < original_count:
            self.logger.info(f"重複除去: {original_count} -> {len(unique_data)}件")
        
        return unique_data
    
    def create_news_articles(self, data: pd.DataFrame) -> List[NewsArticle]:
        """
        DataFrameからNewsArticleオブジェクトリストを作成
        
        Args:
            data: ニュースデータフレーム
        
        Returns:
            NewsArticleオブジェクトリスト
        """
        articles = []
        
        for _, row in data.iterrows():
            try:
                # Refinitivの実際のフィールド名に対応
                story_id = row.get('storyId')
                headline = row.get('text')  # textがヘッドラインに相当
                published_at = row.get('versionCreated')  # versionCreatedが発行日時
                
                if not all([story_id, headline, published_at]):
                    self.logger.debug(f"必須フィールド不足: story_id={story_id}, headline={headline}, published_at={published_at}")
                    continue
                
                # datetime変換
                if isinstance(published_at, str):
                    published_at = pd.to_datetime(published_at, utc=True).to_pydatetime()
                elif hasattr(published_at, 'to_pydatetime'):
                    published_at = published_at.to_pydatetime()
                
                # ソースコードを整形（NS:RTRS → Reuters）
                source_code = row.get('sourceCode', '')
                source = 'Reuters' if 'RTRS' in source_code else source_code
                
                article = NewsArticle(
                    story_id=str(story_id),
                    headline=str(headline),
                    summary=None,  # Refinitivのget_news_headlinesには要約なし
                    body_text=None,  # ヘッドライン取得なので本文なし
                    source=source,
                    published_at=published_at,
                    language='en',  # デフォルト
                    category=None,  # ヘッドライン取得時は不明
                    urgency_level=3  # デフォルト
                )
                
                articles.append(article)
                
            except Exception as e:
                self.logger.warning(f"記事オブジェクト作成エラー: {e}")
                continue
        
        self.logger.info(f"NewsArticle作成完了: {len(articles)}件")
        return articles
    
    def get_api_stats(self) -> Dict[str, Any]:
        """API統計情報取得"""
        return self.api_logger.get_stats()