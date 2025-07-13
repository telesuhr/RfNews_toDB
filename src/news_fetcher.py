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
    
    def fetch_headlines(self, count: int = None, query: str = None, 
                       category: str = None, language: str = None, 
                       start_date: datetime = None, end_date: datetime = None,
                       fetch_body: bool = True) -> Optional[pd.DataFrame]:
        """
        ニュースヘッドライン取得（本文取得がデフォルト）
        
        Args:
            count: 取得件数
            query: 検索クエリ（Refinitiv構文）
            category: カテゴリフィルタ（メタル、商品等）
            language: 言語フィルタ
            start_date: 開始日時
            end_date: 終了日時
            fetch_body: 本文も取得するか（デフォルト: True）
        
        Returns:
            ニュースデータフレーム
        """
        if not self.is_connected:
            self.logger.error("EIKON未接続")
            return None
        
        try:
            # パラメータ設定
            count_val = count or self.fetch_config.get('default_count', 100)
            count_val = min(count_val, self.fetch_config.get('max_count', 500))
            
            # クエリ構築
            search_query = self._build_search_query(query, category, language)
            
            # パラメータ辞書構築
            params = {
                'query': search_query,
                'count': count_val
            }
            
            # 日付範囲設定
            if start_date:
                params['date_from'] = start_date.strftime('%Y-%m-%dT%H:%M:%S')
            if end_date:
                params['date_to'] = end_date.strftime('%Y-%m-%dT%H:%M:%S')
            
            # レート制限適用
            self._apply_rate_limit()
            
            # API呼び出し
            start_time = time.time()
            news_data = ek.get_news_headlines(**params)
            response_time = time.time() - start_time
            
            # ログ記録
            self.api_logger.log_api_call(
                method='get_news_headlines',
                params=params,
                success=True,
                response_time=response_time
            )
            
            if news_data is not None and not news_data.empty:
                # データ検証・クリーニング
                cleaned_data = self._validate_and_clean_data(news_data)
                
                # 本文取得（オプション）
                if fetch_body and not cleaned_data.empty:
                    cleaned_data = self._fetch_news_bodies(cleaned_data)
                
                # カテゴリ情報を自動検出・追加
                detected_categories = []
                
                # 各記事のヘッドラインと本文をチェックして複数カテゴリを検出
                for _, row in cleaned_data.iterrows():
                    headline = str(row.get('text', '')).lower()
                    categories_found = []
                    
                    # 非鉄金属6種のキーワード検出
                    if 'copper' in headline or '銅' in headline:
                        categories_found.append('COPPER')
                    if 'aluminum' in headline or 'aluminium' in headline or 'アルミ' in headline:
                        categories_found.append('ALUMINIUM')
                    if 'zinc' in headline or '亜鉛' in headline:
                        categories_found.append('ZINC')
                    if 'lead' in headline or '鉛' in headline:
                        categories_found.append('LEAD')
                    if 'nickel' in headline or 'ニッケル' in headline:
                        categories_found.append('NICKEL')
                    if 'tin' in headline or 'スズ' in headline:
                        categories_found.append('TIN')
                    
                    # 基本カテゴリ3種
                    if 'equity' in headline or '株式' in headline or 'stock' in headline:
                        categories_found.append('EQUITY')
                    if 'forex' in headline or '外国為替' in headline or 'currency' in headline:
                        categories_found.append('FOREX')
                    if 'commodity' in headline or 'commodities' in headline or '商品' in headline:
                        categories_found.append('COMMODITIES')
                    
                    # 特別カテゴリ  
                    if 'ny市場サマリー' in headline or 'ＮＹ市場サマリー' in headline or 'ｎｙ市場サマリー' in headline:
                        categories_found.append('NY_MARKET')
                    
                    # 指定されたカテゴリも追加（重複チェック）
                    if category and category not in categories_found:
                        categories_found.append(category)
                    
                    # カンマ区切りで結合
                    detected_categories.append(','.join(categories_found) if categories_found else '')
                
                cleaned_data['detected_category'] = detected_categories
                
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
    
    def _build_search_query(self, query: str = None, category: str = None, 
                           language: str = None) -> str:
        """
        Refinitiv検索クエリを構築
        
        Args:
            query: カスタムクエリ
            category: カテゴリ
            language: 言語
        
        Returns:
            検索クエリ文字列
        """
        if query:
            return query
        
        # カテゴリ別クエリマッピング（簡素化版）
        category_queries = {
            # 非鉄金属6種
            'COPPER': 'copper OR "copper prices"',
            'ALUMINIUM': 'aluminum OR aluminium',
            'ZINC': 'zinc',
            'LEAD': 'lead',
            'NICKEL': 'nickel',
            'TIN': 'tin',
            # 基本カテゴリ3種
            'EQUITY': 'Topic:EQUITY',
            'FOREX': 'Topic:FOREX',
            'COMMODITIES': 'commodities OR metals',
            # 特別カテゴリ
            'NY_MARKET': 'NY市場サマリー OR ＮＹ市場サマリー'
        }
        
        # カテゴリクエリを取得
        base_query = category_queries.get(category, 'Topic:TOPALL')
        
        # 言語フィルタを追加（シンプルなクエリに変更）
        if language:
            lang_map = {'en': 'LEN', 'ja': 'LJA'}
            lang_code = lang_map.get(language, 'LEN')
            base_query = f'{base_query} AND Language:{lang_code}'
        # デフォルトでは言語フィルタを適用しない（より柔軟な検索のため）
        
        return base_query
    
    def _fetch_news_bodies(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        ニュース本文を取得
        
        Args:
            data: ヘッドラインデータ
        
        Returns:
            本文付きデータ
        """
        self.logger.info(f"本文取得開始: {len(data)}件")
        
        body_texts = []
        for _, row in data.iterrows():
            try:
                story_id = row['storyId']
                
                # レート制限
                self._apply_rate_limit()
                
                # 本文取得
                body = ek.get_news_story(story_id)
                
                if body:
                    # HTMLタグを除去
                    clean_body = self._clean_html(str(body))
                    body_texts.append(clean_body)
                    self.logger.debug(f"本文取得成功: {story_id} ({len(clean_body)}文字)")
                else:
                    body_texts.append(None)
                    self.logger.debug(f"本文取得失敗: {story_id}")
                    
            except Exception as e:
                self.logger.warning(f"本文取得エラー {story_id}: {e}")
                body_texts.append(None)
        
        # 本文をデータフレームに追加
        data = data.copy()
        data['body_text'] = body_texts
        
        self.logger.info(f"本文取得完了: {len([b for b in body_texts if b])}件成功")
        return data
    
    def _clean_html(self, html_text: str) -> str:
        """
        HTMLタグを除去してプレーンテキストを取得
        
        Args:
            html_text: HTML文字列
        
        Returns:
            クリーンなテキスト
        """
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_text, 'html.parser')
            
            # テキストのみ抽出
            text = soup.get_text()
            
            # 余分な空白を除去
            import re
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text
        except Exception as e:
            self.logger.warning(f"HTML クリーニングエラー: {e}")
            return html_text
    
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
            # datetime64ユニット指定によるエラーを回避
            data[datetime_column] = pd.to_datetime(data[datetime_column], utc=True, errors='coerce')
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
                    body_text=row.get('body_text'),  # 本文（取得している場合）
                    source=source,
                    published_at=published_at,
                    language='en',  # デフォルト
                    category=row.get('detected_category'),  # 検出されたカテゴリ
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