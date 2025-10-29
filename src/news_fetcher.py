"""
Refinitiv EIKON ニュース取得モジュール
Refinitiv EIKON APIガイドの標準パターンに基づく実装
"""
import os
import json
import time
import warnings
import re
from datetime import datetime, timedelta, timezone
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

try:
    from langdetect import detect, LangDetectException
except ImportError:
    print("Warning: langdetect module not found. Language detection will be disabled.")
    detect = None
    LangDetectException = Exception

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
    priority_score: int = 0
    
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
        self.filter_config = self.config.get('news_filtering', {})
    
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
            api_key = self.eikon_config.get('api_key') or os.getenv('REFINITIV_API_KEY')
            
            # デモモードチェック
            if api_key == 'DEMO_MODE':
                self.is_connected = True
                self.logger.info("デモモードで実行中")
                return True
            
            if not ek:
                raise ImportError("eikon module not available")
            
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
            
            # デモモードチェック
            if self.eikon_config.get('api_key') == 'DEMO_MODE':
                news_data = self._generate_demo_data(count_val, category)
                response_time = 0.1
            else:
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
                
                # ソースフィルタリング
                if self.filter_config.get('source_filtering', {}).get('enabled', False):
                    cleaned_data = self._filter_by_source(cleaned_data)
                
                # 本文取得（オプション）
                if fetch_body and not cleaned_data.empty:
                    cleaned_data = self._fetch_news_bodies(cleaned_data)
                
                # カテゴリ情報を自動検出・追加
                detected_categories = []
                
                # 各記事のヘッドラインと本文をチェックして複数カテゴリを検出
                for _, row in cleaned_data.iterrows():
                    categories_found = []
                    
                    # 1. 検索時に指定されたカテゴリを最優先で追加
                    if category:
                        categories_found.append(category)
                    
                    # 2. ヘッドラインと本文の両方を検索対象に
                    headline = str(row.get('text', '')).lower()
                    body = str(row.get('body_text', '')).lower() if row.get('body_text') else ''
                    full_text = headline + ' ' + body
                    
                    # 3. 改善されたキーワード検出（単語境界を考慮）
                    # 非鉄金属6種のキーワード検出
                    if re.search(r'\b(copper|銅)\b', full_text, re.IGNORECASE):
                        if 'COPPER' not in categories_found:
                            categories_found.append('COPPER')
                    if re.search(r'\b(aluminum|aluminium|アルミ)\b', full_text, re.IGNORECASE):
                        if 'ALUMINIUM' not in categories_found:
                            categories_found.append('ALUMINIUM')
                    if re.search(r'\b(zinc|亜鉛)\b', full_text, re.IGNORECASE):
                        if 'ZINC' not in categories_found:
                            categories_found.append('ZINC')
                    if re.search(r'\b(lead|鉛)\b', full_text, re.IGNORECASE):
                        if 'LEAD' not in categories_found:
                            categories_found.append('LEAD')
                    if re.search(r'\b(nickel|ニッケル)\b', full_text, re.IGNORECASE):
                        if 'NICKEL' not in categories_found:
                            categories_found.append('NICKEL')
                    if re.search(r'\b(tin|スズ|錫)\b', full_text, re.IGNORECASE):
                        if 'TIN' not in categories_found:
                            categories_found.append('TIN')
                    
                    # 基本カテゴリ3種
                    if re.search(r'\b(equity|equities|stock|stocks|share|shares|株式|株価)\b', full_text, re.IGNORECASE):
                        if 'EQUITY' not in categories_found:
                            categories_found.append('EQUITY')
                    if re.search(r'\b(forex|fx|currency|currencies|exchange rate|為替|外国為替|通貨)\b', full_text, re.IGNORECASE):
                        if 'FOREX' not in categories_found:
                            categories_found.append('FOREX')
                    if re.search(r'\b(commodity|commodities|商品|コモディティ)\b', full_text, re.IGNORECASE):
                        if 'COMMODITIES' not in categories_found:
                            categories_found.append('COMMODITIES')
                    
                    # 特別カテゴリ（ヘッドラインのみで判定）
                    if re.search(r'(ny市場サマリー|ＮＹ市場サマリー|ｎｙ市場サマリー|NY市場サマリー)', headline, re.IGNORECASE):
                        if 'NY_MARKET' not in categories_found:
                            categories_found.append('NY_MARKET')
                    
                    # カンマ区切りで結合（検索カテゴリが先頭）
                    detected_categories.append(','.join(categories_found) if categories_found else '')
                
                cleaned_data['detected_category'] = detected_categories
                
                # 優先度スコアリング
                if self.filter_config.get('priority_scoring', {}).get('enabled', False):
                    cleaned_data = self._calculate_priority_scores(cleaned_data)
                
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
        
        # カテゴリ別クエリマッピング（Refinitivのトピックコードを使用）
        category_queries = {
            # 非鉄金属6種 - より正確なクエリ
            'COPPER': 'Topic:COPP OR copper OR 銅',
            'ALUMINIUM': 'Topic:ALU OR aluminium OR aluminum OR アルミ',
            'ZINC': 'Topic:ZINC OR zinc OR 亜鉛',
            'LEAD': 'Topic:LEAD OR lead metal OR 鉛',
            'NICKEL': 'Topic:NI OR nickel OR ニッケル',
            'TIN': 'Topic:TIN OR tin OR スズ OR 錫',
            # 基本カテゴリ
            'EQUITY': 'Topic:EQU OR equity OR stock',
            'FOREX': 'Topic:FX OR forex OR foreign exchange',
            'COMMODITIES': 'Topic:COM OR commodity OR commodities',
            # 特別カテゴリ
            'NY_MARKET': '(ＮＹ市場サマリー OR NY市場 OR ニューヨーク市場)'
        }
        
        # カテゴリクエリを取得（カテゴリ未指定の場合はNoneを返す）
        if category:
            base_query = category_queries.get(category)
            if not base_query:
                self.logger.warning(f"未定義のカテゴリ: {category}")
                return None
        else:
            # カテゴリ未指定の場合は設定されたカテゴリのみを取得
            return None
        
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
                
                # レート制限（本文取得用）
                self._apply_rate_limit(is_body_fetch=True)
                
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
    
    def _apply_rate_limit(self, is_body_fetch=False):
        """レート制限適用"""
        if is_body_fetch:
            delay = self.eikon_config.get('body_fetch_delay', 5.0)
        else:
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
            # コピーを作成して、元のデータを保持
            data = data.copy()
            # datetime64ユニット指定によるエラーを回避
            # pandasの新しいバージョンでは、既にdatetime型の場合にエラーが発生する可能性があるため、
            # まず型をチェック
            if data[datetime_column].dtype == 'object':
                data.loc[:, datetime_column] = pd.to_datetime(data[datetime_column], utc=True, errors='coerce')
            elif 'datetime' not in str(data[datetime_column].dtype).lower():
                data.loc[:, datetime_column] = pd.to_datetime(data[datetime_column], utc=True, errors='coerce')
            # 既にdatetime型の場合は、UTCに変換するだけ
            elif data[datetime_column].dt.tz is None:
                data.loc[:, datetime_column] = data[datetime_column].dt.tz_localize('UTC')
            elif str(data[datetime_column].dt.tz) != 'UTC':
                data.loc[:, datetime_column] = data[datetime_column].dt.tz_convert('UTC')

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

                # 本文取得
                body_text = row.get('body_text')

                # 言語検出
                detected_language = self._detect_language(str(headline))

                # 緊急度判定
                detected_urgency = self._detect_urgency(str(headline), body_text)

                article = NewsArticle(
                    story_id=str(story_id),
                    headline=str(headline),
                    summary=None,  # Refinitivのget_news_headlinesには要約なし
                    body_text=body_text,  # 本文（取得している場合）
                    source=source,
                    published_at=published_at,
                    language=detected_language,  # 自動検出
                    category=row.get('detected_category'),  # 検出されたカテゴリ
                    urgency_level=detected_urgency,  # 自動判定
                    priority_score=row.get('priority_score', 0)  # 優先度スコア
                )
                
                articles.append(article)
                
            except Exception as e:
                self.logger.warning(f"記事オブジェクト作成エラー: {e}")
                continue
        
        self.logger.info(f"NewsArticle作成完了: {len(articles)}件")
        return articles

    def _detect_language(self, text: str) -> str:
        """
        テキストから言語を自動検出

        Args:
            text: 検出対象テキスト

        Returns:
            言語コード (en, ja, ko, es, fr, de, zh-cn, unknown など)
        """
        try:
            if detect is None:
                return 'unknown'

            if not text or len(text.strip()) < 3:
                return 'unknown'

            language = detect(text)
            return language

        except LangDetectException:
            self.logger.debug(f"言語検出失敗: {text[:50]}...")
            return 'unknown'
        except Exception as e:
            self.logger.warning(f"言語検出エラー: {e}")
            return 'unknown'

    def _detect_urgency(self, headline: str, body_text: str = None) -> int:
        """
        ヘッドラインと本文から緊急度を判定

        Args:
            headline: ヘッドライン
            body_text: 本文（オプション）

        Returns:
            緊急度レベル (1=高, 2=中, 3=通常)
        """
        # ヘッドラインと本文を結合
        text = (headline + ' ' + (body_text or '')).lower()

        # 高緊急度キーワード
        high_keywords = ['breaking', 'urgent', 'alert', 'flash', 'emergency', '速報', '緊急']
        if any(keyword in text for keyword in high_keywords):
            return 1

        # 中緊急度キーワード
        medium_keywords = ['important', 'significant', 'major', '重要', '重大']
        if any(keyword in text for keyword in medium_keywords):
            return 2

        # デフォルト: 通常
        return 3

    def get_api_stats(self) -> Dict[str, Any]:
        """API統計情報取得"""
        return self.api_logger.get_stats()
    
    def _filter_by_source(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        ソースによるフィルタリング
        
        Args:
            data: ニュースデータ
        
        Returns:
            フィルタリング済みデータ
        """
        if data.empty or 'sourceCode' not in data.columns:
            return data
        
        source_config = self.filter_config.get('source_filtering', {})
        reliable_sources = source_config.get('reliable_sources', [])
        excluded_sources = source_config.get('excluded_sources', [])
        
        original_count = len(data)
        
        # ソースコードをクリーンアップ（NS:RTRS → RTRS）
        data['clean_source'] = data['sourceCode'].apply(lambda x: x.split(':')[-1] if ':' in x else x)
        
        # 除外ソースをフィルタ
        for excluded in excluded_sources:
            data = data[~data['clean_source'].str.contains(excluded, case=False, na=False)]
        
        # 信頼できるソースのみを優先（オプション）
        # 現在は除外リストのみ適用し、信頼できるソースリストは参考情報として保持
        
        # 一時カラムを削除
        data = data.drop('clean_source', axis=1)
        
        filtered_count = len(data)
        if filtered_count < original_count:
            self.logger.info(f"ソースフィルタリング: {original_count} -> {filtered_count}件")
        
        return data
    
    def _calculate_priority_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        優先度スコアを計算
        
        Args:
            data: ニュースデータ
        
        Returns:
            スコア付きデータ
        """
        if data.empty:
            return data
        
        scoring_config = self.filter_config.get('priority_scoring', {})
        keywords_config = scoring_config.get('keywords', {})
        
        priority_scores = []
        
        for _, row in data.iterrows():
            score = 0
            # ヘッドラインと本文を結合してチェック
            text_to_check = str(row.get('text', '')).lower() + ' ' + str(row.get('body_text', '')).lower()
            
            # 各優先度レベルのキーワードをチェック
            for level, config in keywords_config.items():
                level_score = config.get('score', 0)
                terms = config.get('terms', [])
                
                for term in terms:
                    if term.lower() in text_to_check:
                        score += level_score
                        self.logger.debug(f"キーワード '{term}' マッチ: +{level_score}点")
            
            priority_scores.append(score)
        
        data['priority_score'] = priority_scores

        # カテゴリ別最小スコアでフィルタリング
        category_min_scores = scoring_config.get('category_minimum_scores', {})
        if category_min_scores and 'detected_category' in data.columns:
            original_count = len(data)

            # 各記事について、含まれるカテゴリの最小スコア要件を判定
            def get_required_score(detected_category):
                if not detected_category or detected_category == '':
                    return 0  # カテゴリなしの場合はグローバル最小スコア

                # カンマ区切りのカテゴリを分割
                categories = [cat.strip() for cat in str(detected_category).split(',')]

                # 含まれるカテゴリの中で最も高い最小スコア要件を取得
                max_required = 0
                for cat in categories:
                    required = category_min_scores.get(cat, 0)
                    if required > max_required:
                        max_required = required

                return max_required

            # 各記事の必要スコアを計算
            data['required_score'] = data['detected_category'].apply(get_required_score)

            # フィルタリング実行
            data = data[data['priority_score'] >= data['required_score']]

            # 一時カラムを削除
            data = data.drop('required_score', axis=1)

            filtered_count = len(data)
            if filtered_count < original_count:
                self.logger.info(f"カテゴリ別優先度フィルタリング: {original_count} -> {filtered_count}件")

        # グローバル最小スコアでフィルタリング（追加の保護）
        min_score = scoring_config.get('minimum_score', 0)
        if min_score > 0:
            original_count = len(data)
            data = data[data['priority_score'] >= min_score]
            filtered_count = len(data)
            if filtered_count < original_count:
                self.logger.info(f"グローバル優先度フィルタリング（最小スコア {min_score}）: {original_count} -> {filtered_count}件")

        # スコアでソート（降順）
        data = data.sort_values('priority_score', ascending=False)

        return data

    def fetch_headlines_paginated(self, per_page: int = 100, query: str = None,
                                  category: str = None, language: str = None,
                                  start_date: datetime = None, end_date: datetime = None,
                                  fetch_body: bool = True, max_pages: int = None) -> Optional[pd.DataFrame]:
        """
        ページネーション機能付きニュースヘッドライン取得（網羅的取得）

        指定した日付範囲のニュースを、per_page件ずつ取得してすべて結合します。

        Args:
            per_page: 1ページあたりの取得件数（デフォルト: 100）
            query: 検索クエリ（Refinitiv構文）
            category: カテゴリフィルタ
            language: 言語フィルタ
            start_date: 開始日時（必須）
            end_date: 終了日時（省略時は現在時刻）
            fetch_body: 本文も取得するか（デフォルト: True）
            max_pages: 最大ページ数（None=無制限、安全装置として使用）

        Returns:
            すべてのページを結合したニュースデータフレーム
        """
        if not self.is_connected:
            self.logger.error("EIKON未接続")
            return None

        if not start_date:
            self.logger.error("start_dateは必須です")
            return None

        # end_dateが未指定の場合は現在時刻
        if not end_date:
            end_date = datetime.now(timezone.utc)

        self.logger.info(f"ページネーション取得開始: {start_date} ～ {end_date}, per_page={per_page}")

        all_news_data = []
        current_start = start_date
        page_count = 0
        total_fetched = 0

        # 重複チェック用のstory_idセット
        seen_story_ids = set()

        while True:
            page_count += 1

            # 最大ページ数チェック（安全装置）
            if max_pages and page_count > max_pages:
                self.logger.warning(f"最大ページ数 {max_pages} に到達しました")
                break

            self.logger.info(f"ページ {page_count}: {current_start.strftime('%Y-%m-%d %H:%M:%S')} ～ {end_date.strftime('%Y-%m-%d %H:%M:%S')}")

            # このページのニュースを取得
            page_data = self.fetch_headlines_with_retry(
                count=per_page,
                query=query,
                category=category,
                language=language,
                start_date=current_start,
                end_date=end_date,
                fetch_body=fetch_body
            )

            # 取得失敗またはデータなし
            if page_data is None or page_data.empty:
                self.logger.info(f"ページ {page_count} でデータなし。取得終了。")
                break

            # 重複除外前のAPI取得件数を保存（終了判定用）
            api_returned_count = len(page_data)

            # 重複除去（既に取得済みのstory_idを除外）
            if 'storyId' in page_data.columns:
                original_count = len(page_data)
                page_data = page_data[~page_data['storyId'].isin(seen_story_ids)]
                # 新しいstory_idをセットに追加
                seen_story_ids.update(page_data['storyId'].tolist())
                duplicate_count = original_count - len(page_data)
                if duplicate_count > 0:
                    self.logger.info(f"重複除外: {duplicate_count}件（新規: {len(page_data)}件）")

            fetched_count = len(page_data)
            total_fetched += fetched_count

            self.logger.info(f"ページ {page_count}: {fetched_count}件取得（累計: {total_fetched}件）")

            # データを保存
            if not page_data.empty:
                all_news_data.append(page_data)

            # 終了条件チェック
            # 1. APIが返した件数がper_page未満 → これ以上データがない
            # 注意: 重複除外後ではなく、API取得件数で判定する
            if api_returned_count < per_page:
                self.logger.info(f"API取得件数が{per_page}件未満（{api_returned_count}件）。全データ取得完了。")
                break

            # 2. 次のページの開始日時を設定
            # versionCreated（発行日時）でソートして最後の日時を取得
            if 'versionCreated' in page_data.columns:
                # 日時でソート（降順：新しい順）
                page_data_sorted = page_data.sort_values('versionCreated', ascending=False)
                last_datetime = page_data_sorted.iloc[-1]['versionCreated']

                # 最後の記事の日時より1秒後を次のstart_dateに設定
                if isinstance(last_datetime, str):
                    last_datetime = pd.to_datetime(last_datetime, utc=True)
                elif hasattr(last_datetime, 'to_pydatetime'):
                    last_datetime = last_datetime.to_pydatetime()

                # 1秒追加（重複回避）
                current_start = last_datetime + timedelta(seconds=1)

                # current_startがend_dateを超えた場合は終了
                if current_start >= end_date:
                    self.logger.info(f"次の開始日時がend_dateを超えました。取得終了。")
                    break

                self.logger.debug(f"次ページ開始日時: {current_start.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                self.logger.warning("versionCreatedカラムが見つかりません。ページネーション継続不可。")
                break

        # すべてのページを結合
        if not all_news_data:
            self.logger.warning("取得されたニュースデータが空です")
            return pd.DataFrame()

        combined_data = pd.concat(all_news_data, ignore_index=True)

        # 最終的な重複除去（念のため）
        if 'storyId' in combined_data.columns:
            original_count = len(combined_data)
            combined_data = combined_data.drop_duplicates(subset=['storyId'])
            final_count = len(combined_data)
            if final_count < original_count:
                self.logger.info(f"最終重複除去: {original_count} -> {final_count}件")

        self.logger.info(f"ページネーション取得完了: 合計 {len(combined_data)}件（{page_count}ページ）")

        return combined_data

    def fetch_headlines_backfill(self, per_page: int = 100, query: str = None,
                                  category: str = None, language: str = None,
                                  start_date: datetime = None, end_date: datetime = None,
                                  fetch_body: bool = True, max_pages: int = None) -> Optional[pd.DataFrame]:
        """
        過去データのバックフィル専用ニュースヘッドライン取得（時系列を遡る）

        Refinitiv APIは最新データを優先して返すため、過去データを取得するには
        時系列を遡るページネーションが必要です。

        動作：
        1. end_date から開始して、最新データを取得
        2. 取得したページの最も古いタイムスタンプを次の end_date とする
        3. start_date に到達するまで繰り返す

        Args:
            per_page: 1ページあたりの取得件数（デフォルト: 100）
            query: 検索クエリ（Refinitiv構文）
            category: カテゴリフィルタ
            language: 言語フィルタ
            start_date: 開始日時（この日時まで遡る、必須）
            end_date: 終了日時（省略時は現在時刻、ここから遡り始める）
            fetch_body: 本文も取得するか（デフォルト: True）
            max_pages: 最大ページ数（None=無制限、安全装置として使用）

        Returns:
            すべてのページを結合したニュースデータフレーム
        """
        if not self.is_connected:
            self.logger.error("EIKON未接続")
            return None

        if not start_date:
            self.logger.error("start_dateは必須です")
            return None

        # end_dateが未指定の場合は現在時刻
        if not end_date:
            end_date = datetime.now(timezone.utc)

        self.logger.info(f"バックフィル取得開始: {start_date} ～ {end_date}, per_page={per_page}")

        all_news_data = []
        current_end = end_date  # 最新時刻から遡り始める
        page_count = 0
        total_fetched = 0

        # 重複チェック用のstory_idセット
        seen_story_ids = set()

        while True:
            page_count += 1

            # 最大ページ数チェック（安全装置）
            if max_pages and page_count > max_pages:
                self.logger.warning(f"最大ページ数 {max_pages} に到達しました")
                break

            self.logger.info(f"ページ {page_count}: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ～ {current_end.strftime('%Y-%m-%d %H:%M:%S')}")

            # このページのニュースを取得
            page_data = self.fetch_headlines_with_retry(
                count=per_page,
                query=query,
                category=category,
                language=language,
                start_date=start_date,
                end_date=current_end,
                fetch_body=fetch_body
            )

            # 取得失敗またはデータなし
            if page_data is None or page_data.empty:
                self.logger.info(f"ページ {page_count} でデータなし。取得終了。")
                break

            # 重複除外前のAPI取得件数を保存（終了判定用）
            api_returned_count = len(page_data)

            # 重複除去（既に取得済みのstory_idを除外）
            if 'storyId' in page_data.columns:
                original_count = len(page_data)
                page_data = page_data[~page_data['storyId'].isin(seen_story_ids)]
                # 新しいstory_idをセットに追加
                seen_story_ids.update(page_data['storyId'].tolist())
                duplicate_count = original_count - len(page_data)
                if duplicate_count > 0:
                    self.logger.info(f"重複除外: {duplicate_count}件（新規: {len(page_data)}件）")

            fetched_count = len(page_data)
            total_fetched += fetched_count

            self.logger.info(f"ページ {page_count}: {fetched_count}件取得（累計: {total_fetched}件）")

            # データを保存
            if not page_data.empty:
                all_news_data.append(page_data)

            # 終了条件チェック
            # 1. APIが返した件数がper_page未満 → これ以上データがない可能性
            if api_returned_count < per_page:
                self.logger.info(f"API取得件数が{per_page}件未満（{api_returned_count}件）。")
                # ただし、start_dateに到達していない場合は続行
                if current_end > start_date + timedelta(hours=1):
                    self.logger.info(f"start_dateまで遡り続けます。")
                else:
                    self.logger.info(f"start_date付近に到達しました。取得終了。")
                    break

            # 2. 次のページのend_dateを設定（時系列を遡る）
            # versionCreated（発行日時）でソートして最も古い日時を取得
            if 'versionCreated' in page_data.columns and not page_data.empty:
                # 日時でソート（昇順：古い順）
                page_data_sorted = page_data.sort_values('versionCreated', ascending=True)
                oldest_datetime = page_data_sorted.iloc[0]['versionCreated']

                # 最も古い記事の日時より1秒前を次のend_dateに設定（時系列を遡る）
                if isinstance(oldest_datetime, str):
                    oldest_datetime = pd.to_datetime(oldest_datetime, utc=True)
                elif hasattr(oldest_datetime, 'to_pydatetime'):
                    oldest_datetime = oldest_datetime.to_pydatetime()

                # 1秒前に設定（重複回避）
                next_end = oldest_datetime - timedelta(seconds=1)

                # next_endがstart_dateより前の場合は終了
                if next_end <= start_date:
                    self.logger.info(f"start_dateに到達しました。取得終了。")
                    break

                current_end = next_end
                self.logger.debug(f"次ページ終了日時: {current_end.strftime('%Y-%m-%d %H:%M:%S')}（時系列を遡る）")
            else:
                self.logger.warning("versionCreatedカラムが見つかりません。ページネーション継続不可。")
                break

        # すべてのページを結合
        if not all_news_data:
            self.logger.warning("取得されたニュースデータが空です")
            return pd.DataFrame()

        combined_data = pd.concat(all_news_data, ignore_index=True)

        # 最終的な重複除去（念のため）
        if 'storyId' in combined_data.columns:
            original_count = len(combined_data)
            combined_data = combined_data.drop_duplicates(subset=['storyId'])
            final_count = len(combined_data)
            if final_count < original_count:
                self.logger.info(f"最終重複除去: {original_count} -> {final_count}件")

        self.logger.info(f"バックフィル取得完了: 合計 {len(combined_data)}件（{page_count}ページ）")

        return combined_data