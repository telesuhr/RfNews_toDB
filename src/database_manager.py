"""
データベース管理モジュール
ニュース記事のCRUD操作とデータベース管理機能を提供
"""
import os
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Union
from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime, DECIMAL, Index
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import enum
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# 設定とログ
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
try:
    from database import DatabaseConfig, Base
except ImportError:
    # テスト実行時の代替パス
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from config.database import DatabaseConfig, Base

from logger import setup_logger, DatabaseLogger
from news_fetcher import NewsArticle

class FetchStatus(enum.Enum):
    """取得ステータス"""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class NewsArticleORM(Base):
    """ニュース記事ORMモデル"""
    __tablename__ = 'news_articles'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    story_id = Column(String(100), unique=True, nullable=False, index=True)
    headline = Column(Text, nullable=False)
    summary = Column(Text)
    body_text = Column(Text)
    source = Column(String(100), index=True)
    published_at = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    language = Column(String(10), default='en')
    category = Column(String(50), index=True)
    urgency_level = Column(Integer, default=3)
    priority_score = Column(Integer, default=0, index=True)
    
    def __repr__(self):
        return f"<NewsArticle(story_id='{self.story_id}', headline='{self.headline[:50]}...')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """辞書形式に変換"""
        return {
            'id': self.id,
            'story_id': self.story_id,
            'headline': self.headline,
            'summary': self.summary,
            'body_text': self.body_text,
            'source': self.source,
            'published_at': self.published_at,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'language': self.language,
            'category': self.category,
            'urgency_level': self.urgency_level,
            'priority_score': self.priority_score
        }
    
    def validate(self):
        """データ検証"""
        if not self.story_id or self.story_id.strip() == "":
            raise ValueError("story_id is required")
        if not self.headline or self.headline.strip() == "":
            raise ValueError("headline is required")
        if not self.published_at:
            raise ValueError("published_at is required")

class NewsRicORM(Base):
    """ニュース記事関連銘柄ORMモデル"""
    __tablename__ = 'news_rics'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    news_id = Column(BigInteger, nullable=False, index=True)
    ric_code = Column(String(50), nullable=False, index=True)
    relevance_score = Column(DECIMAL(3, 2), default=1.00)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class NewsFetchLogORM(Base):
    """ニュース取得ログORMモデル"""
    __tablename__ = 'news_fetch_log'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fetch_start = Column(DateTime(timezone=True), nullable=False)
    fetch_end = Column(DateTime(timezone=True))
    articles_fetched = Column(Integer, default=0)
    articles_inserted = Column(Integer, default=0)
    articles_updated = Column(Integer, default=0)
    status = Column(String(20), default='running')
    error_message = Column(Text)
    api_calls = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class DatabaseManager:
    """データベース管理クラス"""
    
    def __init__(self, config_path: str = "config/config.json"):
        """
        初期化
        
        Args:
            config_path: 設定ファイルパス
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = setup_logger('database_manager', config_path)
        self.db_logger = DatabaseLogger(self.logger)
        self.db_config = DatabaseConfig(config_path)
        self.is_connected = False
        
    def _load_config(self) -> Dict[str, Any]:
        """設定ファイル読み込み"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {}
        except Exception as e:
            print(f"設定ファイル読み込みエラー: {e}")
            return {}
    
    def connect(self) -> bool:
        """
        データベースに接続
        
        Returns:
            接続成功可否
        """
        try:
            self.is_connected = self.db_config.test_connection()
            if self.is_connected:
                self.db_logger.log_connection("成功", success=True)
                self.logger.info("データベース接続成功")
                
                # テーブルが存在しない場合は自動作成
                if self.db_config.create_tables_if_not_exists():
                    self.logger.info("テーブル確認・作成処理完了")
                else:
                    self.logger.warning("テーブル確認・作成処理で警告が発生しました")
            else:
                self.db_logger.log_connection("失敗", success=False)
                self.logger.error("データベース接続失敗")
            return self.is_connected
        except Exception as e:
            self.logger.error(f"データベース接続エラー: {e}")
            self.is_connected = False
            return False
    
    def create_tables(self) -> bool:
        """
        テーブル作成
        
        Returns:
            作成成功可否
        """
        try:
            # SQLAlchemyのcreate_allを使用してテーブルを作成
            result = self.db_config.create_tables_if_not_exists()
            if result:
                self.logger.info("テーブル作成処理完了")
            else:
                self.logger.error("テーブル作成処理失敗")
            return result
        except Exception as e:
            self.logger.error(f"テーブル作成エラー: {e}")
            return False
    
    def insert_news_article(self, article: NewsArticle) -> bool:
        """
        ニュース記事を挿入
        
        Args:
            article: ニュース記事オブジェクト
        
        Returns:
            挿入成功可否
        """
        if not self.is_connected:
            self.logger.error("データベース未接続")
            return False
        
        try:
            with self.db_config.get_session() as session:
                # NewsArticleをORMオブジェクトに変換
                article_orm = NewsArticleORM(
                    story_id=article.story_id,
                    headline=article.headline,
                    summary=article.summary,
                    body_text=article.body_text,
                    source=article.source,
                    published_at=article.published_at,
                    language=article.language,
                    category=article.category,
                    urgency_level=article.urgency_level,
                    priority_score=article.priority_score
                )
                
                # データ検証
                article_orm.validate()
                
                session.add(article_orm)
                session.commit()
                
                self.db_logger.log_query("INSERT", "news_articles", rows_affected=1)
                self.logger.debug(f"記事挿入成功: {article.story_id}")
                return True
                
        except IntegrityError as e:
            session.rollback()
            if "Duplicate entry" in str(e) or "UNIQUE constraint failed" in str(e):
                self.logger.debug(f"重複記事スキップ: {article.story_id}")
                return False
            else:
                self.logger.error(f"記事挿入エラー（整合性）: {e}")
                return False
        except Exception as e:
            if 'session' in locals():
                session.rollback()
            self.logger.error(f"記事挿入エラー: {e}")
            return False
    
    def bulk_insert_news_articles(self, articles: List[NewsArticle]) -> Dict[str, Any]:
        """
        ニュース記事を一括挿入
        
        Args:
            articles: ニュース記事リスト
        
        Returns:
            挿入結果辞書
        """
        if not self.is_connected:
            self.logger.error("データベース未接続")
            return {'success': False, 'inserted_count': 0, 'failed_count': len(articles)}
        
        # 重複検出用の設定を取得
        duplicate_config = self.config.get('news_filtering', {}).get('duplicate_detection', {})
        similarity_enabled = duplicate_config.get('enabled', False)
        similarity_threshold = duplicate_config.get('similarity_threshold', 0.85)
        
        inserted_count = 0
        failed_count = 0
        
        for article in articles:
            # 類似度チェック（有効な場合）
            if similarity_enabled and self._is_duplicate_by_similarity(article, similarity_threshold):
                self.logger.debug(f"類似記事スキップ: {article.headline[:50]}...")
                failed_count += 1
                continue
            
            if self.insert_news_article(article):
                inserted_count += 1
            else:
                failed_count += 1
        
        self.logger.info(f"一括挿入完了: 成功={inserted_count}, 失敗={failed_count}")
        
        return {
            'success': True,
            'inserted_count': inserted_count,
            'failed_count': failed_count,
            'total_processed': len(articles)
        }
    
    def get_news_by_story_id(self, story_id: str) -> Optional[NewsArticleORM]:
        """
        ストーリーIDでニュース記事を取得
        
        Args:
            story_id: ストーリーID
        
        Returns:
            ニュース記事ORM（見つからない場合はNone）
        """
        if not self.is_connected:
            self.logger.error("データベース未接続")
            return None
        
        try:
            with self.db_config.get_session() as session:
                article = session.query(NewsArticleORM).filter(
                    NewsArticleORM.story_id == story_id
                ).first()
                
                self.db_logger.log_query("SELECT", "news_articles")
                return article
                
        except Exception as e:
            self.logger.error(f"記事取得エラー: {e}")
            return None
    
    def get_news_by_date_range(self, start_date: datetime, end_date: datetime) -> List[NewsArticleORM]:
        """
        日付範囲でニュース記事を取得
        
        Args:
            start_date: 開始日時
            end_date: 終了日時
        
        Returns:
            ニュース記事ORMリスト
        """
        if not self.is_connected:
            self.logger.error("データベース未接続")
            return []
        
        try:
            with self.db_config.get_session() as session:
                articles = session.query(NewsArticleORM).filter(
                    NewsArticleORM.published_at >= start_date,
                    NewsArticleORM.published_at <= end_date
                ).all()
                
                self.db_logger.log_query("SELECT", "news_articles", rows_affected=len(articles))
                self.logger.info(f"日付範囲記事取得: {len(articles)}件")
                return articles
                
        except Exception as e:
            self.logger.error(f"日付範囲記事取得エラー: {e}")
            return []
    
    def update_news_article(self, story_id: str, update_data: Dict[str, Any]) -> bool:
        """
        ニュース記事を更新
        
        Args:
            story_id: ストーリーID
            update_data: 更新データ辞書
        
        Returns:
            更新成功可否
        """
        if not self.is_connected:
            self.logger.error("データベース未接続")
            return False
        
        try:
            with self.db_config.get_session() as session:
                article = session.query(NewsArticleORM).filter(
                    NewsArticleORM.story_id == story_id
                ).first()
                
                if not article:
                    self.logger.warning(f"更新対象記事が見つかりません: {story_id}")
                    return False
                
                # 更新データを適用
                for key, value in update_data.items():
                    if hasattr(article, key):
                        setattr(article, key, value)
                
                # 更新日時を設定
                article.updated_at = datetime.now(timezone.utc)
                
                session.commit()
                
                self.db_logger.log_query("UPDATE", "news_articles", rows_affected=1)
                self.logger.info(f"記事更新成功: {story_id}")
                return True
                
        except Exception as e:
            if 'session' in locals():
                session.rollback()
            self.logger.error(f"記事更新エラー: {e}")
            return False
    
    def delete_news_article(self, story_id: str) -> bool:
        """
        ニュース記事を削除
        
        Args:
            story_id: ストーリーID
        
        Returns:
            削除成功可否
        """
        if not self.is_connected:
            self.logger.error("データベース未接続")
            return False
        
        try:
            with self.db_config.get_session() as session:
                article = session.query(NewsArticleORM).filter(
                    NewsArticleORM.story_id == story_id
                ).first()
                
                if not article:
                    self.logger.warning(f"削除対象記事が見つかりません: {story_id}")
                    return False
                
                session.delete(article)
                session.commit()
                
                self.db_logger.log_query("DELETE", "news_articles", rows_affected=1)
                self.logger.info(f"記事削除成功: {story_id}")
                return True
                
        except Exception as e:
            if 'session' in locals():
                session.rollback()
            self.logger.error(f"記事削除エラー: {e}")
            return False
    
    def get_news_statistics(self) -> Optional[Dict[str, Any]]:
        """
        ニュース統計情報を取得
        
        Returns:
            統計情報辞書
        """
        if not self.is_connected:
            self.logger.error("データベース未接続")
            return None
        
        try:
            with self.db_config.get_session() as session:
                from sqlalchemy import text
                # 統計クエリを実行
                stats_query = """
                SELECT 
                    COUNT(*) as total_articles,
                    COUNT(DISTINCT source) as sources_count,
                    MAX(published_at) as latest_article,
                    MIN(published_at) as earliest_article,
                    AVG(LENGTH(headline)) as avg_headline_length
                FROM news_articles
                """
                
                result = session.execute(text(stats_query)).fetchone()
                
                if result:
                    stats = {
                        'total_articles': result.total_articles,
                        'sources_count': result.sources_count,
                        'latest_article': result.latest_article,
                        'earliest_article': result.earliest_article,
                        'avg_headline_length': float(result.avg_headline_length) if result.avg_headline_length else 0
                    }
                    
                    self.db_logger.log_query("SELECT", "news_articles (statistics)")
                    return stats
                else:
                    return None
                    
        except Exception as e:
            self.logger.error(f"統計情報取得エラー: {e}")
            return None
    
    def cleanup_old_articles(self, cutoff_date: datetime) -> int:
        """
        古い記事をクリーンアップ（削除）
        
        Args:
            cutoff_date: カットオフ日時（この日時より古い記事を削除）
        
        Returns:
            削除された記事数
        """
        if not self.is_connected:
            self.logger.error("データベース未接続")
            return 0
        
        try:
            with self.db_config.get_session() as session:
                deleted_count = session.query(NewsArticleORM).filter(
                    NewsArticleORM.published_at < cutoff_date
                ).delete()
                
                session.commit()
                
                self.db_logger.log_query("DELETE", "news_articles", rows_affected=deleted_count)
                self.logger.info(f"古い記事クリーンアップ完了: {deleted_count}件削除")
                return deleted_count
                
        except Exception as e:
            if 'session' in locals():
                session.rollback()
            self.logger.error(f"記事クリーンアップエラー: {e}")
            return 0
    
    # 取得ログ管理メソッド
    def start_fetch_log(self) -> Optional[int]:
        """
        取得ログを開始
        
        Returns:
            ログID
        """
        if not self.is_connected:
            return None
        
        try:
            with self.db_config.get_session() as session:
                fetch_log = NewsFetchLogORM(
                    fetch_start=datetime.now(timezone.utc),
                    status="running"  # 文字列として設定
                )
                
                session.add(fetch_log)
                session.commit()
                
                return fetch_log.id
                
        except Exception as e:
            self.logger.error(f"取得ログ開始エラー: {e}")
            return None
    
    def complete_fetch_log(self, log_id: int, articles_fetched: int = 0, 
                          articles_inserted: int = 0, articles_updated: int = 0,
                          api_calls: int = 0, error_message: str = None) -> bool:
        """
        取得ログを完了
        
        Args:
            log_id: ログID
            articles_fetched: 取得記事数
            articles_inserted: 挿入記事数
            articles_updated: 更新記事数
            api_calls: API呼び出し回数
            error_message: エラーメッセージ
        
        Returns:
            完了成功可否
        """
        if not self.is_connected:
            return False
        
        try:
            with self.db_config.get_session() as session:
                fetch_log = session.query(NewsFetchLogORM).filter(
                    NewsFetchLogORM.id == log_id
                ).first()
                
                if not fetch_log:
                    return False
                
                # ログ情報を更新
                fetch_log.fetch_end = datetime.now(timezone.utc)
                fetch_log.articles_fetched = articles_fetched
                fetch_log.articles_inserted = articles_inserted
                fetch_log.articles_updated = articles_updated
                fetch_log.api_calls = api_calls
                fetch_log.status = "failed" if error_message else "completed"
                fetch_log.error_message = error_message
                
                session.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"取得ログ完了エラー: {e}")
            return False
    
    def _is_duplicate_by_similarity(self, article: NewsArticle, threshold: float = 0.85) -> bool:
        """
        類似度ベースの重複チェック
        
        Args:
            article: チェック対象の記事
            threshold: 類似度の閾値（0-1）
        
        Returns:
            重複している場合True
        """
        try:
            # 設定から時間窓を取得
            check_window_hours = self.config.get('news_filtering', {}).get('duplicate_detection', {}).get('check_window_hours', 24)
            
            with self.db_config.get_session() as session:
                # 指定時間内の記事を取得
                from datetime import timedelta
                time_threshold = datetime.now(timezone.utc) - timedelta(hours=check_window_hours)
                
                recent_articles = session.query(NewsArticleORM).filter(
                    NewsArticleORM.published_at >= time_threshold
                ).all()
                
                if not recent_articles:
                    return False
                
                # ヘッドラインのリストを作成
                existing_headlines = [a.headline for a in recent_articles]
                existing_headlines.append(article.headline)
                
                # TF-IDFベクトル化
                vectorizer = TfidfVectorizer(
                    max_features=100,
                    ngram_range=(1, 2),
                    stop_words=None  # 日本語と英語混在のため
                )
                
                try:
                    tfidf_matrix = vectorizer.fit_transform(existing_headlines)
                except ValueError:
                    # ベクトル化できない場合はスキップ
                    return False
                
                # 最後の要素（新記事）と他の全ての記事の類似度を計算
                new_article_vector = tfidf_matrix[-1]
                similarities = cosine_similarity(new_article_vector, tfidf_matrix[:-1])
                
                # 最大類似度をチェック
                if similarities.size > 0 and np.max(similarities) >= threshold:
                    max_sim_idx = np.argmax(similarities)
                    similar_article = recent_articles[max_sim_idx]
                    self.logger.info(
                        f"類似記事検出 (類似度: {np.max(similarities):.2f}): "
                        f"新: '{article.headline[:50]}...' "
                        f"既存: '{similar_article.headline[:50]}...'"
                    )
                    return True
                
                return False
                
        except Exception as e:
            self.logger.warning(f"類似度チェックエラー: {e}")
            return False