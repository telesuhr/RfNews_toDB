"""
データベース接続設定モジュール
"""
import os
import json
from typing import Dict, Any
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()

class DatabaseConfig:
    """データベース設定管理クラス"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or "config/config.json"
        self.config = self._load_config()
        self.engine = None
        self.SessionLocal = None
        
    def _load_config(self) -> Dict[str, Any]:
        """設定ファイルまたは環境変数から設定を読み込み"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return config.get('database', {})
            else:
                # 環境変数から設定を読み込み
                return {
                    'host': os.getenv('DB_HOST', 'localhost'),
                    'port': int(os.getenv('DB_PORT', 3306)),
                    'username': os.getenv('DB_USERNAME', 'root'),
                    'password': os.getenv('DB_PASSWORD', ''),
                    'database': os.getenv('DB_DATABASE', 'refinitiv_news'),
                    'charset': 'utf8mb4',
                    'pool_size': 10,
                    'pool_timeout': 30
                }
        except Exception as e:
            logger.error(f"設定ファイル読み込みエラー: {e}")
            raise
    
    def get_database_url(self) -> str:
        """データベース接続URLを構築"""
        # SQLiteデータベースの場合（テスト用）
        if self.config.get('type') == 'sqlite' or self.config.get('database', '').endswith('.db'):
            return f"sqlite:///{self.config['database']}"
        
        # PostgreSQLデータベースの場合（ポート5432がデフォルト）
        if self.config.get('port') == 5432:
            return (
                f"postgresql+psycopg2://{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
        
        # MySQLデータベースの場合
        return (
            f"mysql+pymysql://{self.config['username']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            f"?charset={self.config['charset']}"
        )
    
    def create_engine(self):
        """SQLAlchemyエンジンを作成"""
        if self.engine is None:
            database_url = self.get_database_url()
            self.engine = create_engine(
                database_url,
                poolclass=QueuePool,
                pool_size=self.config.get('pool_size', 10),
                pool_timeout=self.config.get('pool_timeout', 30),
                pool_pre_ping=True,
                echo=False  # SQLログ出力制御
            )
            logger.info("データベースエンジンを作成しました")
        return self.engine
    
    def create_session_factory(self):
        """セッションファクトリを作成"""
        if self.SessionLocal is None:
            engine = self.create_engine()
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=engine
            )
            logger.info("セッションファクトリを作成しました")
        return self.SessionLocal
    
    def get_session(self):
        """データベースセッションを取得"""
        SessionLocal = self.create_session_factory()
        return SessionLocal()
    
    def execute_sql_file(self, sql_file_path: str):
        """SQLファイルを実行"""
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            engine = self.create_engine()
            with engine.connect() as connection:
                from sqlalchemy import text
                # 複数のSQL文を分割して実行
                statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                for statement in statements:
                    if statement:
                        connection.execute(text(statement))
                connection.commit()
                        
            logger.info(f"SQLファイル実行完了: {sql_file_path}")
            return True
        except Exception as e:
            logger.error(f"SQLファイル実行エラー: {e}")
            return False
    
    def test_connection(self) -> bool:
        """データベース接続テスト"""
        try:
            engine = self.create_engine()
            with engine.connect() as connection:
                from sqlalchemy import text
                result = connection.execute(text("SELECT 1"))
                logger.info("データベース接続テスト成功")
                return True
        except Exception as e:
            logger.error(f"データベース接続テストエラー: {e}")
            return False

# グローバルインスタンス
db_config = DatabaseConfig()

def get_db():
    """依存性注入用のデータベースセッション取得関数"""
    db = db_config.get_session()
    try:
        yield db
    finally:
        db.close()