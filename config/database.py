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
        # データベースタイプを取得（デフォルトはpostgresql）
        db_type = self.config.get('type', 'postgresql').lower()
        
        # SQLiteデータベースの場合（テスト用）
        if db_type == 'sqlite' or self.config.get('database', '').endswith('.db'):
            return f"sqlite:///{self.config['database']}"
        
        # PostgreSQLデータベースの場合
        if db_type == 'postgresql':
            return (
                f"postgresql+psycopg2://{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config.get('port', 5432)}/{self.config['database']}"
            )
        
        # SQL Serverデータベースの場合
        if db_type == 'sqlserver':
            # ドライバー名を取得（デフォルトはODBC Driver 17 for SQL Server）
            driver = self.config.get('driver', 'ODBC Driver 17 for SQL Server')
            # urllib.parse.quote_plusでドライバー名をエンコード
            import urllib.parse
            driver_encoded = urllib.parse.quote_plus(driver)
            
            # Azure SQL Database用の追加パラメータ
            connection_params = [f"driver={driver_encoded}"]
            
            # ODBC Driver 18を使用している場合は暗号化設定を追加
            if 'ODBC Driver 18' in driver:
                connection_params.append("TrustServerCertificate=yes")
                connection_params.append("Encrypt=yes")
            
            # 接続タイムアウトを追加
            connection_params.append("Connection Timeout=30")
            
            # パスワードとユーザー名をURLエンコード（特殊文字対策）
            username_encoded = urllib.parse.quote_plus(self.config['username'])
            password_encoded = urllib.parse.quote_plus(self.config['password'])
            
            # 接続文字列を生成
            connection_url = (
                f"mssql+pyodbc://{username_encoded}:{password_encoded}"
                f"@{self.config['host']}:{self.config.get('port', 1433)}/{self.config['database']}"
                f"?{'&'.join(connection_params)}"
            )
            
            # デバッグ用のログ出力（パスワードはマスク）
            masked_password = '*' * len(self.config['password']) if self.config['password'] else ''
            debug_url = (
                f"mssql+pyodbc://{self.config['username']}:{masked_password}"
                f"@{self.config['host']}:{self.config.get('port', 1433)}/{self.config['database']}"
                f"?{'&'.join(connection_params)}"
            )
            logger.info(f"SQL Server接続文字列生成: {debug_url}")
            logger.info(f"使用ドライバー: {driver}")
            logger.info(f"接続パラメータ: {connection_params}")
            
            return connection_url
        
        # MySQLデータベースの場合
        if db_type == 'mysql':
            return (
                f"mysql+pymysql://{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config.get('port', 3306)}/{self.config['database']}"
                f"?charset={self.config.get('charset', 'utf8mb4')}"
            )
        
        # デフォルトはPostgreSQL
        logger.warning(f"不明なデータベースタイプ: {db_type}. PostgreSQLとして処理します。")
        return (
            f"postgresql+psycopg2://{self.config['username']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config.get('port', 5432)}/{self.config['database']}"
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
    
    def create_tables_if_not_exists(self) -> bool:
        """テーブルが存在しない場合に作成"""
        try:
            engine = self.create_engine()
            # metadataをインポート
            from sqlalchemy import inspect
            inspector = inspect(engine)
            
            # 既存のテーブルリストを取得
            existing_tables = inspector.get_table_names()
            logger.info(f"既存のテーブル: {existing_tables}")
            
            # ORMモデルからテーブルを作成（存在しないものだけ）
            Base.metadata.create_all(bind=engine, checkfirst=True)
            
            # 作成後のテーブルリストを取得
            new_tables = inspector.get_table_names()
            created_tables = set(new_tables) - set(existing_tables)
            
            if created_tables:
                logger.info(f"新規作成されたテーブル: {list(created_tables)}")
            else:
                logger.info("すべてのテーブルが既に存在しています")
            
            return True
        except Exception as e:
            logger.error(f"テーブル作成エラー: {e}")
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