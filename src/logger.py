"""
ログ設定モジュール
Refinitiv EIKON APIガイドの標準ログ設定パターンに基づく
"""
import os
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Optional, Dict, Any

def setup_logger(name: str, config_path: str = None, log_file: str = None) -> logging.Logger:
    """
    標準ログ設定（ファイル+コンソール出力）
    
    Args:
        name: ログ名
        config_path: 設定ファイルパス
        log_file: ログファイルパス
    
    Returns:
        設定済みロガー
    """
    logger = logging.getLogger(name)
    
    # 設定読み込み
    config = _load_logging_config(config_path)
    
    # ログレベル設定
    log_level = getattr(logging, config.get('level', 'INFO').upper())
    logger.setLevel(log_level)
    
    # 重複ハンドラー防止
    if logger.handlers:
        logger.handlers.clear()
    
    # フォーマッター設定
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # ファイルハンドラー設定
    if config.get('file_path') or log_file:
        file_path = log_file or config['file_path']
        
        # ログディレクトリ作成
        log_dir = os.path.dirname(file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # ローテーションファイルハンドラー
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=config.get('max_file_size_mb', 100) * 1024 * 1024,
            backupCount=config.get('backup_count', 5),
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # コンソールハンドラー設定
    if config.get('console_output', True):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger

def _load_logging_config(config_path: str = None) -> Dict[str, Any]:
    """ログ設定を読み込み"""
    default_config = {
        'level': 'INFO',
        'file_path': 'logs/refinitiv_news.log',
        'max_file_size_mb': 100,
        'backup_count': 5,
        'console_output': True
    }
    
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config.get('logging', default_config)
        except Exception:
            pass
    
    # 環境変数からの設定
    env_config = {}
    if os.getenv('LOG_LEVEL'):
        env_config['level'] = os.getenv('LOG_LEVEL')
    
    return {**default_config, **env_config}

class APICallLogger:
    """API呼び出し専用ロガー"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.call_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
    
    def log_api_call(self, method: str, params: dict = None, success: bool = True, response_time: float = None):
        """API呼び出しログ記録"""
        self.call_count += 1
        if not success:
            self.error_count += 1
        
        log_data = {
            'method': method,
            'call_count': self.call_count,
            'success': success,
            'response_time_ms': round(response_time * 1000, 2) if response_time else None
        }
        
        if params:
            # 機密情報をマスク
            safe_params = self._mask_sensitive_data(params)
            log_data['params'] = safe_params
        
        if success:
            self.logger.info(f"API呼び出し成功: {log_data}")
        else:
            self.logger.error(f"API呼び出し失敗: {log_data}")
    
    def log_rate_limit(self, delay_seconds: float):
        """レート制限待機ログ"""
        self.logger.debug(f"レート制限待機: {delay_seconds}秒")
    
    def log_retry(self, attempt: int, max_retries: int, error: str):
        """リトライログ"""
        self.logger.warning(f"リトライ {attempt}/{max_retries}: {error}")
    
    def get_stats(self) -> Dict[str, Any]:
        """統計情報取得"""
        runtime = (datetime.now() - self.start_time).total_seconds()
        return {
            'total_calls': self.call_count,
            'error_count': self.error_count,
            'success_rate': (self.call_count - self.error_count) / max(self.call_count, 1),
            'runtime_seconds': runtime,
            'calls_per_minute': (self.call_count / max(runtime / 60, 1)) if runtime > 0 else 0
        }
    
    def _mask_sensitive_data(self, data: dict) -> dict:
        """機密情報をマスク"""
        masked = data.copy()
        sensitive_keys = ['api_key', 'password', 'token', 'secret']
        
        for key in masked:
            if any(sensitive in key.lower() for sensitive in sensitive_keys):
                masked[key] = '*' * 8
        
        return masked

class DatabaseLogger:
    """データベース操作専用ロガー"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.query_count = 0
        self.error_count = 0
    
    def log_query(self, operation: str, table: str, rows_affected: int = None, execution_time: float = None):
        """クエリ実行ログ"""
        self.query_count += 1
        
        log_data = {
            'operation': operation,
            'table': table,
            'query_count': self.query_count,
            'rows_affected': rows_affected,
            'execution_time_ms': round(execution_time * 1000, 2) if execution_time else None
        }
        
        self.logger.info(f"DB操作: {log_data}")
    
    def log_transaction(self, action: str, success: bool = True):
        """トランザクションログ"""
        if success:
            self.logger.info(f"トランザクション{action}成功")
        else:
            self.logger.error(f"トランザクション{action}失敗")
            self.error_count += 1
    
    def log_connection(self, action: str, success: bool = True):
        """接続ログ"""
        if success:
            self.logger.debug(f"DB接続{action}")
        else:
            self.logger.error(f"DB接続{action}失敗")

# グローバルロガーインスタンス
main_logger = setup_logger('refinitiv_news')
api_logger = APICallLogger(main_logger)
db_logger = DatabaseLogger(main_logger)