"""
ニュース取得スケジューラーモジュール
定期実行とバッチ処理を管理
"""
import os
import sys
import time
import signal
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Callable
import schedule

# パス設定
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))

from main import RefinitivNewsApp
from logger import setup_logger

class NewsScheduler:
    """ニュース取得スケジューラー"""
    
    def __init__(self, config_path: str = "config/config.json"):
        """
        初期化
        
        Args:
            config_path: 設定ファイルパス
        """
        self.config_path = config_path
        self.logger = setup_logger('news_scheduler', config_path)
        self.app = RefinitivNewsApp(config_path)
        
        # スケジューラー制御フラグ
        self.running = False
        self.should_stop = False
        
        # 統計情報
        self.execution_count = 0
        self.last_execution = None
        self.last_success = None
        
        # シグナルハンドラー設定
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("ニュース取得スケジューラー初期化完了")
    
    def _signal_handler(self, signum, frame):
        """シグナルハンドラー（CTRL+C等での終了）"""
        self.logger.info(f"シグナル受信: {signum}. 終了処理を開始します...")
        self.stop()
    
    def setup_schedules(self):
        """スケジュール設定"""
        try:
            # アプリケーション初期化
            if not self.app.initialize():
                self.logger.error("アプリケーション初期化失敗")
                return False
            
            # リアルタイム取得：5分間隔
            schedule.every(5).minutes.do(self._fetch_latest_news)
            
            # 日次バッチ：毎日午前3時
            schedule.every().day.at("03:00").do(self._daily_batch)
            
            # 週次メンテナンス：毎週日曜日午前2時
            schedule.every().sunday.at("02:00").do(self._weekly_maintenance)
            
            # ヘルスチェック：15分間隔
            schedule.every(15).minutes.do(self._health_check)
            
            self.logger.info("スケジュール設定完了")
            return True
            
        except Exception as e:
            self.logger.error(f"スケジュール設定エラー: {e}")
            return False
    
    def _fetch_latest_news(self):
        """最新ニュース取得ジョブ"""
        job_name = "latest_news_fetch"
        self.logger.info(f"ジョブ開始: {job_name}")
        
        try:
            self.execution_count += 1
            self.last_execution = datetime.now(timezone.utc)
            
            # 最新ニュースを取得（過去1時間分）
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(hours=1)
            
            result = self.app.fetch_and_store_news(
                count=50,
                start_date=start_date,
                end_date=end_date
            )
            
            if result['success']:
                self.last_success = datetime.now(timezone.utc)
                self.logger.info(f"{job_name}成功: {result['articles_stored']}件格納")
            else:
                self.logger.error(f"{job_name}失敗: {result['message']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"{job_name}エラー: {e}")
            return {'success': False, 'message': str(e)}
    
    def _daily_batch(self):
        """日次バッチジョブ"""
        job_name = "daily_batch"
        self.logger.info(f"ジョブ開始: {job_name}")
        
        try:
            # 前日分のニュースを取得
            end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = end_date - timedelta(days=1)
            
            result = self.app.fetch_and_store_news(
                count=500,
                start_date=start_date,
                end_date=end_date
            )
            
            if result['success']:
                self.logger.info(f"{job_name}成功: {result['articles_stored']}件格納")
            else:
                self.logger.error(f"{job_name}失敗: {result['message']}")
            
            # 統計情報をログ出力
            stats = self.app.get_news_statistics()
            if stats:
                self.logger.info(f"総記事数: {stats['total_articles']}, 最新記事: {stats['latest_article']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"{job_name}エラー: {e}")
            return {'success': False, 'message': str(e)}
    
    def _weekly_maintenance(self):
        """週次メンテナンスジョブ"""
        job_name = "weekly_maintenance"
        self.logger.info(f"ジョブ開始: {job_name}")
        
        try:
            # 古い記事のクリーンアップ（1年以上前）
            deleted_count = self.app.cleanup_old_news(days_to_keep=365)
            
            self.logger.info(f"{job_name}成功: {deleted_count}件削除")
            
            # 統計情報レポート
            stats = self.app.get_news_statistics()
            if stats:
                self.logger.info("=== 週次統計レポート ===")
                self.logger.info(f"総記事数: {stats['total_articles']}")
                self.logger.info(f"ソース数: {stats['sources_count']}")
                self.logger.info(f"最新記事: {stats['latest_article']}")
            
            return {'success': True, 'deleted_count': deleted_count}
            
        except Exception as e:
            self.logger.error(f"{job_name}エラー: {e}")
            return {'success': False, 'message': str(e)}
    
    def _health_check(self):
        """ヘルスチェックジョブ"""
        try:
            # データベース接続確認
            if not self.app.db_manager.is_connected:
                if not self.app.db_manager.connect():
                    self.logger.warning("ヘルスチェック: データベース再接続失敗")
                    return False
            
            # EIKON接続確認
            if not self.app.news_fetcher.is_connected:
                if not self.app.news_fetcher.connect():
                    self.logger.warning("ヘルスチェック: EIKON再接続失敗")
                    return False
            
            # 最後の成功実行から時間が経ちすぎていないかチェック
            if self.last_success:
                time_since_success = datetime.now(timezone.utc) - self.last_success
                if time_since_success > timedelta(hours=2):
                    self.logger.warning(f"ヘルスチェック: 長時間成功なし ({time_since_success})")
            
            self.logger.debug("ヘルスチェック: 正常")
            return True
            
        except Exception as e:
            self.logger.error(f"ヘルスチェックエラー: {e}")
            return False
    
    def run(self):
        """スケジューラー実行開始"""
        if self.running:
            self.logger.warning("スケジューラーは既に実行中です")
            return
        
        if not self.setup_schedules():
            self.logger.error("スケジュール設定失敗")
            return
        
        self.running = True
        self.should_stop = False
        
        self.logger.info("スケジューラー実行開始")
        
        try:
            while not self.should_stop:
                schedule.run_pending()
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("キーボード割り込みでスケジューラー停止")
        except Exception as e:
            self.logger.error(f"スケジューラー実行エラー: {e}")
        finally:
            self.running = False
            self.logger.info("スケジューラー実行終了")
    
    def stop(self):
        """スケジューラー停止"""
        self.should_stop = True
        self.logger.info("スケジューラー停止要求")
    
    def run_job_once(self, job_name: str) -> Dict[str, Any]:
        """特定のジョブを1回だけ実行"""
        self.logger.info(f"単発ジョブ実行: {job_name}")
        
        jobs = {
            'latest': self._fetch_latest_news,
            'daily': self._daily_batch,
            'maintenance': self._weekly_maintenance,
            'health': self._health_check
        }
        
        if job_name not in jobs:
            return {'success': False, 'message': f'不明なジョブ: {job_name}'}
        
        try:
            if not self.app.initialize():
                return {'success': False, 'message': 'アプリケーション初期化失敗'}
            
            result = jobs[job_name]()
            self.logger.info(f"単発ジョブ完了: {job_name}")
            return result if isinstance(result, dict) else {'success': True}
            
        except Exception as e:
            self.logger.error(f"単発ジョブエラー: {e}")
            return {'success': False, 'message': str(e)}
    
    def get_status(self) -> Dict[str, Any]:
        """スケジューラー状態取得"""
        return {
            'running': self.running,
            'execution_count': self.execution_count,
            'last_execution': self.last_execution,
            'last_success': self.last_success,
            'scheduled_jobs': len(schedule.jobs),
            'uptime': datetime.now(timezone.utc) if self.running else None
        }

def main():
    """スケジューラーメイン関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Refinitiv ニュース取得スケジューラー")
    parser.add_argument('--config', type=str, default='config/config.json', help='設定ファイルパス')
    parser.add_argument('--job', type=str, choices=['latest', 'daily', 'maintenance', 'health'], 
                       help='単発ジョブ実行')
    parser.add_argument('--daemon', action='store_true', help='デーモンモードで実行')
    
    args = parser.parse_args()
    
    scheduler = NewsScheduler(args.config)
    
    if args.job:
        # 単発ジョブ実行
        result = scheduler.run_job_once(args.job)
        if result['success']:
            print(f"✓ ジョブ実行成功: {args.job}")
            sys.exit(0)
        else:
            print(f"✗ ジョブ実行失敗: {result['message']}")
            sys.exit(1)
    
    elif args.daemon:
        # デーモンモード実行
        scheduler.run()
    
    else:
        # ヘルプ表示
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()