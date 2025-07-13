"""
Refinitiv ニュース情報データベース格納システム
メインアプリケーション
"""
import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

# パス設定
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))

from news_fetcher import NewsFetcher, NewsArticle
from database_manager import DatabaseManager
from logger import setup_logger

class RefinitivNewsApp:
    """Refinitiv ニュース取得・格納アプリケーション"""
    
    def __init__(self, config_path: str = "config/config.json"):
        """
        初期化
        
        Args:
            config_path: 設定ファイルパス
        """
        self.config_path = config_path
        self.logger = setup_logger('refinitiv_news_app', config_path)
        self.news_fetcher = NewsFetcher(config_path)
        self.db_manager = DatabaseManager(config_path)
        
        self.logger.info("Refinitiv ニュースアプリケーション初期化完了")
    
    def initialize(self) -> bool:
        """
        アプリケーション初期化（接続・テーブル作成）
        
        Returns:
            初期化成功可否
        """
        try:
            # Refinitiv EIKON接続
            if not self.news_fetcher.connect():
                self.logger.error("Refinitiv EIKON接続失敗")
                return False
            
            # データベース接続
            if not self.db_manager.connect():
                self.logger.error("データベース接続失敗")
                return False
            
            # テーブル作成（存在しない場合）
            if not self.db_manager.create_tables():
                self.logger.warning("テーブル作成に問題がありました（既存の可能性）")
            
            self.logger.info("アプリケーション初期化成功")
            return True
            
        except Exception as e:
            self.logger.error(f"アプリケーション初期化エラー: {e}")
            return False
    
    def fetch_and_store_news(self, count: int = 100, category: str = None,
                           language: str = None, start_date: datetime = None,
                           end_date: datetime = None) -> Dict[str, Any]:
        """
        ニュースを取得してデータベースに格納
        
        Args:
            count: 取得件数
            category: カテゴリフィルタ
            language: 言語フィルタ
            start_date: 開始日時
            end_date: 終了日時
        
        Returns:
            処理結果辞書
        """
        self.logger.info(f"ニュース取得・格納開始: count={count}, category={category}")
        
        # 取得ログ開始
        log_id = self.db_manager.start_fetch_log()
        
        try:
            # ニュース取得（リトライ付き）
            news_data = self.news_fetcher.fetch_headlines_with_retry(
                count=count,
                category=category,
                language=language,
                start_date=start_date,
                end_date=end_date
            )
            
            if news_data is None or news_data.empty:
                error_msg = "ニュースデータが取得できませんでした"
                self.logger.warning(error_msg)
                
                if log_id:
                    self.db_manager.complete_fetch_log(
                        log_id=log_id,
                        error_message=error_msg
                    )
                
                return {
                    'success': False,
                    'message': error_msg,
                    'articles_fetched': 0,
                    'articles_stored': 0
                }
            
            # NewsArticleオブジェクトに変換
            articles = self.news_fetcher.create_news_articles(news_data)
            
            # データベースに一括挿入
            store_result = self.db_manager.bulk_insert_news_articles(articles)
            
            # API統計情報取得
            api_stats = self.news_fetcher.get_api_stats()
            
            # 取得ログ完了
            if log_id:
                self.db_manager.complete_fetch_log(
                    log_id=log_id,
                    articles_fetched=len(articles),
                    articles_inserted=store_result['inserted_count'],
                    articles_updated=0,  # 現在は更新なし
                    api_calls=api_stats.get('total_calls', 0)
                )
            
            result = {
                'success': True,
                'message': 'ニュース取得・格納完了',
                'articles_fetched': len(articles),
                'articles_stored': store_result['inserted_count'],
                'articles_failed': store_result['failed_count'],
                'api_calls': api_stats.get('total_calls', 0),
                'processing_time': api_stats.get('runtime_seconds', 0)
            }
            
            self.logger.info(f"ニュース取得・格納完了: {result}")
            return result
            
        except Exception as e:
            error_msg = f"ニュース取得・格納エラー: {e}"
            self.logger.error(error_msg)
            
            if log_id:
                self.db_manager.complete_fetch_log(
                    log_id=log_id,
                    error_message=error_msg
                )
            
            return {
                'success': False,
                'message': error_msg,
                'articles_fetched': 0,
                'articles_stored': 0
            }
    
    def get_news_statistics(self) -> Optional[Dict[str, Any]]:
        """
        ニュース統計情報取得
        
        Returns:
            統計情報辞書
        """
        try:
            stats = self.db_manager.get_news_statistics()
            if stats:
                self.logger.info("統計情報取得成功")
            return stats
        except Exception as e:
            self.logger.error(f"統計情報取得エラー: {e}")
            return None
    
    def cleanup_old_news(self, days_to_keep: int = 365) -> int:
        """
        古いニュースをクリーンアップ
        
        Args:
            days_to_keep: 保持日数
        
        Returns:
            削除された記事数
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
            deleted_count = self.db_manager.cleanup_old_articles(cutoff_date)
            
            self.logger.info(f"古いニュースクリーンアップ完了: {deleted_count}件削除")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"ニュースクリーンアップエラー: {e}")
            return 0
    
    def run_once(self, args: argparse.Namespace) -> bool:
        """
        1回だけニュース取得・格納を実行
        
        Args:
            args: コマンドライン引数
        
        Returns:
            実行成功可否
        """
        # 初期化
        if not self.initialize():
            return False
        
        # 日付範囲設定
        start_date = None
        end_date = None
        
        if args.start_date:
            start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
        if args.end_date:
            end_date = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)
        
        # ニュース取得・格納
        result = self.fetch_and_store_news(
            count=args.count,
            category=args.category,
            language=args.language,
            start_date=start_date,
            end_date=end_date
        )
        
        # 結果表示
        if result['success']:
            print(f"✓ {result['message']}")
            print(f"  取得記事数: {result['articles_fetched']}")
            print(f"  格納記事数: {result['articles_stored']}")
            print(f"  失敗記事数: {result['articles_failed']}")
            print(f"  API呼び出し数: {result['api_calls']}")
            return True
        else:
            print(f"✗ {result['message']}")
            return False
    
    def show_statistics(self) -> bool:
        """
        統計情報を表示
        
        Returns:
            表示成功可否
        """
        if not self.db_manager.connect():
            print("✗ データベース接続失敗")
            return False
        
        stats = self.get_news_statistics()
        if not stats:
            print("✗ 統計情報取得失敗")
            return False
        
        print("=== ニュース統計情報 ===")
        print(f"総記事数: {stats['total_articles']:,}")
        print(f"ソース数: {stats['sources_count']}")
        print(f"最新記事: {stats['latest_article']}")
        print(f"最古記事: {stats['earliest_article']}")
        print(f"平均ヘッドライン長: {stats['avg_headline_length']:.1f}文字")
        
        return True

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description="Refinitiv ニュース情報データベース格納システム"
    )
    
    # サブコマンド
    subparsers = parser.add_subparsers(dest='command', help='実行コマンド')
    
    # fetchコマンド
    fetch_parser = subparsers.add_parser('fetch', help='ニュース取得・格納')
    fetch_parser.add_argument('--count', type=int, default=100, help='取得件数')
    fetch_parser.add_argument('--category', type=str, help='カテゴリフィルタ')
    fetch_parser.add_argument('--language', type=str, help='言語フィルタ')
    fetch_parser.add_argument('--start-date', type=str, help='開始日時 (ISO format)')
    fetch_parser.add_argument('--end-date', type=str, help='終了日時 (ISO format)')
    
    # statsコマンド
    stats_parser = subparsers.add_parser('stats', help='統計情報表示')
    
    # cleanupコマンド
    cleanup_parser = subparsers.add_parser('cleanup', help='古い記事クリーンアップ')
    cleanup_parser.add_argument('--days', type=int, default=365, help='保持日数')
    
    # 共通オプション
    parser.add_argument('--config', type=str, default='config/config.json', help='設定ファイルパス')
    parser.add_argument('--verbose', '-v', action='store_true', help='詳細ログ出力')
    
    args = parser.parse_args()
    
    # ログレベル設定
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    # アプリケーション作成
    app = RefinitivNewsApp(args.config)
    
    # コマンド実行
    if args.command == 'fetch':
        success = app.run_once(args)
        sys.exit(0 if success else 1)
    
    elif args.command == 'stats':
        success = app.show_statistics()
        sys.exit(0 if success else 1)
    
    elif args.command == 'cleanup':
        if not app.db_manager.connect():
            print("✗ データベース接続失敗")
            sys.exit(1)
        
        deleted_count = app.cleanup_old_news(args.days)
        print(f"✓ 古い記事削除完了: {deleted_count}件")
        sys.exit(0)
    
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()