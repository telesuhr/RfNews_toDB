#!/usr/bin/env python3
"""
前日分のニュースを全件取得するスクリプト
"""
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))

from main import RefinitivNewsApp

def fetch_yesterday_news():
    """前日のニュースを全件取得"""

    # 前日の日付を計算（UTC）
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    start_date = yesterday
    end_date = yesterday.replace(hour=23, minute=59, second=59)

    print("=" * 80)
    print("前日ニュース全件取得")
    print("=" * 80)
    print(f"対象日: {yesterday.strftime('%Y-%m-%d')}")
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # アプリケーション初期化
    app = RefinitivNewsApp()
    if not app.initialize():
        print("✗ アプリケーション初期化失敗")
        return False

    # ニュース取得
    result = app.fetch_and_store_news_backfill(
        per_page=100,
        start_date=start_date,
        end_date=end_date,
        fetch_body=True
    )

    print()
    print("=" * 80)
    print("取得完了")
    print("=" * 80)

    if result['success']:
        print(f"✓ 成功")
        print(f"  取得記事数: {result['articles_fetched']}")
        print(f"  格納記事数: {result['articles_stored']}")
        print(f"  失敗記事数: {result['articles_failed']}")
        print(f"  API呼び出し数: {result['api_calls']}")
        print(f"  処理時間: {result['processing_time']:.1f}秒")

        # 統計情報を表示
        stats = app.get_news_statistics()
        if stats:
            print()
            print(f"データベース統計:")
            print(f"  総記事数: {stats['total_articles']:,}")
            print(f"  最新記事: {stats['latest_article']}")

        return True
    else:
        print(f"✗ 失敗: {result['message']}")
        return False

if __name__ == "__main__":
    success = fetch_yesterday_news()
    sys.exit(0 if success else 1)
