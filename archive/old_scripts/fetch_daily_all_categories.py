#!/usr/bin/env python3
"""
指定日の全カテゴリーニュースを取得するスクリプト
"""
import os
import sys
import argparse
from datetime import datetime, timedelta
import time

# プロジェクトのルートディレクトリを追加
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

from main import RefinitivNewsApp


def fetch_all_categories_for_date(date_str: str, fetch_body: bool = True):
    """
    指定日の全カテゴリーのニュースを取得
    
    Args:
        date_str: 取得対象日（YYYY-MM-DD形式）
        fetch_body: 本文を取得するかどうか
    """
    # カテゴリーリスト（全カテゴリー）
    categories = ["TOP", "EQUITY", "FOREX", "COMMODITY", "FIXED-INCOME", 
                  "COPPER", "ALUMINIUM", "ZINC", "LEAD", "NICKEL", "TIN"]
    
    # 日付をパース
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print(f"エラー: 日付は YYYY-MM-DD 形式で指定してください（例: 2025-10-21）")
        return
    
    # 開始日時と終了日時を設定（その日の00:00から23:59まで）
    from datetime import timezone
    start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end_date = target_date.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc)
    
    # ISO形式に変換
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    
    print(f"=== {date_str} の全カテゴリーニュース取得開始 ===")
    print(f"期間: {start_date_str} ～ {end_date_str}")
    print(f"カテゴリー: {', '.join(categories)}")
    print(f"本文取得: {'あり' if fetch_body else 'なし'}")
    print()
    
    # アプリケーション初期化（一度だけ）
    app = RefinitivNewsApp()
    
    # 初期化実行
    if not app.initialize():
        print("エラー: アプリケーションの初期化に失敗しました")
        return
    
    total_fetched = 0
    total_stored = 0
    total_failed = 0
    
    # カテゴリーごとに取得
    for category in categories:
        print(f"\n--- カテゴリー: {category} ---")
        
        try:
            # fetch-allコマンドの引数を模擬
            class Args:
                pass
            
            args = Args()
            args.per_page = 50  # 1ページあたりの件数
            args.query = None
            args.category = category
            args.language = None
            args.start_date = start_date_str
            args.end_date = end_date_str
            args.max_pages = None  # 全ページ取得
            args.no_fetch_body = not fetch_body
            
            # 取得実行（日付文字列を日時オブジェクトに変換）
            start_dt = datetime.fromisoformat(args.start_date) if args.start_date else None
            end_dt = datetime.fromisoformat(args.end_date) if args.end_date else None
            
            result = app.fetch_and_store_news_paginated(
                per_page=args.per_page,
                query=args.query,
                category=args.category,
                language=args.language,
                start_date=start_dt,
                end_date=end_dt,
                max_pages=args.max_pages,
                fetch_body=not args.no_fetch_body
            )
            
            if result['success']:
                print(f"成功: 取得{result['articles_fetched']}件、保存{result['articles_stored']}件")
                total_fetched += result['articles_fetched']
                total_stored += result['articles_stored']
                total_failed += result.get('articles_failed', 0)
            else:
                print(f"エラー: {result['message']}")
            
            # カテゴリー間の待機（レート制限対策）
            if category != categories[-1]:  # 最後のカテゴリーでなければ
                wait_time = 60  # 60秒待機
                print(f"次のカテゴリーまで{wait_time}秒待機...")
                time.sleep(wait_time)
                
        except Exception as e:
            print(f"カテゴリー {category} でエラー: {e}")
            import traceback
            traceback.print_exc()
    
    # 総合結果
    print(f"\n=== 取得完了 ===")
    print(f"総取得件数: {total_fetched}")
    print(f"総保存件数: {total_stored}")
    print(f"失敗件数: {total_failed}")
    print(f"対象日: {date_str}")


def main():
    parser = argparse.ArgumentParser(
        description="指定日の全カテゴリーニュースを取得"
    )
    parser.add_argument(
        'date',
        help='取得対象日（YYYY-MM-DD形式）'
    )
    parser.add_argument(
        '--no-fetch-body',
        action='store_true',
        help='本文取得をスキップ（ヘッドラインのみ）'
    )
    
    args = parser.parse_args()
    
    fetch_all_categories_for_date(args.date, fetch_body=not args.no_fetch_body)


if __name__ == "__main__":
    main()