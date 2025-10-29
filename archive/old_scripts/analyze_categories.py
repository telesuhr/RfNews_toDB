#!/usr/bin/env python3
"""
カテゴリ別ニュース件数分析スクリプト
2025年10月17日のデータを分析
"""
import sys
import os
from datetime import datetime, timezone
from collections import Counter

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))

from database_manager import DatabaseManager

def analyze_categories():
    """カテゴリ別の記事数を分析"""

    db_manager = DatabaseManager('config/config.json')
    if not db_manager.connect():
        print("データベース接続失敗")
        return

    # 2025年10月17日のデータを取得
    start_date = datetime(2025, 10, 17, 0, 0, 0, tzinfo=timezone.utc)
    end_date = datetime(2025, 10, 17, 23, 59, 59, tzinfo=timezone.utc)

    articles = db_manager.get_news_by_date_range(start_date, end_date)

    print("=" * 80)
    print("2025年10月17日 カテゴリ別ニュース件数分析")
    print("=" * 80)
    print()

    # 総記事数
    total_articles = len(articles)
    print(f"総記事数: {total_articles}件")
    print()

    # カテゴリ別集計（カンマ区切りで複数カテゴリが入っているので展開）
    category_counter = Counter()
    articles_by_primary_category = {}

    for article in articles:
        if article.category:
            # カンマ区切りで分割
            categories = [c.strip() for c in article.category.split(',')]

            # 各カテゴリをカウント
            for cat in categories:
                if cat:
                    category_counter[cat] += 1

            # 主カテゴリ（最初のカテゴリ）でグループ化
            primary_cat = categories[0] if categories else 'UNKNOWN'
            if primary_cat not in articles_by_primary_category:
                articles_by_primary_category[primary_cat] = []
            articles_by_primary_category[primary_cat].append(article)

    # カテゴリ別件数を表示（検出されたカテゴリ数順）
    print("カテゴリ検出数（記事に含まれるカテゴリタグ数）:")
    print("-" * 80)
    print(f"{'カテゴリ':<20} {'件数':>10}")
    print("-" * 80)
    for cat, count in sorted(category_counter.items(), key=lambda x: x[1], reverse=True):
        print(f"{cat:<20} {count:>10}")
    print()

    # 主カテゴリ別件数を表示
    print("主カテゴリ別記事数（記事の第一カテゴリ）:")
    print("-" * 80)
    print(f"{'主カテゴリ':<20} {'記事数':>10}")
    print("-" * 80)
    for cat in sorted(articles_by_primary_category.keys()):
        count = len(articles_by_primary_category[cat])
        print(f"{cat:<20} {count:>10}")
    print()

    # 言語別集計
    print("言語別記事数:")
    print("-" * 80)
    language_counter = Counter()
    for article in articles:
        language_counter[article.language if article.language else 'unknown'] += 1

    print(f"{'言語':<20} {'記事数':>10}")
    print("-" * 80)
    for lang, count in sorted(language_counter.items(), key=lambda x: x[1], reverse=True):
        print(f"{lang:<20} {count:>10}")
    print()

    # 本文取得状況
    with_body = sum(1 for a in articles if a.body_text and len(a.body_text) > 0)
    without_body = total_articles - with_body
    body_rate = (with_body / total_articles * 100) if total_articles > 0 else 0

    print("本文取得状況:")
    print("-" * 80)
    print(f"本文あり: {with_body}件 ({body_rate:.1f}%)")
    print(f"本文なし: {without_body}件 ({100-body_rate:.1f}%)")
    print()

    # ソース別集計（上位10件）
    print("ソース別記事数（上位10件）:")
    print("-" * 80)
    source_counter = Counter()
    for article in articles:
        source_counter[article.source if article.source else 'unknown'] += 1

    print(f"{'ソース':<30} {'記事数':>10}")
    print("-" * 80)
    for source, count in source_counter.most_common(10):
        print(f"{source:<30} {count:>10}")
    print()

    # カテゴリ別サンプル表示
    print("=" * 80)
    print("各カテゴリのサンプル記事（主カテゴリベース）")
    print("=" * 80)
    print()

    target_categories = ['COPPER', 'ALUMINIUM', 'ZINC', 'LEAD', 'NICKEL', 'TIN',
                        'EQUITY', 'FOREX', 'COMMODITIES', 'NY_MARKET']

    for cat in target_categories:
        if cat in articles_by_primary_category:
            sample_articles = articles_by_primary_category[cat][:3]
            print(f"【{cat}】 {len(articles_by_primary_category[cat])}件")
            print("-" * 80)
            for i, article in enumerate(sample_articles, 1):
                headline = article.headline[:60] + "..." if len(article.headline) > 60 else article.headline
                print(f"  {i}. {headline}")
                print(f"     言語: {article.language} | ソース: {article.source}")
                if article.body_text:
                    print(f"     本文: {len(article.body_text)}文字")
            print()

    print("=" * 80)
    print("分析完了")
    print("=" * 80)

if __name__ == "__main__":
    analyze_categories()
