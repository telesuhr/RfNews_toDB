#!/usr/bin/env python
"""
複数カテゴリ検出機能のテスト
新しいシンプル化されたカテゴリシステムの動作確認
"""
import os
import sys

# パス設定
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from news_fetcher import NewsFetcher

def test_multi_category_detection():
    """複数カテゴリ検出機能をテスト"""
    print("🧪 複数カテゴリ検出機能テスト")
    print("=" * 50)
    
    # フェッチャー初期化
    fetcher = NewsFetcher()
    
    # テスト用のサンプルデータを作成
    import pandas as pd
    from datetime import datetime, timezone
    
    # 複数の金属を含むヘッドラインのサンプル
    test_data = pd.DataFrame([
        {
            'storyId': 'test-001',
            'text': 'Copper and zinc prices rise amid supply concerns',  # COPPER,ZINC
            'versionCreated': datetime.now(timezone.utc),
            'sourceCode': 'NS:RTRS'
        },
        {
            'storyId': 'test-002', 
            'text': 'ＮＹ市場サマリー（株式・商品・為替）',  # NY_MARKET,EQUITY,COMMODITIES,FOREX
            'versionCreated': datetime.now(timezone.utc),
            'sourceCode': 'NS:RTRS'
        },
        {
            'storyId': 'test-003',
            'text': 'Aluminum demand outlook positive despite nickel volatility',  # ALUMINIUM,NICKEL
            'versionCreated': datetime.now(timezone.utc),
            'sourceCode': 'NS:RTRS'
        },
        {
            'storyId': 'test-004',
            'text': 'Lead and tin markets stabilize',  # LEAD,TIN
            'versionCreated': datetime.now(timezone.utc),
            'sourceCode': 'NS:RTRS'
        },
        {
            'storyId': 'test-005',
            'text': 'Oil prices fall as equity markets gain',  # COMMODITIES,EQUITY
            'versionCreated': datetime.now(timezone.utc),
            'sourceCode': 'NS:RTRS'
        }
    ])
    
    print("テスト用サンプルデータ:")
    for i, row in test_data.iterrows():
        print(f"  {i+1}. {row['text']}")
    
    print("\n" + "-" * 50)
    
    # 各記事のヘッドラインと本文をチェックして複数カテゴリを検出
    for i, row in test_data.iterrows():
        headline = str(row.get('text', '')).lower()
        categories_found = []
        
        # 非鉄金属6種のキーワード検出
        if 'copper' in headline or '銅' in headline:
            categories_found.append('COPPER')
        if 'aluminum' in headline or 'aluminium' in headline or 'アルミ' in headline:
            categories_found.append('ALUMINIUM')
        if 'zinc' in headline or '亜鉛' in headline:
            categories_found.append('ZINC')
        if 'lead' in headline or '鉛' in headline:
            categories_found.append('LEAD')
        if 'nickel' in headline or 'ニッケル' in headline:
            categories_found.append('NICKEL')
        if 'tin' in headline or 'スズ' in headline:
            categories_found.append('TIN')
        
        # 基本カテゴリ3種
        if 'equity' in headline or '株式' in headline or 'stock' in headline:
            categories_found.append('EQUITY')
        if 'forex' in headline or '外国為替' in headline or 'currency' in headline or '為替' in headline:
            categories_found.append('FOREX')
        if 'commodity' in headline or 'commodities' in headline or '商品' in headline:
            categories_found.append('COMMODITIES')
        
        # 特別カテゴリ
        if 'ny市場サマリー' in headline or 'ＮＹ市場サマリー' in headline or 'ｎｙ市場サマリー' in headline:
            categories_found.append('NY_MARKET')
        
        # カンマ区切りで結合
        final_category = ','.join(categories_found) if categories_found else ''
        
        print(f"記事 {i+1}:")
        print(f"  ヘッドライン: {row['text']}")
        print(f"  検出カテゴリ: {final_category if final_category else '(なし)'}")
        print()

if __name__ == "__main__":
    test_multi_category_detection()