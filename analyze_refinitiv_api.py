"""
Refinitiv APIの詳細調査
ニュース本文取得とカテゴリ設定機能を確認
"""
import os
import sys
import pandas as pd

# パス設定
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from news_fetcher import NewsFetcher

def analyze_news_api():
    """Refinitiv ニュースAPIの詳細分析"""
    print("🔍 Refinitiv News API 詳細分析")
    print("=" * 60)
    
    # フェッチャー初期化
    fetcher = NewsFetcher()
    
    # 接続
    if not fetcher.connect():
        print("❌ Refinitiv EIKON接続失敗")
        return
    
    print("✅ Refinitiv EIKON接続成功")
    
    try:
        import eikon as ek
        
        # 1. get_news_headlines の詳細調査
        print("\n📰 1. get_news_headlines の機能分析")
        print("-" * 40)
        
        # 基本的な取得
        headlines = ek.get_news_headlines(count=3)
        print(f"基本取得: {headlines.columns.tolist()}")
        print(headlines)
        
        # 2. get_news_story で本文取得を試行
        print("\n📖 2. get_news_story での本文取得テスト")
        print("-" * 40)
        
        if not headlines.empty:
            first_story_id = headlines.iloc[0]['storyId']
            print(f"テスト対象: {first_story_id}")
            
            try:
                story = ek.get_news_story(first_story_id)
                print(f"本文取得成功: {len(str(story))}文字")
                print(f"本文サンプル: {str(story)[:200]}...")
            except Exception as e:
                print(f"本文取得エラー: {e}")
        
        # 3. カテゴリ指定での取得テスト
        print("\n🏷️ 3. カテゴリ指定機能テスト")
        print("-" * 40)
        
        categories_to_test = ['TOP', 'METALS', 'COMMODITIES', 'MINING', 'LME']
        
        for category in categories_to_test:
            try:
                print(f"\nカテゴリ '{category}' での取得テスト:")
                # カテゴリパラメータを試行
                result = ek.get_news_headlines(count=2, category=category)
                print(f"  ✅ 成功: {len(result)}件取得")
            except Exception as e:
                print(f"  ❌ 失敗: {e}")
        
        # 4. その他のパラメータテスト
        print("\n⚙️ 4. その他のパラメータテスト")
        print("-" * 40)
        
        # 日付指定
        try:
            from datetime import datetime, timedelta
            start_date = datetime.now() - timedelta(days=1)
            end_date = datetime.now()
            
            result = ek.get_news_headlines(
                count=2,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            print(f"  ✅ 日付指定: {len(result)}件取得")
        except Exception as e:
            print(f"  ❌ 日付指定失敗: {e}")
        
        # RIC指定
        try:
            result = ek.get_news_headlines(count=2, instruments=['LMCAD03'])
            print(f"  ✅ RIC指定: {len(result)}件取得")
        except Exception as e:
            print(f"  ❌ RIC指定失敗: {e}")
        
        # 5. 利用可能な全パラメータの確認
        print("\n📋 5. get_news_headlines パラメータ仕様")
        print("-" * 40)
        help(ek.get_news_headlines)
        
    except Exception as e:
        print(f"❌ 分析エラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_news_api()