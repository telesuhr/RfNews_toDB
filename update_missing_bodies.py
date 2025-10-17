"""
本文なし記事の本文を取得してデータベースを更新するスクリプト
"""
import sys
import os
from datetime import datetime
import logging

# パス設定（main.pyと同じ方法）
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))

from news_fetcher import NewsFetcher
from database_manager import DatabaseManager

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main(category: str = None, limit: int = None):
    """
    本文なし記事の本文を取得してデータベースを更新

    Args:
        category: 特定カテゴリのみ更新（None=全て）
        limit: 更新件数制限（None=全て）
    """
    # 初期化
    fetcher = NewsFetcher()
    db_manager = DatabaseManager()

    # EIKON接続
    if not fetcher.connect():
        logger.error("EIKON接続失敗")
        return False

    # データベース接続
    if not db_manager.connect():
        logger.error("データベース接続失敗")
        return False

    # 本文なし記事を取得
    query = """
        SELECT id, story_id, headline, category
        FROM news_articles
        WHERE body_text IS NULL OR body_text = ''
    """

    if category:
        query += f" AND category LIKE '%{category}%'"

    query += " ORDER BY published_at DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        # contextマネージャーを使用してセッションを取得
        with db_manager.db_config.get_session() as session:
            from sqlalchemy import text
            result = session.execute(text(query))
            articles = result.fetchall()

            if not articles:
                logger.info("本文なし記事は見つかりませんでした")
                return True

            logger.info(f"本文なし記事: {len(articles)}件")

            # 本文を取得して更新
            updated_count = 0
            failed_count = 0

            for article in articles:
                article_id = article[0]
                story_id = article[1]
                headline = article[2]
                article_category = article[3]

                try:
                    logger.info(f"本文取得中: {story_id} ({headline[:50]}...)")

                    # レート制限
                    fetcher._apply_rate_limit()

                    # 本文取得
                    import eikon as ek
                    body = ek.get_news_story(story_id)

                    if body:
                        # HTMLタグを除去
                        clean_body = fetcher._clean_html(str(body))

                        # データベース更新
                        update_query = text("""
                            UPDATE news_articles
                            SET body_text = :body_text,
                                updated_at = :updated_at
                            WHERE id = :id
                        """)

                        session.execute(update_query, {
                            'body_text': clean_body,
                            'updated_at': datetime.utcnow(),
                            'id': article_id
                        })
                        session.commit()

                        updated_count += 1
                        logger.info(f"更新成功: {story_id} ({len(clean_body)}文字)")
                    else:
                        failed_count += 1
                        logger.warning(f"本文取得失敗: {story_id}")

                except Exception as e:
                    failed_count += 1
                    logger.error(f"エラー: {story_id} - {e}")
                    session.rollback()

            logger.info(f"更新完了: {updated_count}件成功, {failed_count}件失敗")
            return True

    except Exception as e:
        logger.error(f"処理エラー: {e}")
        return False

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='本文なし記事の本文を取得してデータベースを更新')
    parser.add_argument('--category', type=str, help='特定カテゴリのみ更新')
    parser.add_argument('--limit', type=int, help='更新件数制限')

    args = parser.parse_args()

    success = main(category=args.category, limit=args.limit)
    sys.exit(0 if success else 1)
