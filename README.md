# Refinitiv ニュース情報データベース格納システム

Refinitiv EIKON APIを使用してニュース情報を取得し、データベースに格納するシステムです。

## 機能概要

- **ニュース取得**: Refinitiv EIKON APIからニュースヘッドラインを取得
- **データベース格納**: 取得したニュースをMySQLデータベースに格納
- **重複除去**: 同一記事の重複格納を防止
- **スケジューリング**: 定期的な自動取得とバッチ処理
- **エラーハンドリング**: リトライ機能とレート制限対応
- **ログ管理**: 詳細なログ出力と統計情報

## システム要件

- Python 3.8以上
- MySQL 5.7以上（または PostgreSQL）
- Refinitiv EIKON デスクトップアプリケーション
- Refinitiv API キー

## インストール

1. 依存関係のインストール
```bash
pip install -r requirements.txt
```

2. 設定ファイルの作成
```bash
cp config/config_template.json config/config.json
```

3. 設定ファイルの編集
```json
{
  "eikon": {
    "api_key": "YOUR_REFINITIV_API_KEY_HERE"
  },
  "database": {
    "host": "localhost",
    "username": "root", 
    "password": "password",
    "database": "refinitiv_news"
  }
}
```

4. データベースの作成
```sql
CREATE DATABASE refinitiv_news CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

5. テーブルの作成
```bash
python main.py fetch --count 1  # 初回実行でテーブルが自動作成されます
```

## 使用方法

### コマンドライン実行

#### ニュース取得・格納
```bash
# 基本的な使用法（100件取得）
python main.py fetch

# 件数を指定
python main.py fetch --count 200

# カテゴリフィルタ
python main.py fetch --category EQUITY --count 50

# 言語フィルタ
python main.py fetch --language ja --count 30

# 日付範囲指定
python main.py fetch --start-date 2024-01-01T00:00:00 --end-date 2024-01-31T23:59:59
```

#### 統計情報表示
```bash
python main.py stats
```

#### 古い記事のクリーンアップ
```bash
# 365日より古い記事を削除
python main.py cleanup --days 365
```

### スケジューラー実行

#### デーモンモード（定期実行）
```bash
python src/scheduler.py --daemon
```

#### 単発ジョブ実行
```bash
# 最新ニュース取得
python src/scheduler.py --job latest

# 日次バッチ実行
python src/scheduler.py --job daily

# メンテナンス実行
python src/scheduler.py --job maintenance
```

## プロジェクト構造

```
RfNews_toDB/
├── config/
│   ├── config.json              # メイン設定ファイル
│   ├── config_template.json     # 設定テンプレート
│   └── database.py             # データベース接続設定
├── src/
│   ├── news_fetcher.py         # Refinitiv ニュース取得
│   ├── database_manager.py     # データベース操作
│   ├── logger.py               # ログ設定
│   └── scheduler.py           # 定期実行スケジューラー
├── tests/
│   ├── test_news_fetcher.py    # ニュース取得テスト
│   └── test_database_manager.py # データベーステスト
├── sql/
│   └── create_tables.sql       # データベーススキーマ
├── logs/                       # ログディレクトリ
├── requirements.txt            # Python依存関係
├── main.py                    # メインアプリケーション
└── README.md                  # このファイル
```

## データベース構造

### news_articles テーブル
| カラム名 | 型 | 説明 |
|---------|---|------|
| id | BIGINT | 主キー |
| story_id | VARCHAR(100) | Refinitiv記事ID（ユニーク） |
| headline | TEXT | ニュースヘッドライン |
| summary | TEXT | ニュース要約 |
| body_text | LONGTEXT | 記事本文 |
| source | VARCHAR(100) | ニュースソース |
| published_at | DATETIME | 発行日時（UTC） |
| created_at | DATETIME | レコード作成日時 |
| updated_at | DATETIME | レコード更新日時 |
| language | VARCHAR(10) | 言語コード |
| category | VARCHAR(50) | ニュースカテゴリ |

### news_rics テーブル
| カラム名 | 型 | 説明 |
|---------|---|------|
| id | BIGINT | 主キー |
| news_id | BIGINT | ニュース記事ID |
| ric_code | VARCHAR(50) | RICコード |
| relevance_score | DECIMAL(3,2) | 関連度スコア |

### news_fetch_log テーブル
| カラム名 | 型 | 説明 |
|---------|---|------|
| id | BIGINT | 主キー |
| fetch_start | DATETIME | 取得開始日時 |
| fetch_end | DATETIME | 取得終了日時 |
| articles_fetched | INT | 取得記事数 |
| articles_inserted | INT | 新規挿入記事数 |
| status | ENUM | 取得ステータス |

## 設定オプション

### Refinitiv API設定
- `api_key`: Refinitiv APIキー
- `timeout_seconds`: APIタイムアウト（秒）
- `max_retries`: 最大リトライ回数
- `rate_limit_delay`: レート制限遅延（秒）

### データベース設定
- `host`: データベースホスト
- `port`: データベースポート
- `username`: ユーザー名
- `password`: パスワード
- `database`: データベース名

### ニュース取得設定
- `default_count`: デフォルト取得件数
- `max_count`: 最大取得件数
- `languages`: 対象言語
- `categories`: 対象カテゴリ

## ログ出力

ログは以下の場所に出力されます：
- ファイル: `logs/refinitiv_news.log`
- コンソール: 標準出力

ログレベル:
- `INFO`: 通常の処理状況
- `WARNING`: 警告（処理は継続）
- `ERROR`: エラー（処理中断の可能性）
- `DEBUG`: デバッグ情報（`--verbose`オプション時）

## トラブルシューティング

### よくある問題

1. **API接続エラー**
   - Refinitiv EIKONデスクトップが起動していることを確認
   - APIキーが正しく設定されていることを確認

2. **データベース接続エラー**
   - データベースサーバーが起動していることを確認
   - 接続情報（ホスト、ポート、認証情報）を確認

3. **重複エラー**
   - 正常な動作です（同一記事の重複を防止）
   - ログで確認可能

4. **レート制限エラー**
   - 自動的にリトライされます
   - 設定で遅延時間を調整可能

### デバッグ方法

詳細ログ出力:
```bash
python main.py fetch --verbose
```

特定のエラーログ確認:
```bash
tail -f logs/refinitiv_news.log | grep ERROR
```

## 開発者向け情報

### テスト実行
```bash
# 全テスト実行
pytest

# 特定のテストファイル
pytest tests/test_news_fetcher.py

# カバレッジ付き
pytest --cov=src tests/
```

### TDD開発プロセス
このプロジェクトはTDD（テスト駆動開発）で開発されています：
1. テストを先に作成
2. テストが失敗することを確認
3. 実装してテストを通す
4. リファクタリング

### コード品質
- PEP 8準拠
- 型ヒント使用
- docstring記述
- エラーハンドリング実装

## ライセンス

このプロジェクトは内部使用目的で作成されています。

## サポート

質問や問題がある場合は、プロジェクト管理者にお問い合わせください。