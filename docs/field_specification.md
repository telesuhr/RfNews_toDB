# ニュース記事フィールド仕様書

## 現状の問題点と改善方針

### 1. `language` フィールド（言語判定）

#### 現在の仕様（問題あり）
- **実装箇所**: `src/news_fetcher.py:574`
- **現在の動作**: 全記事に対して `language='en'` を固定設定
- **問題点**:
  - 多言語記事が混在しているにもかかわらず、全て英語として扱われている
  - 実際のデータには韓国語、スペイン語、フランス語、ドイツ語、日本語などが含まれる

#### API調査結果
**Refinitiv `get_news_headlines()` のレスポンスフィールド**:
- `versionCreated`: 発行日時
- `text`: ヘッドライン
- `storyId`: ストーリーID
- `sourceCode`: ソースコード（例: NS:RTRS）

**結論**: 言語情報はAPIから取得できない

#### 改善案（確定）

**自動言語検出ライブラリを使用**
- ライブラリ: `langdetect`（軽量・高精度）
- 実装箇所: `src/news_fetcher.py:574` の `create_news_articles()` 関数
- 判定対象: ヘッドライン（`text`フィールド）
- 実装例:
```python
from langdetect import detect, LangDetectException

try:
    language = detect(headline)
except LangDetectException:
    language = 'unknown'
```

#### 対応言語
`langdetect` は以下の言語を検出可能:
- `en`: 英語
- `ja`: 日本語
- `ko`: 韓国語
- `es`: スペイン語
- `fr`: フランス語
- `de`: ドイツ語
- `zh-cn`: 中国語（簡体字）
- その他55言語以上

---

### 2. `urgency_level` フィールド（緊急度）

#### 現在の仕様（形骸化）
- **実装箇所**: `src/news_fetcher.py:576`
- **現在の動作**: 全記事に対して `urgency_level=3` を固定設定
- **問題点**:
  - 全記事が同じ緊急度で区別がつかない
  - フィールドが意味を持たない

#### API調査結果
**結論**: 緊急度情報はAPIから取得できない（上記参照）

#### 改善案（確定）

**キーワードベースで緊急度を判定**
- 実装箇所: `src/news_fetcher.py:576` の `create_news_articles()` 関数
- 判定対象: ヘッドライン + 本文（利用可能な場合）
- 判定ルール:

| urgency_level | 説明 | キーワード |
|--------------|------|-----------|
| 1 | 高緊急度 | `breaking`, `urgent`, `alert`, `flash`, `emergency`, `速報`, `緊急` |
| 2 | 中緊急度 | `important`, `significant`, `major`, `重要`, `重大`, `significant` |
| 3 | 通常 | 上記以外のすべて（デフォルト） |

- 実装例:
```python
def detect_urgency(headline: str, body_text: str = None) -> int:
    text = (headline + ' ' + (body_text or '')).lower()

    high_keywords = ['breaking', 'urgent', 'alert', 'flash', 'emergency', '速報', '緊急']
    medium_keywords = ['important', 'significant', 'major', '重要', '重大']

    if any(keyword in text for keyword in high_keywords):
        return 1
    elif any(keyword in text for keyword in medium_keywords):
        return 2
    else:
        return 3
```

#### 注意事項
- 本文がない場合はヘッドラインのみで判定
- 誤判定を減らすため、単語境界を考慮する実装を推奨

---

### 3. `priority_score` フィールド（優先度スコア）

#### 現在の仕様（部分的に機能）
- **実装箇所**: `src/news_fetcher.py:577`, `_calculate_priority_scores()` (line 631-680)
- **現在の動作**:
  - `config.json` の `priority_scoring.enabled` が `false` のため、スコア計算が実行されない
  - 結果として全記事のスコアが `0` になる
- **設計意図**:
  - キーワードマッチングでスコアを計算
  - 高優先度キーワード: +10点（strike, shutdown, supply cut など）
  - 中優先度キーワード: +5点（price, demand, supply など）
  - 低優先度キーワード: +2点（trade, market, report など）

#### 改善案
以下の2つの方針から選択：

**Option A: 優先度スコアリングを有効化**
- `config.json` で `priority_scoring.enabled: true` に設定
- 現在の実装をそのまま使用
- メリット: 重要度の高い記事を優先的に処理できる
- デメリット: 低スコア記事がフィルタリングされる可能性

**Option B: スコアリングは実施するがフィルタリングしない**
- `priority_scoring.enabled: true` に設定
- `minimum_score: 0` に設定（フィルタリング無効）
- メリット: 全記事を保存しつつ、スコアで並び替え可能
- デメリット: 優先度スコアの意味が薄れる

#### 推奨実装
**Option B（スコアリング有効・フィルタリング無効）**を推奨
- 理由:
  1. データの網羅性を維持
  2. 後からスコアでフィルタリング・分析可能
  3. スコアが低くても重要な記事を見逃さない
- 設定変更:
```json
"priority_scoring": {
  "enabled": true,
  "minimum_score": 0,  // フィルタリング無効
  ...
}
```

---

## 実装優先度

| フィールド | 優先度 | 理由 |
|-----------|--------|------|
| `language` | **High** | 多言語記事の正確な分類が必要 |
| `priority_score` | **Medium** | 既存実装を有効化するだけで改善可能 |
| `urgency_level` | **Low** | 現状の固定値でも最低限の運用は可能 |

---

## 実装手順

### Phase 1: 優先度スコアの有効化（即時実施可能）
1. `config/config.json` を編集:
   - `priority_scoring.enabled: true`
   - `minimum_score: 0`
2. 動作確認

### Phase 2: 言語検出の実装
1. ✅ Refinitiv APIレスポンスフィールドを調査完了
   - **結果**: 言語情報フィールドなし
2. `langdetect` ライブラリをインストール:
   ```bash
   pip install langdetect
   ```
3. `src/news_fetcher.py` に言語検出関数を追加:
   - `_detect_language(headline: str) -> str` 関数を実装
4. `src/news_fetcher.py:574` を修正:
   - `language='en'` → `language=self._detect_language(headline)`
5. テスト実行・動作確認

### Phase 3: 緊急度判定の実装（オプション）
1. ✅ Refinitiv APIレスポンスフィールドを調査完了
   - **結果**: 緊急度情報フィールドなし
2. `src/news_fetcher.py` に緊急度判定関数を追加:
   - `_detect_urgency(headline: str, body_text: str = None) -> int` 関数を実装
3. `src/news_fetcher.py:576` を修正:
   - `urgency_level=3` → `urgency_level=self._detect_urgency(headline, body_text)`
4. テスト実行・動作確認

---

## 注意事項

1. **後方互換性**: 既存データとの整合性を考慮
2. **パフォーマンス**: 言語検出ライブラリは処理時間に影響する可能性
3. **テストデータ**: 各改善後は多様な言語の記事でテスト実施
4. **ログ出力**: 判定結果をログに記録し、精度を検証
