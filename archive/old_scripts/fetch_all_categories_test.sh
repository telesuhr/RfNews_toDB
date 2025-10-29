#!/bin/bash
# 2025年10月17日の全カテゴリニュース取得スクリプト

# 設定
START_DATE="2025-10-17T00:00:00"
END_DATE="2025-10-17T23:59:59"
PER_PAGE=100

# 全カテゴリリスト
CATEGORIES=(
    "ALUMINIUM"
    "ZINC"
    "LEAD"
    "NICKEL"
    "TIN"
    "EQUITY"
    "FOREX"
    "COMMODITIES"
    "NY_MARKET"
)

echo "========================================"
echo "2025年10月17日 全カテゴリニュース取得"
echo "========================================"
echo "開始時刻: $(date)"
echo ""

# COPPERは既に取得済みなのでスキップ
echo "COPPER: スキップ（取得済み）"
echo ""

# 各カテゴリごとに取得
for category in "${CATEGORIES[@]}"
do
    echo "----------------------------------------"
    echo "カテゴリ: $category"
    echo "開始: $(date +%H:%M:%S)"
    echo "----------------------------------------"

    source venv/bin/activate && python main.py fetch-backfill \
        --start-date "$START_DATE" \
        --end-date "$END_DATE" \
        --category "$category" \
        --per-page $PER_PAGE 2>&1 | head -5

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "✓ $category 完了"
    else
        echo "✗ $category 失敗 (終了コード: $EXIT_CODE)"
    fi

    echo "終了: $(date +%H:%M:%S)"
    echo ""

    # レート制限対策（5秒待機）
    sleep 5
done

echo "========================================"
echo "全カテゴリ取得完了"
echo "終了時刻: $(date)"
echo "========================================"
