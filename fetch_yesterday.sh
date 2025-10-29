#!/bin/bash
# 前日分のニュースを全件取得するスクリプト

# 前日の日付を計算（UTC）
YESTERDAY=$(date -u -v-1d '+%Y-%m-%d')
START_DATE="${YESTERDAY}T00:00:00"
END_DATE="${YESTERDAY}T23:59:59"

echo "========================================"
echo "前日ニュース全件取得"
echo "========================================"
echo "対象日: ${YESTERDAY}"
echo "開始: $(date)"
echo ""

# 仮想環境をアクティベート＆実行
source venv/bin/activate && python main.py fetch-backfill \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --per-page 100

EXIT_CODE=$?

echo ""
echo "========================================"
echo "取得完了"
echo "終了: $(date)"
echo "終了コード: ${EXIT_CODE}"
echo "========================================"

exit ${EXIT_CODE}
