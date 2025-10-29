@echo off
REM Refinitiv ニュース日次取得＋定期更新バッチ
REM 朝一で前日分のニュースを取得し、その後5分間隔で更新を継続

echo ========================================
echo Refinitiv News Daily Fetch and Scheduler
echo ========================================
echo.

REM 作業ディレクトリに移動
cd /d "C:\Users\09848\git\RfNews_toDB"

REM 仮想環境をアクティベート
echo 仮想環境をアクティベート中...
call venv\Scripts\activate

REM Refinitiv EIKONが起動するまで少し待機
echo Refinitiv EIKON起動待機中（30秒）...
timeout /t 30 /nobreak > nul

REM 1. 前日分のニュースを取得
echo.
echo ========================================
echo 前日分のニュース取得開始
echo ========================================

REM 前日の日付を計算（PowerShellを使用）
for /f "tokens=*" %%i in ('powershell -Command "(Get-Date).AddDays(-1).ToString('yyyy-MM-dd')"') do set YESTERDAY=%%i
set START_DATE=%YESTERDAY%T00:00:00
set END_DATE=%YESTERDAY%T23:59:59

echo 取得期間: %START_DATE% ～ %END_DATE%
echo.

REM 前日分のニュースを取得（最大500件）
python main.py fetch --count 500 --start-date %START_DATE% --end-date %END_DATE%

if %ERRORLEVEL% EQU 0 (
    echo 前日分のニュース取得完了
) else (
    echo エラー: 前日分のニュース取得に失敗しました
    echo エラーコード: %ERRORLEVEL%
)

REM 統計情報を表示
echo.
echo ========================================
echo データベース統計情報
echo ========================================
python main.py stats

REM 2. スケジューラーを起動（5分間隔で最新ニュースを取得）
echo.
echo ========================================
echo スケジューラー起動（5分間隔）
echo ========================================
echo Ctrl+C で終了できます
echo.

REM スケジューラーをデーモンモードで実行
python src/scheduler.py --daemon

REM スケジューラーが終了した場合（通常はCtrl+Cで停止）
echo.
echo スケジューラーが終了しました
pause