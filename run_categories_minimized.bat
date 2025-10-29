@echo off
REM Refinitiv ニュースカテゴリー別取得（最小化・自動実行対応版）
REM タスクスケジューラやスタートアップから実行用

REM 最小化実行の処理
if not "%minimized%"=="" goto :minimized
set minimized=true
start /min cmd /C "%~0"
goto :EOF
:minimized

REM ログファイルの設定（日付付き）
set LOG_DIR=logs
if not exist %LOG_DIR% mkdir %LOG_DIR%
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do set TODAY=%%c%%a%%b
set LOG_FILE=%LOG_DIR%\category_fetch_%TODAY%.log

echo ======================================== >> %LOG_FILE%
echo Category Fetch Started - %date% %time% >> %LOG_FILE%
echo ======================================== >> %LOG_FILE%

REM 作業ディレクトリに移動
cd /d "C:\Users\09848\git\RfNews_toDB"

REM 仮想環境をアクティベート
call venv\Scripts\activate

REM Refinitiv EIKONの起動を待つ（60秒）
echo Waiting for Refinitiv EIKON (60 seconds)... >> %LOG_FILE%
timeout /t 60 /nobreak > nul

REM 前日の日付を計算
for /f "tokens=*" %%i in ('powershell -Command "(Get-Date).AddDays(-1).ToString('yyyy-MM-dd')"') do set YESTERDAY=%%i
set START_DATE=%YESTERDAY%T00:00:00
set END_DATE=%YESTERDAY%T23:59:59

echo Fetch period: %START_DATE% to %END_DATE% >> %LOG_FILE%

REM カテゴリーリストを定義
set CATEGORIES=COPPER ALUMINIUM ZINC LEAD NICKEL TIN EQUITY FOREX COMMODITIES NY_MARKET

REM 成功/失敗カウンター
set /a SUCCESS_COUNT=0
set /a FAIL_COUNT=0

REM 各カテゴリーをループで処理
for %%C in (%CATEGORIES%) do (
    echo Processing category: %%C >> %LOG_FILE%
    
    python main.py fetch --category %%C --count 200 --start-date %START_DATE% --end-date %END_DATE% >> %LOG_FILE% 2>&1
    
    if errorlevel 1 (
        echo ERROR: Failed to fetch %%C >> %LOG_FILE%
        set /a FAIL_COUNT+=1
    ) else (
        echo SUCCESS: Fetched %%C >> %LOG_FILE%
        set /a SUCCESS_COUNT+=1
    )
    
    REM API制限対策で待機
    timeout /t 5 /nobreak > nul
)

echo ======================================== >> %LOG_FILE%
echo Summary: Success=%SUCCESS_COUNT%, Failed=%FAIL_COUNT% >> %LOG_FILE%
echo ======================================== >> %LOG_FILE%

REM 統計情報を記録
echo Database statistics: >> %LOG_FILE%
python main.py stats >> %LOG_FILE% 2>&1

REM スケジューラーを起動
echo Starting scheduler daemon... >> %LOG_FILE%
python src/scheduler.py --daemon >> %LOG_FILE% 2>&1