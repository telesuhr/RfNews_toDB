@echo off
REM Refinitiv ニュース朝一起動用バッチ（最小化実行対応）
REM タスクスケジューラやスタートアップから自動実行用

REM 最小化実行の処理
if not "%minimized%"=="" goto :minimized
set minimized=true
start /min cmd /C "%~0"
goto :EOF
:minimized

REM ログファイルの設定
set LOG_FILE=logs\morning_startup_%date:~0,4%%date:~5,2%%date:~8,2%.log

echo ======================================== >> %LOG_FILE%
echo Morning Startup - %date% %time% >> %LOG_FILE%
echo ======================================== >> %LOG_FILE%

REM 作業ディレクトリに移動
cd /d "C:\Users\09848\git\RfNews_toDB"

REM 仮想環境をアクティベート
call venv\Scripts\activate

REM Refinitiv EIKONの起動を待つ（60秒）
echo Waiting for Refinitiv EIKON... >> %LOG_FILE%
timeout /t 60 /nobreak > nul

REM 前日の日付を計算
for /f "tokens=*" %%i in ('powershell -Command "(Get-Date).AddDays(-1).ToString('yyyy-MM-dd')"') do set YESTERDAY=%%i
set START_DATE=%YESTERDAY%T00:00:00
set END_DATE=%YESTERDAY%T23:59:59

echo Fetching news from %START_DATE% to %END_DATE% >> %LOG_FILE%

REM 前日分のニュースを取得
python main.py fetch --count 500 --start-date %START_DATE% --end-date %END_DATE% >> %LOG_FILE% 2>&1

if %ERRORLEVEL% EQU 0 (
    echo Daily fetch completed successfully >> %LOG_FILE%
) else (
    echo ERROR: Daily fetch failed with code %ERRORLEVEL% >> %LOG_FILE%
)

REM 少し待機してからスケジューラーを起動
timeout /t 10 /nobreak > nul

echo Starting scheduler daemon... >> %LOG_FILE%

REM スケジューラーを起動（バックグラウンドで継続実行）
python src/scheduler.py --daemon >> %LOG_FILE% 2>&1