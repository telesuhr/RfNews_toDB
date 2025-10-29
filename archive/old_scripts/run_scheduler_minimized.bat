@echo off
REM 最小化ウィンドウで実行するバッチファイル

if not "%minimized%"=="" goto :minimized
set minimized=true
start /min cmd /C "%~0"
goto :EOF

:minimized
REM プロジェクトディレクトリに移動（パスは環境に合わせて変更）
cd /d "C:\path\to\RfNews_toDB"

REM ログファイルに開始時刻を記録
echo [%date% %time%] Starting Refinitiv News Scheduler (minimized) >> logs\scheduler_startup.log

REM Refinitiv EIKON起動待機
timeout /t 60 /nobreak > nul

REM Python仮想環境をアクティベート
call venv\Scripts\activate

REM スケジューラーを起動
python src/scheduler.py --daemon >> logs\scheduler_startup.log 2>&1