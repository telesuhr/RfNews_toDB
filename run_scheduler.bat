@echo off
echo Starting Refinitiv News Scheduler...
echo.

REM プロジェクトディレクトリに移動（パスは環境に合わせて変更）
cd /d "C:\path\to\RfNews_toDB"

REM Refinitiv EIKONが起動するまで待機（オプション）
echo Waiting for Refinitiv EIKON to start...
timeout /t 30 /nobreak > nul

REM Python仮想環境をアクティベート
echo Activating virtual environment...
call venv\Scripts\activate

REM スケジューラーを起動
echo Starting scheduler in daemon mode...
python src/scheduler.py --daemon

REM エラーが発生した場合は一時停止
if errorlevel 1 (
    echo.
    echo ERROR: Scheduler failed to start!
    pause
)