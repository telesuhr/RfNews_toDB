@echo off
REM Refinitiv ニュースカテゴリー別取得＋スケジューラー起動
REM 各カテゴリーの前日分ニュースを取得後、定期更新を開始

echo ========================================
echo Refinitiv News Category Fetch
echo ========================================
echo.

REM 作業ディレクトリに移動
cd /d "C:\Users\09848\git\RfNews_toDB"

REM 仮想環境をアクティベート
echo 仮想環境をアクティベート中...
call venv\Scripts\activate

REM Refinitiv EIKONが起動するまで待機
echo Refinitiv EIKON起動待機中（30秒）...
timeout /t 30 /nobreak > nul

REM 前日の日付を計算（PowerShellを使用）
for /f "tokens=*" %%i in ('powershell -Command "(Get-Date).AddDays(-1).ToString('yyyy-MM-dd')"') do set YESTERDAY=%%i
set START_DATE=%YESTERDAY%T00:00:00
set END_DATE=%YESTERDAY%T23:59:59

echo.
echo 取得期間: %START_DATE% ～ %END_DATE%
echo.

REM カテゴリーリストを定義
set CATEGORIES=COPPER ALUMINIUM ZINC LEAD NICKEL TIN EQUITY FOREX COMMODITIES NY_MARKET

REM 各カテゴリーをループで処理
for %%C in (%CATEGORIES%) do (
    echo ========================================
    echo カテゴリー: %%C のニュース取得開始
    echo ========================================
    
    python main.py fetch --category %%C --count 200 --start-date %START_DATE% --end-date %END_DATE%
    
    if errorlevel 1 (
        echo エラー: %%C カテゴリーの取得に失敗しました
    ) else (
        echo %%C カテゴリーの取得が完了しました
    )
    
    echo.
    REM API制限対策で少し待機
    timeout /t 5 /nobreak > nul
)

REM 統計情報を表示
echo ========================================
echo データベース統計情報
echo ========================================
python main.py stats

REM スケジューラーを起動（5分間隔で最新ニュースを取得）
echo.
echo ========================================
echo スケジューラー起動（5分間隔の定期実行）
echo ========================================
echo Ctrl+C で終了できます
echo.

REM スケジューラーをデーモンモードで実行
python src/scheduler.py --daemon

REM スケジューラーが終了した場合
echo.
echo スケジューラーが終了しました
pause