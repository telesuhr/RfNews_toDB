# Windows タスクスケジューラ設定用PowerShellスクリプト
# 管理者権限で実行してください

$taskName = "RefinitivNewsMorningFetch"
$taskPath = "\"
$scriptPath = "C:\Users\09848\git\RfNews_toDB\run_morning_startup.bat"

# 既存のタスクがある場合は削除
$existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "既存のタスクを削除します..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

# トリガー設定（毎日午前6時に実行）
$trigger = New-ScheduledTaskTrigger -Daily -At "06:00:00"

# アクション設定
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$scriptPath`""

# プリンシパル設定（ログオンしていなくても実行）
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

# 設定
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 23) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5)

# タスクを登録
$task = Register-ScheduledTask `
    -TaskName $taskName `
    -TaskPath $taskPath `
    -Trigger $trigger `
    -Action $action `
    -Principal $principal `
    -Settings $settings `
    -Description "Refinitivニュースを朝6時に前日分取得し、その後5分間隔で更新"

if ($task) {
    Write-Host "タスクが正常に作成されました: $taskName" -ForegroundColor Green
    Write-Host "実行時刻: 毎日 6:00 AM" -ForegroundColor Cyan
    
    # タスクの詳細を表示
    Get-ScheduledTask -TaskName $taskName | Format-List TaskName, State, Description
    Get-ScheduledTaskInfo -TaskName $taskName | Format-List LastRunTime, NextRunTime
} else {
    Write-Host "タスクの作成に失敗しました" -ForegroundColor Red
}