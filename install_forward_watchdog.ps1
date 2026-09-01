param(
    [string]$TrialId = "xauusd-regime-session-open-h1-forward-v3",
    [string]$AccountKey = "Exness-MT5Trial5:277817628",
    [string]$ObserverTaskName = "TradingBot Forward Observer v3",
    [string]$WatchdogTaskName = "TradingBot Forward Watchdog v3",
    [string]$BotRoot = "C:\mtbot2",
    [string]$DashboardRoot = "C:\mtbot-dashboard",
    [string]$BackupRoot = "C:\mtbot-backups\forward-v3",
    [string]$NewsFile = "C:\Users\Administrator\AppData\Roaming\MetaQuotes\Terminal\Common\Files\TradingBot-Mt5\news_calendar.csv"
)

$ErrorActionPreference = "Stop"

if (Get-ScheduledTask -TaskName $WatchdogTaskName -ErrorAction SilentlyContinue) {
    throw "Task '$WatchdogTaskName' already exists; stop and inspect it."
}

$python = Join-Path $BotRoot ".venv\Scripts\python.exe"
$watchdog = Join-Path $DashboardRoot "forward_watchdog.py"
$forwardDb = Join-Path $BotRoot "data\forward\$TrialId.sqlite3"
$executionDb = Join-Path $BotRoot "data\execution\execution.sqlite3"
$manifest = Join-Path $BotRoot "reports\manifests\$TrialId.json"
$statusFile = Join-Path $BackupRoot "watchdog-latest.json"

foreach ($required in @($python, $watchdog, $forwardDb, $executionDb, $manifest, $NewsFile)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Required deployment file is missing: $required"
    }
}

$observerTask = Get-ScheduledTask -TaskName $ObserverTaskName -ErrorAction Stop
if ([string]$observerTask.State -notin @("Ready", "Running")) {
    throw "Observer task '$ObserverTaskName' is not enabled and ready."
}

New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null

function Quote-TaskArgument([string]$Value) {
    return '"' + $Value.Replace('"', '\"') + '"'
}

$argumentList = @(
    (Quote-TaskArgument $watchdog),
    "--trial-id", (Quote-TaskArgument $TrialId),
    "--forward-db", (Quote-TaskArgument $forwardDb),
    "--execution-db", (Quote-TaskArgument $executionDb),
    "--manifest", (Quote-TaskArgument $manifest),
    "--account-key", (Quote-TaskArgument $AccountKey),
    "--task-name", (Quote-TaskArgument $ObserverTaskName),
    "--news-file", (Quote-TaskArgument $NewsFile),
    "--bot-repo", (Quote-TaskArgument $BotRoot),
    "--backup-dir", (Quote-TaskArgument $BackupRoot),
    "--status-file", (Quote-TaskArgument $statusFile),
    "--backup-interval-hours", "24",
    "--retention-count", "14"
)

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument ($argumentList -join " ") `
    -WorkingDirectory $DashboardRoot

$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At ((Get-Date).AddMinutes(2)) `
    -RepetitionInterval (New-TimeSpan -Minutes 15)

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 2)

Register-ScheduledTask `
    -TaskName $WatchdogTaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $observerTask.Principal `
    -Description "P&L-blind health checks and verified online backups for forward v3"

Start-ScheduledTask -TaskName $WatchdogTaskName

Write-Host "Installed: $WatchdogTaskName"
Write-Host "Observer:  $ObserverTaskName"
Write-Host "Backups:   $BackupRoot (14 verified snapshots retained)"
Write-Host "Status:    $statusFile"
Write-Host "Mode:      PAPER / READ-ONLY / PERFORMANCE BLINDED"
