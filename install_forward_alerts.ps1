param(
    [string]$TrialId = "xauusd-regime-session-open-h1-forward-v3",
    [string]$WatchdogTaskName = "TradingBot Forward Watchdog v3",
    [string]$AlertTaskName = "TradingBot Forward Alerts v3",
    [string]$BotRoot = "C:\mtbot2",
    [string]$DashboardRoot = "C:\mtbot-dashboard",
    [string]$BackupRoot = "C:\mtbot-backups\forward-v3"
)

$ErrorActionPreference = "Stop"

if (Get-ScheduledTask -TaskName $AlertTaskName -ErrorAction SilentlyContinue) {
    throw "Task '$AlertTaskName' already exists; stop and inspect it."
}

$python = Join-Path $BotRoot ".venv\Scripts\python.exe"
$script = Join-Path $DashboardRoot "forward_alerts.py"
$watchdogStatus = Join-Path $BackupRoot "watchdog-latest.json"
$alertRoot = Join-Path $BackupRoot "alerts"
$alertJournal = Join-Path $alertRoot "alert-journal.sqlite3"
$alertState = Join-Path $alertRoot "alert-state.json"

foreach ($required in @($python, $script, $watchdogStatus)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Required deployment file is missing: $required"
    }
}

$eventCreate = Join-Path $env:SystemRoot "System32\eventcreate.exe"
if (-not (Test-Path -LiteralPath $eventCreate -PathType Leaf)) {
    throw "Windows Event Log writer is missing: $eventCreate"
}

$watchdogTask = Get-ScheduledTask -TaskName $WatchdogTaskName -ErrorAction Stop
if ([string]$watchdogTask.State -notin @("Ready", "Running")) {
    throw "Watchdog task '$WatchdogTaskName' is not enabled and ready."
}

New-Item -ItemType Directory -Path $alertRoot -Force | Out-Null

& $eventCreate `
    /L APPLICATION `
    /SO "TradingBot Forward Alerts" `
    /T INFORMATION `
    /ID 900 `
    /D "TradingBot forward-v3 blinded alert pipeline commissioned; no incident; performance=BLINDED"
if ($LASTEXITCODE -ne 0) {
    throw "Windows Event Log commissioning probe failed."
}

function Quote-TaskArgument([string]$Value) {
    return '"' + $Value.Replace('"', '\"') + '"'
}

$argumentList = @(
    (Quote-TaskArgument $script),
    "--trial-id", (Quote-TaskArgument $TrialId),
    "--watchdog-status", (Quote-TaskArgument $watchdogStatus),
    "--alert-journal", (Quote-TaskArgument $alertJournal),
    "--alert-state", (Quote-TaskArgument $alertState),
    "--maximum-watchdog-age-seconds", "1800",
    "--maximum-backup-age-hours", "26",
    "--delivery-sink", "windows-event-log"
)

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument ($argumentList -join " ") `
    -WorkingDirectory $DashboardRoot

$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At ((Get-Date).AddMinutes(1)) `
    -RepetitionInterval (New-TimeSpan -Minutes 5)

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 2)

Register-ScheduledTask `
    -TaskName $AlertTaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $watchdogTask.Principal `
    -Description "Blinded incident and recovery alerts for forward v3"

Start-ScheduledTask -TaskName $AlertTaskName

Write-Host "Installed:  $AlertTaskName"
Write-Host "Watchdog:   $WatchdogTaskName"
Write-Host "Journal:    $alertJournal (append-only)"
Write-Host "State:      $alertState"
Write-Host "Delivery:   Windows Application event log"
Write-Host "Mode:       PAPER / READ-ONLY / PERFORMANCE BLINDED"
