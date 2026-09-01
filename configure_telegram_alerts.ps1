param(
    [string]$ChatId,
    [string]$TrialId = "xauusd-regime-session-open-h1-forward-v3",
    [string]$AlertTaskName = "TradingBot Forward Alerts v3",
    [string]$BotRoot = "C:\mtbot2",
    [string]$DashboardRoot = "C:\mtbot-dashboard",
    [string]$BackupRoot = "C:\mtbot-backups\forward-v3"
)

$ErrorActionPreference = "Stop"

function Resolve-AccountSid([string]$AccountName) {
    if ($AccountName -match '^S-1-') {
        return [Security.Principal.SecurityIdentifier]::new($AccountName)
    }
    return ([Security.Principal.NTAccount]::new($AccountName)).Translate(
        [Security.Principal.SecurityIdentifier]
    )
}

function Quote-TaskArgument([string]$Value) {
    return '"' + $Value.Replace('"', '\"') + '"'
}

$task = Get-ScheduledTask -TaskName $AlertTaskName -ErrorAction Stop
$currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
$taskSid = Resolve-AccountSid $task.Principal.UserId
if ($taskSid.Value -ne $currentIdentity.User.Value) {
    throw (
        "Telegram credentials must be configured by the same Windows " +
        "account that runs '$AlertTaskName'."
    )
}

$python = Join-Path $BotRoot ".venv\Scripts\python.exe"
$alertScript = Join-Path $DashboardRoot "forward_alerts.py"
$senderScript = Join-Path $DashboardRoot "send_telegram_alert.ps1"
$watchdogStatus = Join-Path $BackupRoot "watchdog-latest.json"
$alertRoot = Join-Path $BackupRoot "alerts"
$alertJournal = Join-Path $alertRoot "alert-journal.sqlite3"
$alertState = Join-Path $alertRoot "alert-state.json"
$credentialPath = Join-Path $alertRoot "telegram-credential.xml"

foreach ($required in @(
    $python, $alertScript, $senderScript, $watchdogStatus, $alertJournal,
    $alertState
)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Required deployment file is missing: $required"
    }
}

$existingState = Get-Content -LiteralPath $alertState -Raw | ConvertFrom-Json
if (
    $existingState.performance_blinded -ne $true -or
    $existingState.runtime_mode -ne "paper_read_only" -or
    $existingState.overall_status -ne "HEALTHY" -or
    $null -ne $existingState.active_incident -or
    [int]$existingState.pending_notifications -ne 0 -or
    $null -ne $existingState.last_transition
) {
    throw (
        "Telegram must be commissioned while the alert pipeline is healthy, " +
        "has no pending delivery and has never opened an incident."
    )
}

$secureToken = Read-Host `
    "Paste the Telegram bot token from BotFather (input is hidden)" `
    -AsSecureString
if ($secureToken.Length -eq 0) {
    throw "Telegram bot token cannot be empty."
}

$tokenPointer = [IntPtr]::Zero
$plainToken = $null
$botUsername = $null
try {
    $tokenPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR(
        $secureToken
    )
    $plainToken = [Runtime.InteropServices.Marshal]::PtrToStringBSTR(
        $tokenPointer
    )
    $apiRoot = "https://api.telegram.org/bot$plainToken"

    try {
        $identity = Invoke-RestMethod `
            -Method Get `
            -Uri "$apiRoot/getMe" `
            -TimeoutSec 20
    }
    catch {
        throw "Telegram bot authentication failed."
    }
    if ($identity.ok -ne $true -or $identity.result.is_bot -ne $true) {
        throw "Telegram bot authentication was rejected."
    }
    $botUsername = [string]$identity.result.username

    if ([string]::IsNullOrWhiteSpace($ChatId)) {
        try {
            $updates = Invoke-RestMethod `
                -Method Get `
                -Uri "$apiRoot/getUpdates?limit=100&timeout=0" `
                -TimeoutSec 20
        }
        catch {
            throw (
                "Telegram chat discovery failed. Ensure the bot has no " +
                "webhook, then send /start and try again."
            )
        }
        $chatIds = @(
            $updates.result |
                ForEach-Object {
                    if ($null -ne $_.message.chat.id) {
                        [string]$_.message.chat.id
                    }
                    if ($null -ne $_.channel_post.chat.id) {
                        [string]$_.channel_post.chat.id
                    }
                } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
                Sort-Object -Unique
        )
        if ($chatIds.Count -eq 0) {
            throw (
                "No Telegram chat was found. Open the bot in Telegram, " +
                "send /start, and run this script again."
            )
        }
        if ($chatIds.Count -gt 1) {
            throw (
                "More than one Telegram chat was found. Run the script " +
                "again with -ChatId for the intended private chat."
            )
        }
        $ChatId = $chatIds[0]
    }
}
finally {
    if ($tokenPointer -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($tokenPointer)
    }
    $plainToken = $null
}

New-Item -ItemType Directory -Path $alertRoot -Force | Out-Null
$credential = [System.Management.Automation.PSCredential]::new(
    [string]$ChatId,
    $secureToken
)
$credential | Export-Clixml -LiteralPath $credentialPath -Force

# DPAPI already binds the encrypted token to this user and machine. Restrict
# the file ACL as a second boundary and retain SYSTEM access for maintenance.
$acl = [Security.AccessControl.FileSecurity]::new()
$acl.SetAccessRuleProtection($true, $false)
$allow = [Security.AccessControl.AccessControlType]::Allow
$rights = [Security.AccessControl.FileSystemRights]::FullControl
$inheritance = [Security.AccessControl.InheritanceFlags]::None
$propagation = [Security.AccessControl.PropagationFlags]::None
$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
    $currentIdentity.User, $rights, $inheritance, $propagation, $allow
))
$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
    [Security.Principal.SecurityIdentifier]::new("S-1-5-18"),
    $rights, $inheritance, $propagation, $allow
))
Set-Acl -LiteralPath $credentialPath -AclObject $acl

& powershell.exe `
    -NoProfile `
    -NonInteractive `
    -ExecutionPolicy Bypass `
    -File $senderScript `
    -CredentialPath $credentialPath `
    -Message (
        "TradingBot forward-v3 Telegram alert sink commissioned. " +
        "No incident. Performance BLINDED."
    )
if ($LASTEXITCODE -ne 0) {
    throw "Telegram commissioning message failed."
}

$argumentList = @(
    (Quote-TaskArgument $alertScript),
    "--trial-id", (Quote-TaskArgument $TrialId),
    "--watchdog-status", (Quote-TaskArgument $watchdogStatus),
    "--alert-journal", (Quote-TaskArgument $alertJournal),
    "--alert-state", (Quote-TaskArgument $alertState),
    "--maximum-watchdog-age-seconds", "1800",
    "--maximum-backup-age-hours", "26",
    "--delivery-sink", "windows-event-log",
    "--delivery-sink", "telegram",
    "--telegram-credential", (Quote-TaskArgument $credentialPath),
    "--telegram-sender", (Quote-TaskArgument $senderScript)
)
$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument ($argumentList -join " ") `
    -WorkingDirectory $DashboardRoot

Stop-ScheduledTask -TaskName $AlertTaskName -ErrorAction SilentlyContinue
Set-ScheduledTask `
    -TaskName $AlertTaskName `
    -Action $action | Out-Null
Start-ScheduledTask -TaskName $AlertTaskName

Write-Host "Telegram alert sink configured."
Write-Host "Bot:        @$botUsername"
Write-Host "Credential: $credentialPath (DPAPI encrypted; current user only)"
Write-Host "Task:       $AlertTaskName"
Write-Host "Delivery:   Windows Event Log + Telegram"
Write-Host "Mode:       PAPER / READ-ONLY / PERFORMANCE BLINDED"
Write-Host "PASS: no broker order was submitted"
