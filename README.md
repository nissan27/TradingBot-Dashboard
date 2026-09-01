# TradingBot operational dashboard and watchdog

A separate operational layer for the frozen MT5 forward observer. The
localhost-only dashboard opens the forward and execution SQLite journals with
`mode=ro` and exposes a strict health allowlist. The watchdog adds verified
online backups and automated operational checks. Neither component contains a
broker order adapter or a performance query.

This code intentionally remains outside `C:\mtbot2`. Adding it to that frozen
source tree would change forward v3's deployment fingerprint.

## Windows VPS

Copy this directory to `C:\mtbot-dashboard`. Keep the observer repository at
`C:\mtbot2` unchanged. From the dashboard directory, run the prepared launcher:

```powershell
.\start_dashboard.ps1
```

Its equivalent explicit command is:

```powershell
python .\dashboard_server.py `
  --forward-db "C:\mtbot2\data\forward\xauusd-regime-session-open-h1-forward-v3.sqlite3" `
  --execution-db "C:\mtbot2\data\execution\execution.sqlite3" `
  --account-key "Exness-MT5Trial5:277817628" `
  --task-name "TradingBot Forward Observer v3" `
  --news-file "C:\Users\Administrator\AppData\Roaming\MetaQuotes\Terminal\Common\Files\TradingBot-Mt5\news_calendar.csv"
```

Open `http://127.0.0.1:8765` inside the VPS. The server refuses non-loopback
bindings. Closing the browser does not stop the observer; closing this Python
process only stops the dashboard.

## Forward-v3 backup and watchdog

The watchdog runs independently of the dashboard. It:

- validates the frozen manifest, account and journal binding;
- checks SQLite integrity, 06:00 UTC clock evidence and coverage;
- verifies the observer Scheduled Task, exact MT5 account and demo mode;
- validates the MT5 calendar export, execution safety state and VPS memory;
- confirms that the frozen bot worktree is clean;
- creates a transactionally consistent SQLite online backup every 24 hours;
- writes an immutable SHA-256 receipt and retains the newest 14 verified
  snapshot/receipt pairs; and
- writes `watchdog-latest.json` with health fields only.

It never reads a paper-trade payload, exposes performance, modifies the source
journal or submits an order. An unhealthy run exits with code `2`, which is
visible as the watchdog Scheduled Task's `LastTaskResult`.

After pulling this repository to `C:\mtbot-dashboard`, run one manual check:

```powershell
cd C:\mtbot-dashboard

C:\mtbot2\.venv\Scripts\python.exe .\forward_watchdog.py `
  --trial-id "xauusd-regime-session-open-h1-forward-v3" `
  --forward-db "C:\mtbot2\data\forward\xauusd-regime-session-open-h1-forward-v3.sqlite3" `
  --execution-db "C:\mtbot2\data\execution\execution.sqlite3" `
  --manifest "C:\mtbot2\reports\manifests\xauusd-regime-session-open-h1-forward-v3.json" `
  --account-key "Exness-MT5Trial5:277817628" `
  --task-name "TradingBot Forward Observer v3" `
  --news-file "C:\Users\Administrator\AppData\Roaming\MetaQuotes\Terminal\Common\Files\TradingBot-Mt5\news_calendar.csv" `
  --bot-repo "C:\mtbot2" `
  --backup-dir "C:\mtbot-backups\forward-v3"
```

Only after that prints `Forward watchdog: HEALTHY`, install the separate
15-minute Scheduled Task:

```powershell
.\install_forward_watchdog.ps1

Get-ScheduledTaskInfo -TaskName "TradingBot Forward Watchdog v3" |
  Format-List LastRunTime, LastTaskResult, NextRunTime, NumberOfMissedRuns
```

Expected healthy result: `LastTaskResult` is `0`. Do not point this task at a
v1/v2 journal, and never restore a backup over the active v3 journal while the
observer is running. Recovery must first be rehearsed against a disposable
copy.

## Blinded incident and recovery alerts

The alert pipeline is deliberately separate from both the observer and the
watchdog. Every five minutes it reads only `watchdog-latest.json` and opens an
incident when any of these conditions occur:

- the watchdog report is missing, invalid or more than 30 minutes old;
- the watchdog reports an unhealthy operational check; or
- the newest verified backup is missing, invalid or more than 26 hours old.

One contiguous unhealthy period produces one Windows Application event-log
alert. An optional Telegram sink can deliver the same blinded transition
remotely. Repeated checks update the append-only evidence but do not resend a
successful delivery. Each sink has its own delivery receipt, so a Telegram
outage is retried without duplicating the Windows event. The first clean
evaluation records and delivers one recovery event to every configured sink.
Incident transitions and delivery receipts are kept in a separate append-only
SQLite journal. `alert-state.json` supplies a health-only warning banner to the
localhost dashboard. No component opens the forward journal, imports MT5,
reads performance or contains an order adapter.

After pulling the dashboard repository, initialize the alert journal and state
with one manual healthy evaluation:

```powershell
cd C:\mtbot-dashboard

C:\mtbot2\.venv\Scripts\python.exe .\forward_alerts.py `
  --trial-id "xauusd-regime-session-open-h1-forward-v3" `
  --watchdog-status "C:\mtbot-backups\forward-v3\watchdog-latest.json" `
  --alert-journal "C:\mtbot-backups\forward-v3\alerts\alert-journal.sqlite3" `
  --alert-state "C:\mtbot-backups\forward-v3\alerts\alert-state.json" `
  --maximum-watchdog-age-seconds 1800 `
  --maximum-backup-age-hours 26 `
  --delivery-sink windows-event-log
```

The expected result is `Forward alerts: HEALTHY`. Then install the independent
five-minute Scheduled Task:

```powershell
.\install_forward_alerts.ps1

Get-ScheduledTaskInfo -TaskName "TradingBot Forward Alerts v3" |
  Format-List LastRunTime, LastTaskResult, NextRunTime, NumberOfMissedRuns
```

The installer first writes a harmless commissioning event with ID `900` to
prove Windows Event Log access. Operational incidents use ID `901`; recoveries
use ID `902`. Inspect the most recent pipeline events with:

```powershell
Get-WinEvent -FilterHashtable @{
  LogName = "Application"
  ProviderName = "TradingBot Forward Alerts"
} -MaxEvents 10 |
  Select-Object TimeCreated, Id, LevelDisplayName, Message
```

Restart the localhost dashboard after installation so `start_dashboard.ps1`
loads the new alert-state path and can display the incident banner.

### Optional Telegram notifications

Telegram is an additional remote notification sink; it does not replace the
Windows Application event log. Configure it only while the dashboard shows
`HEALTHY`, no notification is pending, and no incident has ever been opened in
this new alert journal. The setup script refuses any other state so enabling a
new sink cannot replay old incident messages.

1. Create a bot with Telegram's official
   [BotFather](https://core.telegram.org/bots/features#botfather).
2. Open that bot in Telegram and send `/start` once. The setup uses the
   official `getMe` method to verify the token and `getUpdates` to discover
   that private chat.
3. On the VPS, run the configuration script. Paste the token only into its
   hidden local prompt—never into source code, Git, a command argument, this
   chat, or a screenshot.

```powershell
cd C:\mtbot-dashboard
.\configure_telegram_alerts.ps1
```

If the bot has messages from more than one chat, the script stops rather than
guessing. Rerun it with the intended numeric chat ID:

```powershell
.\configure_telegram_alerts.ps1 -ChatId "YOUR_CHAT_ID"
```

The token is stored outside Git at
`C:\mtbot-backups\forward-v3\alerts\telegram-credential.xml`. Windows DPAPI
encrypts it for the current VPS machine and Windows user, and the file ACL is
restricted to that user plus `SYSTEM`. The setup verifies that this is the
same account used by `TradingBot Forward Alerts v3`, sends one harmless
commissioning message, then updates that existing Scheduled Task to contain
both sinks. The task command contains only the encrypted credential-file path,
never the token or chat ID.

Verify the upgraded task without displaying the credential contents:

```powershell
(Get-ScheduledTask -TaskName "TradingBot Forward Alerts v3").Actions |
  Format-List Execute, Arguments, WorkingDirectory

Get-ScheduledTaskInfo -TaskName "TradingBot Forward Alerts v3" |
  Format-List LastRunTime, LastTaskResult, NextRunTime, NumberOfMissedRuns

$state = Get-Content `
  "C:\mtbot-backups\forward-v3\alerts\alert-state.json" -Raw |
  ConvertFrom-Json
$state | Select-Object overall_status, pending_notifications, pending_by_sink
```

Expected healthy state: `LastTaskResult` is `0`, `overall_status` is
`HEALTHY`, and both values in `pending_by_sink` are zero. Telegram incident and
recovery messages contain only the frozen trial ID, a shortened incident ID,
operational condition codes, and `performance=BLINDED`. The sender uses the
official HTTPS [`sendMessage`](https://core.telegram.org/bots/api#sendmessage)
method and never opens the trading journals.

## Disposable recovery rehearsal

Run this only after the watchdog is healthy and has created at least one
verified backup/receipt pair. The rehearsal validates the latest receipt,
copies that verified snapshot into a new timestamped drill directory, verifies
its SHA-256, SQLite integrity, frozen observer binding, foreign keys and all 12
append-only triggers, then writes a blinded recovery receipt. The active v3
journal is used only for a same-file safety check and is never opened or used
as the restore target.

```powershell
cd C:\mtbot-dashboard

$receipt = Get-ChildItem `
  "C:\mtbot-backups\forward-v3\*.receipt.json" |
  Sort-Object LastWriteTimeUtc -Descending |
  Select-Object -First 1

if (-not $receipt) {
  throw "No verified forward-v3 backup receipt was found."
}

C:\mtbot2\.venv\Scripts\python.exe .\rehearse_forward_recovery.py `
  --receipt $receipt.FullName `
  --active-journal "C:\mtbot2\data\forward\xauusd-regime-session-open-h1-forward-v3.sqlite3" `
  --drill-root "C:\mtbot-recovery-drills" `
  --trial-id "xauusd-regime-session-open-h1-forward-v3" `
  --account-key "Exness-MT5Trial5:277817628"
```

The successful result starts with `Forward recovery rehearsal: PASS` and keeps
the restored database plus `RECOVERY-DRILL-RECEIPT.json` for inspection. It
does not start an observer, inspect P&L or submit a broker order.

## Tests

```powershell
python -m pytest -q
```
