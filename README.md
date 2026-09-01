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

## Tests

```powershell
python -m pytest -q
```
