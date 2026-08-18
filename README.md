# TradingBot operational dashboard

A separate, localhost-only dashboard for the frozen MT5 forward observer. It
opens the forward and execution SQLite journals with `mode=ro` and exposes a
strict health allowlist. It has no broker adapter and no performance query.

## Windows VPS

Copy this directory to `C:\mtbot-dashboard`. Keep the observer repository at
`C:\mtbot2` unchanged. From the dashboard directory, run the prepared launcher:

```powershell
.\start_dashboard.ps1
```

Its equivalent explicit command is:

```powershell
python .\dashboard_server.py `
  --forward-db "C:\mtbot2\data\forward\xauusd-regime-session-open-h1-forward-v2.sqlite3" `
  --execution-db "C:\mtbot2\data\execution\execution.sqlite3" `
  --account-key "Exness-MT5Trial5:277817628" `
  --task-name "TradingBot Forward Observer v2" `
  --news-file "C:\Users\Administrator\AppData\Roaming\MetaQuotes\Terminal\Common\Files\TradingBot-Mt5\news_calendar.csv"
```

Open `http://127.0.0.1:8765` inside the VPS. The server refuses non-loopback
bindings. Closing the browser does not stop the observer; closing this Python
process only stops the dashboard.

## Tests

```powershell
python -m pytest -q
```
