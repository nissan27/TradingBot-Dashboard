$ErrorActionPreference = "Stop"

$python = "C:\mtbot2\.venv\Scripts\python.exe"
$forwardDb = "C:\mtbot2\data\forward\xauusd-regime-session-open-h1-forward-v3.sqlite3"
$executionDb = "C:\mtbot2\data\execution\execution.sqlite3"
$newsFile = "C:\Users\Administrator\AppData\Roaming\MetaQuotes\Terminal\Common\Files\TradingBot-Mt5\news_calendar.csv"
$alertState = "C:\mtbot-backups\forward-v3\alerts\alert-state.json"

& $python "$PSScriptRoot\dashboard_server.py" `
  --forward-db $forwardDb `
  --execution-db $executionDb `
  --trial-id "xauusd-regime-session-open-h1-forward-v3" `
  --account-key "Exness-MT5Trial5:277817628" `
  --task-name "TradingBot Forward Observer v3" `
  --news-file $newsFile `
  --alert-state-file $alertState
