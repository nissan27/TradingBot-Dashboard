"""Local-only, P&L-blinded operational dashboard for the MT5 observer."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


CLOCK_AMENDMENT_ID = "xauusd-regime-session-open-h1-forward-v2-clock-calendar-v1"
CLOCK_ELIGIBILITY_RULE_ID = "h1_exact_or_immediately_prior_close_v1"
DEFAULT_ACCOUNT_KEY = "Exness-MT5Trial5:277817628"
DEFAULT_TASK_NAME = "TradingBot Forward Observer v2"
FORBIDDEN_RESPONSE_KEYS = {
    "balance", "equity", "profit", "loss", "pnl", "return", "returns",
    "expectancy", "sharpe", "drawdown", "payoff", "win_rate", "profit_factor",
    "gross_pnl", "net_pnl", "gross_return", "net_return",
}


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def utc_age_seconds(value: str | None) -> float | None:
    if not value:
        return None
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds())


def readonly_connection(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(
        path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


@dataclass(frozen=True)
class DashboardConfig:
    forward_db: Path
    execution_db: Path
    static_dir: Path
    account_key: str = DEFAULT_ACCOUNT_KEY
    task_name: str = DEFAULT_TASK_NAME
    news_file: Path | None = None
    news_freshness_seconds: int = 600
    refresh_seconds: int = 15


class HealthReader:
    """Read operational allowlisted data without opening trade payloads."""

    def __init__(self, config: DashboardConfig):
        self.config = config

    def _forward_health(self) -> tuple[str, dict[str, Any]]:
        with readonly_connection(self.config.forward_db) as connection:
            integrity = str(
                connection.execute("PRAGMA integrity_check(1)").fetchone()[0]
            )
            binding_row = connection.execute(
                "SELECT account_key, observation_start_utc "
                "FROM observer_binding WHERE singleton=1"
            ).fetchone()
            if binding_row is None:
                raise RuntimeError("forward journal is not bound to a manifest")

            expectations = connection.execute(
                "SELECT clock_id, clock_close_utc FROM clock_expectations "
                "ORDER BY clock_close_utc"
            ).fetchall()
            exclusions = [
                json.loads(row[0])
                for row in connection.execute(
                    "SELECT payload_json FROM observer_events "
                    "WHERE event_type='clock_expectation_excluded' "
                    "ORDER BY occurred_utc, event_id"
                )
            ]
            raw_ids = {row["clock_id"] for row in expectations}
            excluded_ids = {
                row["clock_id"]
                for row in exclusions
                if row.get("clock_id") in raw_ids
                and row.get("amendment_id") == CLOCK_AMENDMENT_ID
                and row.get("eligibility_rule_id") == CLOCK_ELIGIBILITY_RULE_ID
                and row.get("reason") == "broker_not_open_at_declared_clock"
                and row.get("performance_evaluated") is False
            }
            eligible = [
                row for row in expectations if row["clock_id"] not in excluded_ids
            ]
            captures: list[tuple[str, dict[str, Any]]] = []
            for row in connection.execute(
                "SELECT clock_id, payload_json FROM clock_captures "
                "ORDER BY captured_utc"
            ):
                if row["clock_id"] not in excluded_ids:
                    captures.append(
                        (row["clock_id"], json.loads(row["payload_json"]))
                    )
            captured_ids = {clock_id for clock_id, _ in captures}
            missing = [
                row for row in eligible if row["clock_id"] not in captured_ids
            ]
            fresh = [
                payload for _, payload in captures if bool(payload.get("fresh"))
            ]
            stale = [
                payload for _, payload in captures if not bool(payload.get("fresh"))
            ]
            last_capture = max(
                (payload for _, payload in captures),
                key=lambda row: row.get("clock_close_utc", ""),
                default=None,
            )
            event_counts = {
                row["event_type"]: int(row["n"])
                for row in connection.execute(
                    "SELECT event_type, COUNT(*) AS n FROM observer_events "
                    "GROUP BY event_type"
                )
            }

            def scalar(sql: str) -> int:
                return int(connection.execute(sql).fetchone()[0])

            expected_count = len(eligible)
            status = {
                "integrity": integrity,
                "observation_start_utc": binding_row["observation_start_utc"],
                "bars_captured": scalar("SELECT COUNT(*) FROM captured_bars"),
                "expected_clocks": expected_count,
                "fresh_clocks": len(fresh),
                "stale_clocks": len(stale),
                "missing_clocks": len(missing),
                "excluded_clocks": len(excluded_ids),
                "clock_coverage": (
                    len(fresh) / expected_count if expected_count else None
                ),
                "last_clock_close_utc": (
                    last_capture.get("clock_close_utc") if last_capture else None
                ),
                "last_clock_captured_utc": (
                    last_capture.get("captured_utc") if last_capture else None
                ),
                "last_clock_delay_seconds": (
                    last_capture.get("delay_seconds") if last_capture else None
                ),
                "candidate_signals": event_counts.get("signal_post_atr", 0),
                "risk_vetoes": event_counts.get("risk_veto", 0),
                "intended_orders": event_counts.get("intended_order", 0),
                "closed_candidate_trades": scalar(
                    "SELECT COUNT(*) FROM paper_trades"
                ),
            }
            return str(binding_row["account_key"]), status

    def _safety_health(self, account_key: str) -> dict[str, Any]:
        try:
            with readonly_connection(self.config.execution_db) as connection:
                row = connection.execute(
                    "SELECT kill_switch_engaged, kill_switch_reason, "
                    "last_reconciled_at_msc, last_reconciliation_ok, "
                    "last_reconciliation_error FROM safety_state "
                    "WHERE account_key=?",
                    (account_key,),
                ).fetchone()
                if row is None:
                    return {
                        "available": True,
                        "kill_switch_engaged": False,
                        "reconciliation_ok": None,
                    }
                reconciled = row["last_reconciled_at_msc"]
                age = None
                if reconciled is not None:
                    age = max(
                        0.0,
                        datetime.now(timezone.utc).timestamp()
                        - int(reconciled) / 1000,
                    )
                return {
                    "available": True,
                    "kill_switch_engaged": bool(row["kill_switch_engaged"]),
                    "kill_switch_reason": str(
                        row["kill_switch_reason"] or ""
                    ),
                    "reconciliation_ok": (
                        None
                        if row["last_reconciliation_ok"] is None
                        else bool(row["last_reconciliation_ok"])
                    ),
                    "reconciliation_age_seconds": age,
                    "reconciliation_error": str(
                        row["last_reconciliation_error"] or ""
                    ),
                }
        except (FileNotFoundError, sqlite3.Error) as exc:
            return {"available": False, "error": type(exc).__name__}

    def _scheduler_health(self) -> dict[str, Any]:
        if platform.system() != "Windows":
            return {"available": False, "reason": "Windows only"}
        script = (
            "$i=Get-ScheduledTaskInfo -TaskName $env:MTBOT_DASH_TASK;"
            "$t=Get-ScheduledTask -TaskName $env:MTBOT_DASH_TASK;"
            "[PSCustomObject]@{state=[string]$t.State;"
            "last_result=$i.LastTaskResult;"
            "last_run=$i.LastRunTime.ToUniversalTime().ToString('o');"
            "next_run=$i.NextRunTime.ToUniversalTime().ToString('o');"
            "missed_runs=$i.NumberOfMissedRuns}|ConvertTo-Json -Compress"
        )
        environment = os.environ.copy()
        environment["MTBOT_DASH_TASK"] = self.config.task_name
        try:
            result = subprocess.run(
                [
                    "powershell.exe", "-NoProfile", "-NonInteractive",
                    "-Command", script,
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=8,
                env=environment,
            )
            value = json.loads(result.stdout)
            return {
                "available": True,
                "state": value.get("state"),
                "last_result": int(value.get("last_result", -1)),
                "last_run_utc": value.get("last_run"),
                "last_run_age_seconds": utc_age_seconds(
                    value.get("last_run")
                ),
                "next_run_utc": value.get("next_run"),
                "missed_runs": int(value.get("missed_runs", 0)),
            }
        except (
            OSError, subprocess.SubprocessError, json.JSONDecodeError, ValueError
        ):
            return {"available": False, "reason": "task query failed"}

    def _news_health(self) -> dict[str, Any]:
        path = self.config.news_file
        if path is None:
            return {
                "configured": False,
                "status": "not configured",
                "age_seconds": None,
            }
        try:
            age = max(
                0.0,
                datetime.now(timezone.utc).timestamp() - path.stat().st_mtime,
            )
            return {
                "configured": True,
                "status": (
                    "fresh"
                    if age <= self.config.news_freshness_seconds
                    else "stale"
                ),
                "age_seconds": age,
            }
        except OSError:
            return {
                "configured": True,
                "status": "missing",
                "age_seconds": None,
            }

    def read(self) -> dict[str, Any]:
        account_key, journal = self._forward_health()
        if account_key != self.config.account_key:
            raise RuntimeError(
                "forward journal account key does not match dashboard binding"
            )
        safety = self._safety_health(account_key)
        scheduler = self._scheduler_health()
        news = self._news_health()
        attention = bool(
            journal["integrity"] != "ok"
            or journal["missing_clocks"]
            or journal["stale_clocks"]
            or safety.get("kill_switch_engaged")
            or safety.get("reconciliation_ok") is False
            or (
                scheduler.get("available")
                and (
                    scheduler.get("last_result") != 0
                    or scheduler.get("last_run_age_seconds") is None
                    or scheduler.get("last_run_age_seconds") > 900
                )
            )
            or (
                news.get("configured")
                and news.get("status") != "fresh"
            )
        )
        payload = {
            "schema_version": 1,
            "generated_utc": utc_now_text(),
            "overall_status": "attention" if attention else "healthy",
            "runtime_mode": "paper_read_only",
            "account_key": account_key,
            "journal": journal,
            "safety": safety,
            "scheduler": scheduler,
            "news": news,
            "refresh_seconds": self.config.refresh_seconds,
            "performance_blinded": True,
            "broker_order_adapter_present": False,
        }
        assert_blinded_payload(payload)
        return payload


def assert_blinded_payload(value: Any, path: str = "root") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower()
            if normalized in FORBIDDEN_RESPONSE_KEYS:
                raise RuntimeError(
                    f"forbidden performance field at {path}.{key}"
                )
            assert_blinded_payload(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            assert_blinded_payload(item, f"{path}[{index}]")


class DashboardHandler(SimpleHTTPRequestHandler):
    reader: HealthReader
    static_dir: Path

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, directory=str(self.static_dir), **kwargs)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        path = urlparse(self.path).path
        if path == "/api/health":
            try:
                body = json.dumps(
                    self.reader.read(), separators=(",", ":")
                ).encode("utf-8")
                status = HTTPStatus.OK
            except Exception as exc:  # fail closed; omit filesystem details
                body = json.dumps(
                    {
                        "schema_version": 1,
                        "overall_status": "unavailable",
                        "error": type(exc).__name__,
                        "performance_blinded": True,
                    },
                    separators=(",", ":"),
                ).encode("utf-8")
                status = HTTPStatus.SERVICE_UNAVAILABLE
            self.send_response(status)
            self.send_header(
                "Content-Type", "application/json; charset=utf-8"
            )
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/":
            self.path = "/dashboard.html"
        super().do_GET()

    def end_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "no-store")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "connect-src 'self'; img-src 'self'; frame-ancestors 'none'",
        )
        super().end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        print(f"dashboard: {format % args}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--forward-db", type=Path, required=True)
    parser.add_argument("--execution-db", type=Path, required=True)
    parser.add_argument("--account-key", default=DEFAULT_ACCOUNT_KEY)
    parser.add_argument("--task-name", default=DEFAULT_TASK_NAME)
    parser.add_argument("--news-file", type=Path)
    parser.add_argument("--news-freshness-seconds", type=int, default=600)
    parser.add_argument("--refresh-seconds", type=int, default=15)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        raise SystemExit("dashboard refuses non-loopback hosts")
    static_dir = Path(__file__).resolve().parent / "public"
    config = DashboardConfig(
        forward_db=args.forward_db,
        execution_db=args.execution_db,
        static_dir=static_dir,
        account_key=args.account_key,
        task_name=args.task_name,
        news_file=args.news_file,
        news_freshness_seconds=max(1, args.news_freshness_seconds),
        refresh_seconds=max(5, args.refresh_seconds),
    )
    DashboardHandler.reader = HealthReader(config)
    DashboardHandler.static_dir = static_dir
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"TradingBot operational dashboard: http://{args.host}:{args.port}")
    print("Mode: localhost only / SQLite read only / performance blinded")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
