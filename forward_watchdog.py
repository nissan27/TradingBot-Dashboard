"""P&L-blind health watchdog and online backups for the forward observer.

This module deliberately lives outside the frozen TradingBot-Mt5 source tree.
It opens the active journals read-only, uses SQLite's online-backup API, and
never imports an order adapter or reads paper-trade payloads.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import re
import sqlite3
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from dashboard_server import (
    DashboardConfig,
    HealthReader,
    assert_blinded_payload,
)


UTC = timezone.utc
BACKUP_RECEIPT_SCHEMA = 1
WATCHDOG_REPORT_SCHEMA = 1
TRIAL_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
TASK_RUNNING_RESULT = 267009


class WatchdogError(RuntimeError):
    """An operational check or verified backup could not be completed."""


def utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise WatchdogError("operational timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise WatchdogError("invalid UTC timestamp in operational receipt") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WatchdogError("operational receipt timestamp lacks timezone")
    return parsed.astimezone(UTC)


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def readonly_connection(path: Path) -> Iterator[sqlite3.Connection]:
    if not path.is_file():
        raise WatchdogError(f"SQLite journal is missing: {path}")
    connection = sqlite3.connect(
        path.resolve().as_uri() + "?mode=ro", uri=True, timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    try:
        yield connection
    finally:
        connection.close()


def _database_binding(path: Path) -> dict[str, Any]:
    """Read only deployment identity fields, never performance payloads."""
    with readonly_connection(path) as connection:
        row = connection.execute(
            """SELECT schema_version,hypothesis_id,
                      manifest_identity_sha256,manifest_file_sha256,
                      account_key,observation_start_utc,payload_sha256
               FROM observer_binding WHERE singleton=1"""
        ).fetchone()
        if row is None:
            raise WatchdogError("forward journal has no observer binding")
        return {key: row[key] for key in row.keys()}


def sqlite_integrity(path: Path) -> str:
    with readonly_connection(path) as connection:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])


def verify_manifest_binding(
    *, journal_path: Path, manifest_path: Path, trial_id: str,
    account_key: str,
) -> dict[str, Any]:
    if not manifest_path.is_file():
        raise WatchdogError(f"frozen manifest is missing: {manifest_path}")
    binding = _database_binding(journal_path)
    manifest_bytes_hash = sha256_file(manifest_path)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WatchdogError("frozen manifest cannot be decoded") from exc
    expected = {
        "hypothesis_id": trial_id,
        "account_key": account_key,
        "manifest_identity_sha256": manifest.get(
            "manifest_identity_sha256"
        ),
        "manifest_file_sha256": manifest_bytes_hash,
    }
    mismatches = [
        key for key, value in expected.items() if binding.get(key) != value
    ]
    if manifest.get("hypothesis", {}).get("hypothesis_id") != trial_id:
        mismatches.append("manifest hypothesis_id")
    if manifest.get("account", {}).get("account_key") != account_key:
        mismatches.append("manifest account_key")
    if manifest.get("observer_contract", {}).get(
        "performance_blinded_until_endpoint"
    ) is not True:
        mismatches.append("performance blinding")
    if mismatches:
        raise WatchdogError(
            "forward deployment identity mismatch: " + ", ".join(mismatches)
        )
    return {
        "binding": binding,
        "manifest_file_sha256": manifest_bytes_hash,
        "minimum_clock_coverage": float(
            manifest.get("observer_contract", {}).get(
                "minimum_clock_coverage", 0.95
            )
        ),
    }


@dataclass(frozen=True)
class BackupResult:
    path: Path
    receipt_path: Path
    database_sha256: str
    receipt_sha256: str
    created_utc: str
    created_this_run: bool
    integrity: str = "ok"

    def public(self) -> dict[str, Any]:
        return {
            "path": str(self.path.resolve()),
            "receipt_path": str(self.receipt_path.resolve()),
            "database_sha256": self.database_sha256,
            "receipt_sha256": self.receipt_sha256,
            "created_utc": self.created_utc,
            "created_this_run": self.created_this_run,
            "integrity": self.integrity,
        }


def _write_json_create_once(path: Path, payload: Mapping[str, Any]) -> None:
    raw = json.dumps(
        dict(payload), indent=2, sort_keys=True, ensure_ascii=False,
        allow_nan=False,
    ) + "\n"
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise WatchdogError(f"refusing to overwrite operational receipt: {path}") from exc


def _receipt_backup_path(receipt_path: Path) -> Path:
    suffix = ".receipt.json"
    if not receipt_path.name.endswith(suffix):
        raise WatchdogError("invalid backup receipt filename")
    return receipt_path.with_name(receipt_path.name[: -len(suffix)])


def validate_backup_receipt(
    receipt_path: Path, *, trial_id: str, verify_integrity: bool = True,
) -> BackupResult:
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WatchdogError(f"cannot read backup receipt: {receipt_path}") from exc
    if payload.get("schema_version") != BACKUP_RECEIPT_SCHEMA:
        raise WatchdogError("unsupported backup receipt schema")
    unsigned = dict(payload)
    identity = unsigned.pop("receipt_identity_sha256", None)
    if identity != canonical_sha256(unsigned):
        raise WatchdogError("backup receipt identity hash is invalid")
    if payload.get("trial_id") != trial_id:
        raise WatchdogError("backup receipt belongs to another trial")
    backup_path = _receipt_backup_path(receipt_path)
    backup = payload.get("backup") or {}
    if backup.get("filename") != backup_path.name or not backup_path.is_file():
        raise WatchdogError("verified backup file is missing")
    digest = sha256_file(backup_path)
    if digest != backup.get("sha256"):
        raise WatchdogError("verified backup SHA-256 does not match receipt")
    integrity = sqlite_integrity(backup_path) if verify_integrity else "not_checked"
    if verify_integrity and integrity != "ok":
        raise WatchdogError(f"verified backup integrity is {integrity!r}")
    return BackupResult(
        path=backup_path,
        receipt_path=receipt_path,
        database_sha256=digest,
        receipt_sha256=sha256_file(receipt_path),
        created_utc=str(payload["created_utc"]),
        created_this_run=False,
        integrity=integrity,
    )


def create_verified_backup(
    *, source_path: Path, backup_dir: Path, trial_id: str,
    now_utc: datetime,
) -> BackupResult:
    """Create a consistent online SQLite snapshot and immutable receipt."""
    if not TRIAL_ID_PATTERN.fullmatch(trial_id):
        raise WatchdogError("trial_id contains unsafe path characters")
    now_utc = now_utc.astimezone(UTC)
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = now_utc.strftime("%Y%m%dT%H%M%SZ")
    backup_path = backup_dir / f"{trial_id}.{stamp}.sqlite3"
    receipt_path = backup_path.with_name(backup_path.name + ".receipt.json")
    partial_path = backup_path.with_name(backup_path.name + ".partial")
    if backup_path.exists() or receipt_path.exists() or partial_path.exists():
        raise WatchdogError("backup timestamp already exists; refusing overwrite")

    source_binding = _database_binding(source_path)
    try:
        with readonly_connection(source_path) as source:
            destination = sqlite3.connect(str(partial_path), timeout=30.0)
            try:
                destination.execute("PRAGMA journal_mode=DELETE")
                destination.execute("PRAGMA synchronous=FULL")
                source.backup(destination, pages=256, sleep=0.01)
                destination.commit()
            finally:
                destination.close()
        integrity = sqlite_integrity(partial_path)
        if integrity != "ok":
            raise WatchdogError(f"new backup integrity is {integrity!r}")
        if _database_binding(partial_path) != source_binding:
            raise WatchdogError("new backup observer binding differs from source")
        os.replace(partial_path, backup_path)
        database_hash = sha256_file(backup_path)
        core = {
            "schema_version": BACKUP_RECEIPT_SCHEMA,
            "kind": "forward_sqlite_online_backup",
            "trial_id": trial_id,
            "created_utc": utc_text(now_utc),
            "source": {
                "filename": source_path.name,
                "schema_version": int(source_binding["schema_version"]),
                "manifest_identity_sha256": source_binding[
                    "manifest_identity_sha256"
                ],
                "account_key": source_binding["account_key"],
                "observation_start_utc": source_binding[
                    "observation_start_utc"
                ],
            },
            "backup": {
                "filename": backup_path.name,
                "size_bytes": backup_path.stat().st_size,
                "sha256": database_hash,
                "sqlite_integrity": integrity,
            },
            "performance_blinded": True,
            "broker_order_adapter_present": False,
        }
        receipt = {**core, "receipt_identity_sha256": canonical_sha256(core)}
        assert_blinded_payload(receipt)
        _write_json_create_once(receipt_path, receipt)
        return BackupResult(
            path=backup_path,
            receipt_path=receipt_path,
            database_sha256=database_hash,
            receipt_sha256=sha256_file(receipt_path),
            created_utc=utc_text(now_utc),
            created_this_run=True,
            integrity=integrity,
        )
    except Exception:
        partial_path.unlink(missing_ok=True)
        if backup_path.exists() and not receipt_path.exists():
            backup_path.unlink()
        raise


def _verified_receipts(backup_dir: Path, trial_id: str) -> list[BackupResult]:
    pattern = f"{trial_id}.*.sqlite3.receipt.json"
    results = [
        validate_backup_receipt(path, trial_id=trial_id)
        for path in sorted(backup_dir.glob(pattern))
    ]
    return sorted(results, key=lambda item: parse_utc(item.created_utc))


def apply_retention(
    *, backup_dir: Path, trial_id: str, retention_count: int,
) -> list[str]:
    if retention_count < 2:
        raise WatchdogError("retention_count must keep at least two backups")
    verified = _verified_receipts(backup_dir, trial_id)
    removed: list[str] = []
    for item in verified[:-retention_count]:
        # Delete only files derived from validated receipts in this exact
        # trial directory. Unrelated files and unverified evidence are kept.
        item.receipt_path.unlink()
        item.path.unlink(missing_ok=True)
        removed.append(item.path.name)
    return removed


def ensure_recent_backup(
    *, source_path: Path, backup_dir: Path, trial_id: str,
    now_utc: datetime, interval_hours: float, retention_count: int,
) -> BackupResult:
    if interval_hours <= 0:
        raise WatchdogError("backup interval must be positive")
    backup_dir.mkdir(parents=True, exist_ok=True)
    verified = _verified_receipts(backup_dir, trial_id)
    latest = verified[-1] if verified else None
    due = latest is None or (
        now_utc.astimezone(UTC) - parse_utc(latest.created_utc)
        >= timedelta(hours=interval_hours)
    )
    result = (
        create_verified_backup(
            source_path=source_path, backup_dir=backup_dir,
            trial_id=trial_id, now_utc=now_utc,
        )
        if due else latest
    )
    if result is None:  # for static type checkers; first run always creates
        raise WatchdogError("no verified backup is available")
    apply_retention(
        backup_dir=backup_dir, trial_id=trial_id,
        retention_count=retention_count,
    )
    return result


def windows_snapshot(task_name: str) -> dict[str, Any]:
    if platform.system() != "Windows":
        raise WatchdogError("Windows task and memory probe is unavailable")
    script = (
        "$i=Get-ScheduledTaskInfo -TaskName $env:MTBOT_WATCH_TASK;"
        "$t=Get-ScheduledTask -TaskName $env:MTBOT_WATCH_TASK;"
        "$o=Get-CimInstance Win32_OperatingSystem;"
        "$c=(Get-Counter '\\Memory\\% Committed Bytes In Use')."
        "CounterSamples[0].CookedValue;"
        "$p=(Get-Counter '\\Paging File(_Total)\\% Usage')."
        "CounterSamples[0].CookedValue;"
        "[PSCustomObject]@{state=[string]$t.State;"
        "last_result=[int64]$i.LastTaskResult;"
        "last_run=$i.LastRunTime.ToUniversalTime().ToString('o');"
        "next_run=$i.NextRunTime.ToUniversalTime().ToString('o');"
        "missed_runs=[int64]$i.NumberOfMissedRuns;"
        "available_memory_mib=[double]$o.FreePhysicalMemory/1024;"
        "committed_percent=[double]$c;pagefile_percent=[double]$p}"
        "|ConvertTo-Json -Compress"
    )
    environment = os.environ.copy()
    environment["MTBOT_WATCH_TASK"] = task_name
    try:
        result = subprocess.run(
            [
                "powershell.exe", "-NoProfile", "-NonInteractive",
                "-Command", script,
            ],
            check=True, capture_output=True, text=True, timeout=15,
            env=environment,
        )
        value = json.loads(result.stdout)
    except (
        OSError, subprocess.SubprocessError, json.JSONDecodeError, ValueError
    ) as exc:
        raise WatchdogError("Windows operational probe failed") from exc
    return {
        "task_name": task_name,
        "state": value.get("state"),
        "last_result": int(value.get("last_result", -1)),
        "last_run_utc": value.get("last_run"),
        "next_run_utc": value.get("next_run"),
        "missed_runs": int(value.get("missed_runs", 0)),
        "available_memory_mib": float(value.get("available_memory_mib", 0)),
        "committed_percent": float(value.get("committed_percent", 100)),
        "pagefile_percent": float(value.get("pagefile_percent", 100)),
    }


def mt5_snapshot(expected_account_key: str) -> dict[str, Any]:
    """Read terminal identity directly; no trading adapter is imported."""
    try:
        import MetaTrader5 as mt5  # type: ignore
    except ImportError as exc:
        raise WatchdogError("MetaTrader5 package is unavailable") from exc
    initialized = False
    try:
        if not mt5.initialize():
            raise WatchdogError("MT5 initialization failed")
        initialized = True
        account = mt5.account_info()
        terminal = mt5.terminal_info()
        if account is None or terminal is None:
            raise WatchdogError("MT5 terminal/account identity is unavailable")
        account_key = (
            f"{getattr(account, 'server', 'mt5')}:"
            f"{int(getattr(account, 'login', 0))}"
        )
        return {
            "account_key": account_key,
            "account_matches": account_key == expected_account_key,
            "connected": bool(getattr(terminal, "connected", False)),
            "trade_mode": int(getattr(account, "trade_mode", -1)),
            "real_money": int(getattr(account, "trade_mode", -1)) == 2,
            "read_only_probe": True,
        }
    finally:
        if initialized:
            mt5.shutdown()


def news_snapshot(
    path: Path, *, now_utc: datetime, max_age_seconds: int,
) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.reader(handle))
    except (OSError, UnicodeError, csv.Error) as exc:
        raise WatchdogError("MT5 calendar export cannot be read") from exc
    if not rows or len(rows[0]) != 11 or rows[0][0] != "META":
        raise WatchdogError("MT5 calendar metadata is malformed")
    meta = rows[0]
    try:
        exported_epoch = int(meta[2])
        server_epoch = int(meta[3])
        server_offset = int(meta[4])
        window_start = int(meta[5])
        window_end = int(meta[6])
        event_count = int(meta[9])
    except ValueError as exc:
        raise WatchdogError("MT5 calendar metadata contains non-integers") from exc
    now_epoch = int(now_utc.astimezone(UTC).timestamp())
    age = max(0, now_epoch - exported_epoch)
    coverage = {
        token.split(":", 1)[0].strip().upper(): int(token.split(":", 1)[1])
        for token in meta[7].split("|") if ":" in token
    }
    valid = bool(
        int(meta[1]) == 1
        and abs((server_epoch - server_offset) - exported_epoch) <= 5
        and age <= max_age_seconds
        and window_start <= now_epoch <= window_end
        and len(rows) - 1 == event_count
        and coverage.get("USD", 0) > 0
    )
    return {
        "path": str(path.resolve()),
        "age_seconds": age,
        "max_age_seconds": max_age_seconds,
        "usd_rows": coverage.get("USD", 0),
        "event_rows": event_count,
        "valid": valid,
    }


def git_snapshot(repo: Path) -> dict[str, Any]:
    try:
        head = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=repo,
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--short"], cwd=repo,
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise WatchdogError("bot repository status probe failed") from exc
    return {"head": head, "clean": not bool(status), "changed_paths": len(status.splitlines())}


def _check(name: str, passed: bool, detail: str) -> dict[str, str]:
    return {"name": name, "status": "PASS" if passed else "FAIL", "detail": detail}


def evaluate_health(
    *, dashboard: Mapping[str, Any], deployment: Mapping[str, Any],
    windows: Mapping[str, Any], mt5: Mapping[str, Any],
    news: Mapping[str, Any], repository: Mapping[str, Any],
    backup: BackupResult, now_utc: datetime,
    minimum_memory_mib: float, maximum_commit_percent: float,
    maximum_pagefile_percent: float, maximum_task_age_seconds: float,
) -> dict[str, Any]:
    journal = dashboard.get("journal") or {}
    safety = dashboard.get("safety") or {}
    coverage = journal.get("clock_coverage")
    minimum_coverage = deployment["minimum_clock_coverage"]
    task_age = None
    try:
        task_age = max(
            0.0,
            (now_utc.astimezone(UTC) - parse_utc(windows["last_run_utc"]))
            .total_seconds(),
        )
    except (KeyError, TypeError, WatchdogError):
        pass
    task_result_ok = windows.get("last_result") == 0 or (
        windows.get("state") == "Running"
        and windows.get("last_result") == TASK_RUNNING_RESULT
    )
    checks = [
        _check(
            "deployment_identity", True,
            "manifest, account and append-only journal binding match",
        ),
        _check(
            "journal_integrity", journal.get("integrity") == "ok",
            f"SQLite integrity={journal.get('integrity')}",
        ),
        _check(
            "clock_evidence",
            not journal.get("missing_clocks", 0)
            and not journal.get("stale_clocks", 0)
            and (coverage is None or float(coverage) >= minimum_coverage),
            (
                f"coverage={'n/a' if coverage is None else f'{float(coverage):.2%}'}; "
                f"missing={journal.get('missing_clocks', 0)}; "
                f"stale={journal.get('stale_clocks', 0)}"
            ),
        ),
        _check(
            "observer_task",
            windows.get("state") in {"Ready", "Running"}
            and task_result_ok
            and windows.get("missed_runs") == 0
            and task_age is not None
            and task_age <= maximum_task_age_seconds,
            (
                f"state={windows.get('state')}; result={windows.get('last_result')}; "
                f"age={task_age if task_age is not None else 'n/a'}s; "
                f"missed={windows.get('missed_runs')}"
            ),
        ),
        _check(
            "mt5_identity",
            mt5.get("connected") is True
            and mt5.get("account_matches") is True
            and mt5.get("real_money") is False,
            (
                f"connected={mt5.get('connected')}; "
                f"account_match={mt5.get('account_matches')}; "
                f"real_money={mt5.get('real_money')}"
            ),
        ),
        _check(
            "news_calendar", news.get("valid") is True,
            f"valid={news.get('valid')}; age={news.get('age_seconds')}s",
        ),
        _check(
            "execution_safety",
            safety.get("available") is True
            and not safety.get("kill_switch_engaged")
            and safety.get("reconciliation_ok") is not False,
            (
                f"available={safety.get('available')}; "
                f"kill_switch={safety.get('kill_switch_engaged')}; "
                f"reconciliation={safety.get('reconciliation_ok')}"
            ),
        ),
        _check(
            "vps_memory",
            float(windows.get("available_memory_mib", 0)) >= minimum_memory_mib
            and float(windows.get("committed_percent", 100))
            <= maximum_commit_percent
            and float(windows.get("pagefile_percent", 100))
            <= maximum_pagefile_percent,
            (
                f"available={float(windows.get('available_memory_mib', 0)):.0f}MiB; "
                f"committed={float(windows.get('committed_percent', 100)):.1f}%; "
                f"pagefile={float(windows.get('pagefile_percent', 100)):.1f}%"
            ),
        ),
        _check(
            "bot_worktree", repository.get("clean") is True,
            f"head={repository.get('head')}; changed={repository.get('changed_paths')}",
        ),
        _check(
            "verified_backup", backup.integrity == "ok",
            (
                f"integrity={backup.integrity}; "
                f"created_this_run={backup.created_this_run}; "
                f"sha256={backup.database_sha256}"
            ),
        ),
    ]
    report = {
        "schema_version": WATCHDOG_REPORT_SCHEMA,
        "kind": "forward_operational_watchdog",
        "generated_utc": utc_text(now_utc),
        "trial_id": deployment["binding"]["hypothesis_id"],
        "overall_status": (
            "HEALTHY" if all(item["status"] == "PASS" for item in checks)
            else "UNHEALTHY"
        ),
        "runtime_mode": "paper_read_only",
        "checks": checks,
        "journal": {
            key: journal.get(key) for key in (
                "integrity", "bars_captured", "expected_clocks",
                "fresh_clocks", "stale_clocks", "missing_clocks",
                "excluded_clocks", "clock_coverage",
                "last_clock_close_utc", "last_clock_delay_seconds",
                "candidate_signals", "risk_vetoes", "intended_orders",
                "closed_candidate_trades", "provider_bar_revisions",
            )
        },
        "backup": backup.public(),
        "performance_blinded": True,
        "broker_order_adapter_present": False,
    }
    assert_blinded_payload(report)
    return report


def write_latest_report(path: Path, report: Mapping[str, Any]) -> None:
    assert_blinded_payload(report)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    raw = json.dumps(
        dict(report), indent=2, sort_keys=True, ensure_ascii=False,
        allow_nan=False,
    ) + "\n"
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--forward-db", type=Path, required=True)
    parser.add_argument("--execution-db", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--account-key", required=True)
    parser.add_argument("--task-name", required=True)
    parser.add_argument("--news-file", type=Path, required=True)
    parser.add_argument("--bot-repo", type=Path, required=True)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--status-file", type=Path)
    parser.add_argument("--backup-interval-hours", type=float, default=24.0)
    parser.add_argument("--retention-count", type=int, default=14)
    parser.add_argument("--news-freshness-seconds", type=int, default=900)
    parser.add_argument("--minimum-memory-mib", type=float, default=1536.0)
    parser.add_argument("--maximum-commit-percent", type=float, default=75.0)
    parser.add_argument("--maximum-pagefile-percent", type=float, default=50.0)
    parser.add_argument("--maximum-task-age-seconds", type=float, default=900.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = datetime.now(UTC)
    status_file = args.status_file or (args.backup_dir / "watchdog-latest.json")
    try:
        deployment = verify_manifest_binding(
            journal_path=args.forward_db, manifest_path=args.manifest,
            trial_id=args.trial_id, account_key=args.account_key,
        )
        backup = ensure_recent_backup(
            source_path=args.forward_db, backup_dir=args.backup_dir,
            trial_id=args.trial_id, now_utc=now,
            interval_hours=args.backup_interval_hours,
            retention_count=args.retention_count,
        )
        reader = HealthReader(DashboardConfig(
            forward_db=args.forward_db,
            execution_db=args.execution_db,
            static_dir=Path(__file__).resolve().parent / "public",
            account_key=args.account_key,
            task_name=args.task_name,
            news_file=args.news_file,
            news_freshness_seconds=args.news_freshness_seconds,
        ))
        dashboard = reader.read()
        report = evaluate_health(
            dashboard=dashboard,
            deployment=deployment,
            windows=windows_snapshot(args.task_name),
            mt5=mt5_snapshot(args.account_key),
            news=news_snapshot(
                args.news_file, now_utc=now,
                max_age_seconds=args.news_freshness_seconds,
            ),
            repository=git_snapshot(args.bot_repo),
            backup=backup,
            now_utc=now,
            minimum_memory_mib=args.minimum_memory_mib,
            maximum_commit_percent=args.maximum_commit_percent,
            maximum_pagefile_percent=args.maximum_pagefile_percent,
            maximum_task_age_seconds=args.maximum_task_age_seconds,
        )
    except Exception as exc:
        report = {
            "schema_version": WATCHDOG_REPORT_SCHEMA,
            "kind": "forward_operational_watchdog",
            "generated_utc": utc_text(now),
            "trial_id": args.trial_id,
            "overall_status": "UNHEALTHY",
            "runtime_mode": "paper_read_only",
            "checks": [{
                "name": "watchdog_runtime", "status": "FAIL",
                "detail": type(exc).__name__,
            }],
            "performance_blinded": True,
            "broker_order_adapter_present": False,
        }
        assert_blinded_payload(report)
    write_latest_report(status_file, report)
    print(f"Forward watchdog: {report['overall_status']}")
    print(f"UTC as-of:       {report['generated_utc']}")
    for item in report["checks"]:
        print(f"[{item['status']}] {item['name']}: {item['detail']}")
    if report.get("backup"):
        print(f"Backup:         {report['backup']['path']}")
        print(f"Backup SHA-256: {report['backup']['database_sha256']}")
    print(f"Status file:    {status_file.resolve()}")
    print("Performance:    BLINDED")
    print("PASS: no broker order was submitted")
    return 0 if report["overall_status"] == "HEALTHY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
