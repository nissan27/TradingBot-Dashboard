"""Restore one verified forward backup into a disposable rehearsal copy.

The active journal is used only for a same-file safety check. It is never
opened, copied, replaced or passed to SQLite. The restored copy remains
performance-blinded and is never started as an observer.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from dashboard_server import assert_blinded_payload
from forward_watchdog import (
    WatchdogError,
    canonical_sha256,
    database_binding,
    readonly_connection,
    sha256_file,
    sqlite_integrity,
    utc_text,
    validate_backup_receipt,
)


UTC = timezone.utc
DRILL_RECEIPT_SCHEMA = 1
REQUIRED_TABLES = {
    "observer_binding", "captured_bars", "clock_expectations",
    "clock_captures", "observer_events", "paper_trades",
}


def _same_file(left: Path, right: Path) -> bool:
    try:
        return os.path.samefile(left, right)
    except (FileNotFoundError, OSError):
        return left.resolve() == right.resolve()


def _backup_path_for_receipt(receipt_path: Path) -> Path:
    suffix = ".receipt.json"
    if not receipt_path.name.endswith(suffix):
        raise WatchdogError("invalid backup receipt filename")
    return receipt_path.with_name(receipt_path.name[: -len(suffix)])


def _write_create_once(path: Path, payload: Mapping[str, Any]) -> None:
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
        raise WatchdogError(f"refusing to overwrite drill receipt: {path}") from exc


def _restored_structure(path: Path) -> dict[str, Any]:
    """Inspect schema and counts only; paper payloads remain unopened."""
    with readonly_connection(path) as connection:
        tables = {
            str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        missing = sorted(REQUIRED_TABLES - tables)
        if missing:
            raise WatchdogError(
                "restored journal is missing tables: " + ", ".join(missing)
            )
        foreign_key_violations = int(
            len(connection.execute("PRAGMA foreign_key_check").fetchall())
        )
        triggers = {
            str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
        missing_triggers = sorted(
            name
            for table in REQUIRED_TABLES
            for name in (f"no_update_{table}", f"no_delete_{table}")
            if name not in triggers
        )
        if missing_triggers:
            raise WatchdogError(
                "restored append-only triggers are missing: "
                + ", ".join(missing_triggers)
            )
        counts = {
            label: int(connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0])
            for label, table in (
                ("captured_bars", "captured_bars"),
                ("clock_expectations", "clock_expectations"),
                ("clock_captures", "clock_captures"),
                ("observer_events", "observer_events"),
                ("closed_paper_rows", "paper_trades"),
            )
        }
    return {
        "required_tables": len(REQUIRED_TABLES),
        "append_only_triggers": len(REQUIRED_TABLES) * 2,
        "foreign_key_violations": foreign_key_violations,
        "row_counts": counts,
    }


def run_recovery_drill(
    *, receipt_path: Path, active_journal: Path, drill_root: Path,
    trial_id: str, account_key: str, now_utc: datetime,
) -> dict[str, Any]:
    """Create and validate a disposable byte-identical restored database."""
    if not active_journal.is_file():
        raise WatchdogError("active journal path is missing")
    declared_backup = _backup_path_for_receipt(receipt_path)
    if _same_file(declared_backup, active_journal):
        raise WatchdogError("backup path resolves to the active journal")
    verified = validate_backup_receipt(
        receipt_path, trial_id=trial_id, verify_integrity=True,
    )
    if _same_file(verified.path, active_journal):
        raise WatchdogError("backup path resolves to the active journal")

    now_utc = now_utc.astimezone(UTC)
    drill_root.mkdir(parents=True, exist_ok=True)
    drill_dir = drill_root / (
        f"{trial_id}.{now_utc.strftime('%Y%m%dT%H%M%SZ')}"
    )
    try:
        drill_dir.mkdir()
    except FileExistsError as exc:
        raise WatchdogError(
            "recovery drill timestamp already exists; refusing overwrite"
        ) from exc
    restored_path = drill_dir / "restored-forward.sqlite3"
    drill_receipt_path = drill_dir / "RECOVERY-DRILL-RECEIPT.json"
    if _same_file(restored_path, active_journal):
        raise WatchdogError("restore target resolves to the active journal")

    shutil.copy2(verified.path, restored_path)
    with restored_path.open("rb+") as handle:
        os.fsync(handle.fileno())
    restored_hash = sha256_file(restored_path)
    if restored_hash != verified.database_sha256:
        raise WatchdogError("restored copy SHA-256 differs from verified backup")
    integrity = sqlite_integrity(restored_path)
    if integrity != "ok":
        raise WatchdogError(f"restored SQLite integrity is {integrity!r}")

    backup_binding = database_binding(verified.path)
    restored_binding = database_binding(restored_path)
    if restored_binding != backup_binding:
        raise WatchdogError("restored observer binding differs from backup")
    if restored_binding.get("hypothesis_id") != trial_id:
        raise WatchdogError("restored journal belongs to another trial")
    if restored_binding.get("account_key") != account_key:
        raise WatchdogError("restored journal belongs to another account")
    structure = _restored_structure(restored_path)
    if structure["foreign_key_violations"]:
        raise WatchdogError("restored journal has foreign-key violations")

    core = {
        "schema_version": DRILL_RECEIPT_SCHEMA,
        "kind": "forward_backup_recovery_rehearsal",
        "trial_id": trial_id,
        "completed_utc": utc_text(now_utc),
        "source_backup": {
            "path": str(verified.path.resolve()),
            "receipt_path": str(verified.receipt_path.resolve()),
            "database_sha256": verified.database_sha256,
            "receipt_sha256": verified.receipt_sha256,
        },
        "restored_copy": {
            "path": str(restored_path.resolve()),
            "size_bytes": restored_path.stat().st_size,
            "database_sha256": restored_hash,
            "sqlite_integrity": integrity,
            **structure,
        },
        "binding": {
            key: restored_binding[key] for key in (
                "schema_version", "hypothesis_id",
                "manifest_identity_sha256", "manifest_file_sha256",
                "account_key", "observation_start_utc",
            )
        },
        "active_journal": {
            "path": str(active_journal.resolve()),
            "opened_by_drill": False,
            "used_as_restore_target": False,
        },
        "observer_started_from_copy": False,
        "performance_blinded": True,
        "broker_order_adapter_present": False,
        "verdict": "RECOVERY_REHEARSAL_PASS",
    }
    receipt = {
        **core,
        "receipt_identity_sha256": canonical_sha256(core),
    }
    assert_blinded_payload(receipt)
    _write_create_once(drill_receipt_path, receipt)
    return {
        **receipt,
        "drill_receipt_path": str(drill_receipt_path.resolve()),
        "drill_receipt_sha256": sha256_file(drill_receipt_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--active-journal", type=Path, required=True)
    parser.add_argument("--drill-root", type=Path, required=True)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--account-key", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = run_recovery_drill(
            receipt_path=args.receipt,
            active_journal=args.active_journal,
            drill_root=args.drill_root,
            trial_id=args.trial_id,
            account_key=args.account_key,
            now_utc=datetime.now(UTC),
        )
    except (WatchdogError, OSError, sqlite3.Error, ValueError) as exc:
        raise SystemExit(f"RECOVERY REHEARSAL FAILED: {type(exc).__name__}: {exc}")

    restored = result["restored_copy"]
    print("Forward recovery rehearsal: PASS")
    print(f"Trial:             {result['trial_id']}")
    print(f"Source backup:     {result['source_backup']['path']}")
    print(f"Restored copy:     {restored['path']}")
    print(f"SQLite integrity:  {restored['sqlite_integrity']}")
    print(f"Restored SHA-256:  {restored['database_sha256']}")
    print(f"Foreign-key rows:  {restored['foreign_key_violations']}")
    print(f"Append-only rules: {restored['append_only_triggers']} present")
    print(f"Drill receipt:     {result['drill_receipt_path']}")
    print(f"Receipt SHA-256:   {result['drill_receipt_sha256']}")
    print("Active v3 journal: NOT OPENED / NOT MODIFIED")
    print("Performance:       BLINDED")
    print("PASS: observer was not started from the disposable copy")
    print("PASS: no broker order was submitted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
