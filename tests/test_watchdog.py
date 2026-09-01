from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dashboard_server import assert_blinded_payload
from forward_watchdog import (
    BackupResult,
    apply_retention,
    create_verified_backup,
    ensure_recent_backup,
    evaluate_health,
    sha256_file,
    sqlite_integrity,
    validate_backup_receipt,
    verify_manifest_binding,
)


UTC = timezone.utc
TRIAL_ID = "xauusd-regime-session-open-h1-forward-v3"
ACCOUNT_KEY = "Exness-MT5Trial5:277817628"
MANIFEST_ID = "a" * 64


def _manifest(path: Path) -> None:
    payload = {
        "schema_version": 1,
        "kind": "frozen_forward_paper_observer",
        "hypothesis": {"hypothesis_id": TRIAL_ID},
        "account": {"account_key": ACCOUNT_KEY},
        "observer_contract": {
            "minimum_clock_coverage": 0.95,
            "performance_blinded_until_endpoint": True,
        },
        "manifest_identity_sha256": MANIFEST_ID,
    }
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _forward_database(path: Path, manifest_path: Path) -> sqlite3.Connection:
    manifest_hash = sha256_file(manifest_path)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        """
        CREATE TABLE observer_binding(
            singleton INTEGER PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            hypothesis_id TEXT NOT NULL,
            manifest_identity_sha256 TEXT NOT NULL,
            manifest_file_sha256 TEXT NOT NULL,
            account_key TEXT NOT NULL,
            observation_start_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL
        );
        CREATE TABLE captured_bars(
            bar_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE clock_expectations(
            clock_id TEXT PRIMARY KEY,
            clock_close_utc TEXT NOT NULL
        );
        CREATE TABLE clock_captures(
            clock_id TEXT PRIMARY KEY,
            captured_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE observer_events(
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            occurred_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE paper_trades(
            trade_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL
        );
        """
    )
    binding = {
        "schema_version": 1,
        "hypothesis_id": TRIAL_ID,
        "manifest_identity_sha256": MANIFEST_ID,
        "manifest_file_sha256": manifest_hash,
        "account_key": ACCOUNT_KEY,
        "observation_start_utc": "2026-08-26T00:00:00Z",
    }
    connection.execute(
        "INSERT INTO observer_binding VALUES(1,?,?,?,?,?,?,?,?)",
        (
            1, TRIAL_ID, MANIFEST_ID, manifest_hash, ACCOUNT_KEY,
            "2026-08-26T00:00:00Z", json.dumps(binding), "b" * 64,
        ),
    )
    connection.execute(
        "INSERT INTO captured_bars VALUES(?,?)", ("bar-1", "{}")
    )
    capture = {
        "symbol": "XAUUSD",
        "clock_close_utc": "2026-09-01T06:00:00Z",
        "captured_utc": "2026-09-01T06:03:06Z",
        "delay_seconds": 186,
        "fresh": True,
    }
    connection.execute(
        "INSERT INTO clock_expectations VALUES(?,?)",
        ("clock-1", capture["clock_close_utc"]),
    )
    connection.execute(
        "INSERT INTO clock_captures VALUES(?,?,?)",
        ("clock-1", capture["captured_utc"], json.dumps(capture)),
    )
    connection.execute(
        "INSERT INTO observer_events VALUES(?,?,?,?)",
        (
            "revision-1", "provider_bar_revision",
            "2026-08-31T00:00:00Z", "{}",
        ),
    )
    connection.execute(
        "INSERT INTO paper_trades VALUES(?,?)",
        ("sealed-trade", '{"secret_performance":999999}'),
    )
    connection.commit()
    return connection


def _fixture(tmp_path: Path) -> tuple[Path, Path, sqlite3.Connection]:
    manifest = tmp_path / "manifest.json"
    forward = tmp_path / "forward.sqlite3"
    _manifest(manifest)
    connection = _forward_database(forward, manifest)
    return forward, manifest, connection


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_online_backup_captures_active_wal_without_mutating_source(tmp_path: Path):
    source, manifest, writer = _fixture(tmp_path)
    try:
        writer.execute(
            "INSERT INTO captured_bars VALUES(?,?)", ("bar-in-wal", "{}")
        )
        writer.commit()
        before = _file_digest(source)
        result = create_verified_backup(
            source_path=source,
            backup_dir=tmp_path / "backups",
            trial_id=TRIAL_ID,
            now_utc=datetime(2026, 9, 1, 7, 15, tzinfo=UTC),
        )
        assert _file_digest(source) == before
        assert result.created_this_run is True
        assert result.integrity == "ok"
        assert sqlite_integrity(result.path) == "ok"
        with sqlite3.connect(result.path) as backup:
            assert backup.execute(
                "SELECT COUNT(*) FROM captured_bars"
            ).fetchone()[0] == 2
            assert backup.execute(
                "SELECT payload_json FROM paper_trades"
            ).fetchone()[0] == '{"secret_performance":999999}'

        receipt = result.receipt_path.read_text(encoding="utf-8")
        assert "secret_performance" not in receipt
        assert "999999" not in receipt
        assert "sealed-trade" not in receipt
        validated = validate_backup_receipt(
            result.receipt_path, trial_id=TRIAL_ID
        )
        assert validated.database_sha256 == result.database_sha256
        assert validated.receipt_sha256 == result.receipt_sha256
        assert_blinded_payload(json.loads(receipt))

        deployment = verify_manifest_binding(
            journal_path=source, manifest_path=manifest,
            trial_id=TRIAL_ID, account_key=ACCOUNT_KEY,
        )
        assert deployment["minimum_clock_coverage"] == 0.95
    finally:
        writer.close()


def test_backup_interval_and_retention_keep_verified_pairs_only(tmp_path: Path):
    source, _manifest_path, writer = _fixture(tmp_path)
    backups = tmp_path / "backups"
    unrelated = backups / "keep-me.txt"
    backups.mkdir()
    unrelated.write_text("unrelated", encoding="utf-8")
    start = datetime(2026, 9, 1, 7, 15, tzinfo=UTC)
    try:
        first = ensure_recent_backup(
            source_path=source, backup_dir=backups, trial_id=TRIAL_ID,
            now_utc=start, interval_hours=24, retention_count=2,
        )
        repeated = ensure_recent_backup(
            source_path=source, backup_dir=backups, trial_id=TRIAL_ID,
            now_utc=start + timedelta(hours=1), interval_hours=24,
            retention_count=2,
        )
        assert repeated.path == first.path
        assert repeated.created_this_run is False

        second = ensure_recent_backup(
            source_path=source, backup_dir=backups, trial_id=TRIAL_ID,
            now_utc=start + timedelta(hours=25), interval_hours=24,
            retention_count=2,
        )
        third = ensure_recent_backup(
            source_path=source, backup_dir=backups, trial_id=TRIAL_ID,
            now_utc=start + timedelta(hours=50), interval_hours=24,
            retention_count=2,
        )
        assert second.path.is_file() and third.path.is_file()
        assert not first.path.exists()
        assert not first.receipt_path.exists()
        assert unrelated.read_text(encoding="utf-8") == "unrelated"
        assert len(list(backups.glob("*.sqlite3"))) == 2
        assert len(list(backups.glob("*.receipt.json"))) == 2
        assert apply_retention(
            backup_dir=backups, trial_id=TRIAL_ID, retention_count=2
        ) == []
    finally:
        writer.close()


def _healthy_inputs(tmp_path: Path):
    backup_file = tmp_path / "backup.sqlite3"
    receipt = tmp_path / "backup.sqlite3.receipt.json"
    backup_file.write_bytes(b"verified")
    receipt.write_text("{}", encoding="utf-8")
    backup = BackupResult(
        path=backup_file,
        receipt_path=receipt,
        database_sha256="c" * 64,
        receipt_sha256="d" * 64,
        created_utc="2026-09-01T07:15:00Z",
        created_this_run=True,
    )
    dashboard = {
        "journal": {
            "integrity": "ok", "bars_captured": 4000,
            "expected_clocks": 5, "fresh_clocks": 5,
            "stale_clocks": 0, "missing_clocks": 0,
            "excluded_clocks": 0, "clock_coverage": 1.0,
            "last_clock_close_utc": "2026-09-01T06:00:00Z",
            "last_clock_delay_seconds": 186,
            "candidate_signals": 0, "risk_vetoes": 1,
            "intended_orders": 0, "closed_candidate_trades": 0,
            "provider_bar_revisions": 0,
            # Must be excluded by the watchdog's explicit output allowlist.
            "net_pnl": 999999,
        },
        "safety": {
            "available": True, "kill_switch_engaged": False,
            "reconciliation_ok": True,
        },
    }
    deployment = {
        "binding": {"hypothesis_id": TRIAL_ID},
        "minimum_clock_coverage": 0.95,
    }
    windows = {
        "state": "Ready", "last_result": 0,
        "last_run_utc": "2026-09-01T07:10:00Z", "missed_runs": 0,
        "available_memory_mib": 3175, "committed_percent": 45.3,
        "pagefile_percent": 13.8,
    }
    mt5 = {
        "connected": True, "account_matches": True,
        "real_money": False,
    }
    news = {"valid": True, "age_seconds": 282}
    repository = {"head": "2637c25", "clean": True, "changed_paths": 0}
    return backup, dashboard, deployment, windows, mt5, news, repository


def test_healthy_report_is_strictly_blinded(tmp_path: Path):
    values = _healthy_inputs(tmp_path)
    report = evaluate_health(
        backup=values[0], dashboard=values[1], deployment=values[2],
        windows=values[3], mt5=values[4], news=values[5],
        repository=values[6],
        now_utc=datetime(2026, 9, 1, 7, 15, tzinfo=UTC),
        minimum_memory_mib=1536, maximum_commit_percent=75,
        maximum_pagefile_percent=50, maximum_task_age_seconds=900,
    )
    assert report["overall_status"] == "HEALTHY"
    assert all(item["status"] == "PASS" for item in report["checks"])
    serialized = json.dumps(report).lower()
    assert "net_pnl" not in serialized
    assert "999999" not in serialized
    assert report["performance_blinded"] is True
    assert report["broker_order_adapter_present"] is False
    assert_blinded_payload(report)


def test_watchdog_fails_closed_for_every_operational_fault(tmp_path: Path):
    backup, dashboard, deployment, windows, mt5, news, repository = (
        _healthy_inputs(tmp_path)
    )
    dashboard = copy.deepcopy(dashboard)
    windows = dict(windows)
    mt5 = dict(mt5)
    news = dict(news)
    repository = dict(repository)
    dashboard["journal"]["missing_clocks"] = 1
    dashboard["journal"]["clock_coverage"] = 0.8
    dashboard["safety"]["kill_switch_engaged"] = True
    windows.update({
        "last_result": 2, "missed_runs": 1,
        "available_memory_mib": 900, "committed_percent": 82,
        "pagefile_percent": 55,
    })
    mt5.update({"connected": False, "account_matches": False})
    news["valid"] = False
    repository.update({"clean": False, "changed_paths": 1})
    report = evaluate_health(
        backup=backup, dashboard=dashboard, deployment=deployment,
        windows=windows, mt5=mt5, news=news, repository=repository,
        now_utc=datetime(2026, 9, 1, 7, 15, tzinfo=UTC),
        minimum_memory_mib=1536, maximum_commit_percent=75,
        maximum_pagefile_percent=50, maximum_task_age_seconds=900,
    )
    failed = {
        item["name"] for item in report["checks"]
        if item["status"] == "FAIL"
    }
    assert report["overall_status"] == "UNHEALTHY"
    assert {
        "clock_evidence", "observer_task", "mt5_identity",
        "news_calendar", "execution_safety", "vps_memory",
        "bot_worktree",
    } <= failed
    assert_blinded_payload(report)
