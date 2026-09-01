from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import forward_alerts
from dashboard_server import assert_blinded_payload
from forward_alerts import (
    AlertError,
    AlertJournal,
    evaluate_watchdog_report,
    read_watchdog_report,
    run_alert_cycle,
    telegram_powershell_notifier,
)


UTC = timezone.utc
TRIAL_ID = "xauusd-regime-session-open-h1-forward-v3"


def watchdog_report(
    now: datetime, *, healthy: bool = True,
    backup_age_hours: float = 1.0, failed_name: str = "observer_task",
) -> dict:
    return {
        "schema_version": 1,
        "kind": "forward_operational_watchdog",
        "generated_utc": now.isoformat().replace("+00:00", "Z"),
        "trial_id": TRIAL_ID,
        "overall_status": "HEALTHY" if healthy else "UNHEALTHY",
        "runtime_mode": "paper_read_only",
        "checks": [{
            "name": failed_name,
            "status": "PASS" if healthy else "FAIL",
            "detail": "health-only detail",
        }],
        "backup": {
            "created_utc": (
                now - timedelta(hours=backup_age_hours)
            ).isoformat().replace("+00:00", "Z"),
            "integrity": "ok",
        },
        "performance_blinded": True,
        "broker_order_adapter_present": False,
    }


def evaluation(report: dict, now: datetime):
    return evaluate_watchdog_report(
        report, expected_trial_id=TRIAL_ID, now_utc=now,
        maximum_status_age_seconds=1800,
        maximum_backup_age_hours=26,
    )


def event_types(path: Path) -> list[str]:
    with sqlite3.connect(path) as connection:
        return [
            row[0] for row in connection.execute(
                "SELECT event_type FROM alert_events ORDER BY rowid"
            )
        ]


def test_one_alert_per_incident_and_one_recovery_notification(tmp_path: Path):
    now = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
    journal_path = tmp_path / "alerts.sqlite3"
    state_path = tmp_path / "alert-state.json"
    delivered: list[str] = []

    def notifier(event):
        delivered.append(event["event_type"])

    with AlertJournal(journal_path, trial_id=TRIAL_ID, now_utc=now) as journal:
        healthy = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(now), now),
            trial_id=TRIAL_ID, now_utc=now,
            notifiers={"test": notifier},
        )
        assert healthy["overall_status"] == "HEALTHY"
        assert delivered == []

        incident_report = watchdog_report(now, healthy=False)
        incident = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(incident_report, now),
            trial_id=TRIAL_ID, now_utc=now,
            notifiers={"test": notifier},
        )
        assert incident["overall_status"] == "INCIDENT_ACTIVE"
        assert delivered == ["INCIDENT_OPENED"]

        run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(incident_report, now + timedelta(minutes=5)),
            trial_id=TRIAL_ID, now_utc=now + timedelta(minutes=5),
            notifiers={"test": notifier},
        )
        assert delivered == ["INCIDENT_OPENED"]

        changed = watchdog_report(
            now + timedelta(minutes=10), healthy=False,
            failed_name="news_calendar",
        )
        run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(changed, now + timedelta(minutes=10)),
            trial_id=TRIAL_ID, now_utc=now + timedelta(minutes=10),
            notifiers={"test": notifier},
        )
        assert delivered == ["INCIDENT_OPENED"]

        recovered_at = now + timedelta(minutes=15)
        recovered = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(recovered_at), recovered_at),
            trial_id=TRIAL_ID, now_utc=recovered_at,
            notifiers={"test": notifier},
        )
        assert recovered["overall_status"] == "HEALTHY"
        assert delivered == ["INCIDENT_OPENED", "INCIDENT_RECOVERED"]

        run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(recovered_at), recovered_at),
            trial_id=TRIAL_ID, now_utc=recovered_at,
            notifiers={"test": notifier},
        )
        assert delivered == ["INCIDENT_OPENED", "INCIDENT_RECOVERED"]

        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            journal.connection.execute("DELETE FROM alert_events")

    assert event_types(journal_path) == [
        "INCIDENT_OPENED", "INCIDENT_UPDATED", "INCIDENT_RECOVERED",
    ]
    with sqlite3.connect(journal_path) as connection:
        tables = {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "paper_trades" not in tables
        assert tables == {
            "alert_binding", "alert_events", "notification_deliveries",
        }
    serialized = state_path.read_text(encoding="utf-8")
    assert "expectancy" not in serialized.lower()
    assert "profit" not in serialized.lower()
    assert_blinded_payload(json.loads(serialized))


def test_stale_watchdog_and_backup_fail_closed(tmp_path: Path):
    now = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    report = watchdog_report(
        now - timedelta(minutes=31), backup_age_hours=27,
    )
    # Keep the backup 27 hours behind the actual evaluation time.
    report["backup"]["created_utc"] = (
        now - timedelta(hours=27)
    ).isoformat().replace("+00:00", "Z")
    result = evaluation(report, now)
    assert {item["code"] for item in result.conditions} == {
        "WATCHDOG_STATUS_STALE", "VERIFIED_BACKUP_STALE",
    }


def test_delivery_failure_retries_without_duplicate_incident(tmp_path: Path):
    now = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
    journal_path = tmp_path / "alerts.sqlite3"
    state_path = tmp_path / "alert-state.json"
    calls = 0

    def failing(_event):
        nonlocal calls
        calls += 1
        raise OSError("synthetic sink failure")

    delivered: list[str] = []
    with AlertJournal(journal_path, trial_id=TRIAL_ID, now_utc=now) as journal:
        first = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(now, healthy=False), now),
            trial_id=TRIAL_ID, now_utc=now,
            notifiers={"test": failing},
        )
        assert first["pending_notifications"] == 1
        assert calls == 1

        second = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(now, healthy=False), now),
            trial_id=TRIAL_ID, now_utc=now + timedelta(minutes=5),
            notifiers={"test": lambda event: delivered.append(
                event["event_type"]
            )},
        )
        assert second["pending_notifications"] == 0
        assert delivered == ["INCIDENT_OPENED"]

    assert event_types(journal_path) == ["INCIDENT_OPENED"]
    with sqlite3.connect(journal_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM notification_deliveries"
        ).fetchone()[0] == 1


def test_unavailable_status_opens_blinded_incident(tmp_path: Path):
    now = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
    result = evaluate_watchdog_report(
        None, expected_trial_id=TRIAL_ID, now_utc=now,
        maximum_status_age_seconds=1800,
        maximum_backup_age_hours=26,
        input_error="watchdog status file is missing",
    )
    assert result.conditions == ({
        "code": "WATCHDOG_STATUS_UNAVAILABLE",
        "detail": "watchdog status file is missing",
    },)


def test_watchdog_report_with_performance_field_is_rejected(tmp_path: Path):
    path = tmp_path / "watchdog-latest.json"
    path.write_text(json.dumps({"net_pnl": 123}), encoding="utf-8")
    with pytest.raises(AlertError, match="performance blinding"):
        read_watchdog_report(path)


def test_dual_sink_retries_only_failed_sink_and_recovers_once(tmp_path: Path):
    now = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
    journal_path = tmp_path / "alerts.sqlite3"
    state_path = tmp_path / "alert-state.json"
    windows_calls: list[str] = []
    telegram_attempts: list[str] = []
    telegram_delivered: list[str] = []

    def windows(event):
        windows_calls.append(event["event_type"])

    def telegram_failure(event):
        telegram_attempts.append(event["event_type"])
        raise OSError("synthetic remote outage")

    def telegram_success(event):
        telegram_attempts.append(event["event_type"])
        telegram_delivered.append(event["event_type"])

    with AlertJournal(journal_path, trial_id=TRIAL_ID, now_utc=now) as journal:
        first = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(now, healthy=False), now),
            trial_id=TRIAL_ID, now_utc=now,
            notifiers={
                "windows-event-log": windows,
                "telegram": telegram_failure,
            },
        )
        assert windows_calls == ["INCIDENT_OPENED"]
        assert telegram_attempts == ["INCIDENT_OPENED"]
        assert first["pending_by_sink"] == {
            "windows-event-log": 0,
            "telegram": 1,
        }
        assert first["delivery_errors"] == [{
            "sink": "telegram", "error_type": "OSError",
        }]

        retry = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(now, healthy=False), now),
            trial_id=TRIAL_ID, now_utc=now + timedelta(minutes=5),
            notifiers={
                "windows-event-log": windows,
                "telegram": telegram_success,
            },
        )
        assert windows_calls == ["INCIDENT_OPENED"]
        assert telegram_delivered == ["INCIDENT_OPENED"]
        assert retry["pending_notifications"] == 0

        recovered_at = now + timedelta(minutes=10)
        recovered = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(recovered_at), recovered_at),
            trial_id=TRIAL_ID, now_utc=recovered_at,
            notifiers={
                "windows-event-log": windows,
                "telegram": telegram_success,
            },
        )
        assert recovered["overall_status"] == "HEALTHY"
        assert windows_calls == ["INCIDENT_OPENED", "INCIDENT_RECOVERED"]
        assert telegram_delivered == [
            "INCIDENT_OPENED", "INCIDENT_RECOVERED",
        ]

        run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(recovered_at), recovered_at),
            trial_id=TRIAL_ID, now_utc=recovered_at + timedelta(minutes=5),
            notifiers={
                "windows-event-log": windows,
                "telegram": telegram_success,
            },
        )
        assert windows_calls == ["INCIDENT_OPENED", "INCIDENT_RECOVERED"]
        assert telegram_delivered == [
            "INCIDENT_OPENED", "INCIDENT_RECOVERED",
        ]

    with sqlite3.connect(journal_path) as connection:
        assert connection.execute(
            """SELECT sink,COUNT(*) FROM notification_deliveries
               GROUP BY sink ORDER BY sink"""
        ).fetchall() == [("telegram", 2), ("windows-event-log", 2)]


def test_delivery_exception_text_never_reaches_state_or_journal(tmp_path: Path):
    now = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
    journal_path = tmp_path / "alerts.sqlite3"
    state_path = tmp_path / "alert-state.json"
    secret = "123456789:TOP_SECRET_BOT_TOKEN chat=987654321"

    def leaking_client(_event):
        raise RuntimeError(secret)

    with AlertJournal(journal_path, trial_id=TRIAL_ID, now_utc=now) as journal:
        state = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(now, healthy=False), now),
            trial_id=TRIAL_ID, now_utc=now,
            notifiers={"telegram": leaking_client},
        )
        assert state["delivery_errors"] == [{
            "sink": "telegram", "error_type": "RuntimeError",
        }]

    serialized = state_path.read_text(encoding="utf-8")
    serialized += journal_path.read_bytes().decode("latin-1")
    assert secret not in serialized
    assert "TOP_SECRET_BOT_TOKEN" not in serialized
    assert "987654321" not in serialized


def test_failed_sink_never_delivers_recovery_before_open(tmp_path: Path):
    now = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
    journal_path = tmp_path / "alerts.sqlite3"
    state_path = tmp_path / "alert-state.json"
    attempts: list[str] = []

    def failing(event):
        attempts.append(event["event_type"])
        raise OSError("synthetic outage")

    delivered: list[str] = []
    with AlertJournal(journal_path, trial_id=TRIAL_ID, now_utc=now) as journal:
        run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(now, healthy=False), now),
            trial_id=TRIAL_ID, now_utc=now,
            notifiers={"telegram": failing},
        )
        recovered_at = now + timedelta(minutes=5)
        pending = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(recovered_at), recovered_at),
            trial_id=TRIAL_ID, now_utc=recovered_at,
            notifiers={"telegram": failing},
        )
        assert attempts == ["INCIDENT_OPENED", "INCIDENT_OPENED"]
        assert pending["pending_by_sink"] == {"telegram": 2}

        healthy = run_alert_cycle(
            journal=journal, state_path=state_path,
            evaluation=evaluation(watchdog_report(recovered_at), recovered_at),
            trial_id=TRIAL_ID, now_utc=recovered_at + timedelta(minutes=5),
            notifiers={
                "telegram": lambda event: delivered.append(
                    event["event_type"]
                )
            },
        )
        assert delivered == ["INCIDENT_OPENED", "INCIDENT_RECOVERED"]
        assert healthy["overall_status"] == "HEALTHY"


def test_telegram_notifier_invokes_sender_without_plaintext_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    credential = tmp_path / "telegram-credential.xml"
    sender = tmp_path / "send_telegram_alert.ps1"
    credential.write_text("encrypted DPAPI fixture", encoding="utf-8")
    sender.write_text("# sender fixture", encoding="utf-8")
    commands: list[list[str]] = []

    def fake_run(command, **kwargs):
        commands.append(command)
        assert kwargs["check"] is True
        assert kwargs["capture_output"] is True

    monkeypatch.setattr(forward_alerts.platform, "system", lambda: "Windows")
    monkeypatch.setattr(forward_alerts.subprocess, "run", fake_run)
    notify = telegram_powershell_notifier(
        credential_path=credential,
        sender_path=sender,
    )
    notify({
        "event_type": "INCIDENT_OPENED",
        "trial_id": TRIAL_ID,
        "incident_id": "a" * 64,
        "conditions": [{"code": "WATCHDOG_STATUS_STALE"}],
    })

    assert len(commands) == 1
    command = commands[0]
    assert command[0] == "powershell.exe"
    assert str(credential.resolve()) in command
    assert str(sender.resolve()) in command
    rendered = " ".join(command)
    assert "TOP_SECRET_BOT_TOKEN" not in rendered
    assert "performance=BLINDED" in rendered


def test_powershell_telegram_boundary_uses_dpapi_and_https():
    root = Path(__file__).resolve().parents[1]
    sender = (root / "send_telegram_alert.ps1").read_text(encoding="utf-8")
    config = (root / "configure_telegram_alerts.ps1").read_text(
        encoding="utf-8"
    )
    assert "Import-Clixml" in sender
    assert "sendMessage" in sender
    assert "https://api.telegram.org" in sender
    assert "Export-Clixml" in config
    assert "-AsSecureString" in config
    assert '"--delivery-sink", "windows-event-log"' in config
    assert '"--delivery-sink", "telegram"' in config
