"""Blinded incident alerts and recovery notifications for forward v3.

The pipeline consumes only ``watchdog-latest.json``. It never opens the
forward or execution journals, imports MT5, evaluates performance, or contains
a broker order adapter. Incident transitions are recorded in a separate
append-only SQLite journal and can be delivered independently to the Windows
Application event log and an optional Telegram bot.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from dashboard_server import assert_blinded_payload
from forward_watchdog import canonical_sha256, parse_utc, utc_text


UTC = timezone.utc
ALERT_SCHEMA = 1
EVENT_SOURCE = "TradingBot Forward Alerts"
Notifier = Callable[[Mapping[str, Any]], None]


class AlertError(RuntimeError):
    """Blinded alert evidence could not be evaluated or persisted."""


@dataclass(frozen=True)
class AlertEvaluation:
    conditions: tuple[dict[str, str], ...]
    condition_fingerprint: str
    watchdog_generated_utc: str | None
    watchdog_age_seconds: float | None
    backup_age_hours: float | None


def _condition(code: str, detail: str) -> dict[str, str]:
    return {"code": code, "detail": detail}


def _fingerprint(conditions: tuple[dict[str, str], ...]) -> str:
    return canonical_sha256([
        {
            "code": item["code"],
            # Stale ages advance on every run but remain the same condition.
            # Other details are stable operational classifications and may
            # materially identify a changed failure set.
            "detail": (
                None if item["code"].endswith("_STALE") else item["detail"]
            ),
        }
        for item in conditions
    ])


def read_watchdog_report(path: Path) -> Mapping[str, Any]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AlertError("watchdog status file is missing") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AlertError("watchdog status file is unreadable") from exc
    if not isinstance(report, dict):
        raise AlertError("watchdog status root is not an object")
    try:
        assert_blinded_payload(report)
    except RuntimeError as exc:
        raise AlertError("watchdog status violates performance blinding") from exc
    return report


def evaluate_watchdog_report(
    report: Mapping[str, Any] | None, *, expected_trial_id: str,
    now_utc: datetime, maximum_status_age_seconds: float,
    maximum_backup_age_hours: float, input_error: str | None = None,
) -> AlertEvaluation:
    """Return health-only conditions; report check details are not propagated."""
    now_utc = now_utc.astimezone(UTC)
    conditions: list[dict[str, str]] = []
    generated_text: str | None = None
    status_age: float | None = None
    backup_age: float | None = None

    if input_error is not None or report is None:
        conditions.append(_condition(
            "WATCHDOG_STATUS_UNAVAILABLE", input_error or "status unavailable",
        ))
    else:
        contract_ok = bool(
            report.get("schema_version") == 1
            and report.get("kind") == "forward_operational_watchdog"
            and report.get("trial_id") == expected_trial_id
            and report.get("runtime_mode") == "paper_read_only"
            and report.get("performance_blinded") is True
            and report.get("broker_order_adapter_present") is False
        )
        try:
            assert_blinded_payload(report)
        except RuntimeError:
            contract_ok = False
        if not contract_ok:
            conditions.append(_condition(
                "WATCHDOG_CONTRACT_INVALID",
                "identity, mode or blinding contract does not match",
            ))

        try:
            generated = parse_utc(str(report.get("generated_utc")))
            generated_text = utc_text(generated)
            signed_age = (now_utc - generated).total_seconds()
            status_age = max(0.0, signed_age)
            if signed_age < -60:
                conditions.append(_condition(
                    "WATCHDOG_CLOCK_INVALID", "status timestamp is in the future",
                ))
            elif status_age > maximum_status_age_seconds:
                conditions.append(_condition(
                    "WATCHDOG_STATUS_STALE",
                    f"status age {status_age:.0f}s exceeds "
                    f"{maximum_status_age_seconds:.0f}s",
                ))
        except Exception:
            conditions.append(_condition(
                "WATCHDOG_TIMESTAMP_INVALID", "status timestamp is invalid",
            ))

        if report.get("overall_status") != "HEALTHY":
            failed_names = sorted({
                str(item.get("name", "unknown"))
                for item in (report.get("checks") or [])
                if isinstance(item, dict) and item.get("status") == "FAIL"
            })
            conditions.append(_condition(
                "WATCHDOG_UNHEALTHY",
                "failed checks: " + (", ".join(failed_names) or "unspecified"),
            ))

        backup = report.get("backup")
        try:
            if not isinstance(backup, dict):
                raise ValueError("backup missing")
            backup_created = parse_utc(str(backup.get("created_utc")))
            signed_backup_age = (now_utc - backup_created).total_seconds() / 3600
            backup_age = max(0.0, signed_backup_age)
            if signed_backup_age < -(1 / 60):
                conditions.append(_condition(
                    "VERIFIED_BACKUP_CLOCK_INVALID",
                    "backup timestamp is in the future",
                ))
            elif backup_age > maximum_backup_age_hours:
                conditions.append(_condition(
                    "VERIFIED_BACKUP_STALE",
                    f"backup age {backup_age:.1f}h exceeds "
                    f"{maximum_backup_age_hours:.1f}h",
                ))
            if backup.get("integrity") != "ok":
                conditions.append(_condition(
                    "VERIFIED_BACKUP_INVALID", "backup integrity is not ok",
                ))
        except Exception:
            conditions.append(_condition(
                "VERIFIED_BACKUP_UNAVAILABLE",
                "verified backup evidence is unavailable",
            ))

    ordered = tuple(sorted(conditions, key=lambda item: item["code"]))
    return AlertEvaluation(
        conditions=ordered,
        condition_fingerprint=_fingerprint(ordered),
        watchdog_generated_utc=generated_text,
        watchdog_age_seconds=status_age,
        backup_age_hours=backup_age,
    )


class AlertJournal:
    """Append-only transition and delivery evidence for one frozen trial."""

    def __init__(self, path: Path, *, trial_id: str, now_utc: datetime):
        self.path = path.resolve()
        self.trial_id = trial_id
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.path), timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self._create_schema()
        self._bind(now_utc)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "AlertJournal":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS alert_binding(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                schema_version INTEGER NOT NULL,
                trial_id TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS alert_events(
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                incident_id TEXT NOT NULL,
                occurred_utc TEXT NOT NULL,
                condition_fingerprint TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notification_deliveries(
                delivery_id TEXT PRIMARY KEY,
                alert_event_id TEXT NOT NULL,
                sink TEXT NOT NULL,
                delivered_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                UNIQUE(alert_event_id, sink),
                FOREIGN KEY(alert_event_id) REFERENCES alert_events(event_id)
            );
            """
        )
        for table in (
            "alert_binding", "alert_events", "notification_deliveries",
        ):
            for action in ("UPDATE", "DELETE"):
                self.connection.execute(
                    f"""CREATE TRIGGER IF NOT EXISTS no_{action.lower()}_{table}
                    BEFORE {action} ON {table}
                    BEGIN
                        SELECT RAISE(ABORT, 'alert journal is append-only');
                    END"""
                )
        version = int(self.connection.execute("PRAGMA user_version").fetchone()[0])
        if version not in (0, ALERT_SCHEMA):
            raise AlertError(f"unsupported alert journal schema {version}")
        if version == 0:
            self.connection.execute(f"PRAGMA user_version={ALERT_SCHEMA}")
        self.connection.commit()

    def _bind(self, now_utc: datetime) -> None:
        core = {
            "schema_version": ALERT_SCHEMA,
            "kind": "forward_operational_alert_binding",
            "trial_id": self.trial_id,
            "created_utc": utc_text(now_utc),
            "performance_blinded": True,
            "broker_order_adapter_present": False,
        }
        raw = json.dumps(core, sort_keys=True, separators=(",", ":"))
        digest = canonical_sha256(core)
        existing = self.connection.execute(
            """SELECT schema_version,trial_id,payload_json,payload_sha256
               FROM alert_binding WHERE singleton=1"""
        ).fetchone()
        if existing is None:
            self.connection.execute(
                "INSERT INTO alert_binding VALUES(1,?,?,?,?,?)",
                (ALERT_SCHEMA, self.trial_id, core["created_utc"], raw, digest),
            )
            self.connection.commit()
        else:
            try:
                existing_payload = json.loads(existing["payload_json"])
                valid = bool(
                    int(existing["schema_version"]) == ALERT_SCHEMA
                    and str(existing["trial_id"]) == self.trial_id
                    and existing["payload_sha256"]
                    == canonical_sha256(existing_payload)
                    and existing_payload.get("kind")
                    == "forward_operational_alert_binding"
                    and existing_payload.get("performance_blinded") is True
                    and existing_payload.get("broker_order_adapter_present")
                    is False
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                valid = False
            if not valid:
                raise AlertError("alert journal binding is invalid")

    @staticmethod
    def _payload(value: Mapping[str, Any]) -> tuple[str, str]:
        assert_blinded_payload(value)
        raw = json.dumps(
            dict(value), sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        )
        return raw, canonical_sha256(value)

    def insert_event(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        raw, digest = self._payload(payload)
        event_id = str(payload["event_id"])
        try:
            self.connection.execute(
                "INSERT INTO alert_events VALUES(?,?,?,?,?,?,?)",
                (
                    event_id, payload["event_type"], payload["incident_id"],
                    payload["occurred_utc"], payload["condition_fingerprint"],
                    raw, digest,
                ),
            )
            self.connection.commit()
        except sqlite3.IntegrityError:
            row = self.connection.execute(
                "SELECT payload_sha256 FROM alert_events WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row is None or row["payload_sha256"] != digest:
                raise AlertError("alert event identity has conflicting evidence")
        return dict(payload)

    def active_incident(self) -> dict[str, Any] | None:
        row = self.connection.execute(
            """SELECT event_type,payload_json FROM alert_events
               WHERE event_type IN ('INCIDENT_OPENED','INCIDENT_RECOVERED')
               ORDER BY rowid DESC LIMIT 1"""
        ).fetchone()
        if row is None or row["event_type"] == "INCIDENT_RECOVERED":
            return None
        return json.loads(row["payload_json"])

    def last_condition_fingerprint(self, incident_id: str) -> str | None:
        row = self.connection.execute(
            """SELECT condition_fingerprint FROM alert_events
               WHERE incident_id=? AND event_type IN
                    ('INCIDENT_OPENED','INCIDENT_UPDATED')
               ORDER BY rowid DESC LIMIT 1""",
            (incident_id,),
        ).fetchone()
        return None if row is None else str(row["condition_fingerprint"])

    def latest_transition(self) -> dict[str, Any] | None:
        row = self.connection.execute(
            """SELECT payload_json FROM alert_events
               WHERE event_type IN ('INCIDENT_OPENED','INCIDENT_RECOVERED')
               ORDER BY rowid DESC LIMIT 1"""
        ).fetchone()
        return None if row is None else json.loads(row["payload_json"])

    def pending_notifications(self, sink: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """SELECT e.payload_json FROM alert_events e
               LEFT JOIN notification_deliveries d
                 ON d.alert_event_id=e.event_id AND d.sink=?
               WHERE e.event_type IN ('INCIDENT_OPENED','INCIDENT_RECOVERED')
                 AND d.delivery_id IS NULL
               ORDER BY e.rowid""",
            (sink,),
        ).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]

    def record_delivery(
        self, event: Mapping[str, Any], *, sink: str, now_utc: datetime,
    ) -> None:
        core = {
            "schema_version": ALERT_SCHEMA,
            "kind": "forward_operational_alert_delivery",
            "alert_event_id": event["event_id"],
            "incident_id": event["incident_id"],
            "event_type": event["event_type"],
            "sink": sink,
            "delivered_utc": utc_text(now_utc),
            "performance_blinded": True,
            "broker_order_adapter_present": False,
        }
        raw, digest = self._payload(core)
        delivery_id = canonical_sha256({
            "alert_event_id": event["event_id"], "sink": sink,
        })
        try:
            self.connection.execute(
                "INSERT INTO notification_deliveries VALUES(?,?,?,?,?,?)",
                (
                    delivery_id, event["event_id"], sink,
                    core["delivered_utc"], raw, digest,
                ),
            )
            self.connection.commit()
        except sqlite3.IntegrityError:
            row = self.connection.execute(
                """SELECT payload_sha256 FROM notification_deliveries
                   WHERE delivery_id=?""",
                (delivery_id,),
            ).fetchone()
            if row is None or row["payload_sha256"] != digest:
                raise AlertError("notification identity has conflicting evidence")


def _event_payload(
    *, event_type: str, incident_id: str, trial_id: str,
    occurred_utc: datetime, evaluation: AlertEvaluation,
    opened_utc: str | None = None,
) -> dict[str, Any]:
    core: dict[str, Any] = {
        "schema_version": ALERT_SCHEMA,
        "kind": "forward_operational_alert_event",
        "event_type": event_type,
        "incident_id": incident_id,
        "trial_id": trial_id,
        "occurred_utc": utc_text(occurred_utc),
        "condition_fingerprint": evaluation.condition_fingerprint,
        "conditions": list(evaluation.conditions),
        "watchdog_generated_utc": evaluation.watchdog_generated_utc,
        "performance_blinded": True,
        "broker_order_adapter_present": False,
    }
    if opened_utc is not None:
        core["opened_utc"] = opened_utc
        core["duration_seconds"] = max(
            0.0, (occurred_utc.astimezone(UTC) - parse_utc(opened_utc)).total_seconds(),
        )
    return {
        **core,
        "event_id": canonical_sha256({
            "incident_id": incident_id,
            "event_type": event_type,
            "condition_fingerprint": evaluation.condition_fingerprint,
            "occurred_utc": core["occurred_utc"],
        }),
    }


def _write_state(path: Path, state: Mapping[str, Any]) -> None:
    assert_blinded_payload(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    raw = json.dumps(
        dict(state), indent=2, sort_keys=True, ensure_ascii=False,
        allow_nan=False,
    ) + "\n"
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def run_alert_cycle(
    *, journal: AlertJournal, state_path: Path,
    evaluation: AlertEvaluation, trial_id: str, now_utc: datetime,
    notifiers: Mapping[str, Notifier] | None = None,
    sink: str | None = None, notifier: Notifier | None = None,
) -> dict[str, Any]:
    """Record one transition and attempt each sink independently.

    ``sink``/``notifier`` remain accepted for callers using the original
    single-sink interface. New deployments should pass ``notifiers`` so a
    remote delivery failure never causes a duplicate local notification.
    """
    now_utc = now_utc.astimezone(UTC)
    if notifiers is None:
        if sink is None or notifier is None:
            raise AlertError("at least one alert notifier is required")
        notifiers = {sink: notifier}
    elif sink is not None or notifier is not None:
        raise AlertError("use either notifiers or sink/notifier, not both")
    if not notifiers:
        raise AlertError("at least one alert notifier is required")
    normalized_notifiers = {
        str(name): callback for name, callback in notifiers.items()
    }
    if any(not name or not callable(callback)
           for name, callback in normalized_notifiers.items()):
        raise AlertError("alert notifier mapping is invalid")

    active = journal.active_incident()
    if evaluation.conditions and active is None:
        opened = utc_text(now_utc)
        incident_id = canonical_sha256({
            "trial_id": trial_id,
            "opened_utc": opened,
            "condition_fingerprint": evaluation.condition_fingerprint,
        })
        journal.insert_event(_event_payload(
            event_type="INCIDENT_OPENED", incident_id=incident_id,
            trial_id=trial_id, occurred_utc=now_utc, evaluation=evaluation,
        ))
    elif evaluation.conditions and active is not None:
        incident_id = str(active["incident_id"])
        if (
            journal.last_condition_fingerprint(incident_id)
            != evaluation.condition_fingerprint
        ):
            journal.insert_event(_event_payload(
                event_type="INCIDENT_UPDATED", incident_id=incident_id,
                trial_id=trial_id, occurred_utc=now_utc,
                evaluation=evaluation,
            ))
    elif not evaluation.conditions and active is not None:
        journal.insert_event(_event_payload(
            event_type="INCIDENT_RECOVERED",
            incident_id=str(active["incident_id"]), trial_id=trial_id,
            occurred_utc=now_utc, evaluation=evaluation,
            opened_utc=str(active["occurred_utc"]),
        ))

    delivery_errors: list[dict[str, str]] = []
    for sink_name, callback in normalized_notifiers.items():
        for event in journal.pending_notifications(sink_name):
            try:
                callback(event)
                journal.record_delivery(
                    event, sink=sink_name, now_utc=now_utc,
                )
            except Exception as exc:
                # Error types are operational evidence. Exception text is
                # intentionally excluded because a third-party client could
                # include credential-bearing request details in it.
                delivery_errors.append({
                    "sink": sink_name,
                    "error_type": type(exc).__name__,
                })
                # Preserve transition order for this destination. A pending
                # INCIDENT_OPENED must never be overtaken by its recovery.
                break

    active = journal.active_incident()
    pending_by_sink = {
        sink_name: len(journal.pending_notifications(sink_name))
        for sink_name in normalized_notifiers
    }
    pending = sum(pending_by_sink.values())
    if active is not None:
        status = "INCIDENT_ACTIVE"
    elif delivery_errors or pending:
        status = "DELIVERY_PENDING"
    else:
        status = "HEALTHY"
    state = {
        "schema_version": ALERT_SCHEMA,
        "kind": "forward_operational_alert_state",
        "generated_utc": utc_text(now_utc),
        "trial_id": trial_id,
        "overall_status": status,
        "active_incident": None if active is None else {
            "incident_id": active["incident_id"],
            "opened_utc": active["occurred_utc"],
            "conditions": list(evaluation.conditions),
        },
        "last_transition": journal.latest_transition(),
        "pending_notifications": pending,
        "pending_by_sink": pending_by_sink,
        "delivery_errors": delivery_errors,
        "watchdog": {
            "generated_utc": evaluation.watchdog_generated_utc,
            "age_seconds": evaluation.watchdog_age_seconds,
            "backup_age_hours": evaluation.backup_age_hours,
        },
        "journal": {
            "path": str(journal.path),
            "append_only": True,
        },
        "runtime_mode": "paper_read_only",
        "performance_blinded": True,
        "broker_order_adapter_present": False,
    }
    assert_blinded_payload(state)
    _write_state(state_path, state)
    return state


def _notification_message(event: Mapping[str, Any]) -> str:
    opened = event["event_type"] == "INCIDENT_OPENED"
    codes = ",".join(item["code"] for item in event.get("conditions", []))
    return (
        f"TradingBot {event['trial_id']} "
        f"{'incident opened' if opened else 'recovered'}; "
        f"incident={str(event['incident_id'])[:12]}; "
        f"conditions={codes or 'cleared'}; performance=BLINDED"
    )[:300]


def windows_event_log_notifier(event: Mapping[str, Any]) -> None:
    if platform.system() != "Windows":
        raise AlertError("Windows Event Log delivery is unavailable")
    opened = event["event_type"] == "INCIDENT_OPENED"
    message = _notification_message(event)
    try:
        subprocess.run(
            [
                "eventcreate.exe", "/L", "APPLICATION",
                "/SO", EVENT_SOURCE, "/T", "ERROR" if opened else "INFORMATION",
                "/ID", "901" if opened else "902", "/D", message,
            ],
            check=True, capture_output=True, text=True, timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AlertError("Windows Event Log delivery failed") from exc


def telegram_powershell_notifier(
    *, credential_path: Path, sender_path: Path,
) -> Notifier:
    """Return a notifier that delegates token decryption to Windows DPAPI.

    Python receives only the encrypted credential-file path. The token and
    chat ID are decrypted inside the short-lived PowerShell sender process and
    are never placed in this process, its command line, or the alert journal.
    """
    resolved_credential = credential_path.resolve()
    resolved_sender = sender_path.resolve()

    def notify(event: Mapping[str, Any]) -> None:
        if platform.system() != "Windows":
            raise AlertError("Telegram delivery requires Windows PowerShell")
        if not resolved_credential.is_file():
            raise AlertError("Telegram credential file is unavailable")
        if not resolved_sender.is_file():
            raise AlertError("Telegram sender script is unavailable")
        try:
            subprocess.run(
                [
                    "powershell.exe", "-NoProfile", "-NonInteractive",
                    "-ExecutionPolicy", "Bypass", "-File",
                    str(resolved_sender),
                    "-CredentialPath", str(resolved_credential),
                    "-Message", _notification_message(event),
                ],
                check=True, capture_output=True, text=True, timeout=30,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise AlertError("Telegram delivery failed") from exc

    return notify


def console_notifier(event: Mapping[str, Any]) -> None:
    print(
        f"ALERT {event['event_type']}: incident="
        f"{str(event['incident_id'])[:12]}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--watchdog-status", type=Path, required=True)
    parser.add_argument("--alert-journal", type=Path, required=True)
    parser.add_argument("--alert-state", type=Path, required=True)
    parser.add_argument(
        "--maximum-watchdog-age-seconds", type=float, default=1800.0,
    )
    parser.add_argument("--maximum-backup-age-hours", type=float, default=26.0)
    parser.add_argument(
        "--delivery-sink",
        choices=("windows-event-log", "telegram", "console"),
        action="append",
        help=("delivery sink; repeat to enable independent local and remote "
              "delivery (default: windows-event-log)"),
    )
    parser.add_argument("--telegram-credential", type=Path)
    parser.add_argument("--telegram-sender", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = datetime.now(UTC)
    try:
        try:
            report = read_watchdog_report(args.watchdog_status)
            input_error = None
        except AlertError as exc:
            report = None
            input_error = str(exc)
        evaluation = evaluate_watchdog_report(
            report, expected_trial_id=args.trial_id, now_utc=now,
            maximum_status_age_seconds=max(
                60.0, args.maximum_watchdog_age_seconds,
            ),
            maximum_backup_age_hours=max(1.0, args.maximum_backup_age_hours),
            input_error=input_error,
        )
        sinks = list(dict.fromkeys(
            args.delivery_sink or ["windows-event-log"]
        ))
        notifiers: dict[str, Notifier] = {}
        for sink_name in sinks:
            if sink_name == "windows-event-log":
                notifiers[sink_name] = windows_event_log_notifier
            elif sink_name == "console":
                notifiers[sink_name] = console_notifier
            else:
                if args.telegram_credential is None:
                    raise AlertError(
                        "Telegram delivery requires --telegram-credential"
                    )
                if args.telegram_sender is None:
                    raise AlertError(
                        "Telegram delivery requires --telegram-sender"
                    )
                notifiers[sink_name] = telegram_powershell_notifier(
                    credential_path=args.telegram_credential,
                    sender_path=args.telegram_sender,
                )
        with AlertJournal(
            args.alert_journal, trial_id=args.trial_id, now_utc=now,
        ) as journal:
            state = run_alert_cycle(
                journal=journal, state_path=args.alert_state,
                evaluation=evaluation, trial_id=args.trial_id,
                now_utc=now, notifiers=notifiers,
            )
    except Exception as exc:
        print(f"Forward alerts: PIPELINE FAILURE ({type(exc).__name__})")
        print("Performance:    BLINDED")
        print("PASS: no broker order was submitted")
        return 2

    print(f"Forward alerts: {state['overall_status']}")
    print(f"UTC as-of:      {state['generated_utc']}")
    if state["active_incident"]:
        codes = ", ".join(
            item["code"] for item in state["active_incident"]["conditions"]
        )
        print(f"Incident:       {state['active_incident']['incident_id'][:12]}")
        print(f"Conditions:     {codes}")
    print(f"Pending alerts: {state['pending_notifications']}")
    for sink_name, count in state["pending_by_sink"].items():
        print(f"  {sink_name}: {count}")
    print(f"Alert journal:  {args.alert_journal.resolve()}")
    print(f"Alert state:    {args.alert_state.resolve()}")
    print("Performance:    BLINDED")
    print("PASS: no broker order was submitted")
    return 0 if state["overall_status"] == "HEALTHY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
