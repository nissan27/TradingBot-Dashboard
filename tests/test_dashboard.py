from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from dashboard_server import (
    CLOCK_AMENDMENT_ID,
    CLOCK_ELIGIBILITY_RULE_ID,
    DashboardConfig,
    HealthReader,
    assert_blinded_payload,
    readonly_connection,
)


ACCOUNT = "Exness-MT5Trial5:277817628"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_forward(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE observer_binding(
            singleton INTEGER PRIMARY KEY,
            account_key TEXT,
            observation_start_utc TEXT
        );
        CREATE TABLE captured_bars(bar_id TEXT PRIMARY KEY);
        CREATE TABLE clock_expectations(
            clock_id TEXT PRIMARY KEY,
            clock_close_utc TEXT
        );
        CREATE TABLE clock_captures(
            clock_id TEXT PRIMARY KEY,
            captured_utc TEXT,
            payload_json TEXT
        );
        CREATE TABLE observer_events(
            event_id TEXT PRIMARY KEY,
            event_type TEXT,
            occurred_utc TEXT,
            payload_json TEXT
        );
        CREATE TABLE paper_trades(
            trade_id TEXT PRIMARY KEY,
            payload_json TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO observer_binding VALUES(1,?,?)",
        (ACCOUNT, "2026-08-14T00:00:00Z"),
    )
    connection.executemany(
        "INSERT INTO captured_bars VALUES(?)",
        [(f"bar-{index}",) for index in range(4)],
    )
    clocks = [
        ("fresh-1", "2026-08-14T06:00:00Z"),
        ("false-sunday", "2026-08-16T06:00:00Z"),
        ("fresh-2", "2026-08-17T06:00:00Z"),
    ]
    connection.executemany(
        "INSERT INTO clock_expectations VALUES(?,?)", clocks
    )
    for clock_id, clock in (clocks[0], clocks[2]):
        payload = {
            "symbol": "XAUUSD",
            "clock_close_utc": clock,
            "captured_utc": clock.replace("06:00:00", "06:03:05"),
            "delay_seconds": 185,
            "fresh": True,
        }
        connection.execute(
            "INSERT INTO clock_captures VALUES(?,?,?)",
            (clock_id, payload["captured_utc"], json.dumps(payload)),
        )
    excluded = {
        "event_type": "clock_expectation_excluded",
        "clock_id": "false-sunday",
        "amendment_id": CLOCK_AMENDMENT_ID,
        "eligibility_rule_id": CLOCK_ELIGIBILITY_RULE_ID,
        "reason": "broker_not_open_at_declared_clock",
        "performance_evaluated": False,
    }
    connection.execute(
        "INSERT INTO observer_events VALUES(?,?,?,?)",
        (
            "excluded",
            "clock_expectation_excluded",
            "2026-08-18T11:00:00Z",
            json.dumps(excluded),
        ),
    )
    for index, event_type in enumerate(
        ("signal_post_atr", "risk_veto", "intended_order")
    ):
        connection.execute(
            "INSERT INTO observer_events VALUES(?,?,?,?)",
            (
                f"event-{index}",
                event_type,
                "2026-08-18T06:00:00Z",
                "{}",
            ),
        )
    connection.execute(
        "INSERT INTO paper_trades VALUES('sealed-trade','{\"secret\":123}')"
    )
    connection.commit()
    connection.close()


def build_execution(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE safety_state(
        account_key TEXT PRIMARY KEY,
        kill_switch_engaged INTEGER,
        kill_switch_reason TEXT,
        last_reconciled_at_msc INTEGER,
        last_reconciliation_ok INTEGER,
        last_reconciliation_error TEXT)"""
    )
    connection.execute(
        "INSERT INTO safety_state VALUES(?,?,?,?,?,?)",
        (ACCOUNT, 0, "", 1787040000000, 1, ""),
    )
    connection.commit()
    connection.close()


@pytest.fixture
def reader(tmp_path: Path) -> HealthReader:
    forward = tmp_path / "forward.sqlite3"
    execution = tmp_path / "execution.sqlite3"
    build_forward(forward)
    build_execution(execution)
    return HealthReader(
        DashboardConfig(
            forward_db=forward,
            execution_db=execution,
            static_dir=Path(__file__).parent.parent / "public",
            account_key=ACCOUNT,
        )
    )


def test_health_is_blinded_and_uses_amended_denominator(
    reader: HealthReader,
) -> None:
    payload = reader.read()
    assert payload["performance_blinded"] is True
    assert payload["broker_order_adapter_present"] is False
    assert payload["journal"]["expected_clocks"] == 2
    assert payload["journal"]["fresh_clocks"] == 2
    assert payload["journal"]["excluded_clocks"] == 1
    assert payload["journal"]["clock_coverage"] == 1.0
    assert payload["journal"]["closed_candidate_trades"] == 1
    serialized = json.dumps(payload).lower()
    assert "sealed-trade" not in serialized
    assert "secret" not in serialized
    assert_blinded_payload(payload)


def test_reading_does_not_change_either_database(reader: HealthReader) -> None:
    forward = reader.config.forward_db
    execution = reader.config.execution_db
    before = (digest(forward), digest(execution))
    reader.read()
    reader.read()
    assert (digest(forward), digest(execution)) == before


def test_sqlite_connection_is_query_only(reader: HealthReader) -> None:
    with readonly_connection(reader.config.forward_db) as connection:
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute(
                "INSERT INTO captured_bars VALUES('forbidden')"
            )


def test_performance_key_guard_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="forbidden performance field"):
        assert_blinded_payload({"safe": {"expectancy": 0.4}})


def test_wrong_account_binding_fails_closed(reader: HealthReader) -> None:
    wrong = HealthReader(
        DashboardConfig(
            forward_db=reader.config.forward_db,
            execution_db=reader.config.execution_db,
            static_dir=reader.config.static_dir,
            account_key="Wrong:1",
        )
    )
    with pytest.raises(RuntimeError, match="account key"):
        wrong.read()
