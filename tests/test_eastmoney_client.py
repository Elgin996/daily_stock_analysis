# -*- coding: utf-8 -*-
"""Tests for the shared EastMoney rate-limit gate."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

from src.services import eastmoney_client


def _response(status_code: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    return response


def test_eastmoney_get_enforces_interval_across_calls(tmp_path) -> None:
    session = MagicMock()
    session.get.side_effect = [_response(), _response()]
    now = [100.0]
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    with patch.object(eastmoney_client.time, "time", side_effect=lambda: now[0]), patch.object(
        eastmoney_client.random,
        "uniform",
        return_value=0.0,
    ):
        for _ in range(2):
            eastmoney_client.eastmoney_get(
                "https://push2.eastmoney.com/test",
                session=session,
                min_interval=1.0,
                jitter_range=(0.0, 0.0),
                sleep_func=fake_sleep,
                state_path=tmp_path / "rate-limit.sqlite3",
            )

    assert session.get.call_count == 2
    assert sleeps == [1.0]


def test_eastmoney_get_retries_through_same_throttle(tmp_path) -> None:
    first = _response(503)
    second = _response(200)
    session = MagicMock()
    session.get.side_effect = [first, second]
    now = [200.0]
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    with patch.object(eastmoney_client.time, "time", side_effect=lambda: now[0]), patch.object(
        eastmoney_client.random,
        "uniform",
        return_value=0.0,
    ):
        result = eastmoney_client.eastmoney_get(
            "https://datacenter.eastmoney.com/test",
            session=session,
            min_interval=1.0,
            jitter_range=(0.0, 0.0),
            sleep_func=fake_sleep,
            state_path=tmp_path / "rate-limit.sqlite3",
            max_attempts=2,
            retry_delays=(0.0,),
        )

    assert result is second
    assert session.get.call_count == 2
    assert sleeps == [1.0]
    first.close.assert_called_once()


def test_active_cross_process_lease_is_polled_without_holding_sqlite_lock(tmp_path) -> None:
    state_path = tmp_path / "rate-limit.sqlite3"
    connection = sqlite3.connect(state_path)
    connection.execute(
        "CREATE TABLE throttle ("
        "scope TEXT PRIMARY KEY, last_request_at REAL NOT NULL, "
        "lease_owner TEXT NOT NULL DEFAULT '', "
        "lease_expires_at REAL NOT NULL DEFAULT 0)"
    )
    connection.execute(
        "INSERT INTO throttle VALUES (?, ?, ?, ?)",
        ("eastmoney", 0.0, "another-process", 105.0),
    )
    connection.commit()
    connection.close()

    session = MagicMock()
    session.get.return_value = _response()
    now = [100.0]
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    with patch.object(eastmoney_client.time, "time", side_effect=lambda: now[0]), patch.object(
        eastmoney_client.time,
        "time_ns",
        return_value=100_000_000_000,
    ), patch.object(eastmoney_client.random, "uniform", return_value=0.0):
        eastmoney_client.eastmoney_get(
            "https://push2.eastmoney.com/test",
            session=session,
            min_interval=0.0,
            jitter_range=(0.0, 0.0),
            sleep_func=fake_sleep,
            state_path=state_path,
        )

    assert session.get.call_count == 1
    assert sum(sleeps) == 5.0
    assert max(sleeps) <= 0.25


def test_canonical_interval_overrides_legacy_setting(monkeypatch) -> None:
    monkeypatch.setenv("SCREENING_EASTMONEY_MIN_INTERVAL_SEC", "2.0")
    monkeypatch.setenv("EM_MIN_INTERVAL", "1.5")

    assert eastmoney_client.eastmoney_min_interval_seconds() == 1.5
