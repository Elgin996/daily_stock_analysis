# -*- coding: utf-8 -*-
"""Shared EastMoney HTTP client with process and cross-process throttling."""

from __future__ import annotations

import logging
import os
from pathlib import Path
import random
import sqlite3
import tempfile
import threading
import time
from typing import Callable, Iterable

import requests

logger = logging.getLogger(__name__)

_DEFAULT_MIN_INTERVAL_SECONDS = 1.0
_DEFAULT_JITTER_MIN_SECONDS = 0.1
_DEFAULT_JITTER_MAX_SECONDS = 0.5
_PROCESS_LOCK = threading.RLock()
_SESSION: requests.Session | None = None


def eastmoney_min_interval_seconds() -> float:
    """Return the canonical minimum interval, honoring the legacy setting."""
    return max(
        _float_env(
            "EM_MIN_INTERVAL",
            _float_env(
                "SCREENING_EASTMONEY_MIN_INTERVAL_SEC",
                _DEFAULT_MIN_INTERVAL_SECONDS,
            ),
        ),
        0.0,
    )


def eastmoney_jitter_range_seconds() -> tuple[float, float]:
    legacy_max = _float_env(
        "SCREENING_EASTMONEY_JITTER_SEC",
        _DEFAULT_JITTER_MAX_SECONDS,
    )
    minimum = max(_float_env("EM_JITTER_MIN", _DEFAULT_JITTER_MIN_SECONDS), 0.0)
    maximum = max(_float_env("EM_JITTER_MAX", legacy_max), minimum)
    return minimum, maximum


def eastmoney_get(
    url: str,
    *,
    session: requests.Session | None = None,
    min_interval: float | None = None,
    jitter_range: tuple[float, float] | None = None,
    sleep_func: Callable[[float], None] = time.sleep,
    state_path: str | Path | None = None,
    max_attempts: int = 1,
    retry_delays: Iterable[float] = (0.5, 1.0),
    **kwargs,
) -> requests.Response:
    """GET an EastMoney endpoint through the shared rate-limit gate.

    SQLite holds the limiter lease while the request is in flight, so workers
    in separate Python processes serialize as long as they share
    ``EM_RATE_LIMIT_STATE_PATH`` (the host temp path is shared by default).
    """
    request_session = session or _get_session()
    attempts = max(1, int(max_attempts))
    delays = tuple(max(float(delay), 0.0) for delay in retry_delays)
    retryable_statuses = {429, 500, 502, 503, 504}
    retryable_errors = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
    )
    last_error: BaseException | None = None

    for attempt in range(attempts):
        try:
            response = _throttled_get_once(
                request_session,
                url,
                min_interval=min_interval,
                jitter_range=jitter_range,
                sleep_func=sleep_func,
                state_path=state_path,
                **kwargs,
            )
            status_code = int(getattr(response, "status_code", 200))
            if status_code not in retryable_statuses or attempt >= attempts - 1:
                return response
            response.close()
        except retryable_errors as exc:
            last_error = exc
            if attempt >= attempts - 1:
                raise

        delay = delays[min(attempt, len(delays) - 1)] if delays else 0.0
        if delay > 0:
            sleep_func(delay)

    if last_error is not None:
        raise last_error
    raise RuntimeError("EastMoney request exhausted without a response")


def _throttled_get_once(
    session: requests.Session,
    url: str,
    *,
    min_interval: float | None,
    jitter_range: tuple[float, float] | None,
    sleep_func: Callable[[float], None],
    state_path: str | Path | None,
    **kwargs,
) -> requests.Response:
    minimum = eastmoney_min_interval_seconds() if min_interval is None else max(float(min_interval), 0.0)
    jitter_min, jitter_max = jitter_range or eastmoney_jitter_range_seconds()
    interval = minimum + random.uniform(max(jitter_min, 0.0), max(jitter_max, jitter_min, 0.0))
    limiter_path = _resolve_state_path(state_path)
    lease_owner = f"{os.getpid()}:{threading.get_ident()}:{time.time_ns()}"
    lease_seconds = _request_lease_seconds(kwargs.get("timeout"), interval)

    with _PROCESS_LOCK:
        limiter_path.parent.mkdir(parents=True, exist_ok=True)
        _acquire_lease(
            limiter_path,
            owner=lease_owner,
            interval=interval,
            lease_seconds=lease_seconds,
            sleep_func=sleep_func,
        )
        try:
            return session.get(url, **kwargs)
        finally:
            _release_lease(limiter_path, owner=lease_owner)


def _acquire_lease(
    path: Path,
    *,
    owner: str,
    interval: float,
    lease_seconds: float,
    sleep_func: Callable[[float], None],
) -> None:
    """Acquire the cross-process request lease using short SQLite writes."""
    while True:
        connection = _connect_state(path)
        wait_seconds = 0.05
        poll_active_lease = False
        try:
            _ensure_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT last_request_at, lease_owner, lease_expires_at "
                "FROM throttle WHERE scope = ?",
                ("eastmoney",),
            ).fetchone()
            now = time.time()
            last_request_at = float(row[0]) if row else 0.0
            active_owner = str(row[1] or "") if row else ""
            lease_expires_at = float(row[2] or 0.0) if row else 0.0
            lease_is_active = bool(active_owner) and lease_expires_at > now
            interval_wait = interval - max(now - last_request_at, 0.0)

            if not lease_is_active and interval_wait <= 0:
                connection.execute(
                    "INSERT INTO throttle"
                    "(scope, last_request_at, lease_owner, lease_expires_at) "
                    "VALUES(?, ?, ?, ?) "
                    "ON CONFLICT(scope) DO UPDATE SET "
                    "lease_owner = excluded.lease_owner, "
                    "lease_expires_at = excluded.lease_expires_at",
                    ("eastmoney", last_request_at, owner, now + lease_seconds),
                )
                connection.commit()
                return

            connection.commit()
            candidates = [value for value in (interval_wait, lease_expires_at - now) if value > 0]
            wait_seconds = min(candidates) if candidates else 0.05
            poll_active_lease = lease_is_active
        except sqlite3.OperationalError as exc:
            connection.rollback()
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        # Poll active leases because their owner may finish before expiry. A
        # plain interval wait can sleep once and preserve the configured rate.
        if poll_active_lease:
            wait_seconds = min(wait_seconds, 0.25)
        sleep_func(max(wait_seconds, 0.01))


def _release_lease(path: Path, *, owner: str) -> None:
    """Release a request lease without masking the HTTP result on cleanup errors."""
    connection: sqlite3.Connection | None = None
    try:
        connection = _connect_state(path)
        _ensure_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE throttle SET last_request_at = ?, lease_owner = '', lease_expires_at = 0 "
            "WHERE scope = ? AND lease_owner = ?",
            (time.time(), "eastmoney", owner),
        )
        connection.commit()
    except Exception as exc:
        if connection is not None:
            connection.rollback()
        logger.warning("Failed to release EastMoney rate-limit lease: %s", exc)
    finally:
        if connection is not None:
            connection.close()


def _connect_state(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(
        str(path),
        timeout=max(_float_env("EM_RATE_LIMIT_LOCK_TIMEOUT", 5.0), 0.1),
    )


def _ensure_schema(connection: sqlite3.Connection) -> None:
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS throttle ("
        "scope TEXT PRIMARY KEY, "
        "last_request_at REAL NOT NULL, "
        "lease_owner TEXT NOT NULL DEFAULT '', "
        "lease_expires_at REAL NOT NULL DEFAULT 0)"
    )
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(throttle)").fetchall()
    }
    if "lease_owner" not in columns:
        connection.execute(
            "ALTER TABLE throttle ADD COLUMN lease_owner TEXT NOT NULL DEFAULT ''"
        )
    if "lease_expires_at" not in columns:
        connection.execute(
            "ALTER TABLE throttle ADD COLUMN lease_expires_at REAL NOT NULL DEFAULT 0"
        )
    connection.commit()


def _request_lease_seconds(timeout: object, interval: float) -> float:
    """Bound a stale lease using the request timeout plus a small grace period."""
    timeout_seconds = 30.0
    if isinstance(timeout, (tuple, list)):
        values = [float(value) for value in timeout if value is not None]
        if values:
            timeout_seconds = sum(max(value, 0.0) for value in values)
    elif timeout is not None:
        try:
            timeout_seconds = max(float(timeout), 0.0)
        except (TypeError, ValueError):
            pass
    return max(timeout_seconds + max(interval, 1.0) + 5.0, 10.0)


def _get_session() -> requests.Session:
    global _SESSION
    with _PROCESS_LOCK:
        if _SESSION is None:
            _SESSION = requests.Session()
            _SESSION.headers.update(
                {
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json,text/plain,*/*",
                }
            )
        return _SESSION


def _resolve_state_path(value: str | Path | None) -> Path:
    configured = str(value or os.getenv("EM_RATE_LIMIT_STATE_PATH", "")).strip()
    if configured:
        return Path(configured).expanduser()
    user_scope = str(os.getuid()) if hasattr(os, "getuid") else "user"
    return Path(tempfile.gettempdir()) / f"daily-stock-analysis-eastmoney-rate-limit-{user_scope}.sqlite3"


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %.3f", name, raw, default)
        return float(default)
