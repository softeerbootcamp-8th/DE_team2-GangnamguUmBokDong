"""공통 PostgreSQL 연결의 UTC session 계약을 검증한다."""

from typing import Any

import pytest

from core import db


def _capture_connect(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], object]:
    """psycopg 연결 인자를 기록하는 대체 함수를 설치한다."""
    captured: dict[str, Any] = {}
    connection = object()

    def fake_connect(conninfo: str, **kwargs: Any) -> object:
        """연결 문자열과 keyword option을 기록한다."""
        captured.update(conninfo=conninfo, kwargs=kwargs)
        return connection

    monkeypatch.setattr(db.psycopg, "connect", fake_connect)
    return captured, connection


def test_get_connection_forces_utc_without_discarding_dsn_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DSN의 기존 libpq option 뒤에 UTC session 설정을 추가한다."""
    database_url = (
        "postgresql://user:password@db.example/gold"
        "?options=-c%20statement_timeout%3D1000"
    )
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("PGOPTIONS", "-c lock_timeout=2000")
    captured, connection = _capture_connect(monkeypatch)

    assert db.get_connection() is connection
    assert captured == {
        "conninfo": database_url,
        "kwargs": {"options": "-c statement_timeout=1000 -c timezone=UTC"},
    }


def test_get_connection_preserves_pgoptions_when_dsn_has_no_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DSN option이 없으면 환경의 PGOPTIONS와 UTC 설정을 함께 적용한다."""
    database_url = "postgresql://user:password@db.example/gold"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("PGOPTIONS", "-c lock_timeout=2000")
    captured, connection = _capture_connect(monkeypatch)

    assert db.get_connection() is connection
    assert captured == {
        "conninfo": database_url,
        "kwargs": {"options": "-c lock_timeout=2000 -c timezone=UTC"},
    }
