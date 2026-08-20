"""Legacy loader CLI가 retired derived table을 I/O 전에 거부하는지 검증한다."""

from datetime import UTC, datetime

import pytest

from config import RETIRED_DERIVED_TABLES, RetiredSourceGoldPathError
from main import KST, _parse_window_start, run
from retention_config import DATE_TYPED_EXPIRE_TABLES, RETENTION_GRACE


@pytest.mark.parametrize("table", sorted(RETIRED_DERIVED_TABLES))
def test_run_rejects_derived_legacy_before_io(table: str, monkeypatch) -> None:
    """Forecast/urgency/routes loader가 S3 reader나 DB connection을 열지 않는다."""

    def unexpected_io(*args, **kwargs):
        """Retired path의 첫 I/O를 테스트 실패로 바꾼다."""
        pytest.fail(f"retired path I/O: args={args}, kwargs={kwargs}")

    monkeypatch.setattr("main.get_connection", unexpected_io)
    with pytest.raises(RetiredSourceGoldPathError, match="publication publisher"):
        run(table, datetime(2026, 8, 16, 14, 5, tzinfo=UTC))


def test_retention_registry_is_empty() -> None:
    """Publication-owned reconciliation 뒤 legacy retention authority가 남지 않는다."""
    assert RETENTION_GRACE == {}
    assert DATE_TYPED_EXPIRE_TABLES == frozenset()


def test_window_start_parsing_preserves_existing_compatibility() -> None:
    """Explicit offset은 instant로 유지하고 legacy naive input은 KST로 해석한다."""
    aware = _parse_window_start("2026-08-16T14:05:00+09:00")
    naive = _parse_window_start("2026-08-16T14:05:00")

    assert aware == datetime(2026, 8, 16, 5, 5, tzinfo=UTC)
    assert naive == datetime(2026, 8, 16, 14, 5, tzinfo=KST)
