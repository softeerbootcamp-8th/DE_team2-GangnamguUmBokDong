"""main.py의 만료 행 정리(retention) 로직 — _expire_cutoff/_delete_expired를
검증한다. run() 전체를 목킹하는 대신 이 두 함수로 테스트 표면을 좁혔다(#116/#117)."""

from datetime import UTC, date, datetime

import pytest

from main import KST, _delete_expired, _expire_cutoff, _parse_window_start


class _FakeCursor:
    def __init__(self, rowcount: int):
        self.rowcount = rowcount
        self.executed: list[tuple[str, dict]] = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConnection:
    def __init__(self, rowcount: int = 3):
        self.cursor_obj = _FakeCursor(rowcount)

    def cursor(self):
        return self.cursor_obj


class TestExpireCutoff:
    def test_subtracts_grace_period_for_timestamptz_tables(self):
        window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)

        cutoff = _expire_cutoff("forecast_points", window_start)

        # RETENTION_GRACE["forecast_points"] == 2시간
        assert cutoff == datetime(2026, 8, 16, 12, 5, tzinfo=UTC)

    def test_weather_forecast_uses_its_own_grace_period(self):
        window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
        assert _expire_cutoff("weather_forecast", window_start) == datetime(2026, 8, 16, 12, 5, tzinfo=UTC)

    def test_cultural_events_returns_a_date_not_datetime(self):
        """end_date는 DATE 컬럼이라 datetime이 아니라 date로 비교해야 한다."""
        window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)

        cutoff = _expire_cutoff("cultural_events", window_start)

        # RETENTION_GRACE["cultural_events"] == 3일
        assert cutoff == date(2026, 8, 13)
        assert not isinstance(cutoff, datetime)


class TestDeleteExpired:
    def test_executes_delete_with_expire_col_and_cutoff(self):
        conn = _FakeConnection(rowcount=5)
        cutoff = datetime(2026, 8, 16, 12, 5, tzinfo=UTC)

        deleted = _delete_expired(conn, "forecast_points", "predicted_dttm", cutoff)

        assert deleted == 5
        [(query, params)] = conn.cursor_obj.executed
        assert "DELETE FROM forecast_points" in query
        assert "predicted_dttm < %(cutoff)s" in query
        assert params == {"cutoff": cutoff}

    def test_uses_target_table_not_logical_spec_name(self):
        """weather_forecast_ultra/cultural_events_performance처럼 별칭된 스펙도
        물리 테이블명(target_table)으로 DELETE해야 한다 — 호출부(main.run)가 이미
        별칭 해소를 하고 넘겨준다는 전제를 그대로 검증."""
        conn = _FakeConnection(rowcount=0)

        _delete_expired(conn, "cultural_events", "end_date", date(2026, 8, 13))

        [(query, _params)] = conn.cursor_obj.executed
        assert "DELETE FROM cultural_events" in query
        assert "weather_forecast_ultra" not in query


class TestParseWindowStart:
    def test_keeps_explicit_offset(self):
        parsed = _parse_window_start("2026-08-16T14:05:00+09:00")

        assert parsed == datetime(2026, 8, 16, 5, 5, tzinfo=UTC)

    def test_assumes_kst_when_offset_is_missing(self):
        """naive 값을 그대로 넘기면 DB 세션 TimeZone(UTC)으로 해석돼 cutoff가 9시간
        미래가 되고, 아직 만료되지 않은 예보/예측 행까지 지워진다."""
        parsed = _parse_window_start("2026-08-16T14:05:00")

        assert parsed.tzinfo is not None
        assert parsed == datetime(2026, 8, 16, 14, 5, tzinfo=KST)
        assert parsed.utcoffset().total_seconds() == 9 * 3600

    def test_naive_input_does_not_shift_the_cutoff(self):
        naive = _parse_window_start("2026-08-16T14:05:00")
        aware = _parse_window_start("2026-08-16T14:05:00+09:00")

        assert _expire_cutoff("forecast_points", naive) == _expire_cutoff("forecast_points", aware)


class TestRetentionConfigValidation:
    def test_expire_col_without_grace_fails_at_import_time(self):
        """tables.yaml에 expire_col만 추가하고 유예기간을 빠뜨리면, 적재 도중이
        아니라 설정 로드 시점에 잡혀야 한다(적재 후 롤백 방지)."""
        import config
        from config import TableSpec

        specs = {"brand_new_table": TableSpec(
            source_id="x",
            transform=lambda df: [],
            conflict_cols=["id"],
            update_cols=[],
            expire_col="expired_at",
        )}

        with pytest.raises(ValueError, match="brand_new_table"):
            config._validate_retention_config(specs)
