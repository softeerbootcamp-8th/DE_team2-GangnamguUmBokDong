"""Seed/event Gold CLI와 retired standalone authority 경계를 검증한다."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from core.gold_publication import ContractViolation

import gold_cli


def test_window_start_requires_offset_and_normalizes_to_utc() -> None:
    """Source publication logical time은 offset 필수 UTC instant다."""
    assert gold_cli._parse_window_start("2026-08-20T09:05:00+09:00") == datetime(
        2026, 8, 20, 0, 5, tzinfo=UTC
    )
    with pytest.raises(ContractViolation, match="timezone offset"):
        gold_cli._parse_window_start("2026-08-20T09:05:00")


@pytest.mark.parametrize(
    "publication",
    ["station-release", "station-master-correction", "weather-forecast"],
)
def test_retired_standalone_publication_fails_before_io(
    publication: str,
    monkeypatch,
) -> None:
    """Standalone station/weather mode가 S3 env/client/DB 접근 전에 거부된다."""

    def unexpected_io(*args, **kwargs):
        """Retired path의 I/O를 테스트 실패로 바꾼다."""
        pytest.fail(f"retired path I/O: args={args}, kwargs={kwargs}")

    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.setattr(gold_cli, "_s3_client", unexpected_io)
    monkeypatch.setattr(gold_cli, "get_connection", unexpected_io)

    with pytest.raises(ContractViolation, match="retired"):
        gold_cli.run(publication, datetime(2026, 8, 20, tzinfo=UTC))


def test_static_seed_modes_remain_reachable(monkeypatch) -> None:
    """Manual dispatch/weather-grid seed authority는 verified publisher로 계속 연결된다."""
    connection = object()
    published: list[tuple[str, str]] = []

    def result() -> SimpleNamespace:
        """Published outcome fixture를 만든다."""
        return SimpleNamespace(
            result=SimpleNamespace(outcome=SimpleNamespace(value="published"))
        )

    def publish_dispatch(_connection, _store, *, seed, object_base_uri):
        """Dispatch seed와 object base를 기록한다."""
        assert _connection is connection
        assert object_base_uri == "s3://fixture/gold_publication"
        published.append(("dispatch", seed.seed_version))
        return result()

    def publish_weather(_connection, _store, *, seed, object_base_uri):
        """Weather grid seed와 object base를 기록한다."""
        assert _connection is connection
        assert object_base_uri == "s3://fixture/gold_publication"
        published.append(("weather", seed.seed_version))
        return result()

    monkeypatch.setenv("S3_BUCKET", "fixture")
    monkeypatch.setenv("GOLD_WEATHER_GRID_SEED_VERSION", "approved-grid-v1")
    monkeypatch.setattr(gold_cli, "_s3_client", lambda: object())
    monkeypatch.setattr(gold_cli, "get_connection", lambda: nullcontext(connection))
    monkeypatch.setattr(gold_cli, "publish_dispatch_center", publish_dispatch)
    monkeypatch.setattr(gold_cli, "publish_weather_grid", publish_weather)

    dispatch_time = datetime(2026, 8, 19, 3, 15, 38, tzinfo=UTC)
    weather_time = datetime(2026, 8, 19, 4, 0, tzinfo=UTC)
    assert gold_cli.run("seed:dispatch_center", dispatch_time) == "published"
    assert gold_cli.run("seed:weather_grid", weather_time) == "published"
    assert published == [
        ("dispatch", "dispatch-center-v1"),
        ("weather", "approved-grid-v1"),
    ]


def test_dispatch_seed_rejects_non_ssot_effective_time(monkeypatch) -> None:
    """Dispatch seed effective time을 CLI caller가 임의로 덮어쓰지 못한다."""
    monkeypatch.setenv("S3_BUCKET", "fixture")
    monkeypatch.setattr(gold_cli, "_s3_client", lambda: object())
    monkeypatch.setattr(gold_cli, "get_connection", lambda: nullcontext(object()))

    with pytest.raises(ContractViolation, match="effective_dttm"):
        gold_cli.run(
            "seed:dispatch_center",
            datetime(2026, 8, 19, 3, 15, 39, tzinfo=UTC),
        )
