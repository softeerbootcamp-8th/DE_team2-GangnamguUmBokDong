"""Gold publisher CLI의 fail-closed 시간·인자·환경 계약을 검증한다."""

from __future__ import annotations

import sys
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import gold_cli
import pytest
from core.gold_publication.errors import ContractViolation


def test_window_start_requires_offset_and_normalizes_to_utc() -> None:
    """Airflow KST window를 동일 instant의 UTC source logical time으로 바꾼다."""
    assert gold_cli._parse_window_start("2026-08-20T09:05:00+09:00") == datetime(
        2026,
        8,
        20,
        0,
        5,
        tzinfo=UTC,
    )
    with pytest.raises(ContractViolation, match="timezone offset"):
        gold_cli._parse_window_start("2026-08-20T09:05:00")


def test_station_lookback_environment_is_explicit_canonical_hours(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """station source scan이 무한하거나 암묵적인 기간을 사용하지 않는다."""
    monkeypatch.setenv("GOLD_STATION_REALTIME_LOOKBACK_HOURS", "24")
    assert gold_cli._lookback_from_env(
        "GOLD_STATION_REALTIME_LOOKBACK_HOURS"
    ) == timedelta(hours=24)
    monkeypatch.setenv("GOLD_STATION_REALTIME_LOOKBACK_HOURS", "024")
    with pytest.raises(ContractViolation, match="canonical"):
        gold_cli._lookback_from_env("GOLD_STATION_REALTIME_LOOKBACK_HOURS")


def test_relocation_approval_requires_uri_and_sha_together() -> None:
    """approval identity 절반만 전달해 출처 없는 relocation을 만들지 못하게 한다."""
    with pytest.raises(ContractViolation, match="함께"):
        gold_cli._read_optional_relocation_approval(
            object(),  # type: ignore[arg-type]
            "s3://fixture/approval.json",
            None,
        )


def test_main_accepts_station_master_correction_mode(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """daily master DAG의 station-only 모드가 CLI allowlist에 포함된다."""
    calls: list[tuple[str, datetime]] = []

    def fake_run(publication: str, window_start: datetime, **_kwargs: object) -> str:
        """CLI dispatch 인자를 기록하고 성공 outcome을 반환한다."""
        calls.append((publication, window_start))
        return "published"

    monkeypatch.setattr(gold_cli, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "gold_cli.py",
            "--publication",
            "station-master-correction",
            "--window-start",
            "2026-08-20T09:05:00+09:00",
        ],
    )

    assert gold_cli.main() == 0
    assert calls == [
        (
            "station-master-correction",
            datetime(2026, 8, 20, 0, 5, tzinfo=UTC),
        )
    ]
    assert "published" in capsys.readouterr().out


def test_static_seed_modes_reach_verified_publishers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """manual seed mode를 SSOT 시각과 명시 version으로만 게시한다."""
    connection = object()
    published: list[tuple[str, str, datetime]] = []

    def result() -> SimpleNamespace:
        """검증된 publisher의 성공 outcome fixture를 반환한다."""
        return SimpleNamespace(
            result=SimpleNamespace(outcome=SimpleNamespace(value="published"))
        )

    def publish_dispatch(
        actual_connection: object,
        _store: object,
        *,
        seed: object,
        object_base_uri: str,
    ) -> SimpleNamespace:
        """dispatch seed CLI 인자를 기록한다."""
        assert actual_connection is connection
        assert object_base_uri == "s3://fixture/gold_publication"
        published.append(("dispatch", seed.seed_version, seed.effective_dttm))
        return result()

    def publish_weather(
        actual_connection: object,
        _store: object,
        *,
        seed: object,
        object_base_uri: str,
    ) -> SimpleNamespace:
        """weather seed CLI 인자를 기록한다."""
        assert actual_connection is connection
        assert object_base_uri == "s3://fixture/gold_publication"
        published.append(("weather", seed.seed_version, seed.effective_dttm))
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
        ("dispatch", "dispatch-center-v1", dispatch_time),
        ("weather", "approved-grid-v1", weather_time),
    ]


def test_dispatch_seed_rejects_non_ssot_effective_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dispatch seed는 YAML effective_dttm을 caller 시각으로 덮어쓰지 않는다."""
    monkeypatch.setenv("S3_BUCKET", "fixture")
    monkeypatch.setattr(gold_cli, "_s3_client", lambda: object())
    monkeypatch.setattr(gold_cli, "get_connection", lambda: nullcontext(object()))

    with pytest.raises(ContractViolation, match="effective_dttm"):
        gold_cli.run(
            "seed:dispatch_center",
            datetime(2026, 8, 19, 3, 15, 39, tzinfo=UTC),
        )
