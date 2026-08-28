"""Seed/event Gold CLI와 retired standalone authority 경계를 검증한다."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace

import gold_cli
import pytest
from core.gold_publication import ContractViolation
from core.source_snapshot_io import SourceSnapshotNotFoundError

_EVENT_CASES = (
    ("event:cultural_event", "cultural_event"),
    ("event:performance_event", "performance_event"),
)


def _published_result() -> SimpleNamespace:
    """Published outcome fixture를 만든다."""
    return SimpleNamespace(
        result=SimpleNamespace(outcome=SimpleNamespace(value="published"))
    )


def _configure_event_runtime(monkeypatch, catalog, connection) -> None:
    """Event CLI unit test가 사용할 S3 catalog와 DB connection을 주입한다."""
    monkeypatch.setenv("S3_BUCKET", "fixture")
    monkeypatch.setattr(gold_cli, "_s3_client", lambda: object())
    monkeypatch.setattr(
        gold_cli,
        "S3SourceSnapshotCatalog",
        lambda *_args, **_kwargs: catalog,
    )
    monkeypatch.setattr(
        gold_cli,
        "get_connection",
        lambda: nullcontext(connection),
    )


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

    def publish_dispatch(_connection, _store, *, seed, object_base_uri):
        """Dispatch seed와 object base를 기록한다."""
        assert _connection is connection
        assert object_base_uri == "s3://fixture/gold_publication"
        published.append(("dispatch", seed.seed_version))
        return _published_result()

    def publish_weather(_connection, _store, *, seed, object_base_uri):
        """Weather grid seed와 object base를 기록한다."""
        assert _connection is connection
        assert object_base_uri == "s3://fixture/gold_publication"
        published.append(("weather", seed.seed_version))
        return _published_result()

    monkeypatch.setenv("S3_BUCKET", "fixture")
    monkeypatch.setenv("GOLD_WEATHER_GRID_SEED_VERSION", "approved-grid-v1")
    monkeypatch.setattr(gold_cli, "_s3_client", lambda: object())
    monkeypatch.setattr(gold_cli, "get_connection", lambda: nullcontext(connection))
    monkeypatch.setattr(gold_cli, "publish_dispatch_center", publish_dispatch)
    monkeypatch.setattr(gold_cli, "publish_weather_grid", publish_weather)

    dispatch_time = datetime(2026, 8, 27, 5, 10, tzinfo=UTC)
    weather_time = datetime(2026, 8, 19, 4, 0, tzinfo=UTC)
    assert gold_cli.run("seed:dispatch_center", dispatch_time) == "published"
    assert gold_cli.run("seed:weather_grid", weather_time) == "published"
    assert published == [
        ("dispatch", "dispatch-center-v3"),
        ("weather", "approved-grid-v1"),
    ]


@pytest.mark.parametrize(("publication", "source_id"), _EVENT_CASES)
def test_event_exact_authority_uses_existing_publisher(
    publication: str,
    source_id: str,
    monkeypatch,
) -> None:
    """행사 exact authority가 있으면 기존 Gold publisher 경로를 그대로 사용한다."""
    logical = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)
    connection = object()
    artifact = object()
    catalog = SimpleNamespace(
        exact_window_or_none=lambda actual_source, actual_logical: (
            artifact
            if (actual_source, actual_logical) == (source_id, logical)
            else pytest.fail("잘못된 event source authority 요청")
        )
    )
    _configure_event_runtime(monkeypatch, catalog, connection)

    def publish(
        actual_connection,
        _store,
        *,
        source_artifact,
        source_catalog,
        object_base_uri,
        **_kwargs,
    ):
        """기존 publisher에 전달된 exact 입력을 검증한다."""
        assert actual_connection is connection
        assert source_artifact is artifact
        assert source_catalog is catalog
        assert object_base_uri == "s3://fixture/gold_publication"
        return _published_result()

    monkeypatch.setattr(gold_cli, "publish_cultural_event", publish)
    monkeypatch.setattr(gold_cli, "publish_performance_event", publish)
    monkeypatch.setattr(
        gold_cli,
        "read_partial_source_snapshot",
        lambda *_args, **_kwargs: pytest.fail("exact authority에서 PARTIAL을 읽었다"),
    )

    assert gold_cli.run(publication, logical) == "published"


@pytest.mark.parametrize(("publication", "source_id"), _EVENT_CASES)
def test_completed_partial_retains_verified_event_publication(
    publication: str,
    source_id: str,
    monkeypatch,
) -> None:
    """Completed PARTIAL이면 기존 행사 Gold를 건드리지 않고 stale 성공한다."""
    logical = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)
    connection = object()
    state = object()
    observed: list[tuple[str, object]] = []
    catalog = SimpleNamespace(exact_window_or_none=lambda *_args: None)
    _configure_event_runtime(monkeypatch, catalog, connection)

    def read_partial(actual_source, actual_logical):
        """검증할 PARTIAL exact identity를 기록한다."""
        assert (actual_source, actual_logical) == (source_id, logical)
        observed.append(("partial", actual_source))
        return object()

    def load_state(actual_connection, publication_key):
        """기존 publication state 조회를 기록한다."""
        assert actual_connection is connection
        assert publication_key == publication
        observed.append(("state", publication_key))
        return state

    def read_manifest(_store, actual_state):
        """기존 state의 immutable manifest 검증을 기록한다."""
        assert actual_state is state
        observed.append(("manifest", actual_state))
        return object()

    def unexpected_publish(*_args, **_kwargs):
        """Stale no-op의 Gold mutation을 테스트 실패로 바꾼다."""
        pytest.fail("PARTIAL에서 event publisher를 호출했다")

    monkeypatch.setattr(gold_cli, "read_partial_source_snapshot", read_partial)
    monkeypatch.setattr(gold_cli, "load_publication_state", load_state)
    monkeypatch.setattr(gold_cli, "read_state_manifest", read_manifest)
    monkeypatch.setattr(gold_cli, "publish_cultural_event", unexpected_publish)
    monkeypatch.setattr(gold_cli, "publish_performance_event", unexpected_publish)

    assert gold_cli.run(publication, logical) == "stale"
    assert [item[0] for item in observed] == ["partial", "state", "manifest"]


def test_event_partial_without_previous_publication_fails_closed(monkeypatch) -> None:
    """최초 행사 수집이 PARTIAL이면 유지할 Gold가 없으므로 실패한다."""
    logical = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)
    connection = object()
    catalog = SimpleNamespace(exact_window_or_none=lambda *_args: None)
    _configure_event_runtime(monkeypatch, catalog, connection)
    monkeypatch.setattr(
        gold_cli,
        "read_partial_source_snapshot",
        lambda *_args: object(),
    )
    monkeypatch.setattr(gold_cli, "load_publication_state", lambda *_args: None)

    with pytest.raises(ContractViolation, match="유지할 Gold"):
        gold_cli.run("event:cultural_event", logical)


def test_event_authority_absence_without_completed_partial_fails(monkeypatch) -> None:
    """단순 authority 누락을 이전 Gold 유지 성공으로 숨기지 않는다."""
    logical = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)
    catalog = SimpleNamespace(exact_window_or_none=lambda *_args: None)
    _configure_event_runtime(monkeypatch, catalog, object())

    def missing_partial(*_args):
        """Diagnostic PARTIAL 부재를 typed read failure로 만든다."""
        raise SourceSnapshotNotFoundError("fixture partial 없음")

    monkeypatch.setattr(gold_cli, "read_partial_source_snapshot", missing_partial)

    with pytest.raises(SourceSnapshotNotFoundError, match="partial 없음"):
        gold_cli.run("event:cultural_event", logical)


def test_event_partial_with_corrupt_previous_manifest_fails(monkeypatch) -> None:
    """기존 state의 actual manifest가 손상됐으면 stale 성공으로 숨기지 않는다."""
    logical = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)
    catalog = SimpleNamespace(exact_window_or_none=lambda *_args: None)
    _configure_event_runtime(monkeypatch, catalog, object())
    monkeypatch.setattr(
        gold_cli,
        "read_partial_source_snapshot",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        gold_cli,
        "load_publication_state",
        lambda *_args: object(),
    )

    def corrupt_state_manifest(*_args):
        """기존 immutable publication manifest 검증 실패를 재현한다."""
        raise ContractViolation("fixture state manifest 손상")

    monkeypatch.setattr(gold_cli, "read_state_manifest", corrupt_state_manifest)

    with pytest.raises(ContractViolation, match="state manifest 손상"):
        gold_cli.run("event:cultural_event", logical)


def test_event_corrupt_authority_does_not_enter_partial_fallback(monkeypatch) -> None:
    """비어 있지 않은 손상 authority 오류를 stale로 축소하지 않는다."""
    logical = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)

    def corrupt_authority(*_args):
        """Catalog 검증 실패를 재현한다."""
        raise ContractViolation("fixture revision chain 손상")

    catalog = SimpleNamespace(exact_window_or_none=corrupt_authority)
    _configure_event_runtime(monkeypatch, catalog, object())
    monkeypatch.setattr(
        gold_cli,
        "read_partial_source_snapshot",
        lambda *_args: pytest.fail("손상 authority에서 PARTIAL fallback을 시도했다"),
    )

    with pytest.raises(ContractViolation, match="revision chain 손상"):
        gold_cli.run("event:cultural_event", logical)


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


def test_main_returns_zero_for_stale_event_noop(monkeypatch, capsys) -> None:
    """검증된 행사 PARTIAL의 stale no-op을 Airflow 성공 종료코드로 변환한다."""
    monkeypatch.setattr(
        gold_cli.sys,
        "argv",
        [
            "gold_cli.py",
            "--publication",
            "event:cultural_event",
            "--window-start",
            "2026-08-20T09:05:00+09:00",
        ],
    )
    monkeypatch.setattr(gold_cli, "run", lambda *_args: "stale")

    assert gold_cli.main() == 0
    assert "outcome: stale" in capsys.readouterr().out


def test_main_returns_one_for_event_fail_closed(monkeypatch, capsys) -> None:
    """유지할 Gold가 없는 행사 PARTIAL을 Airflow 실패 종료코드로 변환한다."""
    monkeypatch.setattr(
        gold_cli.sys,
        "argv",
        [
            "gold_cli.py",
            "--publication",
            "event:cultural_event",
            "--window-start",
            "2026-08-20T09:05:00+09:00",
        ],
    )

    def fail_closed(*_args):
        """행사 이전 publication 부재를 재현한다."""
        raise ContractViolation("유지할 Gold publication이 없습니다")

    monkeypatch.setattr(gold_cli, "run", fail_closed)

    assert gold_cli.main() == 1
    assert "유지할 Gold" in capsys.readouterr().err
