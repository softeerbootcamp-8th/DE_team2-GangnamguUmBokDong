"""#7 pipeline.py 테스트: 재개 분기 4가지, 완결도·폐기 게이트, FATAL 즉시 중단."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar
from zoneinfo import ZoneInfo

import httpx
import manifest as manifest_module
import pipeline
import pytest
import storage
from adapters.base import FetchErrorKind, FetchResult, adapter
from config.schema import Backfill, Policies, Quality, Schedule, SourceConfig
from config.schema import Storage as StorageConfig
from manifest import FailureReason, RunStatus, Stage

KST = ZoneInfo("Asia/Seoul")
WINDOW_START = datetime(2026, 8, 12, 14, 10, tzinfo=KST)


def _config(**overrides):
    fields = {
        "source_id": "t_source",
        "description": "테스트 소스",
        "adapter": "t_pipeline_adapter",
        "adapter_params": {},
        "schedule": Schedule(interval="5m"),
        "storage": StorageConfig(bronze_format="json", silver_format="parquet", partition=("dt", "hh")),
        "quality": Quality(max_drop_ratio=0.5, max_missing_ratio=0.0, allow_empty=False),
        "policies": Policies(
            required_missing="drop_row", required_outlier="drop_row",
            optional_missing="keep_null", optional_outlier="set_null",
        ),
        "columns": {},
        "config_version": "v1",
    }
    fields.update(overrides)
    return SourceConfig(**fields)


class _ScriptedAdapter:
    """라운드별로 다른 결과를 내도록 스크립트로 제어하는 테스트 어댑터."""

    fetch_calls: ClassVar[int] = 0
    results: ClassVar[list] = []  # list[list[FetchResult]] — 호출마다 하나씩 소비
    rows_by_key: ClassVar[dict] = {}  # chunk key -> normalize가 반환할 행 하나

    @staticmethod
    def fetch(config, window, *, client, skip=frozenset(), expected_total=None):
        _ScriptedAdapter.fetch_calls += 1
        results = _ScriptedAdapter.results.pop(0) if _ScriptedAdapter.results else []
        for r in results:
            if r.key not in skip:
                yield r

    @staticmethod
    def normalize(chunks, config):
        rows = []
        for chunk in chunks:
            key = chunk.decode()
            rows.append(_ScriptedAdapter.rows_by_key.get(key, {"k": key}))
        return rows


@pytest.fixture
def scripted_adapter(clean_adapter_registry, monkeypatch):
    _ScriptedAdapter.fetch_calls = 0
    _ScriptedAdapter.results = []
    _ScriptedAdapter.rows_by_key = {}
    adapter("t_pipeline_adapter")(_ScriptedAdapter)
    return _ScriptedAdapter


@pytest.fixture
def client():
    return httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200)))


def _chunk(key):
    return key.encode()


class TestFreshFetchSuccess:
    """분기 3: manifest 없음 → 전체 fetch → 성공."""

    def test_writes_bronze_silver_and_completed_manifest(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None),
                FetchResult(key="b", payload=_chunk("b"), error=None, expected_total=None),
            ]
        ]
        config = _config()

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.SUCCEEDED
        assert result.stage == Stage.COMPLETED
        assert result.counts.fetched == 2
        assert result.counts.dropped == 0
        assert result.revision == 1
        assert result.artifacts.silver is not None
        assert set(result.artifacts.bronze.parts) == {"a", "b"}

        saved = manifest_module.load(config.source_id, WINDOW_START)
        assert saved.status == RunStatus.SUCCEEDED
        assert storage.read_bronze(config.source_id, WINDOW_START, ["a", "b"]) == [_chunk("a"), _chunk("b")]

    def test_successful_empty_part_is_not_counted_as_missing(self, scripted_adapter, client, monkeypatch):
        """INFO-200 같은 정상 빈 조각은 row 부족이 아니라 성공한 POI 조각이다."""
        scripted_adapter.results = [
            [
                FetchResult(key="poi-POI001", payload=_chunk("row"), error=None, expected_total=None),
                FetchResult(key="poi-POI002", payload=_chunk("empty"), error=None, expected_total=None),
            ]
        ]
        scripted_adapter.rows_by_key = {"row": {"k": "row"}, "empty": None}

        original_normalize = scripted_adapter.normalize

        def normalize_without_empty(chunks, config):
            return [row for row in original_normalize(chunks, config) if row is not None]

        monkeypatch.setattr(scripted_adapter, "normalize", staticmethod(normalize_without_empty))
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.0, allow_empty=False))

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.SUCCEEDED
        assert result.counts.expected is None
        assert result.counts.fetched == 1
        assert result.missing.parts == ()
        assert result.missing.basis == "parts"
        assert result.completeness == 1.0


class TestSkipBranch:
    """분기 1: stage=completed & 누락 없음 & !force → SKIPPED, 재호출 없음."""

    def test_returns_existing_manifest_without_refetching(self, scripted_adapter, client):
        scripted_adapter.results = [[FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]]
        config = _config()
        pipeline.execute_window(config, WINDOW_START, client=client)
        assert scripted_adapter.fetch_calls == 1

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.SKIPPED
        assert scripted_adapter.fetch_calls == 1  # 다시 호출되지 않았다


class TestForceBranch:
    """분기 3 강제: --force는 완결 여부와 무관하게 clear_bronze 후 재수집한다."""

    def test_force_refetches_even_when_completed(self, scripted_adapter, client):
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)],
            [FetchResult(key="z", payload=_chunk("z"), error=None, expected_total=None)],
        ]
        config = _config()
        pipeline.execute_window(config, WINDOW_START, client=client)

        result = pipeline.execute_window(config, WINDOW_START, client=client, force=True)

        assert scripted_adapter.fetch_calls == 2
        assert set(result.artifacts.bronze.parts) == {"z"}  # 이전 조각(a)은 지워졌다


class TestForceAndBackfillRejected:
    def test_raises_when_both_given(self, scripted_adapter, client):
        with pytest.raises(pipeline.ForceAndBackfillError):
            pipeline.execute_window(_config(), WINDOW_START, client=client, force=True, backfill=True)


class TestMissingRatioGate:
    """max_missing_ratio 초과 시 silver를 쓰지 않고 FAILED/fetch_error로 끝난다."""

    def test_exceeds_gate_fails_without_silver(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.1, allow_empty=True))

        result = pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.FETCH_ERROR
        assert result.artifacts.silver is None

    def test_within_gate_succeeds_as_partial(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=10),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        # expected_total=10, 받은 행 1개 -> missing_ratio = 1 - 1/10 = 0.9
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.95, allow_empty=True))

        result = pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.PARTIAL
        assert result.artifacts.silver is not None
        assert result.missing.parts == ("b",)


class TestFatalAbortsImmediately:
    def test_fatal_skips_gate_and_fails_with_fetch_error(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None),
                FetchResult(key="b", payload=None, error=FetchErrorKind.FATAL, expected_total=None),
            ]
        ]
        config = _config()

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.FETCH_ERROR
        assert result.artifacts.silver is None


class TestBronzeReuseBranch:
    """분기 2: stage>=bronze_written & !force → bronze를 다시 읽고, fetch는 다시 하지 않는다."""

    def test_resumes_from_bronze_without_refetching(self, scripted_adapter, client, monkeypatch):
        scripted_adapter.results = [
            [FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]
        ]
        config = _config()

        # write_silver가 실패해 stage=bronze_written에서 멈춘 상황을 흉내낸다.
        original_write_silver = storage.write_silver

        def _boom(*args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(storage, "write_silver", _boom)
        first = pipeline.execute_window(config, WINDOW_START, client=client)
        assert first.status == RunStatus.FAILED
        assert first.failure_reason == FailureReason.STORAGE_ERROR
        assert first.stage == Stage.VALIDATED

        monkeypatch.setattr(storage, "write_silver", original_write_silver)
        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert scripted_adapter.fetch_calls == 1  # 두 번째 실행에서 다시 fetch하지 않았다
        assert result.status == RunStatus.SUCCEEDED
        assert result.stage == Stage.COMPLETED


class TestBackfillBranch:
    """분기 4: stage=completed & 누락 존재 & --backfill → 누락 조각만 채워 전체 재처리, revision +1."""

    def test_fills_missing_parts_and_bumps_revision(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.6, allow_empty=True))
        first = pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)
        assert first.status == RunStatus.PARTIAL
        assert first.missing.parts == ("b",)
        assert first.revision == 1

        scripted_adapter.results = [[FetchResult(key="b", payload=_chunk("b"), error=None, expected_total=None)]]
        result = pipeline.execute_window(config, WINDOW_START, client=client, backfill=True)

        assert result.status == RunStatus.SUCCEEDED
        assert result.missing.parts == ()
        assert result.revision == 2
        assert set(result.artifacts.bronze.parts) == {"a", "b"}

    def test_backfill_fetches_missing_even_if_stage_is_bronze_written(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        # max_missing_ratio를 0.0으로 두면 조각 하나라도 누락 시 FAILED / BRONZE_WRITTEN이 된다.
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.0, allow_empty=True))
        first = pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)
        assert first.status == RunStatus.FAILED
        assert first.stage == Stage.BRONZE_WRITTEN
        assert first.missing.parts == ("b",)
        assert first.revision == 0

        scripted_adapter.results = [[FetchResult(key="b", payload=_chunk("b"), error=None, expected_total=None)]]
        result = pipeline.execute_window(config, WINDOW_START, client=client, backfill=True)

        assert result.status == RunStatus.SUCCEEDED
        assert result.missing.parts == ()
        assert result.revision == 1
        assert set(result.artifacts.bronze.parts) == {"a", "b"}

    def test_backfill_without_missing_is_a_noop_skip(self, scripted_adapter, client):
        scripted_adapter.results = [[FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]]
        config = _config()
        pipeline.execute_window(config, WINDOW_START, client=client)

        result = pipeline.execute_window(config, WINDOW_START, client=client, backfill=True)

        assert result.status == RunStatus.SKIPPED
        assert scripted_adapter.fetch_calls == 1


class TestDropRatioGate:
    def test_exceeds_drop_ratio_fails_with_quality_gate(self, scripted_adapter, client):
        scripted_adapter.results = [[FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]]
        scripted_adapter.rows_by_key = {"a": {"col": None}}
        config = _config(
            quality=Quality(max_drop_ratio=0.1, max_missing_ratio=1.0, allow_empty=True),
            columns={"col": pipeline_make_required_str_spec()},
        )

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.QUALITY_GATE
        assert result.artifacts.silver is None


def pipeline_make_required_str_spec():
    from config.schema import ColumnSpec

    return ColumnSpec(types=("str",), required=True)


class TestLogging:
    """계획서 9절: 단계 경계마다 로그 한 줄, 행·조각 단위 로그는 없다."""

    def test_success_run_logs_bronze_written_validated_and_completed(self, scripted_adapter, client, caplog):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None),
                FetchResult(key="b", payload=_chunk("b"), error=None, expected_total=None),
            ]
        ]
        config = _config()

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client)

        messages = [r.message for r in caplog.records]
        assert any("stage=bronze_written" in m for m in messages)
        assert any("stage=validated" in m for m in messages)
        assert any("stage=completed" in m for m in messages)
        assert all(r.levelname == "INFO" for r in caplog.records)

    def test_missing_chunks_logs_bronze_written_at_warning(self, scripted_adapter, client, caplog):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=10),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.95, allow_empty=True))

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)

        bronze_record = next(r for r in caplog.records if "stage=bronze_written" in r.message)
        assert bronze_record.levelname == "WARNING"
        assert "missing=b" in bronze_record.message

    def test_missing_ratio_gate_failure_logs_error(self, scripted_adapter, client, caplog):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.1, allow_empty=True))

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(error_records) == 1
        assert "failure_reason=fetch_error" in error_records[0].message

    def test_fatal_logs_error_without_bronze_written_line(self, scripted_adapter, client, caplog):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None),
                FetchResult(key="b", payload=None, error=FetchErrorKind.FATAL, expected_total=None),
            ]
        ]
        config = _config()

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client)

        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "ERROR"
        assert "failure_reason=fetch_error" in caplog.records[0].message

    def test_skip_branch_logs_single_info_line(self, scripted_adapter, client, caplog):
        scripted_adapter.results = [[FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]]
        config = _config()
        pipeline.execute_window(config, WINDOW_START, client=client)
        caplog.clear()

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client)

        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "INFO"
        assert "status=skipped" in caplog.records[0].message

    def test_drop_ratio_gate_failure_logs_error_with_quality_gate(self, scripted_adapter, client, caplog):
        scripted_adapter.results = [[FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]]
        scripted_adapter.rows_by_key = {"a": {"col": None}}
        config = _config(
            quality=Quality(max_drop_ratio=0.1, max_missing_ratio=1.0, allow_empty=True),
            columns={"col": pipeline_make_required_str_spec()},
        )

        with caplog.at_level("INFO", logger="pipeline"):
            pipeline.execute_window(config, WINDOW_START, client=client)

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(error_records) == 1
        assert "failure_reason=quality_gate" in error_records[0].message


class TestEmptyResult:
    def test_zero_rows_with_allow_empty_is_empty_status(self, scripted_adapter, client):
        scripted_adapter.results = [[]]
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=1.0, allow_empty=True))

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.EMPTY
        assert result.artifacts.silver is None

    def test_zero_rows_without_allow_empty_fails_quality_gate(self, scripted_adapter, client):
        scripted_adapter.results = [[]]
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=1.0, allow_empty=False))

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.FAILED
        assert result.failure_reason == FailureReason.QUALITY_GATE


class TestRetryMarkerSync:
    """#11 백필 DAG가 읽을 `_retry_queue` 마커를 pipeline이 실제로 쓰는지 확인한다."""

    def _backfill_config(self, **overrides):
        return _config(backfill=Backfill(enabled=True, max_age="1d"), **overrides)

    def test_marker_written_on_missing_ratio_gate_failure(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = self._backfill_config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.1, allow_empty=True))

        result = pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.FAILED
        assert result.backfill_status == "pending"
        markers = manifest_module.load_retry_markers(config.source_id)
        assert len(markers) == 1
        assert markers[0].missing_parts == ("b",)
        assert markers[0].attempts == 1

    def test_marker_written_on_partial_success_with_missing(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=10),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = self._backfill_config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.95, allow_empty=True))

        result = pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.PARTIAL
        assert result.backfill_status == "pending"
        assert manifest_module.load_retry_markers(config.source_id)[0].missing_parts == ("b",)

    def test_no_marker_when_run_fully_succeeds(self, scripted_adapter, client):
        scripted_adapter.results = [[FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=None)]]
        config = self._backfill_config()

        result = pipeline.execute_window(config, WINDOW_START, client=client)

        assert result.status == RunStatus.SUCCEEDED
        assert result.backfill_status is None
        assert manifest_module.load_retry_markers(config.source_id) == []

    def test_no_marker_when_backfill_disabled(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = _config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.1, allow_empty=True))

        result = pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.FAILED
        assert result.backfill_status is None
        assert manifest_module.load_retry_markers(config.source_id) == []

    def test_marker_update_keeps_first_failed_at_and_bumps_attempts(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=10),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = self._backfill_config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.95, allow_empty=True))
        pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)
        first_marker = manifest_module.load_retry_markers(config.source_id)[0]

        # 다시 --force로 돌려도 같은 조각(b)이 또 실패해 마커가 갱신된다.
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=10),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        pipeline.execute_window(config, WINDOW_START, client=client, force=True, sleep_fn=lambda s: None)
        second_marker = manifest_module.load_retry_markers(config.source_id)[0]

        assert second_marker.first_failed_at == first_marker.first_failed_at
        assert second_marker.attempts == first_marker.attempts + 1

    def test_marker_cleared_once_backfill_fills_all_missing(self, scripted_adapter, client):
        scripted_adapter.results = [
            [
                FetchResult(key="a", payload=_chunk("a"), error=None, expected_total=2),
                FetchResult(key="b", payload=None, error=FetchErrorKind.PERMANENT, expected_total=None),
            ]
        ]
        config = self._backfill_config(quality=Quality(max_drop_ratio=1.0, max_missing_ratio=0.6, allow_empty=True))
        pipeline.execute_window(config, WINDOW_START, client=client, sleep_fn=lambda s: None)
        assert len(manifest_module.load_retry_markers(config.source_id)) == 1

        scripted_adapter.results = [[FetchResult(key="b", payload=_chunk("b"), error=None, expected_total=None)]]
        result = pipeline.execute_window(config, WINDOW_START, client=client, backfill=True)

        assert result.status == RunStatus.SUCCEEDED
        assert result.backfill_status is None
        assert manifest_module.load_retry_markers(config.source_id) == []
