"""manifest.py의 상태 어휘와 Manifest·RetryMarker 모델을 검증한다."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from manifest import (
    Artifacts,
    ColumnIssueCount,
    Counts,
    FailureReason,
    Manifest,
    Missing,
    RetryMarker,
    RunStatus,
    Stage,
    StageField,
    clear_retry_marker,
    load,
    load_retry_markers,
    save,
    save_retry_marker,
)
from pydantic import BaseModel, ValidationError

KST = ZoneInfo("Asia/Seoul")
WINDOW_START = datetime(2026, 8, 12, 14, 10, tzinfo=KST)
WINDOW_END = datetime(2026, 8, 12, 14, 15, tzinfo=KST)


def _minimal_manifest(**overrides) -> Manifest:
    base = {
        "source_id": "test_source",
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
        "status": RunStatus.PARTIAL,
        "stage": Stage.COMPLETED,
        "started_at": WINDOW_START,
        "config_version": "sha256:abc",
    }
    base.update(overrides)
    return Manifest.model_validate(base)


class TestStage:
    def test_ordering(self):
        assert Stage.BRONZE_WRITTEN < Stage.VALIDATED < Stage.COMPLETED
        assert Stage.COMPLETED >= Stage.BRONZE_WRITTEN

    def test_serializes_to_lowercase_name(self):
        class Holder(BaseModel):
            stage: StageField

        dumped = Holder(stage=Stage.BRONZE_WRITTEN).model_dump(mode="json")
        assert dumped == {"stage": "bronze_written"}

    def test_deserializes_from_lowercase_name(self):
        class Holder(BaseModel):
            stage: StageField

        holder = Holder.model_validate({"stage": "completed"})
        assert holder.stage is Stage.COMPLETED

    def test_accepts_stage_instance_directly(self):
        class Holder(BaseModel):
            stage: StageField

        holder = Holder(stage=Stage.VALIDATED)
        assert holder.stage is Stage.VALIDATED


class TestRunStatus:
    def test_values_match_disk_contract(self):
        assert RunStatus.RUNNING.value == "running"
        assert RunStatus.SUCCEEDED.value == "succeeded"
        assert RunStatus.PARTIAL.value == "partial"
        assert RunStatus.FAILED.value == "failed"
        assert RunStatus.EMPTY.value == "empty"
        assert RunStatus.SKIPPED.value == "skipped"


class TestFailureReason:
    def test_values_match_disk_contract(self):
        assert FailureReason.FETCH_ERROR.value == "fetch_error"
        assert FailureReason.STORAGE_ERROR.value == "storage_error"
        assert FailureReason.QUALITY_GATE.value == "quality_gate"
        assert FailureReason.CONFIG_ERROR.value == "config_error"


class TestManifestModel:
    def test_minimal_construction(self):
        m = _minimal_manifest()
        assert m.attempt == 1
        assert m.revision == 0
        assert m.artifacts == Artifacts()
        assert m.counts == Counts()
        assert m.missing == Missing()

    def test_forbids_extra_key(self):
        with pytest.raises(ValidationError):
            _minimal_manifest(unknown_key="x")

    def test_full_example_from_plan_doc_validates(self):
        data = {
            "source_id": "bike_station_realtime",
            "window_start": "2026-08-12T14:10:00Z",
            "window_end": "2026-08-12T14:15:00Z",
            "status": "partial",
            "stage": "completed",
            "failure_reason": None,
            "attempt": 2,
            "revision": 1,
            "started_at": "2026-08-12T14:15:00Z",
            "ended_at": "2026-08-12T14:15:04Z",
            "duration_ms": 4310,
            "artifacts": {
                "bronze": {
                    "prefix": "s3://.../bronze/bike_station_realtime/dt=2026-08-12/hh=14/1410/",
                    "parts": ["page-00001-01000", "page-01001-02000", "page-02001-02765"],
                },
                "silver": "s3://.../silver/bike_station_realtime/dt=2026-08-12/hh=14/1410.parquet",
                "quarantine": "s3://.../quarantine/bike_station_realtime/dt=2026-08-12/hh=14/1410.jsonl",
            },
            "counts": {"expected": 2765, "fetched": 2765, "kept": 2740, "repaired": 31, "dropped": 25},
            "missing": {"parts": [], "rows": 0, "basis": "rows"},
            "drop_ratio": 0.009,
            "completeness": 0.991,
            "backfill_status": None,
            "column_issues": {
                "stationId": {"missing": 25, "outlier": 0, "type_error": 0},
                "parkingBikeTotCnt": {"missing": 3, "outlier": 28, "type_error": 0},
            },
            "policy_actions": {"drop_row": 25, "clip_to_range": 28, "set_null": 3},
            "config_version": "sha256:a3f9",
        }

        m = Manifest.model_validate(data)

        assert m.artifacts.bronze.parts == (
            "page-00001-01000",
            "page-01001-02000",
            "page-02001-02765",
        )
        assert m.column_issues["stationId"] == ColumnIssueCount(missing=25, outlier=0, type_error=0)
        assert m.stage is Stage.COMPLETED


class TestLoadSave:
    def test_save_then_load_round_trip(self):
        m = _minimal_manifest()

        save(m)
        loaded = load(m.source_id, m.window_start)

        assert loaded == m

    def test_load_missing_returns_none(self):
        assert load("never_saved", WINDOW_START) is None


class TestRetryMarker:
    def test_save_then_load_round_trip(self):
        marker = RetryMarker(
            source_id="test_source",
            window_start=WINDOW_START,
            missing_parts=["page-002"],
            first_failed_at=WINDOW_START,
            expires_at=WINDOW_END,
        )

        save_retry_marker(marker)
        loaded = load_retry_markers("test_source")

        assert loaded == [marker]

    def test_default_attempts_is_one(self):
        marker = RetryMarker(
            source_id="test_source",
            window_start=WINDOW_START,
            missing_parts=["page-002"],
            first_failed_at=WINDOW_START,
            expires_at=WINDOW_END,
        )
        assert marker.attempts == 1

    def test_clear_removes_marker(self):
        marker = RetryMarker(
            source_id="test_source",
            window_start=WINDOW_START,
            missing_parts=["page-002"],
            first_failed_at=WINDOW_START,
            expires_at=WINDOW_END,
        )
        save_retry_marker(marker)

        clear_retry_marker("test_source", WINDOW_START)

        assert load_retry_markers("test_source") == []

    def test_load_with_no_markers_returns_empty_list(self):
        assert load_retry_markers("no_markers_here") == []
