"""manifest.py의 상태 어휘와 Manifest·RetryMarker 모델을 검증한다."""

from pydantic import BaseModel

from manifest import FailureReason, RunStatus, Stage, StageField


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
