"""LightGBM 장시간 학습의 phase·round checkpoint를 S3 archive에 보존한다."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
from core import s3 as s3_io
from ml_core import model_io

CHECKPOINT_SCHEMA_VERSION = "training-checkpoint-v1"


class CheckpointContractMismatchError(RuntimeError):
    """현재 학습 계약과 저장된 checkpoint 계약이 다를 때 발생한다."""


@dataclass(frozen=True, slots=True)
class ResumeState:
    """한 phase에서 재개할 Booster와 완료 상태를 표현한다."""

    booster: lgb.Booster | None
    completed_iterations: int
    phase_completed: bool
    early_stopping_state: dict[str, Any] | None


class ResumeAwareEarlyStopping:
    """checkpoint 이전의 최고 점수와 patience를 이어받는 early stopping callback이다.

    현재 학습 경로는 validation Dataset 하나와 objective metric 하나만 전달한다.
    여러 metric을 조용히 잘못 처리하지 않도록 그 계약이 달라지면 즉시 실패한다.
    """

    order = 24
    before_iteration = False

    def __init__(self, stopping_rounds: int, initial_state: dict[str, Any] | None = None) -> None:
        """patience와 선택적인 이전 상태로 callback을 초기화한다."""
        if stopping_rounds <= 0:
            raise ValueError(f"stopping_rounds는 양수여야 합니다: {stopping_rounds}")
        self.stopping_rounds = stopping_rounds
        self.initial_state = initial_state
        self.best_iteration: int | None = None
        self.best_score: float | None = None
        self.best_score_list: list[Any] = []
        self.dataset_name: str | None = None
        self.metric_name: str | None = None
        self.higher_is_better: bool | None = None

    @staticmethod
    def _with_metric_value(item: Any, value: float) -> Any:
        """LightGBM evaluation tuple의 metric 값만 교체한다."""
        if hasattr(item, "_replace"):
            return item._replace(metric_value=value)
        values = list(item)
        values[2] = value
        return type(item)(values) if type(item) is tuple else type(item)(*values)

    def _initialize(self, item: Any, iteration: int) -> None:
        """첫 evaluation 또는 저장된 상태에서 최고 점수를 초기화한다."""
        self.dataset_name = str(item[0])
        self.metric_name = str(item[1])
        self.higher_is_better = bool(item[3])
        if self.initial_state is None:
            self.best_iteration = iteration
            self.best_score = float(item[2])
            self.best_score_list = [item]
            return

        expected = (
            self.initial_state.get("dataset_name"),
            self.initial_state.get("metric_name"),
            bool(self.initial_state.get("higher_is_better")),
        )
        observed = (self.dataset_name, self.metric_name, self.higher_is_better)
        if expected != observed:
            raise CheckpointContractMismatchError(
                f"early-stopping metric 계약이 다릅니다: expected={expected}, observed={observed}"
            )
        self.best_iteration = int(self.initial_state["best_iteration"])
        self.best_score = float(self.initial_state["best_score"])
        self.best_score_list = [self._with_metric_value(item, self.best_score)]

    def snapshot(self) -> dict[str, Any] | None:
        """다음 checkpoint에 넣을 JSON 직렬화 가능한 상태를 반환한다."""
        if self.best_iteration is None or self.best_score is None:
            return None
        return {
            "dataset_name": self.dataset_name,
            "metric_name": self.metric_name,
            "higher_is_better": self.higher_is_better,
            "best_iteration": self.best_iteration,
            "best_score": self.best_score,
        }

    def __call__(self, env: Any) -> None:
        """현재 validation 점수를 반영하고 patience 또는 마지막 round에서 종료한다."""
        evaluations = env.evaluation_result_list or []
        if len(evaluations) != 1:
            raise ValueError(
                "resume-aware early stopping은 validation metric 하나만 지원합니다: "
                f"count={len(evaluations)}"
            )
        item = evaluations[0]
        if self.best_iteration is None:
            self._initialize(item, env.iteration)
        else:
            current_score = float(item[2])
            assert self.best_score is not None
            improved = current_score > self.best_score if self.higher_is_better else current_score < self.best_score
            if improved:
                self.best_iteration = env.iteration
                self.best_score = current_score
                self.best_score_list = [item]

        assert self.best_iteration is not None
        if env.iteration - self.best_iteration >= self.stopping_rounds:
            raise lgb.callback.EarlyStopException(self.best_iteration, self.best_score_list)
        if env.iteration == env.end_iteration - 1:
            raise lgb.callback.EarlyStopException(self.best_iteration, self.best_score_list)


def canonical_sha256(payload: dict[str, Any]) -> str:
    """JSON 객체를 canonical 직렬화한 SHA-256을 반환한다."""
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def runtime_code_fingerprint(paths: list[str | Path]) -> str:
    """학습 핵심 모듈들의 경로와 bytes로 재개용 코드 fingerprint를 만든다."""
    digest = hashlib.sha256()
    for raw_path in sorted(Path(path).resolve() for path in paths):
        digest.update(raw_path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(raw_path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


class TrainingCheckpointManager:
    """한 모델 phase의 checkpoint와 완료 상태를 관리한다.

    Booster 객체를 먼저 업로드하고 작은 state JSON을 마지막에 갱신한다. 업로드 도중
    프로세스가 종료되면 이전 state가 계속 마지막 정상 checkpoint를 가리키므로,
    절반만 기록된 Booster를 재개 대상으로 선택하지 않는다.
    """

    def __init__(
        self,
        models_prefix: str,
        model_name: str,
        phase_name: str,
        contract: dict[str, Any],
        interval_rounds: int,
        resume_enabled: bool,
        compatible_code_fingerprints: frozenset[str] = frozenset(),
    ) -> None:
        """checkpoint 경로와 현재 계약 fingerprint를 초기화한다."""
        self.models_prefix = models_prefix.rstrip("/")
        self.model_name = model_name
        self.phase_name = phase_name
        self.contract = contract
        self.contract_sha256 = canonical_sha256(contract)
        self.interval_rounds = interval_rounds
        self.resume_enabled = resume_enabled
        self.compatible_code_fingerprints = compatible_code_fingerprints
        self.root = f"{self.models_prefix}/_checkpoints/{model_name}/{phase_name}"
        self.state_key = f"{self.root}/state.json"
        self.latest_iteration = 0

    def _checkpoint_key(self, iteration: int) -> str:
        """round 번호에 대응하는 immutable Booster key를 반환한다."""
        return f"{self.root}/round-{iteration:06d}.txt"

    def _read_state(self) -> dict[str, Any] | None:
        """현재 phase state를 읽고 schema와 계약을 검증한다."""
        state = s3_io.read_json(self.state_key)
        if state is None:
            return None
        if state.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            raise CheckpointContractMismatchError(
                f"지원하지 않는 checkpoint schema입니다: key={self.state_key}, "
                f"schema={state.get('schema_version')!r}"
            )
        observed = state.get("contract_sha256")
        if observed != self.contract_sha256 and not self._is_explicitly_compatible(state):
            observed_contract = state.get("contract")
            observed_code_fingerprint = (
                observed_contract.get("code_fingerprint")
                if isinstance(observed_contract, dict)
                else None
            )
            raise CheckpointContractMismatchError(
                "checkpoint 계약이 현재 학습과 다릅니다: "
                f"key={self.state_key}, expected={self.contract_sha256}, observed={observed}, "
                f"observed_code_fingerprint={observed_code_fingerprint!r}, "
                f"compatible_code_fingerprints={sorted(self.compatible_code_fingerprints)!r}"
            )
        return state

    def _is_explicitly_compatible(self, state: dict[str, Any]) -> bool:
        """명시 허용한 이전 코드이며 나머지 계약이 같을 때만 재개를 허용한다."""
        observed_contract = state.get("contract")
        if not isinstance(observed_contract, dict):
            return False
        observed_fingerprint = observed_contract.get("code_fingerprint")
        if observed_fingerprint not in self.compatible_code_fingerprints:
            return False
        expected_without_code = {
            key: value for key, value in self.contract.items() if key != "code_fingerprint"
        }
        observed_without_code = {
            key: value for key, value in observed_contract.items() if key != "code_fingerprint"
        }
        if canonical_sha256(observed_without_code) != canonical_sha256(expected_without_code):
            return False
        print(
            "checkpoint의 이전 코드 fingerprint를 명시적 호환 목록으로 재사용합니다: "
            f"key={self.state_key}, fingerprint={observed_fingerprint}",
            flush=True,
        )
        return True

    def load(self, final_model_key: str) -> ResumeState:
        """마지막 정상 checkpoint 또는 완료된 최종 Booster를 로드한다."""
        if not self.resume_enabled:
            return ResumeState(None, 0, False, None)
        state = self._read_state()
        if state is None:
            return ResumeState(None, 0, False, None)

        if state.get("status") == "completed":
            observed_final_key = state.get("final_model_key")
            if observed_final_key != final_model_key or not s3_io.object_exists(final_model_key):
                raise FileNotFoundError(
                    "완료 checkpoint가 가리키는 최종 모델이 없거나 경로가 다릅니다: "
                    f"state={observed_final_key!r}, expected={final_model_key!r}"
                )
            booster = model_io.download_and_load_booster(final_model_key)
            iteration = int(state.get("completed_iterations", booster.current_iteration()))
            best_iteration = int(state.get("best_iteration", iteration))
            booster.best_iteration = best_iteration if best_iteration > 0 else iteration
            self.latest_iteration = iteration
            return ResumeState(booster, iteration, True, state.get("early_stopping_state"))

        checkpoint_key = state.get("checkpoint_key")
        iteration = int(state.get("completed_iterations", 0))
        if not checkpoint_key or iteration <= 0:
            return ResumeState(None, 0, False, state.get("early_stopping_state"))
        if not s3_io.object_exists(checkpoint_key):
            raise FileNotFoundError(f"checkpoint state가 가리키는 Booster가 없습니다: {checkpoint_key}")
        booster = model_io.download_and_load_booster(checkpoint_key)
        observed_iteration = booster.current_iteration()
        if observed_iteration != iteration:
            raise RuntimeError(
                "checkpoint Booster round와 state가 다릅니다: "
                f"key={checkpoint_key}, booster={observed_iteration}, state={iteration}"
            )
        self.latest_iteration = iteration
        return ResumeState(booster, iteration, False, state.get("early_stopping_state"))

    def _write_state(self, **updates: Any) -> None:
        """공통 계약 정보와 phase 상태를 state JSON에 기록한다."""
        payload = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "contract_sha256": self.contract_sha256,
            "contract": self.contract,
            "model_name": self.model_name,
            "phase_name": self.phase_name,
            **updates,
        }
        s3_io.write_json(self.state_key, payload)

    def save(
        self,
        booster: lgb.Booster,
        evaluation_result_list: list[Any] | None = None,
        early_stopping_state: dict[str, Any] | None = None,
    ) -> None:
        """현재 Booster를 업로드한 뒤 state 포인터를 해당 round로 이동한다."""
        iteration = booster.current_iteration()
        if iteration <= 0:
            return
        checkpoint_key = self._checkpoint_key(iteration)
        model_io.stage_and_upload_booster(booster, checkpoint_key, log_to_mlflow=False)
        evaluations = []
        for item in evaluation_result_list or []:
            evaluations.append(
                {
                    "dataset": item[0],
                    "metric": item[1],
                    "value": float(item[2]),
                    "higher_is_better": bool(item[3]),
                }
            )
        self._write_state(
            status="in_progress",
            checkpoint_key=checkpoint_key,
            completed_iterations=iteration,
            evaluations=evaluations,
            early_stopping_state=early_stopping_state,
        )
        self.latest_iteration = iteration

    def callback(
        self,
        early_stopping_state_provider: Callable[[], dict[str, Any] | None] | None = None,
    ) -> Callable[[Any], None]:
        """설정된 round 간격마다 checkpoint를 저장하는 LightGBM callback을 만든다."""

        def _callback(env: Any) -> None:
            iteration = env.model.current_iteration()
            if self.interval_rounds <= 0 or iteration % self.interval_rounds != 0:
                return
            early_stopping_state = (
                early_stopping_state_provider()
                if early_stopping_state_provider is not None
                else None
            )
            self.save(env.model, env.evaluation_result_list, early_stopping_state)

        _callback.order = 25
        _callback.before_iteration = False
        return _callback

    def mark_completed(self, booster: lgb.Booster, final_model_key: str) -> None:
        """최종 모델 업로드 후 phase 완료 상태를 기록한다."""
        iteration = booster.current_iteration()
        best_iteration = int(booster.best_iteration)
        if best_iteration <= 0:
            best_iteration = iteration
        self._write_state(
            status="completed",
            final_model_key=final_model_key,
            completed_iterations=iteration,
            best_iteration=best_iteration,
        )
        self.latest_iteration = iteration

    def mark_failed(self, reason: str) -> None:
        """잡힌 예외의 원인과 마지막 정상 round를 보존한다."""
        state = self._read_state() or {}
        self._write_state(
            status="failed",
            checkpoint_key=state.get("checkpoint_key"),
            completed_iterations=int(state.get("completed_iterations", self.latest_iteration)),
            early_stopping_state=state.get("early_stopping_state"),
            failure_reason=reason,
        )
