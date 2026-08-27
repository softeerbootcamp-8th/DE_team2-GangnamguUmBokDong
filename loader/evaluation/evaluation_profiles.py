"""재배치 평가 대상과 판정 기준을 profile 데이터로 정의한다."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY
from gold.rebalance_route import (
    FLEET_CAPACITIES,
    ROUTE_ASSUMED_SPEED_KMH,
    ROUTE_SERVICE_MINUTES_PER_STOP,
)

PROFILE_SCHEMA_VERSION = "rebalance-evaluation-profile-v1"
PROFILE_NAMES = ("calibration", "confirmatory", "production")
PRODUCTION_POLICY_NAME = "production_route_v5"
PRODUCTION_TARGET_DATES = tuple(date(2025, month, 17) for month in range(3, 13))
PRODUCTION_CENTER_ID = "hangnyeoul"
PRODUCTION_START_HOUR = 6
PRODUCTION_FLEET_SIZE = len(FLEET_CAPACITIES)
PRODUCTION_EVALUATION_MINUTES = (60, 120, 180)
PRODUCTION_TICK_MINUTES = 5
PRODUCTION_TRUCK_CAPACITY = 20
PRODUCTION_SPEED_KMH = ROUTE_ASSUMED_SPEED_KMH
PRODUCTION_SERVICE_MINUTES_PER_STOP = ROUTE_SERVICE_MINUTES_PER_STOP
PRODUCTION_APPROVAL_DELAY_MINUTES = 0
PRODUCTION_WEATHER_PUBLICATION_LAG_MINUTES = 60
PRODUCTION_POPULATION_LOOKBACK_WEEKS = 4
PRODUCTION_OPERATOR_TIMING_SCENARIOS = (
    "interval_start",
    "interval_midpoint",
    "interval_end",
)
PRODUCTION_MODEL_BUNDLE_SHA256 = (
    "c677e8e192caef85adc7293a26019ea28681199c75bc085ae86702d300bb0afb"
)
PRODUCTION_WEATHER_SHA256 = (
    "fafbedd1157e933fe38c1d0357f1191a48c30ce6ecb5bd2fda5d4c0add9004a5"
)
PRODUCTION_INPUT_PROVENANCE_SHA256_BY_DATE = {
    "2025-03-17": "2dbca9322918c57b2f42571a0942c2c3ba70dc9cf8d5c5c087098df3830b0de7",
    "2025-04-17": "dd6f174ad17e2cf142b24959b67571f60bd68cfbf2b7a5ce134dff36015fa680",
    "2025-05-17": "43e56a86d12bb2a60773d3a363c4e309449666a3bc4e3bc36a3fc9cbd469296d",
    "2025-06-17": "1f19367a4376b169be273797883c54a3bb11c64dd2ee285f9eb6695e95ab8adf",
    "2025-07-17": "7a52f25eb431e3b9b35d5c86b5ee6202a35237e6f482de408ea3e5a4c601e9e4",
    "2025-08-17": "48eb9dbc1f4283b624e9e165c718bc972a4fb01e023c4a1b4bdf5626e159931d",
    "2025-09-17": "d0179704d5f070c99800a034d1b2e3cce5c47ce39ee62401fc73188677153aac",
    "2025-10-17": "059843d6a4c0581f39ae3984d8616f78b5b2f7feb72e0d53d2af105535ce53ac",
    "2025-11-17": "b62c923fa523e3b0460a2e2c292ae23bf1599a17707c74ac706061c7510ed2de",
    "2025-12-17": "d1eda3030747e19cc8f8a5ba950cb67b73b161560e258e991a281107a01b618b",
}
PRODUCTION_STATION_SURFACE_SHA256_BY_DATE = {
    "2025-03-17": "bfda5c8171a1d6aed667f066a4a5f1eb55569dc925289fa84d2eb50a5d24e3bc",
    "2025-04-17": "d430fba59a25949634190d8d894ef46c443dc0dae2a5cbc0ba4b1529731e4d79",
    "2025-05-17": "0bf4a22a71cc7534f96dbf7de1f64ff7e5f2cb346e8b0fe73956a458b3b3f39f",
    "2025-06-17": "115edaefa843df0fcbf2351a5b203f18d548e0ebd077d03c8b2f14b82709a54b",
    "2025-07-17": "115edaefa843df0fcbf2351a5b203f18d548e0ebd077d03c8b2f14b82709a54b",
    "2025-08-17": "a2538b52d41a7231191423139a148df3b9934531e85f81aafcd5c7bf831506c0",
    "2025-09-17": "d6121116c7ec9bba81938a575fb8cb8054cb8413e8b29fa280cdba2dbc9fd9b9",
    "2025-10-17": "d6121116c7ec9bba81938a575fb8cb8054cb8413e8b29fa280cdba2dbc9fd9b9",
    "2025-11-17": "b62358c46b00834e6c9775a781ea4368343cd552e6bf43293f1eed348f18a0d4",
    "2025-12-17": "7f345db04722b305dc0fb4f5ce481c13eb2a9c78454b45d31b17a99c929e863c",
}
PRODUCTION_STATION_COUNT_BY_DATE = {
    "2025-03-17": 308,
    "2025-04-17": 309,
    "2025-05-17": 310,
    "2025-06-17": 311,
    "2025-07-17": 311,
    "2025-08-17": 313,
    "2025-09-17": 314,
    "2025-10-17": 314,
    "2025-11-17": 315,
    "2025-12-17": 315,
}


def production_policy_configuration() -> dict[str, Any]:
    """배포 정책과 혼합 차량 구성을 포함한 exact audit 설정을 반환한다."""
    return {
        **DEFAULT_REBALANCE_POLICY.audit_document(),
        "fleet_capacities": list(FLEET_CAPACITIES),
    }


def production_evidence_scope() -> dict[str, Any]:
    """Production profile에 허용된 데이터·운영·모델 근거 범위를 반환한다."""
    return {
        "target_dates": [target.isoformat() for target in PRODUCTION_TARGET_DATES],
        "result_count": len(PRODUCTION_TARGET_DATES),
        "center_id": PRODUCTION_CENTER_ID,
        "start_hour": PRODUCTION_START_HOUR,
        "evaluation_minutes": list(PRODUCTION_EVALUATION_MINUTES),
        "operation_contract": {
            "tick_minutes": PRODUCTION_TICK_MINUTES,
            "fleet_size": PRODUCTION_FLEET_SIZE,
            "fleet_capacities": list(FLEET_CAPACITIES),
            "truck_capacity": PRODUCTION_TRUCK_CAPACITY,
            "speed_kmh": PRODUCTION_SPEED_KMH,
            "service_minutes_per_stop": PRODUCTION_SERVICE_MINUTES_PER_STOP,
            "approval_delay_minutes": PRODUCTION_APPROVAL_DELAY_MINUTES,
            "weather_publication_lag_minutes": (
                PRODUCTION_WEATHER_PUBLICATION_LAG_MINUTES
            ),
            "population_lookback_weeks": PRODUCTION_POPULATION_LOOKBACK_WEEKS,
            "operator_timing_scenarios": list(
                PRODUCTION_OPERATOR_TIMING_SCENARIOS
            ),
        },
        "model_bundle_sha256": PRODUCTION_MODEL_BUNDLE_SHA256,
        "weather_csv_sha256": PRODUCTION_WEATHER_SHA256,
        "input_provenance_sha256_by_date": dict(
            PRODUCTION_INPUT_PROVENANCE_SHA256_BY_DATE
        ),
        "station_surface_sha256_by_date": dict(
            PRODUCTION_STATION_SURFACE_SHA256_BY_DATE
        ),
        "station_count_by_date": dict(PRODUCTION_STATION_COUNT_BY_DATE),
    }


@dataclass(frozen=True, slots=True, order=True)
class EvaluationCell:
    """평가할 권역·날짜·시작 시각 하나를 표현한다."""

    center_id: str
    target_date: date
    start_hour: int

    def __post_init__(self) -> None:
        """셀 식별값과 held-out 날짜 계약을 검증한다."""
        if not self.center_id.strip() or self.center_id != self.center_id.strip():
            raise ValueError("evaluation cell center_id는 trim된 nonblank여야 합니다.")
        if self.target_date.year != 2025 or self.target_date.day != 17:
            raise ValueError("evaluation cell은 2025년 held-out 17일이어야 합니다.")
        if not 0 <= self.start_hour <= 23:
            raise ValueError("evaluation cell start_hour는 0..23이어야 합니다.")

    @property
    def key(self) -> str:
        """결과와 profile을 연결하는 결정적 셀 식별자를 반환한다."""
        return f"{self.center_id}|{self.target_date.isoformat()}|{self.start_hour:02d}"

    def audit_document(self) -> dict[str, object]:
        """JSON에 기록할 셀 계약을 반환한다."""
        return {
            "center_id": self.center_id,
            "target_date": self.target_date.isoformat(),
            "start_hour": self.start_hour,
        }


@dataclass(frozen=True, slots=True)
class EvaluationGate:
    """profile별로 달라지는 최소 판정 조건만 표현한다."""

    require_aggregate_180_unfulfilled_strict_improvement: bool
    strict_empty_improvement_horizons: tuple[int, ...] = ()
    aggregate_180_empty_reduction_min_pct: float | None = None
    improved_180_cells_min: int | None = None
    max_pickup_dispatch_lag_minutes: float | None = None
    require_planned_bikes_equal_moved_bikes: bool = False
    require_all_routes_finished_by_cutoff: bool = False

    def __post_init__(self) -> None:
        """판정 임계값이 평가 의미를 훼손하지 않는지 검증한다."""
        if any(
            value not in PRODUCTION_EVALUATION_MINUTES
            for value in self.strict_empty_improvement_horizons
        ):
            raise ValueError("strict empty horizon은 60·120·180분 중 하나여야 합니다.")
        if (
            self.aggregate_180_empty_reduction_min_pct is not None
            and self.aggregate_180_empty_reduction_min_pct < 0.0
        ):
            raise ValueError("180분 품절 감소율 하한은 0 이상이어야 합니다.")
        if self.improved_180_cells_min is not None and self.improved_180_cells_min < 0:
            raise ValueError("개선 셀 하한은 0 이상이어야 합니다.")
        if (
            self.max_pickup_dispatch_lag_minutes is not None
            and self.max_pickup_dispatch_lag_minutes < 0.0
        ):
            raise ValueError("pickup 지연 상한은 0 이상이어야 합니다.")

    def audit_document(self) -> dict[str, object]:
        """JSON에 기록할 판정 설정을 반환한다."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EvaluationProfile:
    """하나의 공통 실행기가 소비하는 평가 데이터와 gate를 묶는다."""

    name: str
    purpose: str
    cells: tuple[EvaluationCell, ...]
    gate: EvaluationGate
    release_gate: bool
    expected_evidence_scope: dict[str, Any] | None = None
    evaluation_minutes: tuple[int, ...] = PRODUCTION_EVALUATION_MINUTES
    fleet_size: int = PRODUCTION_FLEET_SIZE
    policy_name: str = PRODUCTION_POLICY_NAME
    model_bundle_sha256: str = PRODUCTION_MODEL_BUNDLE_SHA256

    def __post_init__(self) -> None:
        """profile 이름·셀 집합·공통 운영 계약을 검증한다."""
        if self.name not in PROFILE_NAMES:
            raise ValueError(f"알 수 없는 evaluation profile입니다: {self.name}")
        if not self.cells or len({cell.key for cell in self.cells}) != len(self.cells):
            raise ValueError("evaluation profile cells는 중복 없는 nonempty tuple이어야 합니다.")
        if self.evaluation_minutes != PRODUCTION_EVALUATION_MINUTES:
            raise ValueError("evaluation profile은 60·120·180분을 모두 사용해야 합니다.")
        if self.fleet_size != PRODUCTION_FLEET_SIZE:
            raise ValueError("evaluation profile fleet size는 production 계약과 같아야 합니다.")
        if (
            self.gate.improved_180_cells_min is not None
            and self.gate.improved_180_cells_min > len(self.cells)
        ):
            raise ValueError("개선 셀 하한이 profile 셀 수보다 큽니다.")

    def audit_document(self) -> dict[str, object]:
        """중복 구현 없이 결과에 넣을 profile 계약을 반환한다."""
        return {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "name": self.name,
            "purpose": self.purpose,
            "release_gate": self.release_gate,
            "cells": [cell.audit_document() for cell in self.cells],
            "evaluation_minutes": list(self.evaluation_minutes),
            "fleet_size": self.fleet_size,
            "policy_name": self.policy_name,
            "policy_configuration": production_policy_configuration(),
            "model_bundle_sha256": self.model_bundle_sha256,
            "gate": self.gate.audit_document(),
        }


def _cells(rows: tuple[tuple[str, str, int], ...]) -> tuple[EvaluationCell, ...]:
    """간결한 상수 행을 검증된 평가 셀로 변환한다."""
    return tuple(
        EvaluationCell(center_id, date.fromisoformat(target_date), start_hour)
        for center_id, target_date, start_hour in rows
    )


CALIBRATION_PROFILE = EvaluationProfile(
    name="calibration",
    purpose="후보 선택에 사용한 12개 셀의 안전성과 지표를 재현한다.",
    cells=_cells(
        (
            ("gaehwa", "2025-04-17", 6),
            ("gaehwa", "2025-07-17", 12),
            ("gaehwa", "2025-08-17", 20),
            ("gaehwa", "2025-12-17", 17),
            ("isu", "2025-04-17", 17),
            ("isu", "2025-07-17", 6),
            ("isu", "2025-08-17", 20),
            ("isu", "2025-12-17", 12),
            ("yeongnam", "2025-04-17", 12),
            ("yeongnam", "2025-07-17", 17),
            ("yeongnam", "2025-08-17", 20),
            ("yeongnam", "2025-12-17", 6),
        )
    ),
    gate=EvaluationGate(
        require_aggregate_180_unfulfilled_strict_improvement=False,
        require_planned_bikes_equal_moved_bikes=True,
        require_all_routes_finished_by_cutoff=True,
    ),
    release_gate=False,
)


CONFIRMATORY_PROFILE = EvaluationProfile(
    name="confirmatory",
    purpose="calibration에서 열람하지 않은 4개 권역 12개 셀로 후보를 독립 확인한다.",
    cells=_cells(
        (
            ("sangam", "2025-03-17", 7),
            ("sangam", "2025-06-17", 13),
            ("sangam", "2025-10-17", 18),
            ("jungnang", "2025-05-17", 7),
            ("jungnang", "2025-09-17", 13),
            ("jungnang", "2025-11-17", 18),
            ("cheonwang", "2025-03-17", 13),
            ("cheonwang", "2025-06-17", 18),
            ("cheonwang", "2025-10-17", 7),
            ("cheonho", "2025-05-17", 13),
            ("cheonho", "2025-09-17", 18),
            ("cheonho", "2025-11-17", 7),
        )
    ),
    gate=EvaluationGate(
        require_aggregate_180_unfulfilled_strict_improvement=True,
        aggregate_180_empty_reduction_min_pct=5.0,
        improved_180_cells_min=8,
        require_planned_bikes_equal_moved_bikes=True,
        require_all_routes_finished_by_cutoff=True,
    ),
    release_gate=True,
)


PRODUCTION_PROFILE = EvaluationProfile(
    name="production",
    purpose="고정 배포 권역의 2025년 held-out 날짜 전체에서 release 계약을 확인한다.",
    cells=tuple(
        EvaluationCell(PRODUCTION_CENTER_ID, target_date, PRODUCTION_START_HOUR)
        for target_date in PRODUCTION_TARGET_DATES
    ),
    gate=EvaluationGate(
        require_aggregate_180_unfulfilled_strict_improvement=True,
        strict_empty_improvement_horizons=PRODUCTION_EVALUATION_MINUTES,
    ),
    release_gate=True,
    expected_evidence_scope=production_evidence_scope(),
)


_PROFILES = {
    profile.name: profile
    for profile in (CALIBRATION_PROFILE, CONFIRMATORY_PROFILE, PRODUCTION_PROFILE)
}


def get_evaluation_profile(name: str) -> EvaluationProfile:
    """이름에 해당하는 immutable 평가 profile을 반환한다."""
    try:
        return _PROFILES[name]
    except KeyError as exc:
        raise ValueError(f"알 수 없는 evaluation profile입니다: {name}") from exc
