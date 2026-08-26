"""재배치 평가 대상과 판정 기준을 profile 데이터로 정의한다."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY
from gold.rebalance_route import (
    MAX_STOPS_PER_ROUTE,
    PICKUP_DISPATCH_ASSUMED_SPEED_KMH,
    PICKUP_DISPATCH_SERVICE_MINUTES_PER_STOP,
)

PROFILE_SCHEMA_VERSION = "rebalance-evaluation-profile-v1"
PROFILE_NAMES = ("calibration", "confirmatory", "production")
PRODUCTION_POLICY_NAME = "production_route_v4"
PRODUCTION_TARGET_DATES = tuple(date(2025, month, 17) for month in range(3, 13))
PRODUCTION_CENTER_ID = "hangnyeoul"
PRODUCTION_START_HOUR = 6
PRODUCTION_FLEET_SIZE = 3
PRODUCTION_EVALUATION_MINUTES = (60, 120, 180)
PRODUCTION_TICK_MINUTES = 5
PRODUCTION_TRUCK_CAPACITY = 20
PRODUCTION_SPEED_KMH = PICKUP_DISPATCH_ASSUMED_SPEED_KMH
PRODUCTION_SERVICE_MINUTES_PER_STOP = PICKUP_DISPATCH_SERVICE_MINUTES_PER_STOP
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
    "2025-03-17": "d32e89245e11728cecef3333644492fae32637c6af8522cd9c8ca1ec433d6ba4",
    "2025-04-17": "5249d04d9a5ac616709494111765aa4c486fa73f05c8f64988cb16006c99d4ab",
    "2025-05-17": "dff232ccd391edfe4490eed990e98144666dcac657592aeb2bcf2d1a14839ae0",
    "2025-06-17": "19b2d0fd4a763063f5fa086bdc3d28a0bd905bcc1dcf2cef38b26646a37730f6",
    "2025-07-17": "d9f5a37351628ffb9fa723211cb3cbcec02b18eea04ce97cdadf57f620b29868",
    "2025-08-17": "92732889294d82ae3862e27a417acd9a6012fdfb5eb67062359ec8dfdb7f4a23",
    "2025-09-17": "8ec6e100e7fb3e8248dde909f3195837bb613a9cdeefde9e71310898ca541343",
    "2025-10-17": "51e781d367a26844dc6008b5e593eacde18e2ffc06f553cb96979c8a6b502c74",
    "2025-11-17": "74864cccee867ca3cad9c87f91af4711a9b076ad4b072db3d1af13377fa87463",
    "2025-12-17": "a52cc0ab62fab047892d1c8e48537c08793d1290eb6fb04f64712f3c82f8cbf5",
}
PRODUCTION_STATION_SURFACE_SHA256_BY_DATE = {
    "2025-03-17": "17c2c5b98728d932bb6460db39c6ca2189629d773a3d86c7eb5bc918282a74e4",
    "2025-04-17": "17c2c5b98728d932bb6460db39c6ca2189629d773a3d86c7eb5bc918282a74e4",
    "2025-05-17": "394df2124f4395f7bc772ac82895fb3ecc6f4e81865fa0a8f659f29d32485f19",
    "2025-06-17": "fb34af1b0ea10c67030fced70af9db5016649548562596591be8b8873213e843",
    "2025-07-17": "fb34af1b0ea10c67030fced70af9db5016649548562596591be8b8873213e843",
    "2025-08-17": "30e75ad4bb0d334609c78ca303a88a058cc3b8446e9271adf45ae7d8a9ae6ef2",
    "2025-09-17": "8f5b8ef0cb6ffb8f85f48ab035c56f8ed2715506ccc7f237bbc07378790410a4",
    "2025-10-17": "8f5b8ef0cb6ffb8f85f48ab035c56f8ed2715506ccc7f237bbc07378790410a4",
    "2025-11-17": "88a6edbf7222a241af6eb324fffffa72f91ab554a28bfc962f62096a97f0118b",
    "2025-12-17": "3d8a49f5cb8ec13199047f4007e21b51bd70698c0d761d05461861e193057932",
}
PRODUCTION_STATION_COUNT_BY_DATE = {
    "2025-03-17": 262,
    "2025-04-17": 262,
    "2025-05-17": 263,
    "2025-06-17": 264,
    "2025-07-17": 264,
    "2025-08-17": 265,
    "2025-09-17": 266,
    "2025-10-17": 266,
    "2025-11-17": 267,
    "2025-12-17": 267,
}


def production_policy_configuration() -> dict[str, Any]:
    """배포 정책과 경로 작업 상한을 포함한 exact audit 설정을 반환한다."""
    return {
        **DEFAULT_REBALANCE_POLICY.audit_document(),
        "max_stops_per_route": MAX_STOPS_PER_ROUTE,
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
        max_pickup_dispatch_lag_minutes=30.0,
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
        max_pickup_dispatch_lag_minutes=30.0,
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
