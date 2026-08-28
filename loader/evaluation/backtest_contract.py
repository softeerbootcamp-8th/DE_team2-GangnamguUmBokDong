"""재배치 정책 백테스트의 사전 평가 계약과 주장 한계를 정의한다."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from gold.rebalance_route import (
    FLEET_CAPACITIES,
    ROUTE_ASSUMED_SPEED_KMH,
    ROUTE_SERVICE_MINUTES_PER_STOP,
)

EVIDENCE_GRADE = "retrospective_heldout_replay"
BACKTEST_CONTRACT_VERSION = "point-in-time-policy-backtest-v3"
PRIMARY_METRIC = "observed_demand_fulfillment_rate"
SUPPORTED_CLAIM = (
    "고정된 관측 수요와 명시한 모의 운영 자원 아래에서 우리 정책을 "
    "재배치 없음 및 정책 변형과 비교한다."
)
FORBIDDEN_CLAIM = (
    "운영자 작업 로그와 실패 수요 로그 없이 실제 기존 운영보다 시민의 전체 잠재 "
    "수요를 더 많이 충족한다고 인과적으로 주장할 수 없다."
)


@dataclass(frozen=True, slots=True)
class EvaluationContract:
    """실험 전에 고정해야 하는 정보시점·시간·자원·비교 조건을 표현한다."""

    target_date: date
    start_hour: int
    evaluation_minutes: int = 120
    tick_minutes: int = 5
    fleet_size: int = len(FLEET_CAPACITIES)
    truck_capacity: int = 20
    speed_kmh: float = ROUTE_ASSUMED_SPEED_KMH
    service_minutes_per_stop: float = ROUTE_SERVICE_MINUTES_PER_STOP
    approval_delay_minutes: int = 0
    weather_publication_lag_minutes: int = 60
    population_lookback_weeks: int = 4
    operator_timing_scenarios: tuple[str, ...] = (
        "interval_start",
        "interval_midpoint",
        "interval_end",
    )

    def __post_init__(self) -> None:
        """평가 계약이 재현 가능하고 미래 정보에 의존하지 않는지 검증한다."""
        if not 0 <= self.start_hour <= 23:
            raise ValueError("start_hour는 0..23이어야 합니다.")
        if self.evaluation_minutes not in {60, 120, 180}:
            raise ValueError("평가 시간은 사전 민감도 집합 60/120/180분 중 하나여야 합니다.")
        if self.tick_minutes != 5:
            raise ValueError("운영 DAG와 동일하게 tick_minutes는 5분이어야 합니다.")
        if self.evaluation_minutes % self.tick_minutes:
            raise ValueError("평가 시간은 tick 간격으로 나누어떨어져야 합니다.")
        if type(self.fleet_size) is not int or self.fleet_size <= 0:
            raise ValueError("fleet_size는 양의 정수여야 합니다.")
        if type(self.truck_capacity) is not int or self.truck_capacity != 20:
            raise ValueError("현행 운영 계약과 동일하게 truck_capacity는 20이어야 합니다.")
        if self.speed_kmh <= 0 or self.service_minutes_per_stop < 0:
            raise ValueError("속도는 양수이고 stop 작업시간은 비음수여야 합니다.")
        if self.approval_delay_minutes < 0:
            raise ValueError("승인 지연은 비음수여야 합니다.")
        if self.weather_publication_lag_minutes < 0:
            raise ValueError("날씨 게시 지연은 비음수여야 합니다.")
        if self.population_lookback_weeks != 4:
            raise ValueError("현행 nowcaster와 같은 1~4주 입력을 사용해야 합니다.")
        expected = ("interval_start", "interval_midpoint", "interval_end")
        if self.operator_timing_scenarios != expected:
            raise ValueError("기존 운영 시각 불확실성은 start/midpoint/end를 모두 계산해야 합니다.")

    def audit_document(self) -> dict[str, Any]:
        """결과와 함께 저장할 사전 계약 및 허용·금지 주장을 반환한다."""
        return {
            "evidence_grade": EVIDENCE_GRADE,
            "primary_metric": PRIMARY_METRIC,
            "supported_claim": SUPPORTED_CLAIM,
            "forbidden_claim": FORBIDDEN_CLAIM,
            "contract": {
                **asdict(self),
                "target_date": self.target_date.isoformat(),
            },
            "information_policy": {
                "citizen_demand": (
                    "동일한 실제 대여 요청을 시간순으로 재생하되 실패한 대여의 반납은 제거"
                ),
                "rental_lag": (
                    "anchor 기준 [T-100분,T-40분) 대여 중 T까지 반납되어 관측 가능한 성공 건만 사용"
                ),
                "return_lag": "anchor 기준 [T-60분,T) 성공 반납만 사용",
                "weather": "anchor에서 publication lag 이전의 최신 관측만 전 horizon에 사용",
                "population": "대상일보다 1~4주 전 자료만으로 운영 nowcaster 방식 재구성",
                "stock": "평가 시작 실측 재고 이후에는 각 정책의 사건 재생 상태만 사용",
                "model": (
                    "고정 aws-temporary-model-2025-d20-h12-r20 bundle; "
                    "2025년 17일은 학습 제외 test split"
                ),
            },
            "operation_policy": {
                "decision": "평가 시작부터 종료 직전까지 5분마다 모델·urgency·route 재계산",
                "dispatch": "승인 지연 뒤 idle truck에 우선순위 route 자동 배차",
                "active_work": "진행 중 작업은 재계획 coverage에 포함하고 완료 뒤 센터 복귀까지 truck 점유",
                "budget": "정책 변형은 같은 fleet·capacity·속도·stop 시간·평가 시간을 사용",
                "cutoff": "평가 종료 전 센터 복귀가 가능한 route만 배차하여 작업 블록을 닫음",
            },
        }


def validate_sensitivity_contracts(
    contracts: tuple[EvaluationContract, ...],
) -> None:
    """60/120/180분 외 조건이 동일한 민감도 실험 묶음인지 검증한다."""
    if tuple(contract.evaluation_minutes for contract in contracts) != (60, 120, 180):
        raise ValueError("민감도 실험은 60/120/180분을 순서대로 모두 포함해야 합니다.")
    comparable_fields = (
        "target_date",
        "start_hour",
        "tick_minutes",
        "fleet_size",
        "truck_capacity",
        "speed_kmh",
        "service_minutes_per_stop",
        "approval_delay_minutes",
        "weather_publication_lag_minutes",
        "population_lookback_weeks",
        "operator_timing_scenarios",
    )
    reference = contracts[0]
    for contract in contracts[1:]:
        differing = [
            field
            for field in comparable_fields
            if getattr(contract, field) != getattr(reference, field)
        ]
        if differing:
            raise ValueError(f"민감도 계약에서 평가 시간 외 조건이 달라졌습니다: {differing}")
