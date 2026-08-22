"""재배치 백테스트 사전 계약이 임의 조건 변경과 과장 주장을 막는지 검증한다."""

from datetime import date

import pytest
from evaluation.backtest_contract import (
    EvaluationContract,
    validate_sensitivity_contracts,
)


def test_contract_matches_realtime_operating_cadence() -> None:
    """기본 계약은 2시간 작업 블록에서 5분마다 세 트럭을 운용한다."""
    contract = EvaluationContract(date(2025, 6, 17), 6)
    audit = contract.audit_document()
    assert contract.evaluation_minutes == 120
    assert contract.tick_minutes == 5
    assert contract.truck_capacity == 20
    assert audit["evidence_grade"] == "retrospective_heldout_replay"
    assert "인과적으로 주장할 수 없다" in audit["forbidden_claim"]


def test_contract_rejects_arbitrary_six_hour_window() -> None:
    """사전에 선언하지 않은 6시간을 결과를 보고 임의로 선택할 수 없다."""
    with pytest.raises(ValueError, match="60/120/180"):
        EvaluationContract(date(2025, 6, 17), 6, evaluation_minutes=360)


def test_sensitivity_requires_only_duration_to_change() -> None:
    """민감도 실험에서는 평가 시간 외 자원 조건이 완전히 같아야 한다."""
    valid = tuple(
        EvaluationContract(date(2025, 6, 17), 6, evaluation_minutes=minutes)
        for minutes in (60, 120, 180)
    )
    validate_sensitivity_contracts(valid)
    invalid = (
        valid[0],
        EvaluationContract(date(2025, 6, 17), 6, evaluation_minutes=120, fleet_size=2),
        valid[2],
    )
    with pytest.raises(ValueError, match="fleet_size"):
        validate_sensitivity_contracts(invalid)
