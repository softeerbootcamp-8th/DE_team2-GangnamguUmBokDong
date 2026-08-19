"""Gold urgency projection의 완전성·artifact 분리 계약을 검증한다."""

from datetime import UTC, datetime, timedelta, timezone

import pyarrow as pa
import pytest
from core.gold_publication import ContractViolation
from gold.common import parquet_bytes
from gold.urgency import (
    StationUrgencyRecord,
    UrgencyProjection,
    UrgencyRecord,
    build_urgency_projection,
    urgency_records_from_parquet,
    urgency_records_to_parquet,
)

BASE = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)


def _record(
    station_id: str,
    *,
    base: datetime = BASE,
    score: float = 42.5,
    critical: int = 25,
    need: str = "supply_needed",
    bike_qty: int = 3,
) -> UrgencyRecord:
    """테스트용 유효 urgency 계산 행을 반환한다."""
    return UrgencyRecord(station_id, base, score, critical, need, bike_qty)


def test_projection_uses_exact_authoritative_intersection_and_sorts() -> None:
    """active·current stock·demand support 교집합을 한 번씩 정렬한다."""
    projection = build_urgency_projection(
        (_record("ST-2"), _record("ST-1")),
        base_dttm=BASE,
        active_station_ids=("ST-3", "ST-2", "ST-1"),
        current_stock_station_ids=("ST-1", "ST-2", "ST-4"),
        demand_support_station_ids=("ST-2", "ST-1", "ST-5"),
    )
    assert projection.expected_sta_ids == ("ST-1", "ST-2")
    assert tuple(record.sta_id for record in projection.records) == (
        "ST-1",
        "ST-2",
    )
    assert all(record.base_dttm.tzinfo is UTC for record in projection.records)


def test_target_records_omit_route_only_bike_quantity() -> None:
    """RDS target 행에는 route producer 전용 bike_qty를 중복 저장하지 않는다."""
    projection = build_urgency_projection(
        (_record("ST-1", bike_qty=7),),
        base_dttm=BASE,
        active_station_ids=("ST-1",),
        current_stock_station_ids=("ST-1",),
        demand_support_station_ids=("ST-1",),
    )
    [target] = projection.target_records
    assert target.sta_id == "ST-1"
    assert not hasattr(target, "bike_qty")
    assert projection.records[0].bike_qty == 7


def test_public_target_record_revalidates_ddl_values() -> None:
    """publisher가 target record를 직접 만들더라도 DDL 밖 값을 허용하지 않는다."""
    with pytest.raises(ContractViolation, match="0..100"):
        StationUrgencyRecord(
            sta_id="ST-1",
            base_dttm=BASE,
            urgency_score=101.0,
            critical_remaining_min=5,
            rebalance_need_type_cd="normal",
        )


@pytest.mark.parametrize(
    ("records", "active", "stock", "demand", "message"),
    [
        ((), ("ST-1",), ("ST-1",), ("ST-1",), "missing"),
        ((_record("ST-2"),), ("ST-1",), ("ST-1",), ("ST-1",), "extra"),
        (
            (_record("ST-1"), _record("ST-1")),
            ("ST-1",),
            ("ST-1",),
            ("ST-1",),
            "중복",
        ),
    ],
)
def test_projection_rejects_missing_extra_and_duplicate_results(
    records: tuple[UrgencyRecord, ...],
    active: tuple[str, ...],
    stock: tuple[str, ...],
    demand: tuple[str, ...],
    message: str,
) -> None:
    """기대 ID 누락·extra·중복에서 부분 projection을 게시하지 않는다."""
    with pytest.raises(ContractViolation, match=message):
        build_urgency_projection(
            records,
            base_dttm=BASE,
            active_station_ids=active,
            current_stock_station_ids=stock,
            demand_support_station_ids=demand,
        )


def test_projection_accepts_empty_only_when_expected_intersection_is_empty() -> None:
    """조건부 EMPTY는 authoritative 기대 교집합이 빈 때만 만든다."""
    projection = build_urgency_projection(
        (),
        base_dttm=BASE,
        active_station_ids=("ST-1",),
        current_stock_station_ids=(),
        demand_support_station_ids=("ST-1",),
    )
    assert projection.records == ()
    assert projection.expected_sta_ids == ()


def test_projection_normalizes_equivalent_timezone_to_utc() -> None:
    """같은 instant의 timezone 표기는 output에서 UTC로 고정한다."""
    kst = timezone(timedelta(hours=9))
    local = BASE.astimezone(kst)
    projection = build_urgency_projection(
        (_record("ST-1", base=local),),
        base_dttm=local,
        active_station_ids=("ST-1",),
        current_stock_station_ids=("ST-1",),
        demand_support_station_ids=("ST-1",),
    )
    assert projection.base_dttm == BASE
    assert projection.records[0].base_dttm == BASE


def test_projection_rejects_different_anchor() -> None:
    """다른 stock tick의 urgency 행을 같은 publication에 섞지 않는다."""
    with pytest.raises(ContractViolation, match="anchor"):
        build_urgency_projection(
            (_record("ST-1", base=BASE - timedelta(minutes=5)),),
            base_dttm=BASE,
            active_station_ids=("ST-1",),
            current_stock_station_ids=("ST-1",),
            demand_support_station_ids=("ST-1",),
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"station_id": "BAD"}, "ST-숫자"),
        ({"score": float("nan")}, "finite"),
        ({"score": 100.1}, "0..100"),
        ({"critical": -1}, "비음수"),
        ({"critical": 2_147_483_648}, "INTEGER"),
        ({"need": "urgent"}, "allowlist"),
        ({"bike_qty": -1}, "비음수"),
        ({"bike_qty": 2_147_483_648}, "INTEGER"),
    ],
)
def test_record_rejects_values_outside_target_and_artifact_contract(
    overrides: dict[str, object],
    message: str,
) -> None:
    """DDL 범위 밖 값과 route에 쓸 수 없는 수량을 immutable output 전에 거부한다."""
    arguments: dict[str, object] = {
        "station_id": "ST-1",
        "score": 42.5,
        "critical": 25,
        "need": "supply_needed",
        "bike_qty": 3,
    }
    arguments.update(overrides)
    with pytest.raises(ContractViolation, match=message):
        _record(**arguments)  # type: ignore[arg-type]


def test_expected_sets_reject_duplicates_before_intersection() -> None:
    """입력 집합 중복을 set 변환으로 숨기지 않는다."""
    with pytest.raises(ContractViolation, match="중복"):
        build_urgency_projection(
            (_record("ST-1"),),
            base_dttm=BASE,
            active_station_ids=("ST-1", "ST-1"),
            current_stock_station_ids=("ST-1",),
            demand_support_station_ids=("ST-1",),
        )


def test_projection_object_rejects_noncanonical_expected_id_order() -> None:
    """직접 만든 projection도 expected ID canonical order를 우회하지 못한다."""
    with pytest.raises(ContractViolation, match="UTF-8"):
        UrgencyProjection(
            records=(_record("ST-1"), _record("ST-2")),
            base_dttm=BASE,
            expected_sta_ids=("ST-2", "ST-1"),
        )


def test_urgency_parquet_round_trip_is_deterministic_and_preserves_bike_qty() -> None:
    """fixed schema urgency artifact를 재실행해도 같은 bytes와 값을 만든다."""
    records = (
        _record("ST-1", need="normal", bike_qty=0),
        _record("ST-2", need="retrieval_needed", bike_qty=4),
    )
    first = urgency_records_to_parquet(
        records,
        expected_sta_ids=("ST-1", "ST-2"),
    )
    second = urgency_records_to_parquet(
        records,
        expected_sta_ids=("ST-1", "ST-2"),
    )
    assert first == second
    assert (
        urgency_records_from_parquet(
            first,
            expected_base_dttm=BASE,
            expected_sta_ids=("ST-1", "ST-2"),
        )
        == records
    )


def test_urgency_parquet_rejects_wrong_schema() -> None:
    """이름이 비슷해도 bike_qty가 빠진 artifact는 route 입력으로 받지 않는다."""
    payload = parquet_bytes(
        pa.table(
            {
                "sta_id": ["ST-1"],
                "base_dttm": [BASE],
                "urgency_score": [42.5],
                "critical_remaining_min": [25],
                "rebalance_need_type_cd": ["supply_needed"],
            }
        )
    )
    with pytest.raises(ContractViolation, match="schema"):
        urgency_records_from_parquet(
            payload,
            expected_base_dttm=BASE,
            expected_sta_ids=("ST-1",),
        )


def test_urgency_parquet_requires_canonical_record_order() -> None:
    """artifact row order를 sta_id UTF-8 순으로 고정한다."""
    with pytest.raises(ContractViolation, match="exact"):
        urgency_records_to_parquet(
            (_record("ST-2"), _record("ST-1")),
            expected_sta_ids=("ST-1", "ST-2"),
        )


def test_urgency_parquet_rejects_whole_station_omission() -> None:
    """artifact payload에서 기대 집합을 역추론해 station 누락을 숨기지 않는다."""
    with pytest.raises(ContractViolation, match="exact"):
        urgency_records_to_parquet(
            (_record("ST-1"),),
            expected_sta_ids=("ST-1", "ST-2"),
        )


def test_empty_urgency_uses_no_artifact_instead_of_empty_parquet() -> None:
    """조건부 EMPTY publication은 artifacts=[] 계약을 우회하지 않는다."""
    with pytest.raises(ContractViolation, match=r"artifacts=\[\]"):
        urgency_records_to_parquet((), expected_sta_ids=())
