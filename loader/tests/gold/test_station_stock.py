"""Gold station_stock projection의 candidate 결합 계약을 검증한다."""

from datetime import UTC, datetime

import pytest
from core.gold_publication import ContractViolation
from gold.station_stock import build_station_stock_projection

BASE = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)


def _row(station_id: str, parking: object) -> dict[str, object]:
    """station realtime 재고 행을 반환한다."""
    return {"stationId": station_id, "parkingBikeTotCnt": parking}


def test_projection_uses_exact_candidate_base_and_no_capacity_cap() -> None:
    """재고를 candidate base에 결합하고 정원 상한으로 잘라내지 않는다."""
    projection = build_station_stock_projection(
        (_row("ST-2", 500), _row("ST-1", 0)),
        published_station_ids=("ST-1", "ST-2"),
        candidate_logical_dttm=BASE,
    )
    assert [record.sta_id for record in projection.records] == ["ST-1", "ST-2"]
    assert [record.parking_bike_tot_cnt for record in projection.records] == [0, 500]
    assert {record.base_dttm for record in projection.records} == {BASE}


@pytest.mark.parametrize("parking", [None, "", -1, 1.5, float("nan")])
def test_missing_or_invalid_parking_removes_current_row(parking: object) -> None:
    """현재 candidate의 결측·무효 재고는 projection에서 제외한다."""
    projection = build_station_stock_projection(
        (_row("ST-1", parking),),
        published_station_ids=("ST-1",),
        candidate_logical_dttm=BASE,
    )
    assert projection.records == ()
    assert projection.excluded_missing_or_invalid_count == 1


def test_rows_without_published_station_remain_silver_only() -> None:
    """Gold station projection에 없는 realtime ID를 FK target에 넣지 않는다."""
    projection = build_station_stock_projection(
        (_row("ST-1", 3), _row("ST-3", 4)),
        published_station_ids=("ST-1",),
        candidate_logical_dttm=BASE,
    )
    assert [record.sta_id for record in projection.records] == ["ST-1"]
    assert projection.excluded_missing_or_invalid_count == 0


def test_duplicate_realtime_station_id_rejects_whole_candidate() -> None:
    """authoritative candidate의 중복 natural key에서 임의 승자를 고르지 않는다."""
    with pytest.raises(ContractViolation, match="중복 ID"):
        build_station_stock_projection(
            (_row("ST-1", 3), _row("ST-1", 4)),
            published_station_ids=("ST-1",),
            candidate_logical_dttm=BASE,
        )


def test_station_id_must_be_exact_nfc_nonblank() -> None:
    """station ID trim·NFC 변조로 identity가 바뀌지 않게 거부한다."""
    with pytest.raises(ContractViolation, match="ST-숫자"):
        build_station_stock_projection(
            (_row(" ST-1", 3),),
            published_station_ids=("ST-1",),
            candidate_logical_dttm=BASE,
        )


def test_station_id_must_match_target_schema_pattern() -> None:
    """immutable station_stock output 전에 DDL 밖 ID를 거부한다."""
    with pytest.raises(ContractViolation, match="ST-숫자"):
        build_station_stock_projection(
            (_row("BAD", 3),),
            published_station_ids=("BAD",),
            candidate_logical_dttm=BASE,
        )
