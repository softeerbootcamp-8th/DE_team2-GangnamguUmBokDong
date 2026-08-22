"""Gold station lifecycle·LKG·relocation 계약을 검증한다."""

from datetime import UTC, datetime, timedelta

import pytest
from core.gold_publication import (
    ContractViolation,
    RelocationApproval,
    build_station_relocation_approval,
    point_ewkb_xdr_hex,
)
from gold.station import (
    DispatchCenterReference,
    MasterSnapshot,
    RealtimeWindowSnapshot,
    StationRecord,
    build_station_projection,
)

MASTER_TIME = datetime(2026, 8, 20, 0, 0, tzinfo=UTC)
CANDIDATE_TIME = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)
LON = 127.0473
LAT = 37.5172
GRID_IDS = ("61_126",)
CENTERS = (
    DispatchCenterReference("CENTER-1", 127.05, 37.52, True),
    DispatchCenterReference("CENTER-OFF", 127.04, 37.51, False),
)


def _master_row(**overrides: object) -> dict[str, object]:
    """station master 기본 행을 반환한다."""
    row: dict[str, object] = {
        "RNTLS_ID": "ST-1",
        "ADDR1": "서울 강남구 테헤란로",
        "LOT": LON,
        "LAT": LAT,
    }
    row.update(overrides)
    return row


def _realtime_row(**overrides: object) -> dict[str, object]:
    """station realtime serving-valid 기본 행을 반환한다."""
    row: dict[str, object] = {
        "stationId": "ST-1",
        "stationName": "강남역 대여소",
        "rackTotCnt": 20,
        "parkingBikeTotCnt": 5,
        "stationLongitude": LON,
        "stationLatitude": LAT,
    }
    row.update(overrides)
    return row


def _window(
    logical_dttm: datetime,
    *rows: dict[str, object],
    revision_no: int = 0,
) -> RealtimeWindowSnapshot:
    """authoritative realtime window를 반환한다."""
    return RealtimeWindowSnapshot(logical_dttm, revision_no, tuple(rows))


def _previous(**overrides: object) -> StationRecord:
    """prior Gold station LKG 행을 반환한다."""
    values: dict[str, object] = {
        "sta_id": "ST-1",
        "sta_nm": "기존 대여소",
        "sta_addr": "기존 주소",
        "hold_cnt": 10,
        "longitude": LON,
        "latitude": LAT,
        "sta_point_source_cd": "bike_station_master",
        "weather_grid_id": "61_126",
        "dispatch_center_id": "CENTER-1",
        "master_base_dttm": MASTER_TIME - timedelta(days=1),
        "last_seen_dttm": CANDIDATE_TIME - timedelta(minutes=5),
        "is_active": True,
    }
    values.update(overrides)
    return StationRecord(**values)  # type: ignore[arg-type]


def _build(
    *,
    master_rows: tuple[dict[str, object], ...] = (_master_row(),),
    windows: tuple[RealtimeWindowSnapshot, ...] | None = None,
    previous: tuple[StationRecord, ...] | None = None,
    activation_ready: tuple[str, ...] = (),
    approval=None,
    centers: tuple[DispatchCenterReference, ...] = CENTERS,
    grids: tuple[str, ...] = GRID_IDS,
    distance_meters=None,
):
    """station projection 기본 입력을 결합해 실행한다."""
    if windows is None:
        windows = (_window(CANDIDATE_TIME, _realtime_row()),)
    return build_station_projection(
        master_snapshot=MasterSnapshot(MASTER_TIME, master_rows),
        realtime_windows=windows,
        previous_records=previous,
        weather_grid_ids=grids,
        dispatch_centers=centers,
        activation_ready_station_ids=activation_ready,
        relocation_approval=approval,
        distance_meters=distance_meters,
    )


def test_new_station_is_published_inactive_until_activation_is_locked_ready() -> None:
    """신규 station은 소비 projection 동시 조건 없이 active로 추론하지 않는다."""
    [inactive] = _build().records
    [active] = _build(activation_ready=("ST-1",)).records
    assert inactive.is_active is False
    assert active.is_active is True
    assert active.sta_nm == "강남역 대여소"
    assert active.sta_addr == "서울 강남구 테헤란로"
    assert active.hold_cnt == 20
    assert active.last_seen_dttm == CANDIDATE_TIME
    assert active.master_base_dttm == MASTER_TIME


def test_new_master_only_station_is_not_published() -> None:
    """신규 master-only ID를 realtime 관측 전에 Gold에 넣지 않는다."""
    with pytest.raises(ContractViolation, match="EMPTY"):
        _build(windows=(_window(CANDIDATE_TIME),))


def test_station_id_must_match_target_schema_pattern() -> None:
    """DDL의 ST-숫자 identity 밖 행은 immutable station output 전에 거부한다."""
    with pytest.raises(ContractViolation, match="ST-숫자"):
        _build(
            master_rows=(_master_row(RNTLS_ID="BAD"),),
            windows=(_window(CANDIDATE_TIME, _realtime_row(stationId="BAD")),),
        )


def test_new_station_uses_realtime_point_only_when_master_point_is_invalid() -> None:
    """master Point만 무효할 때 유효 realtime Point를 fallback으로 쓴다."""
    [record] = _build(master_rows=(_master_row(LOT=0, LAT=0),)).records
    assert record.longitude == LON
    assert record.latitude == LAT
    assert record.sta_point_source_cd == "bike_station_realtime_fallback"


def test_existing_station_keeps_master_lkg_but_updates_valid_realtime_fields() -> None:
    """master 누락 중에도 prior 주소·Point를 유지하고 유효 realtime을 반영한다."""
    previous = _previous()
    [record] = _build(master_rows=(), previous=(previous,)).records
    assert record.sta_addr == previous.sta_addr
    assert record.longitude == previous.longitude
    assert record.master_base_dttm == previous.master_base_dttm
    assert record.sta_nm == "강남역 대여소"
    assert record.hold_cnt == 20
    assert record.last_seen_dttm == CANDIDATE_TIME


@pytest.mark.parametrize("invalid_window_count", [1, 2])
def test_one_or_two_invalid_windows_keep_existing_active_state(
    invalid_window_count: int,
) -> None:
    """최신 1·2개 authoritative invalid window는 기존 active를 즉시 끄지 않는다."""
    windows = tuple(
        _window(
            CANDIDATE_TIME - timedelta(minutes=5 * offset),
            _realtime_row(stationName=None),
        )
        for offset in range(invalid_window_count)
    )
    [record] = _build(windows=windows, previous=(_previous(),)).records
    assert record.is_active is True
    assert record.sta_nm == "기존 대여소"
    assert record.last_seen_dttm == CANDIDATE_TIME


def test_three_distinct_invalid_windows_deactivate_existing_station() -> None:
    """최신 서로 다른 3개 authoritative invalid window만 비활성화한다."""
    windows = tuple(
        _window(
            CANDIDATE_TIME - timedelta(minutes=5 * offset),
            _realtime_row(rackTotCnt=0),
        )
        for offset in range(3)
    )
    [record] = _build(windows=windows, previous=(_previous(),)).records
    assert record.is_active is False


def test_same_logical_window_corrections_cannot_count_twice() -> None:
    """같은 logical window의 correction을 두 번 streak으로 세지 않는다."""
    with pytest.raises(ContractViolation, match="correction 최신"):
        _build(
            windows=(
                _window(CANDIDATE_TIME, _realtime_row(stationName=None)),
                _window(
                    CANDIDATE_TIME,
                    _realtime_row(stationName=None),
                    revision_no=1,
                ),
            ),
            previous=(_previous(),),
        )


def test_valid_reappearance_of_inactive_station_waits_for_activation_gate() -> None:
    """유효하게 재등장한 inactive station도 lock-ready 집합 전에는 active가 아니다."""
    previous = _previous(is_active=False)
    [waiting] = _build(previous=(previous,)).records
    [ready] = _build(previous=(previous,), activation_ready=("ST-1",)).records
    assert waiting.is_active is False
    assert ready.is_active is True


def test_over_100m_unapproved_master_change_keeps_prior_lkg() -> None:
    """100m 초과 master Point를 승인 없이 덮어쓰지 않는다."""
    moved_lon = LON + 0.01
    [record] = _build(
        master_rows=(_master_row(LOT=moved_lon),),
        windows=(_window(CANDIDATE_TIME, _realtime_row(stationLongitude=moved_lon)),),
        previous=(_previous(),),
    ).records
    assert record.longitude == LON
    assert record.sta_addr == "기존 주소"


def test_over_100m_exact_approval_applies_master_change() -> None:
    """>100m master candidate의 exact reference·candidate EWKB 승인만 반영한다."""
    moved_lon = LON + 0.01
    approval = build_station_relocation_approval(
        (
            RelocationApproval(
                approval_id="REL-1",
                approved_by="data-owner",
                approved_dttm=CANDIDATE_TIME,
                candidate_point_ewkb=point_ewkb_xdr_hex(moved_lon, LAT),
                comparison_cd="gold_vs_master",
                reference_point_ewkb=point_ewkb_xdr_hex(LON, LAT),
                sta_id="ST-1",
            ),
        )
    )
    projection = _build(
        master_rows=(_master_row(LOT=moved_lon),),
        windows=(_window(CANDIDATE_TIME, _realtime_row(stationLongitude=moved_lon)),),
        previous=(_previous(),),
        approval=approval,
    )
    [record] = projection.records
    assert record.longitude == moved_lon
    assert record.master_base_dttm == MASTER_TIME
    assert projection.relocation_applied is True


def test_exactly_100m_is_auto_allowed_without_approval() -> None:
    """PostGIS 거리 callback이 정확히 100m면 승인 없이 허용한다."""
    moved_lon = LON + 0.01
    [record] = _build(
        master_rows=(_master_row(LOT=moved_lon),),
        windows=(_window(CANDIDATE_TIME, _realtime_row(stationLongitude=moved_lon)),),
        previous=(_previous(),),
        distance_meters=lambda *_: 100.0,
    ).records
    assert record.longitude == moved_lon


def test_extra_relocation_approval_is_rejected() -> None:
    """실제 >100m 반영에 쓰지 않은 여분 승인을 거부한다."""
    approval = build_station_relocation_approval(
        (
            RelocationApproval(
                approval_id="REL-EXTRA",
                approved_by="data-owner",
                approved_dttm=CANDIDATE_TIME,
                candidate_point_ewkb=point_ewkb_xdr_hex(LON, LAT),
                comparison_cd="gold_vs_master",
                reference_point_ewkb=point_ewkb_xdr_hex(LON, LAT),
                sta_id="ST-1",
            ),
        )
    )
    with pytest.raises(ContractViolation, match="여분"):
        _build(previous=(_previous(),), approval=approval)


def test_new_master_realtime_over_100m_mismatch_requires_exact_approval() -> None:
    """신규 master·realtime Point가 >100m 다르면 승인 전에 게시하지 않는다."""
    realtime_lon = LON + 0.01
    windows = (_window(CANDIDATE_TIME, _realtime_row(stationLongitude=realtime_lon)),)
    with pytest.raises(ContractViolation, match="EMPTY"):
        _build(windows=windows)
    approval = build_station_relocation_approval(
        (
            RelocationApproval(
                approval_id="REL-NEW",
                approved_by="data-owner",
                approved_dttm=CANDIDATE_TIME,
                candidate_point_ewkb=point_ewkb_xdr_hex(LON, LAT),
                comparison_cd="master_vs_realtime",
                reference_point_ewkb=point_ewkb_xdr_hex(realtime_lon, LAT),
                sta_id="ST-1",
            ),
        )
    )
    [record] = _build(windows=windows, approval=approval).records
    assert record.sta_point_source_cd == "bike_station_master"


def test_station_grid_must_exist_in_published_seed() -> None:
    """station이 weather_grid dependency에 없는 FK를 생성하지 않는다."""
    with pytest.raises(ContractViolation, match="seed 밖"):
        _build(grids=("60_127",))


def test_nearest_center_tie_breaks_by_utf8_id() -> None:
    """active center 거리가 동일하면 ID UTF-8 오름차순으로 고른다."""
    centers = (
        DispatchCenterReference("CENTER-B", 127.06, 37.53, True),
        DispatchCenterReference("CENTER-A", 127.03, 37.50, True),
    )
    [record] = _build(centers=centers, distance_meters=lambda *_: 1.0).records
    assert record.dispatch_center_id == "CENTER-A"


def test_batch_capable_distance_callback_replaces_per_pair_calls() -> None:
    """callback이 batch를 노출하면 쌍마다 스칼라로 부르지 않고 묶어서 부른다."""
    centers = (
        DispatchCenterReference("CENTER-B", 127.06, 37.53, True),
        DispatchCenterReference("CENTER-A", 127.03, 37.50, True),
    )
    scalar_calls: list[tuple[float, float, float, float]] = []
    batch_calls: list[tuple[tuple[tuple[float, float], tuple[float, float]], ...]] = []

    def distance(
        longitude_a: float, latitude_a: float, longitude_b: float, latitude_b: float
    ) -> float:
        """스칼라 경로가 쓰이면 기록해 배치 우회를 잡아낸다."""
        scalar_calls.append((longitude_a, latitude_a, longitude_b, latitude_b))
        return 1.0

    def batch(
        pairs: tuple[tuple[tuple[float, float], tuple[float, float]], ...],
    ) -> tuple[float, ...]:
        """center 순서대로 거리를 돌려준다 — CENTER-A가 더 가깝다."""
        batch_calls.append(pairs)
        return tuple(2.0 if index == 0 else 1.0 for index in range(len(pairs)))

    distance.batch = batch  # type: ignore[attr-defined]

    [record] = _build(centers=centers, distance_meters=distance).records

    assert record.dispatch_center_id == "CENTER-A"
    assert scalar_calls == []
    # relocation 비교 쌍 1회 + center 12개(여기선 2개) 1회 = 정류소당 2회.
    # 쌍마다 부르면 3회가 된다.
    assert len(batch_calls) == 2
    assert batch_calls[-1] == (
        ((record.longitude, record.latitude), (127.06, 37.53)),
        ((record.longitude, record.latitude), (127.03, 37.50)),
    )


def test_batch_distance_result_count_mismatch_is_rejected() -> None:
    """batch가 입력보다 적게 돌려주면 조용히 넘기지 않는다."""
    centers = (
        DispatchCenterReference("CENTER-B", 127.06, 37.53, True),
        DispatchCenterReference("CENTER-A", 127.03, 37.50, True),
    )

    def distance(*_: float) -> float:
        """스칼라 경로는 이 테스트에서 쓰이지 않는다."""
        return 1.0

    distance.batch = lambda pairs: (1.0,)  # type: ignore[attr-defined]

    with pytest.raises(ContractViolation):
        _build(centers=centers, distance_meters=distance)


def test_batch_distance_rejects_non_finite_values() -> None:
    """batch 결과도 스칼라와 같은 유한 비음수 계약을 통과해야 한다."""
    centers = (DispatchCenterReference("CENTER-A", 127.03, 37.50, True),)

    def distance(*_: float) -> float:
        """스칼라 경로는 이 테스트에서 쓰이지 않는다."""
        return 1.0

    distance.batch = lambda pairs: (float("nan"),) * len(pairs)  # type: ignore[attr-defined]

    with pytest.raises(ContractViolation):
        _build(centers=centers, distance_meters=distance)
