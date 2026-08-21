"""재배치 정책 백테스트의 잔차·재생·route-v2 연결을 검증한다."""

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from evaluation.rebalance_backtest import (
    RentalTrip,
    RouteAction,
    StationMetadata,
    StockObservation,
    build_current_route_plan,
    build_oracle_urgency,
    build_station_metadata,
    detect_relocation_candidates,
    estimate_existing_operations,
    read_bike_relocation_intervals,
    read_rental_trips,
    read_station_crosswalk,
    read_stock_observations,
    replay_policy,
    schedule_route_actions,
)
from gold.rebalance_route import DispatchCenterTopology

SEOUL = ZoneInfo("Asia/Seoul")
START = datetime(2025, 1, 17, 6, tzinfo=SEOUL)
END = datetime(2025, 1, 17, 8, tzinfo=SEOUL)


def _trip(
    bike_id: str,
    minute: int,
    rent_station: int,
    return_station: int,
    duration: int = 10,
) -> RentalTrip:
    """합성 대여 이력 한 건을 만든다."""
    return RentalTrip(
        bike_id=bike_id,
        rented_at=START + timedelta(minutes=minute),
        rent_station_no=rent_station,
        returned_at=START + timedelta(minutes=minute + duration),
        return_station_no=return_station,
    )


def test_existing_operation_estimate_reconciles_hourly_residual() -> None:
    """시민 흐름으로 설명되지 않는 양·음 재고를 균형 이동량으로 센다."""
    observations = (
        StockObservation(START, 1, 5),
        StockObservation(START, 2, 1),
        StockObservation(START + timedelta(hours=1), 1, 2),
        StockObservation(START + timedelta(hours=1), 2, 4),
    )
    estimate = estimate_existing_operations(
        observations=observations,
        trips=(),
        station_nos=frozenset((1, 2)),
        window_start=START,
        window_end=START + timedelta(hours=1),
    )
    assert estimate.balanced_moved_bikes == 3
    assert estimate.external_imbalance_bikes == 0
    assert estimate.station_hour_residual_mae == 3


def test_replay_drops_return_when_observed_rental_is_unfulfilled() -> None:
    """품절로 실패한 대여의 실제 목적지 반납은 시뮬레이션에 주입하지 않는다."""
    metrics = replay_policy(
        policy="none",
        trips=(_trip("A", 5, 1, 2),),
        initial_stock={1: 0, 2: 0},
        station_nos=frozenset((1, 2)),
        window_start=START,
        window_end=END,
        checkpoints=(START, START + timedelta(hours=1)),
    )
    assert metrics.unfulfilled_requests == 1
    assert metrics.fulfilled_requests == 0
    assert metrics.empty_station_hours == 4


def test_replay_applies_pickup_then_dropoff_with_truck_conservation() -> None:
    """트럭은 실제 회수한 수량까지만 뒤 대여소에 배치한다."""
    actions = (
        RouteAction(START, "route", 1, "pickup", 3),
        RouteAction(START + timedelta(minutes=5), "route", 2, "dropoff", 3),
    )
    metrics = replay_policy(
        policy="route",
        trips=(_trip("A", 10, 2, 1),),
        initial_stock={1: 2, 2: 0},
        station_nos=frozenset((1, 2)),
        window_start=START,
        window_end=END,
        checkpoints=(START, START + timedelta(hours=1)),
        route_actions=actions,
    )
    assert metrics.moved_bikes == 2
    assert metrics.planned_bikes == 3
    assert metrics.fulfilled_requests == 1


def test_oracle_need_runs_current_route_v2_without_copying_planner() -> None:
    """실제 미래 shortage와 surplus가 운영 route-v2의 완결 작업으로 이어진다."""
    center = DispatchCenterTopology("center", 127.0, 37.5, True)
    stations = {
        1: StationMetadata(1, "ST-1", "회수", 37.501, 127.001, "center"),
        2: StationMetadata(2, "ST-2", "배치", 37.502, 127.002, "center"),
    }
    trips = tuple(
        _trip(f"B{index}", 10 + index, 2, 99, duration=30) for index in range(3)
    )
    urgency = build_oracle_urgency(
        trips=trips,
        initial_stock={1: 5, 2: 1},
        stations=stations,
        window_start=START,
        window_end=END,
        movement_budget=3,
    )
    plan = build_current_route_plan(
        logical_dttm=START,
        center=center,
        stations=stations,
        urgency=urgency,
    )
    actions = schedule_route_actions(
        plan=plan,
        center=center,
        stations=stations,
        window_start=START,
    )
    assert len(plan.routes) == 1
    assert [(row.action, row.quantity) for row in actions] == [
        ("pickup", 2),
        ("dropoff", 2),
    ]
    assert all(row.executed_at > START for row in actions)


def test_current_planner_accepts_evaluation_stop_limit() -> None:
    """운영 기본값을 바꾸지 않고 평가 실행만 작업 대여소 상한을 제한한다."""
    center = DispatchCenterTopology("center", 127.0, 37.5, True)
    stations = {
        index: StationMetadata(
            index,
            f"ST-{index}",
            f"대여소 {index}",
            37.5 + index / 1000,
            127.0 + index / 1000,
            "center",
        )
        for index in range(1, 9)
    }
    trips = tuple(
        _trip(f"D{index}", 10 + index, index, 99, duration=30) for index in range(5, 9)
    )
    urgency = build_oracle_urgency(
        trips=trips,
        initial_stock={
            **{index: 5 for index in range(1, 5)},
            **{index: 0 for index in range(5, 9)},
        },
        stations=stations,
        window_start=START,
        window_end=END,
        movement_budget=4,
    )
    plan = build_current_route_plan(
        logical_dttm=START,
        center=center,
        stations=stations,
        urgency=urgency,
        max_stops_per_route=5,
    )
    stops_by_route = {
        route.route_id: [
            stop for stop in plan.route_stops if stop.route_id == route.route_id
        ]
        for route in plan.routes
    }
    assert all(len(stops) <= 5 for stops in stops_by_route.values())


def test_detect_relocation_candidates_uses_consecutive_bike_locations() -> None:
    """동일 자전거의 반납지와 다음 대여지가 다를 때만 후보로 센다."""
    first = _trip("A", 0, 1, 2)
    relocated = RentalTrip(
        "A", START + timedelta(minutes=30), 3, START + timedelta(minutes=40), 4
    )
    same_place = RentalTrip(
        "A", START + timedelta(minutes=60), 4, START + timedelta(minutes=70), 5
    )
    assert (
        detect_relocation_candidates(
            (first, relocated, same_place),
            station_nos=frozenset((1, 2, 3, 4, 5)),
            window_start=START,
            window_end=END,
        )
        == 1
    )


def test_station_without_trip_gets_noncolliding_evaluation_id() -> None:
    """목표일 이용이 없는 대여소도 synthetic ID로 평가 집합에 남긴다."""
    center = DispatchCenterTopology("center", 127.0, 37.5, True)
    metadata = build_station_metadata(
        coordinates={
            1: ("관측 ID", 37.5, 127.0),
            2: ("이용 없음", 37.51, 127.01),
        },
        station_ids={1: "ST-10000002"},
        centers=((center, "센터"),),
    )
    assert metadata[1].station_id == "ST-10000002"
    assert metadata[2].station_id == "ST-20000002"


def test_public_station_numbers_are_remapped_to_internal_model_ids(
    tmp_path: Path,
) -> None:
    """공공 번호 재고·이용을 내부 ST suffix로 바꿔 station master와 결합한다."""
    rental_path = tmp_path / "rental.csv"
    rental_path.write_text(
        "자전거번호,대여일시,대여 대여소번호,반납일시,반납대여소번호,"
        "대여대여소ID,반납대여소ID\n"
        "B1,2025-01-17 06:10:00,00781,2025-01-17 06:20:00,02251,"
        "ST-2007,ST-781\n",
        encoding="cp949",
    )
    stock_path = tmp_path / "stock.csv"
    stock_path.write_text(
        "일시,대여소번호,시간대,거치대수량\n"
        "2025-01-17,00781,6,76\n"
        "2025-01-17,02251,6,19\n",
        encoding="cp949",
    )
    crosswalk = read_station_crosswalk(rental_path)
    trips = read_rental_trips(
        rental_path,
        START.date(),
        station_crosswalk=crosswalk,
    )
    stock = read_stock_observations(
        stock_path,
        START.date(),
        station_crosswalk=crosswalk,
    )
    assert crosswalk == {781: 2007, 2251: 781}
    assert [(trip.rent_station_no, trip.return_station_no) for trip in trips] == [
        (2007, 781)
    ]
    assert [(row.station_no, row.quantity) for row in stock] == [(781, 19), (2007, 76)]


def test_bike_lineage_preserves_relocation_time_interval(tmp_path: Path) -> None:
    """이전 반납지와 다음 대여지가 다른 자전거의 이동 가능구간을 보존한다."""
    rental_path = tmp_path / "rental.csv"
    rental_path.write_text(
        "자전거번호,대여일시,대여 대여소번호,반납일시,반납대여소번호,"
        "대여대여소ID,반납대여소ID\n"
        "B1,2025-01-17 05:30:00,1,2025-01-17 05:50:00,2,ST-1,ST-2\n"
        "B2,2025-01-17 05:55:00,4,2025-01-17 06:05:00,5,ST-4,ST-5\n"
        "B1,2025-01-17 06:20:00,3,2025-01-17 06:30:00,4,ST-3,ST-4\n"
        "B2,2025-01-17 06:25:00,6,2025-01-17 06:35:00,7,ST-6,ST-7\n",
        encoding="cp949",
    )
    lineage = read_bike_relocation_intervals(
        rental_path,
        window_start=START,
        window_end=START + timedelta(hours=1),
        station_crosswalk={index: index for index in range(1, 8)},
    )
    assert lineage.source_trip_count == 4
    assert lineage.consecutive_pair_count == 2
    assert lineage.changed_station_pair_count == 2
    assert lineage.overlapping_pair_count == 0
    assert [
        (
            row.bike_id,
            row.origin_station_no,
            row.destination_station_no,
            row.earliest_at,
            row.latest_at,
        )
        for row in lineage.intervals
    ] == [
        ("B1", 2, 3, START - timedelta(minutes=10), START + timedelta(minutes=20)),
        ("B2", 5, 6, START + timedelta(minutes=5), START + timedelta(minutes=25)),
    ]


def test_station_crosswalk_rejects_many_to_one_mapping(tmp_path: Path) -> None:
    """같은 내부 ST ID가 여러 공공 번호에 연결되면 평가를 중단한다."""
    rental_path = tmp_path / "ambiguous.csv"
    rental_path.write_text(
        "대여 대여소번호,대여대여소ID,반납대여소번호,반납대여소ID\n"
        "00781,ST-2007,02251,ST-781\n"
        "00782,ST-2007,02251,ST-781\n",
        encoding="cp949",
    )
    with pytest.raises(ValueError, match="1:1"):
        read_station_crosswalk(rental_path)
