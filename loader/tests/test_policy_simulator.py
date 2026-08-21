"""5분 재계획 시뮬레이터의 시민·작업·트럭 상태 보존을 검증한다."""

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from evaluation.backtest_contract import EvaluationContract
from evaluation.historical_inputs import HistoricalStation, PredictionAudit
from evaluation.policy_simulator import simulate_no_rebalance, simulate_policy
from evaluation.rebalance_backtest import RentalTrip
from gold.demand import DemandForecastRecord
from gold.rebalance_route import DispatchCenterTopology

SEOUL = ZoneInfo("Asia/Seoul")
START = datetime(2025, 6, 17, 6, tzinfo=SEOUL)


def _station(number: int, capacity: int, latitude: float) -> HistoricalStation:
    """시뮬레이터 합성 대여소를 만든다."""
    return HistoricalStation(
        station_id=f"ST-{number}",
        station_no=number,
        station_name=f"대여소 {number}",
        capacity=capacity,
        latitude=latitude,
        longitude=127.0,
        grid_id=f"GRID-{number}",
    )


def _trip(minute: int, rent_station: int, return_station: int) -> RentalTrip:
    """10분 길이의 합성 시민 대여를 만든다."""
    return RentalTrip(
        bike_id=f"B-{minute}-{rent_station}",
        rented_at=START + timedelta(minutes=minute),
        rent_station_no=rent_station,
        returned_at=START + timedelta(minutes=minute + 10),
        return_station_no=return_station,
    )


def _forecast(anchor, stock, successful):
    """회수 대여소에서 배치 대여소로 5대를 옮기게 하는 12시간 예측을 만든다."""
    del stock, successful
    base = anchor.astimezone(UTC)
    rows = []
    for station_id in ("ST-1", "ST-2"):
        for horizon in range(1, 13):
            rows.append(
                DemandForecastRecord(
                    base_dttm=base,
                    sta_id=station_id,
                    predicted_dttm=base + timedelta(hours=horizon),
                    predicted_rent_cnt=5 if station_id == "ST-2" and horizon == 1 else 0,
                    predicted_rtn_cnt=5 if station_id == "ST-1" and horizon == 1 else 0,
                )
            )
    rows.sort(key=lambda row: (row.sta_id, row.predicted_dttm))
    audit = PredictionAudit(
        anchor=anchor.isoformat(),
        weather_observed_at=(anchor - timedelta(hours=1)).replace(tzinfo=None).isoformat(),
        weather_cutoff=(anchor - timedelta(hours=1)).isoformat(),
        population_candidate_dates=("2025-06-10",),
        rental_lag_start=(anchor - timedelta(minutes=100)).isoformat(),
        rental_lag_end=(anchor - timedelta(minutes=40)).isoformat(),
        rental_visibility_cutoff=anchor.isoformat(),
        return_lag_start=(anchor - timedelta(minutes=60)).isoformat(),
        return_lag_end=anchor.isoformat(),
        model_bundle_sha256="0" * 64,
        station_count=2,
    )
    return tuple(rows), audit


def test_no_rebalance_removes_return_of_failed_rental() -> None:
    """재고가 없어 실패한 관측 요청은 목적지 반납도 만들지 않는다."""
    contract = EvaluationContract(date(2025, 6, 17), 6, evaluation_minutes=60)
    stations = (_station(1, 10, 37.5), _station(2, 10, 37.501))
    result = simulate_no_rebalance(
        contract=contract,
        center=DispatchCenterTopology("center", 127.0, 37.5, True),
        stations=stations,
        initial_stock={1: 0, 2: 0},
        trips=(_trip(5, 1, 2),),
    )
    assert result.observed_requests == 1
    assert result.unfulfilled_requests == 1
    assert result.fulfilled_requests == 0


def test_policy_replans_every_five_minutes_without_double_dispatching_covered_work() -> None:
    """진행 중 route coverage와 truck 점유가 다음 5분 tick의 중복 작업을 막는다."""
    contract = EvaluationContract(
        date(2025, 6, 17),
        6,
        evaluation_minutes=60,
        fleet_size=1,
    )
    stations = (_station(1, 10, 37.5001), _station(2, 10, 37.5002))
    result = simulate_policy(
        policy="model_route_v2",
        contract=contract,
        center=DispatchCenterTopology("center", 127.0, 37.5, True),
        stations=stations,
        initial_stock={1: 10, 2: 0},
        trips=(_trip(15, 2, 1),),
        forecast_provider=_forecast,
        max_stops_per_route=8,
        movement_budget=5,
    )
    assert result.decision_ticks == 12
    assert result.dispatched_routes == 1
    assert result.movement_budget_used == 5
    assert result.moved_bikes == 5
    assert result.fulfilled_requests == 1
    assert result.completed_routes_by_cutoff == 1
    assert result.trucks_still_busy_at_cutoff == 0


def test_policy_does_not_dispatch_route_that_cannot_return_before_cutoff() -> None:
    """고정 작업 블록이 끝나기 전 센터 복귀가 불가능한 route는 시작하지 않는다."""
    contract = EvaluationContract(
        date(2025, 6, 17),
        6,
        evaluation_minutes=60,
        fleet_size=1,
        speed_kmh=1.0,
    )
    stations = (_station(1, 10, 37.51), _station(2, 10, 37.52))
    result = simulate_policy(
        policy="slow_truck",
        contract=contract,
        center=DispatchCenterTopology("center", 127.0, 37.5, True),
        stations=stations,
        initial_stock={1: 10, 2: 0},
        trips=(),
        forecast_provider=_forecast,
        max_stops_per_route=8,
        movement_budget=10,
    )
    assert result.dispatched_routes == 0
    assert result.trucks_still_busy_at_cutoff == 0
    assert all(audit.idle_trucks_before == 1 for audit in result.tick_audits)
