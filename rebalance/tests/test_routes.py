"""routes.py: 잔여수요 산출, dispatched 넷팅, 적재량 제약, 방문순서, 권역 배정 테스트.

`_dispatched_qty`(RDS 접근)와 `reader.read_urgency_result`(S3 접근)는 monkeypatch로
대체해서 순수 로직만 검증한다 — 실제 DB/S3 연동은 이 배치가 도는 Airflow
환경에서 확인한다(#109 검증 방법 참고).
"""

import pandas as pd

import routes
from routes import (
    _action_for,
    _build_region_routes,
    _nearest_neighbor_order,
    _remaining_need,
    _select_up_to_capacity,
)
from routes_config import TRUCK_CAPACITY

NOW = pd.Timestamp(2026, 8, 16, 14, 5)

# 세종로 권역센터 근처 좌표(37.5716, 126.9769) 기준 미세하게 떨어뜨린 시험용 좌표.
_SEJONGNO = (37.5716, 126.9769)


def _station(sta_id, action_type, bike_qty, urgency_score, lat_offset=0.0, lon_offset=0.0):
    return {
        "sta_id": sta_id,
        "lat": _SEJONGNO[0] + lat_offset,
        "lon": _SEJONGNO[1] + lon_offset,
        "urgency_score": urgency_score,
        "minutes_until_critical": 0,
        "action_type": action_type,
        "bike_qty": bike_qty,
    }


class TestActionFor:
    def test_retrieval_needed_is_pickup(self):
        assert _action_for("retrieval_needed") == "pickup"

    def test_supply_needed_is_dropoff(self):
        assert _action_for("supply_needed") == "dropoff"

    def test_normal_is_none(self):
        assert _action_for("normal") is None


class TestRemainingNeed:
    def test_normal_action_type_excluded(self):
        stations = pd.DataFrame([_station("101", "normal", bike_qty=5, urgency_score=10)])
        result = _remaining_need(stations, dispatched={})
        assert result.empty

    def test_zero_bike_qty_excluded(self):
        stations = pd.DataFrame([_station("101", "supply_needed", bike_qty=0, urgency_score=10)])
        result = _remaining_need(stations, dispatched={})
        assert result.empty

    def test_dispatched_qty_is_subtracted(self):
        stations = pd.DataFrame([_station("101", "supply_needed", bike_qty=8, urgency_score=10)])
        result = _remaining_need(stations, dispatched={("101", "dropoff"): 5})
        [row] = result.to_dict("records")
        assert row["remaining_qty"] == 3
        assert row["action"] == "dropoff"

    def test_fully_covered_by_dispatched_is_excluded(self):
        stations = pd.DataFrame([_station("101", "supply_needed", bike_qty=8, urgency_score=10)])
        result = _remaining_need(stations, dispatched={("101", "dropoff"): 8})
        assert result.empty

    def test_dispatched_for_different_action_does_not_net(self):
        # 같은 대여소라도 dispatched가 다른 action(pickup)이면 이 dropoff 수요에는
        # 영향이 없어야 한다.
        stations = pd.DataFrame([_station("101", "supply_needed", bike_qty=8, urgency_score=10)])
        result = _remaining_need(stations, dispatched={("101", "pickup"): 8})
        [row] = result.to_dict("records")
        assert row["remaining_qty"] == 8


class TestSelectUpToCapacity:
    def test_selects_highest_urgency_first(self):
        candidates = pd.DataFrame(
            [
                {**_station("101", "supply_needed", 5, urgency_score=10), "action": "dropoff", "remaining_qty": 5},
                {**_station("102", "supply_needed", 5, urgency_score=50), "action": "dropoff", "remaining_qty": 5},
            ]
        )
        selected, leftover = _select_up_to_capacity(candidates, capacity=5)
        assert [s["sta_id"] for s in selected] == ["102"]
        assert leftover["sta_id"].tolist() == ["101"]

    def test_quantity_capped_by_remaining_capacity(self):
        candidates = pd.DataFrame(
            [{**_station("101", "supply_needed", 15, urgency_score=10), "action": "dropoff", "remaining_qty": 15}]
        )
        selected, leftover = _select_up_to_capacity(candidates, capacity=10)
        assert selected[0]["qty"] == 10
        # 15대 수요 중 10대만 이번 회차에 실렸으니, 남은 5대는 다음 회차(추가
        # 트럭)가 마저 처리할 수 있게 leftover에 남아야 한다 — 통째로 빠지면 안 됨.
        [leftover_row] = leftover.to_dict("records")
        assert leftover_row["sta_id"] == "101"
        assert leftover_row["remaining_qty"] == 5

    def test_capacity_zero_selects_nothing(self):
        candidates = pd.DataFrame(
            [{**_station("101", "supply_needed", 5, urgency_score=10), "action": "dropoff", "remaining_qty": 5}]
        )
        selected, leftover = _select_up_to_capacity(candidates, capacity=0)
        assert selected == []
        assert len(leftover) == 1

    def test_empty_candidates_returns_empty(self):
        selected, leftover = _select_up_to_capacity(pd.DataFrame(), capacity=10)
        assert selected == []
        assert leftover.empty


class TestNearestNeighborOrder:
    def test_orders_by_distance_from_depot_then_from_last_visited(self):
        far = {"sta_id": "far", "lat": _SEJONGNO[0] + 0.05, "lon": _SEJONGNO[1]}
        near = {"sta_id": "near", "lat": _SEJONGNO[0] + 0.001, "lon": _SEJONGNO[1]}
        mid = {"sta_id": "mid", "lat": _SEJONGNO[0] + 0.01, "lon": _SEJONGNO[1]}

        ordered = _nearest_neighbor_order(_SEJONGNO, [far, mid, near])

        assert [s["sta_id"] for s in ordered] == ["near", "mid", "far"]

    def test_empty_stations_returns_empty(self):
        assert _nearest_neighbor_order(_SEJONGNO, []) == []


class TestBuildRegionRoutes:
    def test_pickup_and_dropoff_combined_into_one_route(self):
        stations = pd.DataFrame(
            [
                {
                    **_station("pick", "retrieval_needed", 8, urgency_score=90, lat_offset=0.001),
                    "action": "pickup",
                    "remaining_qty": 8,
                },
                {
                    **_station("drop", "supply_needed", 8, urgency_score=80, lat_offset=0.002),
                    "action": "dropoff",
                    "remaining_qty": 8,
                },
            ]
        )
        route_rows, stop_rows = _build_region_routes("세종로", stations, NOW)

        assert len(route_rows) == 1
        [route] = route_rows
        assert route == {"route_id": route["route_id"], "region": "세종로", "status": "proposed", "proposed_at": NOW}
        assert [s["sta_id"] for s in stop_rows] == ["pick", "drop"]
        assert [s["action"] for s in stop_rows] == ["pickup", "dropoff"]
        assert [s["bike_cnt"] for s in stop_rows] == [8, 8]
        assert [s["visit_order"] for s in stop_rows] == [1, 2]
        assert all(s["route_id"] == route["route_id"] for s in stop_rows)

    def test_pickup_demand_over_capacity_splits_into_two_routes(self):
        # TRUCK_CAPACITY(20)를 넘는 두 픽업 대상(15+15=30) -> 첫 라우트가 20을 다
        # 못 채우는 15+5, 둘째 라우트가 나머지 10.
        stations = pd.DataFrame(
            [
                {
                    **_station("a", "retrieval_needed", 15, urgency_score=90, lat_offset=0.001),
                    "action": "pickup",
                    "remaining_qty": 15,
                },
                {
                    **_station("b", "retrieval_needed", 15, urgency_score=80, lat_offset=0.002),
                    "action": "pickup",
                    "remaining_qty": 15,
                },
            ]
        )
        route_rows, stop_rows = _build_region_routes("세종로", stations, NOW)

        assert len(route_rows) == 2
        total_by_route = {}
        for stop in stop_rows:
            total_by_route.setdefault(stop["route_id"], 0)
            total_by_route[stop["route_id"]] += stop["bike_cnt"]
        assert all(qty <= TRUCK_CAPACITY for qty in total_by_route.values())
        assert sum(total_by_route.values()) == 30

    def test_pickup_only_route_is_valid_without_dropoff(self):
        stations = pd.DataFrame(
            [
                {
                    **_station("pick", "retrieval_needed", 5, urgency_score=90, lat_offset=0.001),
                    "action": "pickup",
                    "remaining_qty": 5,
                }
            ]
        )
        route_rows, stop_rows = _build_region_routes("세종로", stations, NOW)
        assert len(route_rows) == 1
        assert [s["action"] for s in stop_rows] == ["pickup"]

    def test_dropoff_only_produces_no_route(self):
        # 픽업 없이 드롭 수요만 있으면 채워줄 방법이 없어 라우트를 만들지 않는다.
        stations = pd.DataFrame(
            [
                {
                    **_station("drop", "supply_needed", 5, urgency_score=90, lat_offset=0.001),
                    "action": "dropoff",
                    "remaining_qty": 5,
                }
            ]
        )
        route_rows, stop_rows = _build_region_routes("세종로", stations, NOW)
        assert route_rows == []
        assert stop_rows == []


class TestComputeAll:
    def test_full_pipeline_nets_dispatched_and_groups_by_region(self, monkeypatch):
        stations = pd.DataFrame(
            [
                _station("pick", "retrieval_needed", bike_qty=8, urgency_score=90, lat_offset=0.001),
                _station("drop", "supply_needed", bike_qty=8, urgency_score=80, lat_offset=0.002),
                _station("normal", "normal", bike_qty=0, urgency_score=0),
            ]
        )
        monkeypatch.setattr(routes.reader, "read_urgency_result", lambda anchor: stations)
        monkeypatch.setattr(routes, "_dispatched_qty", lambda: {("drop", "dropoff"): 8})

        route_rows, stop_rows = routes.compute_all(NOW)

        # drop의 수요(8)가 이미 dispatched로 다 커버돼서, pickup만 남아 pickup-only
        # 라우트 하나만 생성돼야 한다.
        assert len(route_rows) == 1
        assert [s["sta_id"] for s in stop_rows] == ["pick"]
        assert [s["action"] for s in stop_rows] == ["pickup"]

    def test_no_remaining_need_produces_no_routes(self, monkeypatch):
        stations = pd.DataFrame([_station("normal", "normal", bike_qty=0, urgency_score=0)])
        monkeypatch.setattr(routes.reader, "read_urgency_result", lambda anchor: stations)
        monkeypatch.setattr(routes, "_dispatched_qty", dict)

        route_rows, stop_rows = routes.compute_all(NOW)

        assert route_rows == []
        assert stop_rows == []
