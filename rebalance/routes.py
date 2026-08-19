"""권역별 재배치 라우트 생성.

station_urgency(urgency.compute_all)로 "어디가 급한지"는 알지만 "트럭이 어디서
어디로 몇 대를 옮겨야 하는지"는 없었다. 11개 권역(core.regions)별로 독립적으로,
트럭 적재(수거) TRUCK_CAPACITY대 이하 제약 하에 그리디로 라우트를 만든다.

우선순위는 compute_urgency 배치가 이미 계산해 S3에 써둔 결과를
reader.read_urgency_result로 그대로 읽어서 쓴다 — urgency.compute_all()을
여기서 다시 부르지 않는다. urgency 자체가 재계산은 결정적(같은 입력 -> 같은
출력)이라 값은 같겠지만, Airflow에서 compute_urgency -> compute_routes로
이미 순서가 있는데 그 순서가 실제 데이터 의존이 아니라 이름뿐인 의존이 되는
건 물론, 같은 계산을 두 배치가 각각 처음부터 다시 하는 낭비이기도 하다.

dispatched(운영자가 실행 선택) 상태인 라우트의 수요는 라우트를 만들기 *전에*
순수요에서 빼야 한다 — 완성된 라우트에서 스톱만 사후 삭제하면 적재/방문순서
정합성이 깨지기 때문이다. 이 넷팅을 위한 RDS 조회가 이 모듈에서 유일하게
RDS를 건드리는 지점이다.
"""

from __future__ import annotations

import math
import uuid
from datetime import datetime

import pandas as pd
from core.db import fetch_all
from core.regions import DISPATCH_CENTERS, nearest_region

import reader
from routes_config import TRUCK_CAPACITY

_REGION_COORDS = {name: (lat, lon) for name, lat, lon in DISPATCH_CENTERS}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """두 좌표 사이의 거리를 구면 거리(km)로 계산한다(core.regions.nearest_region이
    이름만 반환해서, 방문 순서 정렬에 필요한 거리값 자체는 여기서 다시 구한다)."""
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _dispatched_qty() -> dict[tuple[str, str], int]:
    """현재 dispatched 상태인 라우트가 대여소별로 이미 커버하는 대수를
    (sta_id, action) 기준으로 합산해 반환한다."""
    rows = fetch_all(
        """
        SELECT s.sta_id, s.action, SUM(s.bike_cnt) AS qty
        FROM rebalance_route_stops s
        JOIN rebalance_routes r ON r.route_id = s.route_id
        WHERE r.status = 'dispatched'
        GROUP BY s.sta_id, s.action
        """
    )
    return {(row["sta_id"], row["action"]): int(row["qty"]) for row in rows}


def _action_for(action_type: str) -> str | None:
    if action_type == "retrieval_needed":
        return "pickup"
    if action_type == "supply_needed":
        return "dropoff"
    return None


def _remaining_need(stations: pd.DataFrame, dispatched: dict[tuple[str, str], int]) -> pd.DataFrame:
    """station_urgency 결과에서 action_type이 normal이거나 bike_qty가 0 이하인
    대여소를 빼고, 이미 dispatched로 커버된 만큼을 뺀 나머지(remaining_qty > 0)만
    남긴다."""
    rows = []
    for row in stations.to_dict("records"):
        action = _action_for(row["action_type"])
        if action is None or row["bike_qty"] <= 0:
            continue
        already = dispatched.get((row["sta_id"], action), 0)
        remaining_qty = row["bike_qty"] - already
        if remaining_qty <= 0:
            continue
        rows.append({**row, "action": action, "remaining_qty": remaining_qty})
    return pd.DataFrame(rows)


def _nearest_neighbor_order(depot: tuple[float, float], stations: list[dict]) -> list[dict]:
    """depot에서 출발해 가장 가까운 미방문 대여소를 계속 골라 방문 순서를 정한다."""
    remaining = list(stations)
    ordered = []
    lat, lon = depot
    while remaining:
        nearest = min(remaining, key=lambda s: _haversine_km(lat, lon, s["lat"], s["lon"]))
        ordered.append(nearest)
        remaining.remove(nearest)
        lat, lon = nearest["lat"], nearest["lon"]
    return ordered


def _select_up_to_capacity(candidates: pd.DataFrame, capacity: int) -> tuple[list[dict], pd.DataFrame]:
    """urgency_score 내림차순으로 훑으며 capacity를 넘지 않는 선에서 대여소를
    고른다. 용량이 모자라 일부만 실은 대여소는 남은 수요만큼 leftover에 남겨
    다음 회차(추가 트럭)가 마저 처리할 수 있게 한다 — 통째로 빠지면 그 대여소의
    남은 수요가 이번 배치에서 아예 누락된다."""
    if candidates.empty or capacity <= 0:
        return [], candidates

    ordered = candidates.sort_values("urgency_score", ascending=False)
    selected = []
    leftover_rows = []
    used_capacity = 0
    for _, row in ordered.iterrows():
        if used_capacity >= capacity:
            leftover_rows.append(row)
            continue
        qty = min(row["remaining_qty"], capacity - used_capacity)
        selected.append({**row.to_dict(), "qty": qty})
        used_capacity += qty
        if qty < row["remaining_qty"]:
            partial = row.copy()
            partial["remaining_qty"] = row["remaining_qty"] - qty
            leftover_rows.append(partial)
    leftover = pd.DataFrame(leftover_rows) if leftover_rows else candidates.iloc[0:0]
    return selected, leftover


def _build_region_routes(region: str, stations: pd.DataFrame, now: datetime) -> tuple[list[dict], list[dict]]:
    """한 권역의 잔여 수요로 라우트(들)를 만든다. 픽업 대상 총수요가
    TRUCK_CAPACITY를 넘으면 여러 라우트(트럭 추가 회차)로 나눈다. 드롭 대상이
    없어도 픽업만 있는 라우트는 유효하다(다음 사이클 재분배용으로 실어둠).
    픽업이 하나도 없으면(드롭 수요만 있어도) 채워줄 방법이 없어 라우트를 만들지
    않는다."""
    depot = _REGION_COORDS[region]
    pickups_left = stations[stations["action"] == "pickup"]
    dropoffs_left = stations[stations["action"] == "dropoff"]

    route_rows: list[dict] = []
    stop_rows: list[dict] = []
    while not pickups_left.empty:
        picked, pickups_left = _select_up_to_capacity(pickups_left, TRUCK_CAPACITY)
        capacity_left = TRUCK_CAPACITY - sum(p["qty"] for p in picked)
        dropped, dropoffs_left = _select_up_to_capacity(dropoffs_left, capacity_left)

        route_id = str(uuid.uuid4())
        route_rows.append({"route_id": route_id, "region": region, "status": "proposed", "proposed_at": now})

        ordered = _nearest_neighbor_order(depot, picked) + _nearest_neighbor_order(depot, dropped)
        for visit_order, stop in enumerate(ordered, start=1):
            stop_rows.append(
                {
                    "route_id": route_id,
                    "visit_order": visit_order,
                    "sta_id": stop["sta_id"],
                    "action": stop["action"],
                    "bike_cnt": stop["qty"],
                }
            )

    return route_rows, stop_rows


def compute_all(anchor: datetime) -> tuple[list[dict], list[dict]]:
    """anchor 시점 기준 11개 권역 전체의 재배치 라우트를 만든다.

    입력(reader.read_urgency_result)은 S3만 읽고, 여기서 유일하게 RDS(dispatched
    넷팅)를 좁게 읽는다 — 나머지는 순수 계산이라 RDS/S3에 영향 없음.
    """
    stations = reader.read_urgency_result(anchor)
    dispatched = _dispatched_qty()
    remaining = _remaining_need(stations, dispatched)
    if remaining.empty:
        return [], []

    remaining = remaining.assign(region=remaining.apply(lambda r: nearest_region(r["lat"], r["lon"]), axis=1))

    route_rows: list[dict] = []
    stop_rows: list[dict] = []
    for region, group in remaining.groupby("region"):
        routes, stops = _build_region_routes(region, group, anchor)
        route_rows.extend(routes)
        stop_rows.extend(stops)
    return route_rows, stop_rows
