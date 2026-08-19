import math
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

from core.db import fetch_all, fetch_one

STOCK_HISTORY_WINDOW_MIN = 25

# station_urgency는 sta_id당 최신 1건만 upsert되므로(#124), 배치가 몇 회 연속으로
# 멈춰도 마지막 값이 그대로 남는다. "낡은 값을 최신인 것처럼 보여주지 않는다"는
# 원칙(#107)을 유지하려면 조회 시점에 신선도를 직접 걸러야 한다 — 5분 배치가
# 한 번 밀리는 것까지는 허용하고, 그보다 오래되면 알림에서 제외한다.
ALERTS_FRESHNESS_WINDOW_MIN = 10

# 대여소 주변 몇 km 이내 행사를 "주변 행사"로 볼지. 지도의 주변 회수필요 후보
# 반경(StationMap.tsx의 NEARBY_RADIUS_KM=1, 도보 이동 기준)보다 넉넉하게 잡았다
# — 행사는 도보 범위를 넘어 대중교통으로도 사람을 끌어모으기 때문이다. 실측
# 검증은 아직 없는 첫 추정값이라, 실제 행사-수요 상관관계 데이터가 쌓이면
# 조정해야 한다(#102 완료 기준 참고).
NEARBY_EVENT_RADIUS_KM = 1.5
_EARTH_RADIUS_KM = 6371.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """두 위경도 지점 사이의 대권거리(직선거리)를 km로 반환한다."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(h))


def _floor_to_5min(dt: datetime) -> datetime:
    """주어진 시각을 5분 단위로 내림한다."""
    return dt - timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)


def _group_by_sta_id(rows: list[dict]) -> dict[str, list[dict]]:
    """sta_id 컬럼 기준으로 행을 묶는다(그 컬럼은 결과 dict에서 빠진다)."""
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        sta_id = row.pop("sta_id")
        grouped[sta_id].append(row)
    return grouped


def fetch_stations() -> list[dict]:
    """대여소 마스터와 각 대여소의 최신 재고를 반환한다."""
    query = """
        SELECT s.sta_id, s.sta_nm, s.gu, s.sta_addr, s.lat, s.lon, s.hold_cnt,
               stock.parking_bike_tot_cnt, stock.observed_at AS base_dttm
        FROM stations s
        JOIN LATERAL (
            SELECT parking_bike_tot_cnt, observed_at
            FROM station_stock
            WHERE station_stock.sta_id = s.sta_id
            ORDER BY observed_at DESC
            LIMIT 1
        ) stock ON true
        ORDER BY s.sta_id
    """
    return fetch_all(query)


def fetch_station(sta_id: str) -> dict | None:
    """대여소 하나의 마스터 + 최신 재고를 반환한다. 없으면 None."""
    query = """
        SELECT s.sta_id, s.sta_nm, s.gu, s.sta_addr, s.lat, s.lon, s.hold_cnt,
               stock.parking_bike_tot_cnt, stock.observed_at AS base_dttm
        FROM stations s
        JOIN LATERAL (
            SELECT parking_bike_tot_cnt, observed_at
            FROM station_stock
            WHERE station_stock.sta_id = s.sta_id
            ORDER BY observed_at DESC
            LIMIT 1
        ) stock ON true
        WHERE s.sta_id = %(sta_id)s
    """
    return fetch_one(query, {"sta_id": sta_id})


def fetch_forecast_points(sta_id: str, now: datetime) -> list[dict]:
    """미래 구간을 가진 최신 배치 한 건의 예측만 시간순으로 반환한다."""
    query = """
        WITH latest_batch AS (
            SELECT max(batch_run_at) AS batch_run_at
            FROM forecast_points
            WHERE predicted_dttm > %(now)s
        )
        SELECT predicted_dttm, predicted_rent_cnt, predicted_return_cnt
        FROM forecast_points
        WHERE sta_id = %(sta_id)s
          AND predicted_dttm > %(now)s
          AND batch_run_at = (SELECT batch_run_at FROM latest_batch)
        ORDER BY predicted_dttm
    """
    return fetch_all(query, {"sta_id": sta_id, "now": now})


def fetch_all_stock_history(sta_ids: list[str], now: datetime) -> dict[str, list[dict]]:
    """여러 대여소의 최근 재고 이력을 대여소당 1번이 아니라 쿼리 1번으로 가져와
    sta_id별로 묶어서 반환한다(/alerts처럼 전체 대여소를 훑는 경우 N+1을 피하려고)."""
    query = """
        SELECT sta_id, observed_at, parking_bike_tot_cnt
        FROM station_stock
        WHERE sta_id = ANY(%(sta_ids)s) AND observed_at >= %(since)s
        ORDER BY sta_id, observed_at
    """
    since = now - timedelta(minutes=STOCK_HISTORY_WINDOW_MIN)
    rows = fetch_all(query, {"sta_ids": sta_ids, "since": since})
    grouped = _group_by_sta_id(rows)
    return {sta_id: grouped.get(sta_id, []) for sta_id in sta_ids}


def fetch_alerts(now: datetime) -> list[dict]:
    """전체 대여소의 재배치 우선순위 알림을 station_urgency(배치가 미리 계산한
    결과)에서 urgency_score 내림차순으로 조회한다. ALERTS_FRESHNESS_WINDOW_MIN보다
    오래된 값(배치가 멈췄거나 지연된 대여소)은 낡은 값을 최신인 것처럼 보여주지
    않기 위해 제외한다. region은 위경도가 있어야 계산되므로 stations와 조인해서
    같이 가져온다."""
    query = """
        SELECT s.sta_id, s.sta_nm, s.lat, s.lon,
               u.action_type, u.urgency_score, u.minutes_until_critical
        FROM station_urgency u
        JOIN stations s ON s.sta_id = u.sta_id
        WHERE u.batch_run_at >= %(cutoff)s
        ORDER BY u.urgency_score DESC
    """
    cutoff = now - timedelta(minutes=ALERTS_FRESHNESS_WINDOW_MIN)
    return fetch_all(query, {"cutoff": cutoff})


def fetch_batch_run_at(now: datetime) -> datetime:
    """미래 예측이 있는 최신 배치 시각. 결과가 없으면 전체 최신값 또는 현재 시각."""
    row = fetch_one(
        """
        SELECT COALESCE(
            max(batch_run_at) FILTER (WHERE predicted_dttm > %(now)s),
            max(batch_run_at)
        ) AS latest
        FROM forecast_points
        """,
        {"now": now},
    )
    latest = row["latest"] if row else None
    return latest if latest is not None else _floor_to_5min(now)


def now_utc() -> datetime:
    """UTC 현재 시각(timezone-aware)을 반환한다."""
    return datetime.now(UTC)


def _fetch_stops_for_routes(route_ids: list[str]) -> dict[str, list[dict]]:
    """여러 라우트의 스톱을 쿼리 1번으로 가져와 route_id별로 묶어서 반환한다
    (fetch_all_stock_history와 같은 N+1 방지 패턴). stations와 조인해 지도 렌더링에
    필요한 sta_nm/lat/lon도 같이 준다."""
    if not route_ids:
        return {}
    query = """
        SELECT s.route_id, s.visit_order, s.sta_id, st.sta_nm, st.lat, st.lon, s.action, s.bike_cnt
        FROM rebalance_route_stops s
        JOIN stations st ON st.sta_id = s.sta_id
        WHERE s.route_id = ANY(%(route_ids)s)
        ORDER BY s.route_id, s.visit_order
    """
    rows = fetch_all(query, {"route_ids": route_ids})
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row.pop("route_id")].append(row)
    return dict(grouped)


def fetch_routes(
    region: str | None = None, status: str | None = None, limit: int = 100, offset: int = 0
) -> list[dict]:
    """재배치 라우트 목록을 스톱과 함께 조회한다. region/status로 선택적으로 필터링한다.

    compute_routes는 5분마다 여러 권역에 걸쳐 라우트를 새로 만들기 때문에(#114),
    limit/offset 없이 전부 반환하면 응답이 무한정 커질 수 있다 — proposed_at
    내림차순으로 최신 것부터 limit개만 반환한다."""
    conditions: list[str] = []
    params: dict = {"limit": limit, "offset": offset}
    if region is not None:
        conditions.append("region = %(region)s")
        params["region"] = region
    if status is not None:
        conditions.append("status = %(status)s")
        params["status"] = status
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT route_id, region, status, proposed_at, dispatched_at, completed_at
        FROM rebalance_routes
        {where}
        ORDER BY proposed_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    routes = fetch_all(query, params)
    stops_by_route = _fetch_stops_for_routes([route["route_id"] for route in routes])
    for route in routes:
        route["stops"] = stops_by_route.get(route["route_id"], [])
    return routes


def fetch_route(route_id: str) -> dict | None:
    """라우트 하나를 스톱과 함께 조회한다. 없으면 None."""
    query = """
        SELECT route_id, region, status, proposed_at, dispatched_at, completed_at
        FROM rebalance_routes
        WHERE route_id = %(route_id)s
    """
    route = fetch_one(query, {"route_id": route_id})
    if route is None:
        return None
    route["stops"] = _fetch_stops_for_routes([route_id]).get(route_id, [])
    return route


def dispatch_route(route_id: str, now: datetime) -> dict | str:
    """proposed 상태인 라우트를 dispatched로 전이한다. 상태 체크를 UPDATE의 WHERE절에
    넣고 RETURNING으로 전이된 행을 그 자리에서 바로 받는다 — UPDATE 따로,
    조회 따로 하면 그 사이에 다른 요청이 상태를 또 바꿔서(예: 곧바로 complete)
    응답이 실제로 일어난 일과 다른 상태를 보여줄 수 있다.

    returns: 성공 시 stops 포함 라우트(dict) | "not_found"(라우트 없음) | "wrong_status"(proposed가 아님)
    """
    row = fetch_one(
        """
        UPDATE rebalance_routes
        SET status = 'dispatched', dispatched_at = %(now)s
        WHERE route_id = %(route_id)s AND status = 'proposed'
        RETURNING route_id, region, status, proposed_at, dispatched_at, completed_at
        """,
        {"route_id": route_id, "now": now},
    )
    if row is None:
        return "not_found" if fetch_route(route_id) is None else "wrong_status"
    row["stops"] = _fetch_stops_for_routes([route_id]).get(route_id, [])
    return row


def complete_route(route_id: str, now: datetime) -> dict | str:
    """dispatched 상태인 라우트를 completed로 전이한다. dispatch_route와 동일한 패턴
    (RETURNING으로 원자적 응답).

    returns: 성공 시 stops 포함 라우트(dict) | "not_found"(라우트 없음) | "wrong_status"(dispatched가 아님)
    """
    row = fetch_one(
        """
        UPDATE rebalance_routes
        SET status = 'completed', completed_at = %(now)s
        WHERE route_id = %(route_id)s AND status = 'dispatched'
        RETURNING route_id, region, status, proposed_at, dispatched_at, completed_at
        """,
        {"route_id": route_id, "now": now},
    )
    if row is None:
        return "not_found" if fetch_route(route_id) is None else "wrong_status"
    row["stops"] = _fetch_stops_for_routes([route_id]).get(route_id, [])
    return row


def fetch_nearby_events(lat: float, lon: float, today: date) -> list[dict]:
    """(lat, lon) 기준 NEARBY_EVENT_RADIUS_KM 이내에서 아직 끝나지 않은 문화행사를
    가까운 순으로 반환한다.

    cultural_events에 위경도 없는 색인이 없어서, SQL에서는 위경도 사각형으로
    싸게 후보만 추리고(위도 1도 ≈ 111km 근사), 정확한 거리·반경 판정과 정렬은
    Python에서 haversine으로 한다 — StationMap.tsx가 주변 회수필요 후보를
    고를 때 쓰는 것과 같은 방식이다.
    """
    lat_delta = NEARBY_EVENT_RADIUS_KM / 111.0
    lon_delta = NEARBY_EVENT_RADIUS_KM / (111.0 * math.cos(math.radians(lat)))
    query = """
        SELECT event_id, title, category, place, start_date, end_date, is_free, lat, lon
        FROM cultural_events
        WHERE lat BETWEEN %(min_lat)s AND %(max_lat)s
          AND lon BETWEEN %(min_lon)s AND %(max_lon)s
          AND (end_date IS NULL OR end_date >= %(today)s)
    """
    rows = fetch_all(
        query,
        {
            "min_lat": lat - lat_delta,
            "max_lat": lat + lat_delta,
            "min_lon": lon - lon_delta,
            "max_lon": lon + lon_delta,
            "today": today,
        },
    )

    events = []
    for row in rows:
        distance_km = _haversine_km(lat, lon, row["lat"], row["lon"])
        if distance_km <= NEARBY_EVENT_RADIUS_KM:
            events.append({**row, "distance_km": round(distance_km, 2)})
    return sorted(events, key=lambda e: e["distance_km"])
