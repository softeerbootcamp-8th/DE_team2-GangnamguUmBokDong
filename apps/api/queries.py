import math
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

from core.db import fetch_all, fetch_one


STOCK_HISTORY_WINDOW_MIN = 25

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
    """대여소 마스터 + 최신 재고 한 줄씩을 합쳐서 전체 목록을 반환한다."""
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
    """now 이후 시점의 예측 원본치(대여·반납량)를 시간순으로 반환한다."""
    query = """
        SELECT predicted_dttm, predicted_rent_cnt, predicted_return_cnt
        FROM forecast_points
        WHERE sta_id = %(sta_id)s AND predicted_dttm > %(now)s
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


def fetch_all_forecast_points(sta_ids: list[str], now: datetime) -> dict[str, list[dict]]:
    """여러 대여소의 예측 원본치를 쿼리 1번으로 가져와 sta_id별로 묶어서 반환한다."""
    query = """
        SELECT sta_id, predicted_dttm, predicted_rent_cnt, predicted_return_cnt
        FROM forecast_points
        WHERE sta_id = ANY(%(sta_ids)s) AND predicted_dttm > %(now)s
        ORDER BY sta_id, predicted_dttm
    """
    rows = fetch_all(query, {"sta_ids": sta_ids, "now": now})
    grouped = _group_by_sta_id(rows)
    return {sta_id: grouped.get(sta_id, []) for sta_id in sta_ids}


def fetch_batch_run_at(now: datetime) -> datetime:
    """가장 최근 예측 배치 실행 시각. 배치 결과가 아직 없으면 지금을 5분 단위로 내림해 대신한다.    """
    row = fetch_one("SELECT max(batch_run_at) as latest FROM forecast_points")
    latest = row["latest"] if row else None
    return latest if latest is not None else _floor_to_5min(now)


def now_utc() -> datetime:
    """UTC 현재 시각(timezone-aware)을 반환한다."""
    return datetime.now(UTC)


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
