"""위경도 좌표 및 기상청 격자(nx, ny)를 서울시 자치구(gu) 이름으로 매핑한다.

격자 <-> 위경도 변환 자체는 `core.weather_grid`가 갖는다(normalizer도 같은 함수를
쓴다). 이 모듈은 하위 호환을 위해 두 함수를 그대로 재수출한다.
"""

from __future__ import annotations

import heapq
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

from core.weather_grid import grid_to_latlon, latlon_to_grid
from shapely.geometry import LineString, Point, shape
from shapely.ops import nearest_points

__all__ = [
    "MANAGEMENT_AREA_BY_CENTER_ID",
    "MANAGEMENT_AREA_BY_GU",
    "grid_to_gu",
    "grid_to_latlon",
    "latlon_to_grid",
    "latlon_to_gu",
    "latlon_to_management_area",
    "seoul_constrained_distance_m",
]

_BOUNDARY_PATH = Path(__file__).parent / "assets" / "seoul_gu_boundary.geojson"
_SEOUL_BOUNDARY_PATH = Path(__file__).parent / "assets" / "seoul_boundary.json"
_BOUNDARY_NUMERIC_EPSILON = 1e-12

# 서울 시내 격자(5km 간격)가 인접 구로 잘못 배정되는 것은 막지 않되, 서울 밖 좌표 방지용 거리 상한(degree)
_MAX_NEAREST_GU_DEGREES = 0.3

MANAGEMENT_AREA_BY_GU = {
    "강북구": "gangbuk",
    "광진구": "gangbuk",
    "노원구": "gangbuk",
    "도봉구": "gangbuk",
    "동대문구": "gangbuk",
    "마포구": "gangbuk",
    "서대문구": "gangbuk",
    "성동구": "gangbuk",
    "성북구": "gangbuk",
    "은평구": "gangbuk",
    "용산구": "gangbuk",
    "종로구": "gangbuk",
    "중구": "gangbuk",
    "중랑구": "gangbuk",
    "강남구": "gangnam",
    "강동구": "gangnam",
    "강서구": "gangnam",
    "관악구": "gangnam",
    "구로구": "gangnam",
    "금천구": "gangnam",
    "동작구": "gangnam",
    "서초구": "gangnam",
    "송파구": "gangnam",
    "양천구": "gangnam",
    "영등포구": "gangnam",
}
MANAGEMENT_AREA_BY_CENTER_ID = {
    "sangam": "gangbuk",
    "jungnang": "gangbuk",
    "sejongno": "gangbuk",
    "dobong": "gangbuk",
    "hunryeonwon": "gangbuk",
    "isu": "gangnam",
    "gaehwa": "gangnam",
    "cheonwang": "gangnam",
    "yeongnam": "gangnam",
    "cheonho": "gangnam",
    "hangnyeoul": "gangnam",
}

_GU_POLYGONS: list[tuple[str, object]] | None = None
_GU_CENTROIDS: list[tuple[str, float, float]] | None = None


@lru_cache(maxsize=1)
def _seoul_visibility_model() -> tuple[
    Any,
    Any,
    tuple[tuple[float, float], ...],
    tuple[tuple[tuple[int, float], ...], ...],
]:
    """서울 외곽 Polygon과 경계 vertex 가시성 graph를 한 번만 만든다."""
    document = json.loads(_SEOUL_BOUNDARY_PATH.read_text(encoding="utf-8"))
    polygon = shape(document["geometry"])
    coverage = polygon.buffer(_BOUNDARY_NUMERIC_EPSILON)
    vertices = tuple((float(lon), float(lat)) for lon, lat in polygon.exterior.coords[:-1])
    adjacency: list[list[tuple[int, float]]] = [[] for _ in vertices]
    for source_index, source in enumerate(vertices):
        for target_index in range(source_index + 1, len(vertices)):
            target = vertices[target_index]
            if not coverage.covers(LineString((source, target))):
                continue
            meters = _haversine_m(source[1], source[0], target[1], target[0])
            adjacency[source_index].append((target_index, meters))
            adjacency[target_index].append((source_index, meters))
    return polygon, coverage, vertices, tuple(tuple(row) for row in adjacency)


def seoul_constrained_distance_m(
    longitude_a: float,
    latitude_a: float,
    longitude_b: float,
    latitude_b: float,
    *,
    direct_distance_m: float | None = None,
) -> float:
    """서울 외곽 Polygon 밖을 통과하지 않는 Point 간 최단거리를 반환한다.

    직선이 Polygon에 포함되면 기존 거리를 그대로 쓴다. 외곽을
    뚫으면 경계 vertex 가시성 graph의 최단 경로를 쓴다. 기존
    경계 asset의 단순화 차이로 밖에 있는 점은 가장 가까운 외곽점까지의
    필수 진입 구간만 더하고, 그 다음부터는 Polygon 내부만 쓴다.
    """
    polygon, coverage, _, _ = _seoul_visibility_model()
    source, source_connector = _snap_to_seoul(
        longitude_b,
        latitude_b,
        polygon,
        coverage,
    )
    target, target_connector = _snap_to_seoul(
        longitude_a,
        latitude_a,
        polygon,
        coverage,
    )
    connector = source_connector + target_connector
    if coverage.covers(LineString((source, target))):
        if connector == 0.0 and direct_distance_m is not None:
            return float(direct_distance_m)
        return connector + _haversine_m(source[1], source[0], target[1], target[0])

    source_distances = _source_vertex_distances(*source)
    candidates = (
        source_distances[index] + target_distance
        for index, target_distance in _visible_vertex_edges(*target)
    )
    internal_distance = min(candidates, default=math.inf)
    if not math.isfinite(internal_distance):
        raise ValueError("서울 외곽 Polygon 내부 최단 경로를 계산할 수 없습니다.")
    return connector + internal_distance


def _snap_to_seoul(
    longitude: float,
    latitude: float,
    polygon: Any,
    coverage: Any,
) -> tuple[tuple[float, float], float]:
    """Polygon 밖 점을 가장 가까운 외곽점과 필수 연결 거리로 바꾼다."""
    point = Point(longitude, latitude)
    if coverage.covers(point):
        return (longitude, latitude), 0.0
    boundary_point = nearest_points(point, polygon)[1]
    snapped = (float(boundary_point.x), float(boundary_point.y))
    connector = _haversine_m(latitude, longitude, snapped[1], snapped[0])
    return snapped, connector


@lru_cache(maxsize=4096)
def _visible_vertex_edges(
    longitude: float,
    latitude: float,
) -> tuple[tuple[int, float], ...]:
    """Point에서 직선으로 보이는 외곽 vertex와 거리를 반환한다."""
    _, coverage, vertices, _ = _seoul_visibility_model()
    point = (longitude, latitude)
    return tuple(
        (
            index,
            _haversine_m(latitude, longitude, vertex[1], vertex[0]),
        )
        for index, vertex in enumerate(vertices)
        if coverage.covers(LineString((point, vertex)))
    )


@lru_cache(maxsize=64)
def _source_vertex_distances(
    longitude: float,
    latitude: float,
) -> tuple[float, ...]:
    """Source Point에서 모든 외곽 vertex까지의 내부 최단거리를 반환한다."""
    _, _, vertices, adjacency = _seoul_visibility_model()
    distances = [math.inf] * len(vertices)
    queue: list[tuple[float, int]] = []
    for index, meters in _visible_vertex_edges(longitude, latitude):
        distances[index] = meters
        heapq.heappush(queue, (meters, index))
    while queue:
        meters, index = heapq.heappop(queue)
        if meters != distances[index]:
            continue
        for neighbor, edge_meters in adjacency[index]:
            candidate = meters + edge_meters
            if candidate < distances[neighbor]:
                distances[neighbor] = candidate
                heapq.heappush(queue, (candidate, neighbor))
    return tuple(distances)


def _load_gu_polygons() -> list[tuple[str, object]]:
    """서울시 자치구 경계 GeoJSON을 읽어 (구 이름, Polygon) 목록을 반환한다."""
    global _GU_POLYGONS
    if _GU_POLYGONS is None:
        data = json.loads(_BOUNDARY_PATH.read_text(encoding="utf-8"))
        _GU_POLYGONS = [
            (feature["properties"]["SIG_KOR_NM"], shape(feature["geometry"]))
            for feature in data["features"]
        ]
    return _GU_POLYGONS


def _load_gu_centroids() -> list[tuple[str, float, float]]:
    """서울시 자치구별 중심점 좌표 (구 이름, lat, lon) 목록을 반환한다."""
    global _GU_CENTROIDS
    if _GU_CENTROIDS is None:
        _GU_CENTROIDS = [
            (gu_name, polygon.centroid.y, polygon.centroid.x) for gu_name, polygon in _load_gu_polygons()
        ]
    return _GU_CENTROIDS


def latlon_to_gu(lat: float, lon: float) -> str | None:
    """위경도(WGS84) 좌표가 속한 서울 자치구 이름을 반환한다 (서울 경계 밖이면 None).

    args:
        lat: 위도 (WGS84)
        lon: 경도 (WGS84)
    returns:
        매핑된 서울 자치구 이름 또는 None
    """
    point = Point(lon, lat)
    for gu_name, polygon in _load_gu_polygons():
        if polygon.covers(point):
            return gu_name
    return None


def latlon_to_management_area(lat: float, lon: float) -> str | None:
    """자치구 Polygon 또는 가장 가까운 자치구로 문서상 관리소를 판정한다."""
    gu_name = latlon_to_gu(lat, lon) or _nearest_gu_polygon(lat, lon)
    return MANAGEMENT_AREA_BY_GU.get(gu_name) if gu_name is not None else None


def _nearest_gu_polygon(lat: float, lon: float) -> str | None:
    """Polygon 밖 좌표를 가장 가까운 서울 자치구 하나에 귀속한다."""
    point = Point(lon, lat)
    candidates: list[tuple[float, bytes, str]] = []
    for gu_name, polygon in _load_gu_polygons():
        boundary_point = nearest_points(point, polygon)[1]
        meters = _haversine_m(lat, lon, boundary_point.y, boundary_point.x)
        candidates.append((meters, gu_name.encode("utf-8"), gu_name))
    return min(candidates)[2] if candidates else None


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """두 WGS84 좌표 사이 구면 거리를 meter로 반환한다."""
    radius_m = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    value = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return 2 * radius_m * math.asin(math.sqrt(value))


def _nearest_gu(lat: float, lon: float) -> str | None:
    """25개 자치구 중심점 중 주어진 위경도에 가장 가까운 자치구 이름을 반환한다."""
    nearest_gu, nearest_dist = None, None
    for gu_name, gu_lat, gu_lon in _load_gu_centroids():
        # 유클리드 거리 계산: sqrt(dx^2 + dy^2)
        dist = math.hypot(lat - gu_lat, lon - gu_lon)
        if nearest_dist is None or dist < nearest_dist:
            nearest_gu, nearest_dist = gu_name, dist
    # (안전 장치) 계산된 최단 거리가 거리 상한 이내인 경우에만 해당 구 반환
    if nearest_dist is not None and nearest_dist <= _MAX_NEAREST_GU_DEGREES:
        return nearest_gu
    return None


def grid_to_gu(nx: float, ny: float) -> str | None:
    """기상청 격자 좌표(nx, ny)를 서울 자치구 이름으로 매핑한다.

    위경도로 변환한 뒤 Point-in-Polygon으로 판정하고, 경계 밖이면 최근접
    중심점(`_nearest_gu`)으로 매핑한다.

    args:
        nx: 기상청 X 격자 좌표
        ny: 기상청 Y 격자 좌표
    returns:
        매핑된 서울 자치구 이름 또는 None
    """
    lat, lon = grid_to_latlon(nx, ny)
    return latlon_to_gu(lat, lon) or _nearest_gu(lat, lon)
