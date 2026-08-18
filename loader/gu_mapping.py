"""위경도 좌표 및 기상청 격자(nx, ny)를 서울시 자치구(gu) 이름으로 매핑한다."""

from __future__ import annotations

import json
import math
from pathlib import Path

from shapely.geometry import Point, shape

_BOUNDARY_PATH = Path(__file__).parent / "assets" / "seoul_gu_boundary.geojson"

# 서울 시내 격자(5km 간격)가 인접 구로 잘못 배정되는 것은 막지 않되, 서울 밖 좌표 방지용 거리 상한(degree)
_MAX_NEAREST_GU_DEGREES = 0.3

# collector/sources/weather_*.yaml의 25개 자치구 대표 격자 1:1 매핑 테이블
_GRID_TO_GU_TABLE: dict[tuple[int, int], str] = {
    (61, 125): "강남구",
    (63, 126): "강동구",
    (60, 129): "강북구",
    (57, 127): "강서구",
    (59, 124): "관악구",
    (62, 126): "광진구",
    (58, 125): "구로구",
    (58, 124): "금천구",
    (62, 129): "노원구",
    (61, 129): "도봉구",
    (61, 127): "동대문구",
    (59, 125): "동작구",
    (58, 127): "마포구",
    (59, 127): "서대문구",
    (61, 124): "서초구",
    (61, 126): "성동구",
    (61, 128): "성북구",
    (62, 125): "송파구",
    (58, 126): "양천구",
    (59, 126): "영등포구",
    (60, 126): "용산구",
    (59, 128): "은평구",
    (60, 127): "종로구",
    (60, 128): "중구",
    (62, 127): "중랑구",
}

_GU_POLYGONS: list[tuple[str, object]] | None = None
_GU_CENTROIDS: list[tuple[str, float, float]] | None = None


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
        if polygon.contains(point):
            return gu_name
    return None


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


# 기상청 5km 격자(nx, ny) <-> 위경도 변환을 위한 람베르트 등각원추투영 공식 계수
_RE = 6371.00877  # 지구 반경(km)
_GRID = 5.0  # 격자 간격(km)
_SLAT1 = 30.0  # 투영 표준위도1(degree)
_SLAT2 = 60.0  # 투영 표준위도2(degree)
_OLON = 126.0  # 기준점 경도(degree)
_OLAT = 38.0  # 기준점 위도(degree)
_XO = 43  # 기준점 X 격자좌표
_YO = 136  # 기준점 Y 격자좌표

_DEGRAD = math.pi / 180.0


def grid_to_latlon(nx: float, ny: float) -> tuple[float, float]:
    """기상청 격자 좌표(nx, ny)를 WGS84 위경도 좌표(lat, lon)로 변환한다.

    args:
        nx: 기상청 X 격자 좌표
        ny: 기상청 Y 격자 좌표
    returns:
        (lat, lon) 위경도 좌표 튜플
    """
    re = _RE / _GRID
    slat1 = _SLAT1 * _DEGRAD
    slat2 = _SLAT2 * _DEGRAD
    olon = _OLON * _DEGRAD
    olat = _OLAT * _DEGRAD

    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = sf**sn * math.cos(slat1) / sn
    ro = math.tan(math.pi * 0.25 + olat * 0.5)
    ro = re * sf / ro**sn

    xn = nx - _XO
    yn = ro - (ny - _YO)
    ra = math.hypot(xn, yn)
    if sn < 0.0:
        ra = -ra
    alat = (re * sf / ra) ** (1.0 / sn)
    alat = 2.0 * math.atan(alat) - math.pi * 0.5
    theta = math.atan2(xn, yn) if (xn, yn) != (0.0, 0.0) else 0.0
    alon = theta / sn + olon

    return math.degrees(alat), math.degrees(alon)


def grid_to_gu(nx: float, ny: float) -> str | None:
    """기상청 격자 좌표(nx, ny)를 서울 자치구 이름으로 매핑한다.

    사전 정의된 25개 자치구 1:1 고정 매핑 테이블을 우선 조회하며,
    미등록 격자는 Point-in-Polygon 및 최근접 중심점 순으로 계산합니다.

    args:
        nx: 기상청 X 격자 좌표
        ny: 기상청 Y 격자 좌표
    returns:
        매핑된 서울 자치구 이름 또는 None
    """
    key = (int(nx), int(ny))
    # (1순위) 미리 정의된 격자-자치구 매핑 테이블 조회
    if key in _GRID_TO_GU_TABLE:
        return _GRID_TO_GU_TABLE[key]

    # (2순위 fallback) 테이블에 없는 격자는 위경도로 변환 후 geometry 기반 매핑
    lat, lon = grid_to_latlon(nx, ny)
    return latlon_to_gu(lat, lon) or _nearest_gu(lat, lon)
