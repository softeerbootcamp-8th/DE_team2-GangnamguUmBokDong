"""위경도 좌표 및 기상청 격자(nx, ny)를 서울시 자치구(gu) 이름으로 매핑한다."""

from __future__ import annotations

import json
import math
from pathlib import Path

from shapely.geometry import Point, shape

_BOUNDARY_PATH = Path(__file__).parent / "assets" / "seoul_gu_boundary.geojson"

# 서울 시내 격자(5km 간격)가 인접 구로 잘못 배정되는 것은 막지 않되, 서울 밖 좌표 방지용 거리 상한(degree)
_MAX_NEAREST_GU_DEGREES = 0.3

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


def latlon_to_grid(lat: float, lon: float) -> tuple[int, int]:
    """WGS84 위경도 좌표를 가장 가까운 기상청 격자 좌표(nx, ny)로 변환한다.

    `grid_to_latlon`의 정확한 역변환이다(같은 람베르트 등각원추투영 계수를 쓴다).

    args:
        lat: 위도 (WGS84)
        lon: 경도 (WGS84)
    returns:
        가장 가까운 격자의 (nx, ny) 정수 좌표
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

    ra = math.tan(math.pi * 0.25 + (lat * _DEGRAD) * 0.5)
    ra = re * sf / ra**sn
    theta = lon * _DEGRAD - olon
    if theta > math.pi:
        theta -= 2.0 * math.pi
    if theta < -math.pi:
        theta += 2.0 * math.pi
    theta *= sn

    xn = ra * math.sin(theta)
    yn = ra * math.cos(theta)
    nx = xn + _XO
    ny = _YO + ro - yn
    return round(nx), round(ny)
