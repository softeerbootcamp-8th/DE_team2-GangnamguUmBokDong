"""위경도·기상청 격자를 서울 자치구(gu) 이름으로 변환한다.

`stations`의 stationName에는 구 이름이 없고, 기상 격자(nx, ny)도 구 정보를 직접
주지 않는다. 여기서 위경도 -> gu는 서울시 행정구역 경계(assets/seoul_gu_boundary.geojson)
기준 point-in-polygon으로 처리한다(정확한 좌표이므로 포함 여부 판정이 적절함).

기상 격자(nx, ny) -> gu는 다르다. 격자 간격(5km)이 굵어서, point-in-polygon으로도
"가장 가까운 구 중심점"으로도 완전히 풀리지 않는다: 면적이 작거나 폭이 좁은 구
(중구·성동구·광진구·동작구 등)는 그 구 폴리곤 안에 격자 교차점이 아예 없을 뿐 아니라,
주변 정수 격자 어디를 봐도 이웃한 더 큰 구의 중심점이 항상 더 가깝다 — 기하학적으로
계산만으로는 이 구들을 영영 대표 격자에 배정할 수 없다.

그래서 grid_to_gu는 기하 계산으로 실시간 판정하는 대신, `collector/sources/
weather_*.yaml`이 요청하는 격자 목록과 1:1로 맞춘 **고정 매핑 테이블**
(`_GRID_TO_GU_TABLE`, 25개 구 전부를 커버)을 1순위로 쓴다. 이 테이블은 각 구
중심점에 가장 가까운 정수 격자를 그리디하게(이미 다른 구가 선점한 격자는 건너뛰고
차순위로) 배정해 만들었으므로, 여기 실린 격자는 항상 의도한 그 구 하나로만
매핑된다. 테이블에 없는 격자(collector 설정이 바뀌어 새로운 격자가 들어오는 경우
등)에 한해서만 point-in-polygon -> 최근접 구 중심점 순으로 기하 계산에 fallback한다.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

from shapely.geometry import Point, shape

_BOUNDARY_PATH = Path(__file__).parent / "assets" / "seoul_gu_boundary.geojson"

# 서울 시내 격자(5km 간격)가 인접 구로 잘못 배정되는 것은 막지 않되, 부산처럼 서울과
# 무관한 좌표까지 "가장 가까운 구"로 우겨넣는 것은 막기 위한 거리 상한(degree).
# 서울 시가지 반경(~15km)의 두 배 이상 여유를 둔 값이다.
_MAX_NEAREST_GU_DEGREES = 0.3

# collector/sources/weather_ultra_short_term.yaml, weather_short_term_forecast.yaml의
# grids 목록과 반드시 같은 25개 좌표를 유지해야 한다(각 구 중심점에 가장 가까운
# 정수 격자를 그리디 배정한 결과 — loader/implementation-plan.md 참고).
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
    global _GU_POLYGONS
    if _GU_POLYGONS is None:
        data = json.loads(_BOUNDARY_PATH.read_text(encoding="utf-8"))
        _GU_POLYGONS = [
            (feature["properties"]["SIG_KOR_NM"], shape(feature["geometry"]))
            for feature in data["features"]
        ]
    return _GU_POLYGONS


def _load_gu_centroids() -> list[tuple[str, float, float]]:
    global _GU_CENTROIDS
    if _GU_CENTROIDS is None:
        _GU_CENTROIDS = [
            (gu_name, polygon.centroid.y, polygon.centroid.x) for gu_name, polygon in _load_gu_polygons()
        ]
    return _GU_CENTROIDS


def latlon_to_gu(lat: float, lon: float) -> str | None:
    """위경도(WGS84)가 속한 서울 자치구 이름을 반환한다. 서울 밖이면 None."""
    point = Point(lon, lat)
    for gu_name, polygon in _load_gu_polygons():
        if polygon.contains(point):
            return gu_name
    return None


def _nearest_gu(lat: float, lon: float) -> str | None:
    """25개 구 중심점 중 (lat, lon)에 가장 가까운 구를 반환한다. 상한 밖이면 None."""
    nearest_gu, nearest_dist = None, None
    for gu_name, gu_lat, gu_lon in _load_gu_centroids():
        dist = math.hypot(lat - gu_lat, lon - gu_lon)
        if nearest_dist is None or dist < nearest_dist:
            nearest_gu, nearest_dist = gu_name, dist
    if nearest_dist is not None and nearest_dist <= _MAX_NEAREST_GU_DEGREES:
        return nearest_gu
    return None


# 기상청 격자(nx, ny) <-> 위경도 변환 계수. 기상청이 공개한 Lambert Conformal Conic
# 변환식(격자 간격 5km, 표준위도 30/60도, 기준점 (128E, 38N) -> (nx=43, ny=136))을 그대로 쓴다.
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
    """기상청 격자(nx, ny)를 (lat, lon)으로 변환한다."""
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
    """기상청 격자(nx, ny)가 속한 서울 자치구 이름을 반환한다. 서울과 무관하면 None.

    `_GRID_TO_GU_TABLE`에 있는 격자는 그 값을 그대로 쓴다(25개 구 전부를 보장하는
    고정 배정). 테이블에 없는 격자만 point-in-polygon -> 최근접 구 중심점 순으로
    기하 계산에 fallback한다.
    """
    key = (int(nx), int(ny))
    if key in _GRID_TO_GU_TABLE:
        return _GRID_TO_GU_TABLE[key]

    lat, lon = grid_to_latlon(nx, ny)
    return latlon_to_gu(lat, lon) or _nearest_gu(lat, lon)
