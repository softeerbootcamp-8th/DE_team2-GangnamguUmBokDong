import json
import math
from pathlib import Path

from gu_mapping import (
    grid_to_gu,
    grid_to_latlon,
    latlon_to_grid,
    latlon_to_gu,
)

_STATIONS_PATH = Path(__file__).resolve().parents[2] / "apps" / "api" / "seed_data" / "stations_seoul.json"

# 삭제된 gu_mapping._GRID_TO_GU_TABLE의 스냅샷. 개선 전 오차를 재현하기 위한
# 회귀 비교 기준으로만 쓴다 — 프로덕션 코드는 더 이상 이 테이블을 쓰지 않는다.
_OLD_GRID_TO_GU_TABLE = {
    (61, 125): "강남구", (63, 126): "강동구", (60, 129): "강북구", (57, 127): "강서구",
    (59, 124): "관악구", (62, 126): "광진구", (58, 125): "구로구", (58, 124): "금천구",
    (62, 129): "노원구", (61, 129): "도봉구", (61, 127): "동대문구", (59, 125): "동작구",
    (58, 127): "마포구", (59, 127): "서대문구", (61, 124): "서초구", (61, 126): "성동구",
    (61, 128): "성북구", (62, 125): "송파구", (58, 126): "양천구", (59, 126): "영등포구",
    (60, 126): "용산구", (59, 128): "은평구", (60, 127): "종로구", (60, 128): "중구",
    (62, 127): "중랑구",
}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


GANGNAM_GU_OFFICE = (37.5172, 127.0473)
SEOUL_CITY_HALL = (37.5663, 126.9779)
BUSAN_CITY_HALL = (35.1796, 129.0756)


def test_latlon_to_gu_known_points():
    assert latlon_to_gu(*GANGNAM_GU_OFFICE) == "강남구"
    assert latlon_to_gu(*SEOUL_CITY_HALL) == "중구"


def test_latlon_to_gu_outside_seoul_returns_none():
    assert latlon_to_gu(*BUSAN_CITY_HALL) is None


def test_grid_to_latlon_roundtrips_within_seoul():
    lat, lon = grid_to_latlon(60, 127)
    assert 37.0 < lat < 38.0
    assert 126.5 < lon < 127.5


def test_grid_to_gu_maps_known_seoul_grid():
    assert grid_to_gu(60, 127) is not None


def test_grid_to_gu_uses_geometry_not_hardcoded_table():
    """(60, 128)은 삭제된 하드코딩 배정 테이블에서 '중구'로 잘못 배정돼 있었지만,
    실제로는 성북구 영역에 위치한다. 순수 지오메트리 기반 판정으로 바뀐 뒤에는
    실제 위치인 성북구를 반환해야 한다(회귀 방지)."""
    assert grid_to_gu(60, 128) == "성북구"


def test_latlon_to_grid_roundtrips_grid_to_latlon():
    for nx, ny in [(60, 127), (61, 125), (57, 127), (63, 126)]:
        lat, lon = grid_to_latlon(nx, ny)
        assert latlon_to_grid(lat, lon) == (nx, ny)


def test_latlon_to_grid_returns_int_tuple():
    nx, ny = latlon_to_grid(*GANGNAM_GU_OFFICE)
    assert isinstance(nx, int)
    assert isinstance(ny, int)


def test_grid_matching_error_shrinks_after_switching_to_nearest_grid():
    """개선 전(구 단위 고정 격자 25개)과 개선 후(실제 최근접 격자) 매칭 오차를
    실제 대여소 2,746곳으로 비교한다. 조사 당시 확인한 수치(개선 전 평균
    3.27km/최대 9.21km, 개선 후 평균 2.00km/최대 3.58km)를 회귀로 고정한다."""
    stations = json.loads(_STATIONS_PATH.read_text(encoding="utf-8"))
    gu_to_old_grid = {gu: grid for grid, gu in _OLD_GRID_TO_GU_TABLE.items()}

    old_distances = []
    new_distances = []
    for station in stations:
        lat, lon = station["lat"], station["lon"]

        old_grid = gu_to_old_grid[station["gu"]]
        old_lat, old_lon = grid_to_latlon(*old_grid)
        old_distances.append(_haversine_km(lat, lon, old_lat, old_lon))

        new_grid = latlon_to_grid(lat, lon)
        new_lat, new_lon = grid_to_latlon(*new_grid)
        new_distances.append(_haversine_km(lat, lon, new_lat, new_lon))

    print(f"개선 전: 평균 {sum(old_distances) / len(old_distances):.2f}km, 최대 {max(old_distances):.2f}km")
    print(f"개선 후: 평균 {sum(new_distances) / len(new_distances):.2f}km, 최대 {max(new_distances):.2f}km")

    # 5km 격자 한 칸의 절반 대각선(약 3.54km) + 투영 왜곡 여유분.
    # latlon_to_grid는 항상 실제 존재하는 모든 격자 중 최근접을 고르므로,
    # 어떤 대여소든 이 상한을 넘을 수 없다.
    assert max(new_distances) <= 3.6
    assert sum(new_distances) < sum(old_distances)
    assert all(new <= old + 1e-9 for new, old in zip(new_distances, old_distances))


def test_grid_conversion_is_the_shared_core_implementation():
    """격자 변환 공식은 `core.weather_grid` 한 곳에만 존재해야 한다.

    loader가 자체 구현을 되살리거나 재수출이 끊기면, 같은 대여소의
    `stations.grid_nx`(loader)와 `weather_nx`(normalizer)가 조용히 어긋날 수 있다.
    """
    from core import weather_grid

    assert latlon_to_grid is weather_grid.latlon_to_grid
    assert grid_to_latlon is weather_grid.grid_to_latlon
