"""기상청 5km 격자 <-> 위경도 변환 계약을 검증한다.

loader/normalizer/격자 목록 생성 스크립트가 모두 이 한 곳의 공식을 쓴다.
"""

from core.weather_grid import grid_to_latlon, latlon_to_grid

GANGNAM_GU_OFFICE = (37.5172, 127.0473)
SEOUL_CITY_HALL = (37.5663, 126.9779)

# 운영 격자 34개 중 서울 중심부에 해당하는 표본
SEOUL_GRIDS = [(61, 125), (60, 127), (62, 129), (58, 124)]


def test_latlon_to_grid_maps_known_seoul_points():
    assert latlon_to_grid(*GANGNAM_GU_OFFICE) == (61, 126)
    assert latlon_to_grid(*SEOUL_CITY_HALL) == (60, 127)


def test_latlon_to_grid_returns_int_tuple():
    nx, ny = latlon_to_grid(*GANGNAM_GU_OFFICE)
    assert isinstance(nx, int)
    assert isinstance(ny, int)


def test_latlon_to_grid_roundtrips_grid_to_latlon():
    """격자 중심 좌표를 다시 격자로 바꾸면 같은 격자가 나온다(정확한 역변환)."""
    for nx, ny in SEOUL_GRIDS:
        lat, lon = grid_to_latlon(nx, ny)
        assert latlon_to_grid(lat, lon) == (nx, ny)
