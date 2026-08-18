from gu_mapping import (
    grid_to_gu,
    grid_to_latlon,
    latlon_to_grid,
    latlon_to_gu,
)

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
