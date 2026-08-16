from gu_mapping import _GRID_TO_GU_TABLE, _load_gu_centroids, grid_to_gu, grid_to_latlon, latlon_to_gu

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


def test_grid_to_gu_table_covers_all_25_gu():
    """point-in-polygon만으로는 좁은 구(예: 중구)에 격자점이 아예 없어서 절대 채워지지
    않는다. `_GRID_TO_GU_TABLE`이 collector가 요청하는 격자 목록과 1:1로 맞춰 25개 구
    전부를 정확히 하나씩 대표하는지 회귀로 잡는다."""
    all_gu = {name for name, _, _ in _load_gu_centroids()}
    table_gu = set(_GRID_TO_GU_TABLE.values())

    assert table_gu == all_gu
    assert len(_GRID_TO_GU_TABLE) == len(all_gu)  # 격자 하나당 구 하나(중복 배정 없음)
    for (nx, ny), expected_gu in _GRID_TO_GU_TABLE.items():
        assert grid_to_gu(nx, ny) == expected_gu
