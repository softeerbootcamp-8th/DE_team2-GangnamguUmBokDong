"""grid.py: CELL_ID <-> EPSG:5179 좌표 변환, 격자 폴리곤 생성 테스트."""

import pytest

from grid import GRID_AREA_M2, cell_id_to_epsg5179_sw_corner, cell_id_to_polygon


def test_known_cell_sw_corner_matches_measured_value():
    """plan.md에 실측으로 검증된 좌표: 다사53815262 -> (953810.0, 1952620.0)."""
    assert cell_id_to_epsg5179_sw_corner("다사53815262") == (953810.0, 1952620.0)


def test_ga_ga_0000_0000_is_grid_origin():
    """가가00000000 -> 격자 좌표계의 원점(700000, 1300000)."""
    assert cell_id_to_epsg5179_sw_corner("가가00000000") == (700000.0, 1300000.0)


def test_polygon_is_250m_square_at_sw_corner():
    poly = cell_id_to_polygon("다사53815262")
    assert poly.bounds == (953810.0, 1952620.0, 954060.0, 1952870.0)


def test_polygon_area_is_62500_m2():
    poly = cell_id_to_polygon("다사53815262")
    assert poly.area == GRID_AREA_M2 == 62500.0


def test_invalid_length_raises_value_error():
    with pytest.raises(ValueError):
        cell_id_to_epsg5179_sw_corner("다사5381526")  # 9자리(1자리 부족)


def test_invalid_digits_raises_value_error():
    with pytest.raises(ValueError):
        cell_id_to_epsg5179_sw_corner("다사5381526X")


def test_unknown_letter_raises_value_error():
    with pytest.raises(ValueError):
        cell_id_to_epsg5179_sw_corner("ZZ53815262")
