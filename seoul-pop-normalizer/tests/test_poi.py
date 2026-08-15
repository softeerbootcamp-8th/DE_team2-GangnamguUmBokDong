"""poi.py: POI shapefile 로딩, 위상 오류 복구, 캐싱 테스트."""

import pytest
from shapely.geometry import GeometryCollection, LineString, Point

import poi

EXPECTED_MISSING_CODES = {
    "POI022", "POI028", "POI057", "POI062", "POI065",
    "POI069", "POI075", "POI097", "POI099", "POI113",
}


def test_load_real_shapefile_reads_121_areas():
    areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    assert len(areas) == 121
    assert len({a.area_cd for a in areas}) == 121


def test_area_codes_have_expected_gaps():
    areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    codes = {a.area_cd for a in areas}
    all_expected = {f"POI{i:03d}" for i in range(1, 132)} - EXPECTED_MISSING_CODES
    assert codes == all_expected


def test_all_geometries_are_valid_polygons_in_epsg5179():
    areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    for area in areas:
        assert area.geometry.geom_type == "Polygon"
        assert area.geometry.is_valid
        assert area.area_m2 > 0
        # 서울 지역 EPSG:5179 좌표는 대략 900,000~980,000 / 1,930,000~1,970,000 범위에 있어야 한다.
        minx, miny, maxx, maxy = area.geometry.bounds
        assert 900_000 < minx < maxx < 990_000
        assert 1_920_000 < miny < maxy < 1_980_000


def test_poi070_topology_error_is_recovered():
    """실측: POI070("쌍문역")은 shapely가 TopologyException을 던질 정도로 자기교차한다."""
    areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    poi070 = next(a for a in areas if a.area_cd == "POI070")
    assert poi070.area_nm == "쌍문역"
    assert poi070.geometry.geom_type == "Polygon"
    assert poi070.geometry.is_valid
    assert poi070.area_m2 > 0


def test_load_poi_areas_is_cached():
    first = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    second = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    assert first is second  # lru_cache로 동일 튜플 객체가 반환돼야 함


def test_select_largest_polygon_raises_when_no_polygon_remains():
    """make_valid 결과에 Polygon 조각이 하나도 없으면 명확히 실패해야 한다."""
    collection = GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)])])
    with pytest.raises(ValueError, match="Polygon 조각이 없음"):
        poi._select_largest_polygon(collection, area_cd="POI999")


def test_select_largest_polygon_picks_biggest_piece_from_multipolygon():
    from shapely.geometry import MultiPolygon, box

    small = box(0, 0, 1, 1)  # area 1
    big = box(10, 10, 13, 13)  # area 9
    multi = MultiPolygon([small, big])
    result = poi._select_largest_polygon(multi, area_cd="POI999")
    assert result.equals(big)
