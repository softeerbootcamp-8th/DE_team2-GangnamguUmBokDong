"""대여소 master의 위경도-CELL_ID 공간 조인 계약을 검증한다."""

import pyarrow as pa
import pytest
from core.weather_grid import latlon_to_grid
from pyproj import Transformer

from station_master import _OUTPUT_SCHEMA, enrich_station_master


def _cell_center_wgs84(cell_id: str) -> tuple[float, float]:
    """테스트 CELL_ID 중심점을 (lat, lon)으로 변환한다."""
    from grid import cell_id_to_epsg5179_sw_corner

    x, y = cell_id_to_epsg5179_sw_corner(cell_id)
    transformer = Transformer.from_crs("EPSG:5179", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(x + 125, y + 125)
    return lat, lon


def test_enrich_station_master_maps_api_and_realtime_coordinates():
    """API 좌표와 API 결측을 보완한 실시간 좌표가 같은 CELL_ID로 매핑된다."""
    cell_id = "다사53815262"
    lat, lon = _cell_center_wgs84(cell_id)
    master = pa.Table.from_pylist(
        [
            {"RNTLS_ID": "ST-1", "ADDR1": "주소1", "ADDR2": "1", "LAT": lat, "LOT": lon},
            {"RNTLS_ID": "ST-2", "ADDR1": "주소2", "ADDR2": "2", "LAT": 0.0, "LOT": 0.0},
        ]
    )
    realtime = pa.Table.from_pylist(
        [
            {
                "stationId": "ST-2",
                "stationName": "실시간 이름",
                "rackTotCnt": "15",
                "stationLatitude": lat,
                "stationLongitude": lon,
            }
        ]
    )
    baseline = pa.Table.from_pylist([{"CELL_ID": cell_id}, {"CELL_ID": cell_id}])

    result, metrics = enrich_station_master(master, baseline, realtime)
    rows = {row["station_id"]: row for row in result.to_pylist()}

    assert rows["ST-1"]["grid_id"] == cell_id
    assert rows["ST-2"]["grid_id"] == cell_id
    assert rows["ST-2"]["station_name"] == "실시간 이름"
    assert rows["ST-2"]["capacity"] == 15
    assert metrics["grid_coverage"] == 1.0


def test_enrich_station_master_rejects_low_grid_coverage():
    """CELL_ID가 거의 연결되지 않으면 성공 파일을 쓰지 않도록 실패시킨다."""
    master = pa.Table.from_pylist(
        [{"RNTLS_ID": "ST-1", "ADDR1": "주소", "ADDR2": "1", "LAT": 0.0, "LOT": 0.0}]
    )
    baseline = pa.Table.from_pylist([{"CELL_ID": "다사53815262"}])

    with pytest.raises(ValueError, match="매핑률이 기준 미달"):
        enrich_station_master(master, baseline)


def test_enrich_station_master_adds_weather_grid_for_valid_coordinates():
    """유효 좌표 대여소에는 기상청 5km 격자(nx, ny)가 함께 실린다."""
    cell_id = "다사53815262"
    lat, lon = _cell_center_wgs84(cell_id)
    master = pa.Table.from_pylist(
        [{"RNTLS_ID": "ST-1", "ADDR1": "주소", "ADDR2": "1", "LAT": lat, "LOT": lon}]
    )
    baseline = pa.Table.from_pylist([{"CELL_ID": cell_id}])

    result, _ = enrich_station_master(master, baseline)
    [row] = result.to_pylist()

    assert (row["weather_nx"], row["weather_ny"]) == latlon_to_grid(lat, lon)


def test_enrich_station_master_weather_grid_is_none_when_coordinates_missing():
    """좌표가 유효하지 않은 대여소의 weather_nx/weather_ny는 None이다."""
    cell_id = "다사53815262"
    lat, lon = _cell_center_wgs84(cell_id)
    master = pa.Table.from_pylist(
        [{"RNTLS_ID": f"ST-{i}", "ADDR1": "주소", "ADDR2": str(i), "LAT": lat, "LOT": lon} for i in range(19)]
        + [{"RNTLS_ID": "ST-BAD", "ADDR1": "주소", "ADDR2": "bad", "LAT": 0.0, "LOT": 0.0}]
    )
    baseline = pa.Table.from_pylist([{"CELL_ID": cell_id}])

    result, metrics = enrich_station_master(master, baseline)
    rows = {row["station_id"]: row for row in result.to_pylist()}

    assert metrics["grid_coverage"] == 0.95
    assert rows["ST-BAD"]["weather_nx"] is None
    assert rows["ST-BAD"]["weather_ny"] is None


def test_output_schema_declares_weather_grid_columns():
    """스키마 계약: 소비 측이 컬럼 존재를 가정할 수 있어야 한다."""
    assert _OUTPUT_SCHEMA.field("weather_nx").type == pa.int64()
    assert _OUTPUT_SCHEMA.field("weather_ny").type == pa.int64()
