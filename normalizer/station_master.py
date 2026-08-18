"""대여소 API 위경도에 생활인구 250m CELL_ID를 보강한다."""

from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime

import grid
import pyarrow as pa
import storage
from pyproj import Transformer
from shapely import STRtree
from shapely.geometry import Point

MIN_GRID_COVERAGE = 0.95

_TO_EPSG5179 = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)

_OUTPUT_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("station_no", pa.string()),
        ("station_name", pa.string()),
        ("capacity", pa.int64()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("grid_id", pa.string()),
    ]
)


def _number(value: object) -> float | None:
    """값을 유한한 실수로 변환하고 변환할 수 없으면 None을 반환한다."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _valid_wgs84(lat: object, lon: object) -> bool:
    """서울 인근에서 유효한 WGS84 위경도인지 확인한다."""
    latitude = _number(lat)
    longitude = _number(lon)
    return (
        latitude is not None
        and longitude is not None
        and 36.5 <= latitude <= 38.5
        and 125.5 <= longitude <= 128.5
    )


def _realtime_by_station(table: pa.Table | None) -> dict[str, dict]:
    """실시간 대여소 표를 stationId 기준 최신 행 사전으로 바꾼다."""
    if table is None:
        return {}
    return {
        str(row["stationId"]): row
        for row in table.to_pylist()
        if row.get("stationId") is not None
    }


def enrich_station_master(
    master_table: pa.Table,
    grid_table: pa.Table,
    realtime_table: pa.Table | None = None,
) -> tuple[pa.Table, dict[str, int | float]]:
    """대여소 위경도를 실제 생활인구 격자 폴리곤과 공간 조인한다.

    API master의 좌표가 0이거나 결측이면 최신 실시간 대여소 좌표를 사용한다.
    격자 목록은 생활인구 baseline의 CELL_ID에서 가져오며 동일 CELL_ID의 24시간
    행은 하나로 중복 제거한다.
    """
    cell_ids = sorted(
        {
            str(row["CELL_ID"])
            for row in grid_table.to_pylist()
            if row.get("CELL_ID") is not None
        }
    )
    if not cell_ids:
        raise ValueError("living_population_grid에 CELL_ID가 없음")

    polygons = [grid.cell_id_to_polygon(cell_id) for cell_id in cell_ids]
    tree = STRtree(polygons)
    realtime = _realtime_by_station(realtime_table)

    rows_by_station: dict[str, dict] = {}
    for raw in master_table.to_pylist():
        if raw.get("RNTLS_ID") is None:
            continue
        station_id = str(raw["RNTLS_ID"])
        live = realtime.get(station_id, {})

        lat = raw.get("LAT")
        lon = raw.get("LOT")
        if not _valid_wgs84(lat, lon) and _valid_wgs84(
            live.get("stationLatitude"), live.get("stationLongitude")
        ):
            lat = live.get("stationLatitude")
            lon = live.get("stationLongitude")

        latitude = _number(lat)
        longitude = _number(lon)
        grid_id = None
        if _valid_wgs84(latitude, longitude):
            x, y = _TO_EPSG5179.transform(longitude, latitude)
            point = Point(x, y)
            candidates = tree.query(point)
            matches = [int(index) for index in candidates if polygons[int(index)].covers(point)]
            if matches:
                grid_id = cell_ids[matches[0]]

        capacity_value = _number(live.get("rackTotCnt"))
        rows_by_station[station_id] = {
            "station_id": station_id,
            "station_no": str(raw["ADDR2"]) if raw.get("ADDR2") is not None else None,
            "station_name": live.get("stationName") or raw.get("ADDR1") or raw.get("ADDR2"),
            "capacity": int(capacity_value) if capacity_value is not None else None,
            "lat": latitude,
            "lon": longitude,
            "grid_id": grid_id,
        }

    rows = list(rows_by_station.values())
    if not rows:
        raise ValueError("bike_station_master에 유효한 대여소가 없음")
    mapped_count = sum(row["grid_id"] is not None for row in rows)
    coverage = mapped_count / len(rows)
    if coverage < MIN_GRID_COVERAGE:
        raise ValueError(
            f"station master CELL_ID 매핑률이 기준 미달: {coverage:.3%} < {MIN_GRID_COVERAGE:.1%}"
        )
    return pa.Table.from_pylist(rows, schema=_OUTPUT_SCHEMA), {
        "station_count": len(rows),
        "grid_mapped_count": mapped_count,
        "grid_unmapped_count": len(rows) - mapped_count,
        "grid_coverage": coverage,
    }


def run(window_start: datetime, baseline_date_mode: str) -> int:
    """같은 window의 API master를 보강해 파티션 Silver와 manifest를 쓴다."""
    if baseline_date_mode == "strict":
        baseline_date = window_start.date()
        if not storage.partition_exists(storage.GRID_SOURCE_ID, baseline_date):
            raise storage.PartitionNotFoundError(
                f"living_population_grid의 dt={baseline_date:%Y-%m-%d} 파티션이 없음(strict 모드)"
            )
    else:
        baseline_date = storage.find_latest_partition_date_on_or_before(
            storage.GRID_SOURCE_ID, window_start.date()
        )

    master_table = storage.read_station_master_silver(window_start)
    grid_table = storage.read_grid_silver(baseline_date)
    realtime_table = storage.read_latest_bike_realtime_silver(window_start)
    output, metrics = enrich_station_master(master_table, grid_table, realtime_table)
    output_key = storage.write_enriched_station_master(window_start, output)
    storage.write_manifest(
        window_start,
        {
            "source_id": storage.ENRICHED_STATION_MASTER_SOURCE_ID,
            "baseline_date": baseline_date.isoformat(),
            "baseline_date_mode": baseline_date_mode,
            "output_key": output_key,
            **metrics,
        },
        source_id=storage.ENRICHED_STATION_MASTER_SOURCE_ID,
    )
    print(
        f"station master enriched rows={metrics['station_count']} "
        f"mapped={metrics['grid_mapped_count']} coverage={metrics['grid_coverage']:.3%} "
        f"output={output_key}"
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(prog="station_master.py")
    parser.add_argument("--window-start", required=True, help="ISO8601, KST 오프셋(+09:00) 포함")
    parser.add_argument(
        "--baseline-date-mode",
        choices=["strict", "latest"],
        default="latest",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI 실행 오류를 Airflow가 감지할 수 있는 종료 코드로 바꾼다."""
    args = parse_args(argv)
    try:
        return run(datetime.fromisoformat(args.window_start), args.baseline_date_mode)
    except (storage.PartitionNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
