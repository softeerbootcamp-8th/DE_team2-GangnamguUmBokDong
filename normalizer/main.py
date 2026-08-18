"""서울 생활인구 격자 베이스라인과 실시간 POI 인구를 공간 정규화하는 CLI 진입점."""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime

# pyrefly: ignore [missing-import]
import pyarrow as pa

import grid
import merge
import poi
import storage

DEFAULT_BASELINE_MODE = "strict"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.

    args:
        argv: 파싱할 인자 목록 (생략 시 sys.argv 사용)
    returns:
        window_start 및 baseline_date_mode가 포함된 Namespace
    """
    parser = argparse.ArgumentParser(prog="main.py")
    parser.add_argument("--window-start", required=True, help="ISO8601, KST 오프셋(+09:00) 포함")
    parser.add_argument(
        "--baseline-date-mode",
        choices=["strict", "latest"],
        default=DEFAULT_BASELINE_MODE,
        help="living_population_grid baseline 날짜 선택 모드(기본 strict)",
    )
    return parser.parse_args(argv)


def _resolve_baseline_date(window_start: datetime, mode: str) -> date:
    """모드에 따라 사용할 격자 인구 베이스라인 날짜를 결정한다.

    args:
        window_start: 수집 기준 시각
        mode: 베이스라인 선택 모드 ('strict'는 당일, 'latest'는 최신 파티션)
    returns:
        결정된 베이스라인 날짜
    raises:
        storage.PartitionNotFoundError: strict 모드에서 해당 일자 파티션이 없을 때
    """
    if mode == "strict":
        baseline_date = window_start.date()
        if not storage.partition_exists(storage.GRID_SOURCE_ID, baseline_date):
            raise storage.PartitionNotFoundError(
                f"living_population_grid의 dt={baseline_date:%Y-%m-%d} 파티션이 없음(strict 모드)"
            )
        return baseline_date
    return storage.find_latest_partition_date(storage.GRID_SOURCE_ID)


def _filter_grid_rows_for_hour(grid_table: pa.Table, hour: int) -> dict[str, merge.GridCell]:
    """해당 시간대(TT)의 격자 데이터만 필터링하여 GridCell 맵으로 변환한다.

    args:
        grid_table: 24시간 생활인구 격자 테이블
        hour: 대상 시각 (0~23)
    returns:
        CELL_ID를 키로 하는 GridCell 딕셔너리
    """
    hour_str = f"{hour:02d}"
    cells: dict[str, merge.GridCell] = {}
    for row in grid_table.to_pylist():
        if row["TT"] != hour_str:
            continue
        ages = {c: (row.get(c) or 0.0) for c in merge.AGE_COLUMNS}
        cells[row["CELL_ID"]] = merge.GridCell(
            cell_id=row["CELL_ID"],
            h_dng_cd=row["H_DNG_CD"],
            spop=float(row["SPOP"] or 0.0),
            ages=ages,
            geometry=grid.cell_id_to_polygon(row["CELL_ID"]),
        )
    return cells


def _build_poi_snapshots(
    poi_areas: tuple[poi.PoiArea, ...], realtime_table: pa.Table
) -> list[merge.PoiSnapshot]:
    """POI 지오메트리와 실시간 인구 관측치를 결합하여 PoiSnapshot 목록을 생성한다.

    args:
        poi_areas: POI 영역 지오메트리 목록
        realtime_table: 실시간 POI 인구 수집 테이블
    returns:
        결합된 PoiSnapshot 목록
    """
    realtime_by_code = {row["AREA_CD"]: row for row in realtime_table.to_pylist()}
    snapshots: list[merge.PoiSnapshot] = []
    for area in poi_areas:
        realtime_row = realtime_by_code.get(area.area_cd)
        if realtime_row is None:
            continue
        pop_estimate = (realtime_row["AREA_PPLTN_MIN"] + realtime_row["AREA_PPLTN_MAX"]) / 2.0
        snapshots.append(
            merge.PoiSnapshot(
                area_cd=area.area_cd,
                male_rate=float(realtime_row["MALE_PPLTN_RATE"]),
                female_rate=float(realtime_row["FEMALE_PPLTN_RATE"]),
                pop_estimate=pop_estimate,
                geometry=area.geometry,
                area_m2=area.area_m2,
            )
        )
    return snapshots


_OUTPUT_SCHEMA = pa.schema(
    [("CELL_ID", pa.string()), ("H_DNG_CD", pa.string()), ("SPOP", pa.int64())]
    + [(c, pa.int64()) for c in merge.AGE_COLUMNS]
)


def run(window_start: datetime, baseline_date_mode: str) -> int:
    """격자 베이스라인과 실시간 POI 인구를 합성하여 정규화된 Silver 테이블을 생성한다.

    args:
        window_start: 수집 기준 시각
        baseline_date_mode: 베이스라인 날짜 선택 모드 ('strict' 또는 'latest')
    returns:
        종료 코드 (성공 시 0)
    """
    baseline_date = _resolve_baseline_date(window_start, baseline_date_mode)

    grid_table = storage.read_grid_silver(baseline_date)
    cells_by_id = _filter_grid_rows_for_hour(grid_table, window_start.hour)

    realtime_table = storage.read_realtime_silver(window_start)

    poi_areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
    poi_snapshots = _build_poi_snapshots(poi_areas, realtime_table)

    cells = list(cells_by_id.values())
    overlaps = merge.find_overlaps(cells, poi_snapshots)

    output_rows = [
        merge.round_output_row(merge.merge_cell(cell, overlaps.get(cell.cell_id, [])))
        for cell in cells
    ]

    table = pa.Table.from_pylist(output_rows, schema=_OUTPUT_SCHEMA)
    storage.write_normalized_silver(window_start, table)
    storage.write_manifest(
        window_start,
        {
            "baseline_date": baseline_date.isoformat(),
            "baseline_date_mode": baseline_date_mode,
            "cell_count": len(output_rows),
            "poi_matched_count": len(poi_snapshots),
        },
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점으로 인자를 파싱하고 정규화 파이프라인을 실행한다."""
    args = parse_args(argv)
    window_start = datetime.fromisoformat(args.window_start)
    try:
        return run(window_start, args.baseline_date_mode)
    except (storage.PartitionNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
