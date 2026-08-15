"""seoul-pop-normalizer CLI 진입점.

    cd seoul-pop-normalizer
    uv run python main.py --window-start 2026-08-15T14:05:00+09:00 [--baseline-date-mode latest]

exit 0 = 성공, non-zero = 실패(Airflow retry 대상).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime

import pyarrow as pa

import grid
import merge
import poi
import storage

DEFAULT_BASELINE_MODE = "strict"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.

    Args:
        argv: 파싱할 인자 목록. 생략하면 `sys.argv`를 그대로 쓴다.

    Returns:
        `window_start`, `baseline_date_mode` 필드를 담은 네임스페이스.
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
    """baseline 날짜를 결정한다.

    strict: window_start의 날짜. 해당 파티션이 없으면 예외(spec 3.7 — 조용한 폴백 금지).
    latest: S3에 존재하는 가장 최신 dt= 파티션(Airflow fallback 태스크 전용).
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
    """TT가 window_start.hour와 같은 행만 남기고, CELL_ID 중복은 마지막 값으로 정리한다.

    실측 버그(plan.md): living_population_grid는 CELL_ID당 하루 24행(TT별)을 가지므로
    이 필터링을 빠뜨리면 결과가 24배 가까이 부풀려진다. null 연령대(마스킹 `*`가
    collector에서 이미 null로 정규화된 것)는 0.0으로 취급한다(spec 3.5).
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
            spop=float(row["SPOP"]),
            ages=ages,
            geometry=grid.cell_id_to_polygon(row["CELL_ID"]),
        )
    return cells


def _build_poi_snapshots(
    poi_areas: tuple[poi.PoiArea, ...], realtime_table: pa.Table
) -> list[merge.PoiSnapshot]:
    """POI 폴리곤과 해당 window의 실시간 인구 행을 AREA_CD로 조인한다.

    shapefile에는 있지만 이번 window의 population_realtime 응답에 없는 AREA_CD는
    이번 window에 영향이 없는 것으로 보고 건너뛴다(이 계획의 설계 가정 — spec에
    명시되지 않은 부분).
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
    """무상태 정규화 실행 1회. 항상 서울 전체 격자를 출력한다(spec 3.6)."""
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
    args = parse_args(argv)
    window_start = datetime.fromisoformat(args.window_start)
    try:
        return run(window_start, args.baseline_date_mode)
    except (storage.PartitionNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
