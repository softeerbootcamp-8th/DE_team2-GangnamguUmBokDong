"""서울 생활인구 격자 베이스라인과 실시간 POI 인구를 공간 정규화하는 CLI 진입점.

한 번 실행하면 **현재 시각 + 실시간 도시데이터가 주는 향후 12시간 예측 시각**을 각각
보정해 그 시각의 tick 키에 쓴다. 미래 시각의 baseline은 nowcaster가 만든 추정치이고
(`storage.read_nowcast_grid`), 보정값은 `FCST_n_*` 컬럼이다. 소비자(추론기)는 미래
시각으로 조회하면 그 파일을 그대로 읽는다 — 조회 경로가 현재분과 같다.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from datetime import date, datetime, timedelta, tzinfo

# pyrefly: ignore [missing-import]
import pyarrow as pa

import grid
import merge
import poi
import storage

# 실시간 도시데이터의 예측 시각 포맷(실측: "2026-08-19 22:00", KST 정시).
_FORECAST_TIME_FORMAT = "%Y-%m-%d %H:%M"
# 어댑터가 펼쳐 놓은 슬롯 수. 슬롯 번호는 "n시간 후"가 아니라 시각 오름차순 순번이다.
_FORECAST_SLOTS = 12
# 이보다 먼 예측은 쓰지 않는다. API가 간격을 바꿔도 미래로 무한히 번지지 않게 하는 상한.
_MAX_HORIZON = timedelta(hours=12)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.

    args:
        argv: 파싱할 인자 목록 (생략 시 sys.argv 사용)
    returns:
        window_start가 포함된 Namespace
    """
    parser = argparse.ArgumentParser(prog="main.py")
    parser.add_argument("--window-start", required=True, help="ISO8601, KST 오프셋(+09:00) 포함")
    return parser.parse_args(argv)


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


def _parse_forecast_time(raw: object, tzinfo: tzinfo | None) -> datetime | None:
    """`FCST_n_TIME` 문자열을 window_start와 같은 시간대의 datetime으로 바꾼다. 실패하면 None."""
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.strptime(raw, _FORECAST_TIME_FORMAT)  # noqa: DTZ007 - 아래에서 tz를 붙인다
    except ValueError:
        return None
    return parsed.replace(tzinfo=tzinfo)


def _forecast_by_time(row: dict, tzinfo: tzinfo | None) -> dict[datetime, tuple[float, float]]:
    """POI 한 행의 예측 슬롯을 {예측시각: (MIN, MAX)}로 바꾼다.

    슬롯 번호를 "n시간 후"로 해석하지 않는다 — 실측(2026-08-19 20:55 관측)에서 첫 슬롯이
    22:00이었다. 어느 시각의 예측인지는 `FCST_n_TIME`만이 말한다.
    """
    if row.get("FCST_YN") != "Y":
        return {}

    forecasts: dict[datetime, tuple[float, float]] = {}
    for slot in range(1, _FORECAST_SLOTS + 1):
        target = _parse_forecast_time(row.get(f"FCST_{slot}_TIME"), tzinfo)
        pop_min = row.get(f"FCST_{slot}_PPLTN_MIN")
        pop_max = row.get(f"FCST_{slot}_PPLTN_MAX")
        if target is None or pop_min is None or pop_max is None:
            continue
        forecasts[target] = (float(pop_min), float(pop_max))
    return forecasts


def _collect_forecasts(
    realtime_table: pa.Table, window_start: datetime
) -> dict[str, dict[datetime, tuple[float, float]]]:
    """POI 코드별 {예측시각: (MIN, MAX)} 맵을 만든다. 과거·12시간 초과 시각은 버린다."""
    horizon_end = window_start + _MAX_HORIZON
    by_code: dict[str, dict[datetime, tuple[float, float]]] = {}
    for row in realtime_table.to_pylist():
        forecasts = {
            target: bounds
            for target, bounds in _forecast_by_time(row, window_start.tzinfo).items()
            if window_start < target <= horizon_end
        }
        if forecasts:
            by_code[row["AREA_CD"]] = forecasts
    return by_code


def _forecast_targets(
    forecasts_by_code: dict[str, dict[datetime, tuple[float, float]]]
) -> list[datetime]:
    """보정할 미래 시각 목록(오름차순). POI마다 슬롯이 어긋나도 합집합으로 모은다."""
    return sorted({target for forecasts in forecasts_by_code.values() for target in forecasts})


def _build_forecast_snapshots(
    poi_areas: tuple[poi.PoiArea, ...],
    forecasts_by_code: dict[str, dict[datetime, tuple[float, float]]],
    target: datetime,
) -> dict[str, merge.PoiSnapshot]:
    """해당 미래 시각의 PoiSnapshot 맵을 만든다.

    성비(`male_rate`/`female_rate`)는 넣지 않는다(0.0) — 예측값에 성비가 없어 미래 시각은
    `merge.merge_cell_total_only()`가 총량만 합성하고 성·연령 구성은 baseline을 따른다.

    args:
        poi_areas: POI 영역 지오메트리 목록
        forecasts_by_code: POI 코드별 예측 맵
        target: 대상 예측 시각
    returns:
        POI 코드를 키로 하는 PoiSnapshot 맵(그 시각 예측이 없는 POI는 빠진다)
    """
    snapshots: dict[str, merge.PoiSnapshot] = {}
    for area in poi_areas:
        bounds = forecasts_by_code.get(area.area_cd, {}).get(target)
        if bounds is None:
            continue
        pop_min, pop_max = bounds
        snapshots[area.area_cd] = merge.PoiSnapshot(
            area_cd=area.area_cd,
            male_rate=0.0,
            female_rate=0.0,
            pop_estimate=(pop_min + pop_max) / 2.0,
            geometry=area.geometry,
            area_m2=area.area_m2,
        )
    return snapshots


_OUTPUT_SCHEMA = pa.schema(
    [("CELL_ID", pa.string()), ("H_DNG_CD", pa.string()), ("SPOP", pa.int64())]
    + [(c, pa.int64()) for c in merge.AGE_COLUMNS]
)


def _baseline_cells(
    baseline_cache: dict[date, pa.Table], target: datetime
) -> dict[str, merge.GridCell]:
    """해당 시각의 baseline 격자(nowcaster 추정치)를 GridCell 맵으로 돌려준다.

    날짜별 테이블을 캐시한다 — 12시간 앞이 자정을 넘으면 두 날짜(오늘/내일)를 읽게 되고,
    시각마다 다시 읽으면 같은 파일을 13번 내려받는다.
    """
    target_date = target.date()
    if target_date not in baseline_cache:
        baseline_cache[target_date] = storage.read_nowcast_grid(target_date)
    return _filter_grid_rows_for_hour(baseline_cache[target_date], target.hour)


def _write_normalized(
    target: datetime,
    cells_by_id: dict[str, merge.GridCell],
    overlap_areas: dict[str, list[tuple[str, float]]],
    snapshots_by_code: dict[str, merge.PoiSnapshot],
    merge_cell: Callable[[merge.GridCell, list[tuple[merge.PoiSnapshot, float]]], merge.MergedCell],
) -> str:
    """한 시각의 보정 결과를 그 시각의 tick 키에 쓰고 키를 돌려준다."""
    output_rows = [
        merge.round_output_row(
            merge_cell(cell, merge.bind_snapshots(overlap_areas.get(cell_id, []), snapshots_by_code))
        )
        for cell_id, cell in cells_by_id.items()
    ]
    table = pa.Table.from_pylist(output_rows, schema=_OUTPUT_SCHEMA)
    return storage.write_normalized_silver(target, table)


def run(window_start: datetime) -> int:
    """현재 시각과 향후 12시간 예측 시각을 각각 보정해 그 시각의 Silver tick에 쓴다.

    현재 시각은 실측 POI 인구와 성비로 성·연령까지 재분배하고(`merge.merge_cell`), 미래
    시각은 예측 인구로 총량만 합성한다(`merge.merge_cell_total_only`) — 예측값에는 성비가
    없다. 시각과 무관한 교차 면적은 한 번만 계산해 모든 시각에 재사용한다.

    args:
        window_start: 수집 기준 시각
    returns:
        종료 코드 (성공 시 0)
    """
    realtime_table = storage.read_realtime_silver(window_start)
    poi_areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)

    forecasts_by_code = _collect_forecasts(realtime_table, window_start)
    targets = _forecast_targets(forecasts_by_code)

    baseline_cache: dict[date, pa.Table] = {}
    cells_by_target: dict[datetime, dict[str, merge.GridCell]] = {}
    skipped: list[str] = []
    for target in [window_start, *targets]:
        try:
            cells = _baseline_cells(baseline_cache, target)
        except storage.PartitionNotFoundError:
            # 현재 시각의 baseline이 없으면 보정 자체가 불가능하므로 실패로 올린다.
            # 미래 날짜만 없으면 그 시각을 건너뛰고 나머지를 쓴다.
            if target == window_start:
                raise
            skipped.append(target.isoformat())
            continue
        if cells:
            cells_by_target[target] = cells
        else:
            skipped.append(target.isoformat())

    # 교차 면적은 지오메트리만 보므로 시각과 무관하다 — 모든 시각의 셀 합집합으로 1회 계산한다.
    union_cells: dict[str, merge.GridCell] = {}
    for cells in cells_by_target.values():
        for cell_id, cell in cells.items():
            union_cells.setdefault(cell_id, cell)
    overlap_areas = merge.find_overlap_areas(list(union_cells.values()), list(poi_areas))

    observed_snapshots = _build_poi_snapshots(poi_areas, realtime_table)
    written: dict[str, str] = {}
    if window_start in cells_by_target:
        written[window_start.isoformat()] = _write_normalized(
            window_start,
            cells_by_target[window_start],
            overlap_areas,
            {snapshot.area_cd: snapshot for snapshot in observed_snapshots},
            merge.merge_cell,
        )

    for target in targets:
        if target not in cells_by_target:
            continue
        written[target.isoformat()] = _write_normalized(
            target,
            cells_by_target[target],
            overlap_areas,
            _build_forecast_snapshots(poi_areas, forecasts_by_code, target),
            merge.merge_cell_total_only,
        )

    storage.write_manifest(
        window_start,
        {
            "baseline_dates": sorted(d.isoformat() for d in baseline_cache),
            "cell_count": len(union_cells),
            "poi_matched_count": len(observed_snapshots),
            "poi_forecast_count": len(forecasts_by_code),
            "forecast_horizons": len(targets),
            "written_keys": written,
            "skipped_targets": skipped,
        },
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점으로 인자를 파싱하고 정규화 파이프라인을 실행한다."""
    args = parse_args(argv)
    window_start = datetime.fromisoformat(args.window_start)
    try:
        return run(window_start)
    except (storage.PartitionNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
