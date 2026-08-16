"""공간 조인(STRtree), 밀도 순차 갱신, 연령/성별 재분배.

핵심 공식(spec 3.4/3.5, plan.md 4절):

    D_new = (1 - W_intersect) * D_current + W_intersect * D_poi
    SPOP_new = D_new * GRID_AREA_M2

여러 POI가 한 격자에 겹치면 면적 내림차순(동률이면 AREA_CD 오름차순)으로
정렬해 순차 적용한다(작은 POI가 마지막에 덮어씀). 연령/성별 재분배는
마지막에 적용된 POI의 성비를 쓰고, 그 안에서 원본 연령대 비중을 그대로
유지한다(spec 3.5의 (A) 채택안).
"""

from __future__ import annotations

from dataclasses import dataclass

from shapely import STRtree
from shapely.geometry.base import BaseGeometry

from grid import GRID_AREA_M2

MALE_AGE_COLUMNS = (
    "M00", "M10", "M15", "M20", "M25", "M30", "M35",
    "M40", "M45", "M50", "M55", "M60", "M65", "M70",
)
FEMALE_AGE_COLUMNS = (
    "F00", "F10", "F15", "F20", "F25", "F30", "F35",
    "F40", "F45", "F50", "F55", "F60", "F65", "F70",
)
AGE_COLUMNS = MALE_AGE_COLUMNS + FEMALE_AGE_COLUMNS


@dataclass(frozen=True)
class GridCell:
    """living_population_grid에서 온 격자 하나(해당 window의 TT 필터링 완료)."""

    cell_id: str
    h_dng_cd: str
    spop: float
    ages: dict[str, float]  # 28개 키, null은 호출자가 0.0으로 치환해서 넘김
    geometry: BaseGeometry


@dataclass(frozen=True)
class PoiSnapshot:
    """population_realtime의 해당 window 행 + POI 폴리곤을 합친 스냅샷."""

    area_cd: str
    male_rate: float
    female_rate: float
    pop_estimate: float  # (AREA_PPLTN_MIN + AREA_PPLTN_MAX) / 2
    geometry: BaseGeometry
    area_m2: float


@dataclass(frozen=True)
class MergedCell:
    """병합 결과(아직 반올림 전, float)."""

    cell_id: str
    h_dng_cd: str
    spop: float
    ages: dict[str, float]


def find_overlaps(
    cells: list[GridCell], pois: list[PoiSnapshot]
) -> dict[str, list[tuple[PoiSnapshot, float]]]:
    """STRtree로 격자-POI 겹침 후보를 좁힌 뒤, 실제 교차면적을 계산한다.

    Returns:
        cell_id -> [(poi, 교차면적_m2), ...] (겹치는 POI가 없는 cell_id는 키에서 제외).
    """
    if not pois:
        return {}

    tree = STRtree([p.geometry for p in pois])
    overlaps: dict[str, list[tuple[PoiSnapshot, float]]] = {}

    for cell in cells:
        candidate_idx = tree.query(cell.geometry, predicate="intersects")
        pairs: list[tuple[PoiSnapshot, float]] = []
        for i in candidate_idx:
            candidate = pois[i]
            intersection_area = cell.geometry.intersection(candidate.geometry).area
            if intersection_area > 0:
                pairs.append((candidate, intersection_area))
        if pairs:
            overlaps[cell.cell_id] = pairs

    return overlaps


def _update_density(current_spop: float, poi: PoiSnapshot, intersection_area: float) -> float:
    d_current = current_spop / GRID_AREA_M2
    d_poi = poi.pop_estimate / poi.area_m2
    w = intersection_area / GRID_AREA_M2
    d_new = (1 - w) * d_current + w * d_poi
    return d_new * GRID_AREA_M2


def _redistribute_ages(
    original_ages: dict[str, float], spop_new: float, male_rate: float, female_rate: float
) -> dict[str, float]:
    male_total_orig = sum(original_ages[c] for c in MALE_AGE_COLUMNS)
    female_total_orig = sum(original_ages[c] for c in FEMALE_AGE_COLUMNS)
    male_total_new = spop_new * male_rate / 100.0
    female_total_new = spop_new * female_rate / 100.0

    result: dict[str, float] = {}

    if male_total_orig > 0:
        for c in MALE_AGE_COLUMNS:
            result[c] = male_total_new * (original_ages[c] / male_total_orig)
    else:
        even_share = male_total_new / len(MALE_AGE_COLUMNS)
        for c in MALE_AGE_COLUMNS:
            result[c] = even_share

    if female_total_orig > 0:
        for c in FEMALE_AGE_COLUMNS:
            result[c] = female_total_new * (original_ages[c] / female_total_orig)
    else:
        even_share = female_total_new / len(FEMALE_AGE_COLUMNS)
        for c in FEMALE_AGE_COLUMNS:
            result[c] = even_share

    return result


def merge_cell(cell: GridCell, overlapping: list[tuple[PoiSnapshot, float]]) -> MergedCell:
    """격자 하나에 겹치는 POI들을 면적 내림차순으로 순차 적용한다.

    Args:
        cell: 원본 격자.
        overlapping: (poi, 교차면적_m2) 리스트. 순서는 무관(내부에서 정렬).

    Returns:
        병합된 결과(겹치는 POI가 없으면 원본 값을 그대로 pass-through).
    """
    if not overlapping:
        return MergedCell(
            cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=cell.spop, ages=dict(cell.ages)
        )

    ordered = sorted(overlapping, key=lambda pair: (-pair[0].area_m2, pair[0].area_cd))

    spop = cell.spop
    for poi, intersection_area in ordered:
        spop = _update_density(spop, poi, intersection_area)

    last_poi = ordered[-1][0]
    ages = _redistribute_ages(cell.ages, spop, last_poi.male_rate, last_poi.female_rate)

    return MergedCell(cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=spop, ages=ages)


def round_output_row(merged: MergedCell) -> dict[str, int | str]:
    """int64 출력 스키마에 맞게 SPOP과 28개 연령 컬럼을 독립적으로 반올림한다.

    반올림 오차로 SPOP != sum(연령 컬럼)이 ±1~2 정도 어긋날 수 있다(spec 3.5에서 허용).
    """
    row: dict[str, int | str] = {
        "CELL_ID": merged.cell_id,
        "H_DNG_CD": merged.h_dng_cd,
        "SPOP": round(merged.spop),
    }
    for c in AGE_COLUMNS:
        row[c] = round(merged.ages[c])
    return row
