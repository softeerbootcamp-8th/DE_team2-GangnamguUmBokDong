"""공간 교차(STRtree)를 통해 격자 인구와 실시간 POI 인구를 합성하고 연령·성별을 재분배한다."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from grid import GRID_AREA_M2
from shapely import STRtree
from shapely.geometry.base import BaseGeometry

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
    """생활인구 격자 데이터의 단일 셀 정보."""

    cell_id: str
    h_dng_cd: str
    spop: float
    ages: dict[str, float]
    geometry: BaseGeometry


@dataclass(frozen=True)
class PoiSnapshot:
    """실시간 POI 인구 관측치와 지오메트리가 결합된 스냅샷."""

    area_cd: str
    male_rate: float
    female_rate: float
    pop_estimate: float
    geometry: BaseGeometry
    area_m2: float


@dataclass(frozen=True)
class MergedCell:
    """POI 인구 합성이 완료된 격자 셀 정보(반올림 전 소수점 유지)."""

    cell_id: str
    h_dng_cd: str
    spop: float
    ages: dict[str, float]


class OverlappingGeometry(Protocol):
    """교차 계산에 필요한 최소 계약 — POI 코드와 지오메트리."""

    area_cd: str
    geometry: BaseGeometry


def find_overlap_areas(
    cells: list[GridCell], pois: list[OverlappingGeometry]
) -> dict[str, list[tuple[str, float]]]:
    """STRtree 공간 인덱스로 격자와 POI의 교차 면적을 계산한다(POI 코드 기준).

    인구값이 아니라 **지오메트리만** 보므로 결과는 시각과 무관하다. 그래서 여러 시각
    (현재 + 향후 12시간)을 보정할 때 이 계산을 한 번만 하고 시각별로 재사용할 수 있다 —
    시각마다 다시 돌리면 가장 비싼 단계를 13번 반복하게 된다.

    args:
        cells: 대상 GridCell 목록(여러 시각의 셀 합집합을 넣어도 된다)
        pois: `area_cd`와 `geometry`를 가진 POI 목록
    returns:
        CELL_ID를 키로 하고 (POI 코드, 교차면적_m2) 튜플 목록을 값으로 갖는 딕셔너리
    """
    if not pois:
        return {}

    tree = STRtree([p.geometry for p in pois])
    overlaps: dict[str, list[tuple[str, float]]] = {}

    for cell in cells:
        candidate_idx = tree.query(cell.geometry, predicate="intersects")
        pairs: list[tuple[str, float]] = []
        for i in candidate_idx:
            candidate = pois[i]
            intersection_area = cell.geometry.intersection(candidate.geometry).area
            if intersection_area > 0:
                pairs.append((candidate.area_cd, intersection_area))
        if pairs:
            overlaps[cell.cell_id] = pairs

    return overlaps


def bind_snapshots(
    overlap_areas: list[tuple[str, float]], snapshots_by_code: dict[str, PoiSnapshot]
) -> list[tuple[PoiSnapshot, float]]:
    """교차 면적 목록에 해당 시각의 PoiSnapshot을 붙인다.

    그 시각의 관측·예측값이 없는 POI는 제외된다 — 예측을 제공하지 않는 지점
    (`FCST_YN='N'`)이 겹친 셀은 baseline 값이 그대로 남는다.

    args:
        overlap_areas: (POI 코드, 교차면적_m2) 목록
        snapshots_by_code: 그 시각의 POI 코드별 스냅샷
    returns:
        (PoiSnapshot, 교차면적_m2) 목록
    """
    return [
        (snapshots_by_code[area_cd], area)
        for area_cd, area in overlap_areas
        if area_cd in snapshots_by_code
    ]


def find_overlaps(
    cells: list[GridCell], pois: list[PoiSnapshot]
) -> dict[str, list[tuple[PoiSnapshot, float]]]:
    """`find_overlap_areas()`에 스냅샷을 붙인 결과를 돌려준다(단일 시각용 편의 함수)."""
    snapshots_by_code = {p.area_cd: p for p in pois}
    return {
        cell_id: bind_snapshots(areas, snapshots_by_code)
        for cell_id, areas in find_overlap_areas(cells, pois).items()
    }


def _update_density(current_spop: float, poi: PoiSnapshot, intersection_area: float) -> float:
    """격자와 POI의 겹침 비율에 따라 밀도 가중치를 적용하여 새로운 생활인구 합계를 계산한다."""
    # 1. 격자 및 POI의 면적당 인구 밀도(명/m²) 계산
    d_current = current_spop / GRID_AREA_M2
    d_poi = poi.pop_estimate / poi.area_m2

    # 2. 격자 전체 면적(62,500m²) 대비 POI와 겹친 면적의 가중치 비율(w)
    w = intersection_area / GRID_AREA_M2

    # 3. 겹치지 않은 영역(1-w)은 기존 밀도, 겹친 영역(w)은 POI 실시간 밀도로 가중평균
    d_new = (1 - w) * d_current + w * d_poi

    # 4. 갱신된 밀도를 격자 전체 면적에 곱해 최종 생활인구 산출
    return d_new * GRID_AREA_M2


def _redistribute_ages(
    original_ages: dict[str, float], spop_new: float, male_rate: float, female_rate: float
) -> dict[str, float]:
    """합성된 생활인구 총합과 POI 성비를 바탕으로 연령대별 인구를 재분배한다."""
    # 1. 기존 격자의 남성 및 여성 인구 총합 계산
    male_total_orig = sum(original_ages[c] for c in MALE_AGE_COLUMNS)
    female_total_orig = sum(original_ages[c] for c in FEMALE_AGE_COLUMNS)

    # 2. POI 실시간 성비(%)를 적용한 새로운 남녀 목표 총인구 산출
    male_total_new = spop_new * male_rate / 100.0
    female_total_new = spop_new * female_rate / 100.0

    result: dict[str, float] = {}

    # 3. 남성 연령대(M00~M70): 기존 연령대 비중을 유지하며 새 남성 인구에 비례 배분
    if male_total_orig > 0:
        for c in MALE_AGE_COLUMNS:
            result[c] = male_total_new * (original_ages[c] / male_total_orig)
    else:
        # 기존 남성 인구가 0이었던 경우 14개 연령대에 균등 배분
        even_share = male_total_new / len(MALE_AGE_COLUMNS)
        for c in MALE_AGE_COLUMNS:
            result[c] = even_share

    # 4. 여성 연령대(F00~F70): 기존 연령대 비중을 유지하며 새 여성 인구에 비례 배분
    if female_total_orig > 0:
        for c in FEMALE_AGE_COLUMNS:
            result[c] = female_total_new * (original_ages[c] / female_total_orig)
    else:
        # 기존 여성 인구가 0이었던 경우 14개 연령대에 균등 배분
        even_share = female_total_new / len(FEMALE_AGE_COLUMNS)
        for c in FEMALE_AGE_COLUMNS:
            result[c] = even_share

    return result


def _scale_ages(original_ages: dict[str, float], spop_new: float) -> dict[str, float]:
    """성·연령 구성비를 그대로 두고 총량만 `spop_new`에 맞춰 비례 조정한다.

    미래 시각 보정에 쓴다. `FCST_PPLTN`은 인구 수만 주고 성비를 주지 않으므로, 그 시각의
    성·연령 구조는 baseline(nowcaster 추정치)이 가진 것을 신뢰한다 — 관측 시점의 성비를
    12시간 뒤에 씌우면 있는 정보를 오히려 버린다.

    args:
        original_ages: baseline 격자의 연령·성별 인구
        spop_new: 합성된 새 생활인구 총합
    returns:
        총합이 `spop_new`가 되도록 조정된 연령·성별 인구
    """
    total_orig = sum(original_ages[c] for c in AGE_COLUMNS)
    # baseline이 0인 셀은 비율을 알 수 없다. `_redistribute_ages()`와 같은 규칙으로
    # 균등 배분해 SPOP == sum(ages) 불변식을 지킨다.
    if total_orig <= 0:
        even_share = spop_new / len(AGE_COLUMNS)
        return {c: even_share for c in AGE_COLUMNS}
    ratio = spop_new / total_orig
    return {c: original_ages[c] * ratio for c in AGE_COLUMNS}


def _ordered_by_area(
    overlapping: list[tuple[PoiSnapshot, float]]
) -> list[tuple[PoiSnapshot, float]]:
    """광역 POI를 먼저 반영하고 국소 POI가 최종 덮어쓰도록 면적 내림차순(동률 시 코드순) 정렬한다."""
    return sorted(overlapping, key=lambda pair: (-pair[0].area_m2, pair[0].area_cd))


def _composite_spop(cell: GridCell, ordered: list[tuple[PoiSnapshot, float]]) -> float:
    """큰 POI부터 작은 POI 순으로 밀도를 누적 갱신한 생활인구 총합을 반환한다."""
    spop = cell.spop
    for poi, intersection_area in ordered:
        spop = _update_density(spop, poi, intersection_area)
    return spop


def merge_cell_total_only(
    cell: GridCell, overlapping: list[tuple[PoiSnapshot, float]]
) -> MergedCell:
    """총량만 POI 예측 인구로 합성하고 성·연령 구성비는 baseline 그대로 유지한다.

    미래 시각(`FCST_n_*`)용이다. `merge_cell()`은 POI의 실측 성비로 28개 컬럼을
    재분배하지만, 예측값에는 성비가 없어 그 경로를 쓸 수 없다.

    args:
        cell: 대상 GridCell 객체(baseline = nowcaster 추정치)
        overlapping: (PoiSnapshot, 교차면적_m2) 목록. `pop_estimate`가 예측 인구다
    returns:
        합성된 MergedCell 객체
    """
    if not overlapping:
        return MergedCell(
            cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=cell.spop, ages=dict(cell.ages)
        )

    spop = _composite_spop(cell, _ordered_by_area(overlapping))
    return MergedCell(
        cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=spop, ages=_scale_ages(cell.ages, spop)
    )


def merge_cell(cell: GridCell, overlapping: list[tuple[PoiSnapshot, float]]) -> MergedCell:
    """단일 격자에 겹치는 POI들을 면적 내림차순으로 순차 적용하여 최종 인구 및 성·연령 분포를 합성한다.

    광역 POI의 배경 밀도를 먼저 반영하고 국소 핫스팟의 세부 특성이 최종 반영되도록 면적 내림차순으로 순차 적용합니다.

    args:
        cell: 대상 GridCell 객체
        overlapping: (PoiSnapshot, 교차면적_m2) 목록
    returns:
        합성된 MergedCell 객체
    """
    # 1. 겹치는 POI가 없으면 기존 격자 데이터를 그대로 반환
    if not overlapping:
        return MergedCell(
            cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=cell.spop, ages=dict(cell.ages)
        )

    # 2~3. 면적 내림차순으로 정렬해 큰 POI부터 밀도를 순차적 누적 갱신
    ordered = _ordered_by_area(overlapping)
    spop = _composite_spop(cell, ordered)

    # 4. 가장 국소적인 마지막 POI의 성비를 채택하여 28개 연령대 재분배
    last_poi = ordered[-1][0]
    ages = _redistribute_ages(cell.ages, spop, last_poi.male_rate, last_poi.female_rate)

    return MergedCell(cell_id=cell.cell_id, h_dng_cd=cell.h_dng_cd, spop=spop, ages=ages)


def round_output_row(merged: MergedCell) -> dict[str, int | str]:
    """합성된 실수형 인구 데이터를 정수형(int64) 출력 레코드 형태로 반올림한다."""
    row: dict[str, int | str] = {
        "CELL_ID": merged.cell_id,
        "H_DNG_CD": merged.h_dng_cd,
        "SPOP": round(merged.spop),
    }
    for c in AGE_COLUMNS:
        row[c] = round(merged.ages[c])
    return row
