"""CELL_ID <-> EPSG:5179 좌표 변환, 250m 격자 폴리곤 생성.

CELL_ID 형식: 한글 2자(동서글자+남북글자) + 숫자 8자리(동서4자리+남북4자리).
행정안전부 국가지점번호 체계와 동일하며, 한글은 "가나다라마바사아자차카타파하"
순서의 인덱스(가=0)를 쓴다.

    X(EPSG:5179) = 700,000 + 동서글자_인덱스 * 100,000 + 앞4자리_숫자 * 10
    Y(EPSG:5179) = 1,300,000 + 남북글자_인덱스 * 100,000 + 뒤4자리_숫자 * 10

이 좌표가 250m 정사각 격자의 남서쪽 꼭짓점이다.
실측 검증: cell_id_to_epsg5179_sw_corner("다사53815262") == (953810.0, 1952620.0).
"""

from __future__ import annotations

from shapely.geometry import Polygon, box

GRID_SIZE_M = 250.0
GRID_AREA_M2 = GRID_SIZE_M * GRID_SIZE_M  # 62500.0

_GRID_LETTERS = "가나다라마바사아자차카타파하"


def cell_id_to_epsg5179_sw_corner(cell_id: str) -> tuple[float, float]:
    """CELL_ID를 EPSG:5179 남서쪽 꼭짓점 좌표로 변환한다.

    Args:
        cell_id: "한글2자+숫자8자리" 형식(예: "다사53815262").

    Returns:
        (x, y) EPSG:5179 좌표.

    Raises:
        ValueError: 길이가 10이 아니거나, 뒤 8자리가 숫자가 아니거나,
            앞 2자에 격자 문자표에 없는 문자가 있을 때.
    """
    if len(cell_id) != 10:
        raise ValueError(f"CELL_ID 형식이 아님(길이 10 기대): {cell_id!r}")

    ew_letter, ns_letter = cell_id[0], cell_id[1]
    digits = cell_id[2:]
    if not digits.isdigit():
        raise ValueError(f"CELL_ID 형식이 아님(숫자 8자리 기대): {cell_id!r}")

    try:
        ew_idx = _GRID_LETTERS.index(ew_letter)
        ns_idx = _GRID_LETTERS.index(ns_letter)
    except ValueError as exc:
        raise ValueError(f"CELL_ID의 한글 문자가 격자 문자표에 없음: {cell_id!r}") from exc

    ew_num = int(digits[:4])
    ns_num = int(digits[4:])

    x = 700_000 + ew_idx * 100_000 + ew_num * 10
    y = 1_300_000 + ns_idx * 100_000 + ns_num * 10
    return (float(x), float(y))


def cell_id_to_polygon(cell_id: str) -> Polygon:
    """CELL_ID에 대응하는 250m x 250m 정사각 격자 폴리곤(EPSG:5179, 축 정렬)을 만든다."""
    x, y = cell_id_to_epsg5179_sw_corner(cell_id)
    return box(x, y, x + GRID_SIZE_M, y + GRID_SIZE_M)
