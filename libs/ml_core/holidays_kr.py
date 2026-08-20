"""임의 연도의 대한민국 공휴일 목록 — `analysis_summary.json`의 `holidays_2025`
하드코딩 1개 연도짜리 목록을 대체한다.

`holidays` PyPI 패키지(`nowcaster/holiday.py`가 이미 같은 용도로 씀,
`holidays.KR()`)로 오프라인·무료로 계산한다 — 음력 기반 공휴일(설날/추석 등)도
연도별로 정확히 계산되므로, 매년 사람이 목록을 새로 채워넣을 필요가 없다.
"""

from __future__ import annotations

import holidays as _holidays_lib


def korean_holidays(years: int | list[int]) -> set[str]:
    """주어진 연도의 대한민국 공휴일을 'YYYY-MM-DD' 문자열 set으로 반환한다.

    args:
        years: 연도 하나(int) 또는 여러 연도 목록 — 데이터가 연도 경계에 걸치는
            경우(예: 12월 31일 앵커의 horizon이 다음 해로 넘어감) 걸치는 연도를
            전부 넘겨야 그 경계에서 공휴일 판정이 누락되지 않는다.
    returns:
        set[str]: 'YYYY-MM-DD' 형식 날짜 문자열 집합
    """
    year_list = [years] if isinstance(years, int) else list(years)
    kr = _holidays_lib.KR(years=year_list)
    return {d.isoformat() for d in kr}
