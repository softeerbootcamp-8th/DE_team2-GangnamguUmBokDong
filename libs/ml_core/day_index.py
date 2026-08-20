"""날짜를 정수 하나로 표현하는 `day` 피처 — 연도 경계 문제를 해결한다.

`month`/`hour_sin`·`cos` 같은 순환 인코딩은 "12월과 1월이 가깝다"는 것만 알고
"어느 해의 12월/1월인지"는 모른다 — 그래서 2025-12-31과 2026-01-01(진짜로 가까운
날)도, 2025-01-10과 2025-12-20(같은 해라는 것만 같고 실제로는 먼 날)도 구분하지
못한다. `2000-01-01`을 기준으로 한 경과일수(정수)는 단조증가하면서 연도 정보를
그대로 담고 있어 이 문제가 생기지 않는다.

Spark 쪽은 `F.datediff(date_col, F.lit(DAY_INDEX_EPOCH.isoformat()))`로 동일한
값을 계산한다(별도 UDF 불필요) — 이 상수와 반드시 같은 epoch을 써야 한다.
"""

from __future__ import annotations

from datetime import date

DAY_INDEX_EPOCH = date(2000, 1, 1)


def day_index(d: date) -> int:
    """`d`가 `DAY_INDEX_EPOCH`로부터 며칠째인지(0-based)를 반환한다."""
    return (d - DAY_INDEX_EPOCH).days
