"""시각을 자정 기준 경과분(0~1439) 하나로 표현하는 `minute` 피처 — `hour` 단독보다
tick(20분) 단위 세부 시각을 그대로 반영한다.

`hour`만 쓰면 같은 시간 안의 서로 다른 tick(예: 17:00/17:20/17:40)이 모델에는
전부 같은 값으로 보인다 — 그리드 자체가 20분 tick인데 feature가 시간 단위로만
뭉개는 셈이다. `hour*60+minute`(00:00=0, 01:20=80, ...)은 하루 안에서 단조증가하며
tick 단위 구분을 그대로 담는다.

Spark 쪽은 `F.hour(ts_col)*60 + F.minute(ts_col)`로 동일한 값을 계산한다(별도 UDF
불필요) — `day_index.py`와 같은 이유로 여기 있는 함수를 직접 호출하지 않고 같은
공식을 Spark 표현식으로 복제한다.
"""

from __future__ import annotations

import pandas as pd

MINUTES_PER_DAY = 1440


def minute_of_day(ts: pd.Timestamp) -> int:
    """`ts`의 자정 기준 경과분(0~1439)을 반환한다."""
    return ts.hour * 60 + ts.minute
