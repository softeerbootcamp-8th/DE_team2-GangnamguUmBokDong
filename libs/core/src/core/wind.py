"""풍속·풍향을 동서(`UUU`)·남북(`VVV`) 성분으로 분해한다.

기상청 실황 API(`getUltraSrtNcst`)는 `WSD`·`VEC`와 함께 `UUU`·`VVV`를 이미 계산해서
준다. 과거 CSV(ASOS 시간자료)에는 그 두 성분이 없고 풍속·풍향만 있어서, bootstrap이
같은 공식으로 채워 운영 수집분과 컬럼을 맞춘다.

`core.precip`·`core.snow`와 같은 자리에 두는 이유는 셋 다 **기상청이 정의한 값 변환
규칙**이라서다. 규칙을 한 곳에 모아두면 "이 컬럼은 어떻게 만들어졌나"를 한 군데서 찾는다.

## 부호 관례

기상학에서 풍향은 바람이 **불어오는** 방향이다. 그래서 성분에 음수가 붙는다.

    u = -WSD * sin(VEC)      # 동서 성분: 양수면 서 -> 동
    v = -WSD * cos(VEC)      # 남북 성분: 양수면 남 -> 북

북풍(`VEC=0`)은 남쪽으로 부는 바람이므로 `v`가 음수다.

## 실API 대조 검증

2026-08-19 실황 API에서 격자 8곳의 `WSD`·`VEC`·`UUU`·`VVV`를 받아 대조했다.
최대 절대오차는 `UUU` 0.10 / `VVV` 0.11 m/s였고, 그 오차는 API가 주는 `WSD`·`VEC`가
이미 소수 첫째 자리로 반올림된 값이라는 것만으로 설명된다(`VEC=274`는 실제
273.5~274.4 중 하나다). 즉 **이 함수는 값을 만들어내지 않고, 가진 값을 다르게
표현할 뿐이다.** 검증 표본은 `collector/tests/test_wind.py`에 회귀로 박혀 있다.
"""

from __future__ import annotations

import math
from typing import Any


def wind_components(speed: Any, direction: Any) -> tuple[float, float]:
    """풍속·풍향을 (동서 성분, 남북 성분)으로 분해한다.

    args:
        speed: 풍속(m/s). `WSD`에 해당한다.
        direction: 풍향(도, 0~360). `VEC`에 해당하며 바람이 불어오는 방향이다.
    returns:
        `(u, v)` — 각각 `UUU`·`VVV`에 해당하는 m/s 실수.
    raises:
        ValueError: 숫자로 읽을 수 없는 값일 때.
        TypeError: `None`처럼 실수로 다룰 수 없는 값일 때.
    """
    speed_value = float(speed)
    radians = math.radians(float(direction))
    return -speed_value * math.sin(radians), -speed_value * math.cos(radians)
