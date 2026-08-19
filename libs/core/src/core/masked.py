"""서울 생활인구의 비식별 마스킹 표기(`*`)를 결측으로 판정시키는 캐스터.

`living_population_grid`(`Se250MSpopLocalResd`)는 소수 인구를 `*`로 가려서 보낸다.
실측(2026-08-19, 표본 1,000행)에서 나이·성별 28컬럼 28,000칸 중 14,945칸(53.4%),
`SPOP` 98행(9.8%)이 `*`였다. CSV 원본(`250_LOCAL_RESD_YYYYMMDD.csv`)도 같은 표기를 쓴다.

`docs/collector/DataSchema.md`는 이 값의 결측 기준을 "값 없음(마스킹 `*` → null)"으로
명시한다 — **null이 되는 것 자체는 의도된 동작이다.** 이 모듈이 고치는 것은 판정
라벨이다. `types: [float]`로 두면 `float("*")`이 실패해 TYPE_ERROR가 되고
`optional_outlier` 정책을 타는데, 그러면:

- manifest의 `column_issues`가 `type_error`로 채워져, 정상 마스킹과 진짜 형식 오류를
  지표로 구분할 수 없다.
- `optional_outlier`를 `drop_row`로 바꾸는 순간 정상 마스킹 행이 통째로 폐기되고,
  `optional_missing`을 조정해도 마스킹에는 아무 영향이 없다 — 손잡이가 반대로 걸린다.

그래서 `MaskedValue`를 따로 두고 검증 엔진이 이것만 결측으로 되돌린다.
`ValueError`·`TypeError`를 **상속하지 않아야** 한다 — `_try_cast`가 그 둘을 삼켜
다음 타입으로 넘어가므로, 상속하면 다시 TYPE_ERROR로 돌아간다.
"""

from __future__ import annotations

from typing import Any

# 서울 열린데이터광장이 소수 인구를 가릴 때 쓰는 표기.
_MASK_MARKER = "*"


class MaskedValue(Exception):
    """비식별 마스킹된 값. 검증 엔진이 결측(MISSING)으로 판정한다.

    `Exception`을 직접 상속한다. `ValueError`/`TypeError` 계열이면 `_try_cast`가
    잡아 삼켜서 이 예외의 목적이 사라진다.
    """


def parse_masked_float(value: Any) -> float:
    """마스킹 표기를 결측으로 넘기고, 나머지는 float으로 바꾼다.

    args:
        value: 원본 값. 문자열이 보통이지만 이미 숫자로 저장된 silver를 다시
            넘기는 경로가 있어 숫자도 받는다.
    returns:
        float 값.
    raises:
        MaskedValue: 값이 마스킹 표기(`*`)일 때. 결측으로 판정된다.
        ValueError: 마스킹도 아니고 숫자로도 읽을 수 없을 때. 진짜 형식 오류이므로
            TYPE_ERROR로 남아야 한다.
        TypeError: `value`가 `None`처럼 float으로 다룰 수 없는 값일 때.
    """
    if isinstance(value, str) and value.strip() == _MASK_MARKER:
        raise MaskedValue("비식별 마스킹된 값이다")
    return float(value)
