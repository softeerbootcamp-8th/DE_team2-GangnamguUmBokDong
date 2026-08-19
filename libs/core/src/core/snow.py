"""기상청 적설 범주 표기를 cm 실수로 바꾼다.

단기예보 `SNO`(3시간 신적설)는 숫자가 아니라 범주 문자열을 준다
(`"적설없음"`, `"1.0cm 미만"`, `"1.0~4.9cm"`, `"5.0cm 이상"`, `"5.0cm"`, `"0"`).

`PCP`(강수량)와 표기 **형태**는 같지만 **단위가 cm다.** 그래서 `core.precip`과
함수를 나눈다 — 형태 규칙은 `core._amount.parse_amount`가 공유하고, 단위와 없음
레이블만 여기서 정한다. 자세한 이유는 그 모듈의 docstring을 참고한다.

collector의 캐스터로 등록되므로(`validation/engine.py`의 `_CASTERS`) 해석 실패는
`None`이 아니라 예외다. `None`을 돌려주면 검증 엔진이 결측(MISSING)과 타입 오류
(TYPE_ERROR)를 구분하지 못해 서로 다른 정책이 섞인다.
"""

from __future__ import annotations

from typing import Any

from core._amount import parse_amount


def parse_snow(value: Any) -> float:
    """적설 표기를 cm 단위 실수로 바꾼다.

    args:
        value: 원본 값. 문자열이 보통이지만 이미 숫자로 저장된 silver를 다시
            넘기는 경로가 있어 숫자도 받는다.
    returns:
        cm 단위 실수.
    raises:
        ValueError: 어느 규칙에도 해당하지 않을 때. 강수 표기(`"강수없음"`, `"2.0mm"`)를
            넘긴 경우도 여기 걸린다.
        TypeError: `value`가 `None`처럼 문자열로 다룰 수 없는 값일 때.
    """
    return parse_amount(value, unit="cm", none_label="적설없음")
