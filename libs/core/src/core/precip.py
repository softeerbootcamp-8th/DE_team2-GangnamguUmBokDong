"""기상청 강수량 범주 표기를 mm 실수로 바꾼다.

단기예보 `PCP`와 초단기예보 `RN1`은 숫자가 아니라 범주 문자열을 준다
(`"강수없음"`, `"1.0mm 미만"`, `"30.0~50.0mm"`, `"50.0mm 이상"`, `"2.0mm"`, `"0"`).
collector가 silver에 쓸 때와 loader가 RDB에 넣을 때 **같은 값**이 나와야 하므로
규칙을 이 모듈 하나에 둔다. 두 모듈 모두 `core`에 의존한다.

범위형은 **하한**을 취한다. 상한이 없는 `"50.0mm 이상"`은 평균을 정의할 수 없어
같은 하한 규칙으로 처리한다 — 과소추정이 확정적이지만 근거가 있는 유일한 값이다.

collector의 캐스터로 등록되므로(`validation/engine.py`의 `_CASTERS`) 해석 실패는
`None`이 아니라 예외다. `None`을 돌려주면 검증 엔진이 결측(MISSING)과 타입 오류
(TYPE_ERROR)를 구분하지 못해 서로 다른 정책이 섞인다.
"""

from __future__ import annotations

from typing import Any

# 강수·적설이 "없음"으로 표기되는 값들. 실제 구간은 0.1mm 미만이지만 0으로 본다.
_NONE_LABELS = ("강수없음", "적설없음")

# "1.0mm 미만"의 실제 구간은 0.1~1.0mm다. 그 대표값으로 0.5를 쓴다.
_BELOW_THRESHOLD_MM = 0.5


def parse_precip(value: Any) -> float:
    """강수량 표기를 mm 실수로 바꾼다.

    args:
        value: 원본 값. 문자열이 보통이지만 이미 숫자로 저장된 silver를 다시
            넘기는 경로가 있어 숫자도 받는다.
    returns:
        mm 단위 실수.
    raises:
        ValueError: 어느 규칙에도 해당하지 않아 숫자로 읽을 수 없을 때.
        TypeError: `value`가 `None`처럼 문자열로 다룰 수 없는 값일 때.
    """
    if value is None:
        raise TypeError("강수량이 None이다")

    text = str(value).strip()
    if text in _NONE_LABELS:
        return 0.0
    if "미만" in text:
        return _BELOW_THRESHOLD_MM

    # "50.0mm 이상" → "50.0", "30.0~50.0mm" → "30.0~50.0"
    text = text.replace("mm", "").replace("이상", "").strip()
    if "~" in text:
        text = text.split("~", 1)[0].strip()
    return float(text)
