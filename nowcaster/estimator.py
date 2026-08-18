"""과거 관측치 기반 가중평균 및 결측 폴백 추정을 수행한다.

S3/IO 의존성이 없는 순수 함수로 구성되어 있습니다.
"""

from __future__ import annotations

_WEIGHTS = {1: 0.4, 2: 0.3, 3: 0.2, 4: 0.1}


def estimate(
    candidates: list[float | None],
    extended: list[float | None] = (),
    historical_avg: float | None = None,
) -> tuple[float, str]:
    """과거 관측치를 바탕으로 가중평균 또는 폴백 방식을 적용해 값을 추정한다.

    args:
        candidates: 1~4주 전 관측값 목록 (결측값은 None)
        extended: 5~8주 전 관측값 목록
        historical_avg: 격자의 과거 전체 평균값
    returns:
        (추정치, 추정 방식) 튜플
    """
    valid = [(week, value) for week, value in zip((1, 2, 3, 4), candidates) if value is not None]

    if len(valid) == 4:
        value = sum(_WEIGHTS[week] * v for week, v in valid)
        return value, "weighted_avg"

    if len(valid) == 1:
        return valid[0][1], "single_week_fallback"

    if valid:
        total_weight = sum(_WEIGHTS[week] for week, _ in valid)
        value = sum(_WEIGHTS[week] * v for week, v in valid) / total_weight
        return value, "reweighted_avg"

    for value in extended:
        if value is not None:
            return value, "extended_lookback_fallback"

    if historical_avg is not None:
        return historical_avg, "grid_historical_avg"

    return 0.0, "no_data"
