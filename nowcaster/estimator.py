"""점진적 감소 가중평균 + 결측 재조정 + 전체 결측 폴백 체인.

S3/IO를 모르는 순수 함수. 호출자가 이미 "결측 또는 공휴일 타입 불일치"를
`None`으로 표시해 넘겨준다고 가정한다.
"""

from __future__ import annotations

_WEIGHTS = {1: 0.4, 2: 0.3, 3: 0.2, 4: 0.1}


def estimate(
    candidates: list[float | None],
    extended: list[float | None] = (),
    historical_avg: float | None = None,
) -> tuple[float, str]:
    """`candidates`는 [1주전, 2주전, 3주전, 4주전] 값(결측/불일치는 None).

    반환값: (추정치, estimation_method)
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
