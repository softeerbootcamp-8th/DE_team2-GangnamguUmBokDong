"""공휴일/일요일 판정. "동일 요일" 매칭이 깨지는 날(공휴일)을 가려낸다."""

from __future__ import annotations

from datetime import date, timedelta

import holidays

_KR = holidays.KR()


def is_special_day(d: date) -> bool:
    """일요일이거나 대한민국 공휴일이면 True."""
    return d.weekday() == 6 or d in _KR


def matches_target_pattern(candidate: date, target: date) -> bool:
    """후보 날짜의 공휴일 타입이 대상 날짜와 같은 타입인지."""
    return is_special_day(candidate) == is_special_day(target)


def candidate_dates(target: date, count: int = 4, max_lookback_days: int = 60) -> list[date]:
    """가중평균에 쓸 후보 날짜 목록(가까운 순).

    평일이면 최근 `count`회의 동일 요일(7일 간격)을, 공휴일/일요일이면
    하루씩 거슬러 올라가며 동일 타입(공휴일 또는 일요일)인 날을 `count`개 찾는다.
    """
    if not is_special_day(target):
        return [target - timedelta(weeks=i) for i in range(1, count + 1)]

    result: list[date] = []
    cursor = target - timedelta(days=1)
    checked = 0
    while len(result) < count and checked < max_lookback_days:
        if is_special_day(cursor):
            result.append(cursor)
        cursor -= timedelta(days=1)
        checked += 1
    return result


def extended_candidate_dates(target: date, start_week: int = 5, end_week: int = 8) -> list[date]:
    """전체 결측 시 폴백용: 5~8주 전 동일 요일(가까운 순)."""
    return [target - timedelta(weeks=i) for i in range(start_week, end_week + 1)]
