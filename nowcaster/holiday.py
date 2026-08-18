"""공휴일 및 일요일 판정과 날짜 패턴 매칭 유틸리티를 제공한다."""

from __future__ import annotations

from datetime import date, timedelta

# pyrefly: ignore [missing-import]
import holidays

_KR = holidays.KR()


def is_special_day(d: date) -> bool:
    """일요일 또는 대한민국 공휴일 여부를 반환한다."""
    return d.weekday() == 6 or d in _KR


def matches_target_pattern(candidate: date, target: date) -> bool:
    """두 날짜의 특수일(휴일/평일) 분류가 일치하는지 확인한다."""
    return is_special_day(candidate) == is_special_day(target)


def candidate_dates(target: date, count: int = 4, max_lookback_days: int = 60) -> list[date]:
    """가중평균 추정에 사용할 과거 후보 날짜 목록을 반환한다.

    args:
        target: 기준 대상 날짜
        count: 추출할 후보 날짜 수
        max_lookback_days: 특수일 탐색 최대 과거 일수
    returns:
        가까운 순으로 정렬된 후보 날짜 목록
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
    """전체 결측 폴백용 확장 주차의 동일 요일 날짜 목록을 반환한다."""
    return [target - timedelta(weeks=i) for i in range(start_week, end_week + 1)]
