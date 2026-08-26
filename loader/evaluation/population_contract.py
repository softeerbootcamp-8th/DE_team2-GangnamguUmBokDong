"""생활인구 nowcast가 허용하는 과거 원천 날짜 계약을 정의한다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from ml_core.holidays_kr import korean_holidays


@dataclass(frozen=True, slots=True)
class PopulationSourceDateContract:
    """대상일별 필수 네 후보일과 결측 보완 후보일을 표현한다."""

    base_dates: tuple[date, date, date, date]
    fallback_dates: tuple[date, date, date, date]


def population_source_date_contract(target: date) -> PopulationSourceDateContract:
    """운영 nowcaster와 provenance 검증이 공유할 과거 날짜 계약을 반환한다."""
    holidays = korean_holidays([target.year - 1, target.year])

    def special(day: date) -> bool:
        """일요일 또는 대한민국 공휴일인지 반환한다."""
        return day.weekday() == 6 or day.isoformat() in holidays

    if special(target):
        values = []
        cursor = target - timedelta(days=1)
        while len(values) < 4 and (target - cursor).days <= 60:
            if special(cursor):
                values.append(cursor)
            cursor -= timedelta(days=1)
        if len(values) != 4:
            raise ValueError(f"특수일 인구 후보 네 날짜를 찾지 못했습니다: {target}")
        base_dates = tuple(values)
    else:
        base_dates = tuple(target - timedelta(weeks=week) for week in range(1, 5))
    fallback_dates = tuple(
        target - timedelta(weeks=week) for week in range(5, 9)
    )
    return PopulationSourceDateContract(
        base_dates=base_dates,  # type: ignore[arg-type]
        fallback_dates=fallback_dates,  # type: ignore[arg-type]
    )
