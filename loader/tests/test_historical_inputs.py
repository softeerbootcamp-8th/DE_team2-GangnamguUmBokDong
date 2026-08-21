"""과거 모델 입력이 anchor 이후 정보를 참조하지 않는지 검증한다."""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from evaluation.historical_inputs import (
    WeatherObservation,
    _lag_counts,
    _population_candidate_dates,
    build_population_nowcast,
    latest_published_weather,
)
from evaluation.rebalance_backtest import RentalTrip

SEOUL = ZoneInfo("Asia/Seoul")
ANCHOR = datetime(2025, 6, 17, 6, tzinfo=SEOUL)


def test_weather_uses_only_observation_before_publication_cutoff() -> None:
    """06시 anchor와 60분 지연에서는 05시 관측까지만 선택한다."""
    observations = (
        WeatherObservation(datetime(2025, 6, 17, 5), 20.0, 0.0),
        WeatherObservation(datetime(2025, 6, 17, 6), 21.0, 0.0),
    )
    selected = latest_published_weather(
        observations,
        anchor=ANCHOR,
        publication_lag_minutes=60,
    )
    assert selected.observed_at == datetime(2025, 6, 17, 5)


def test_population_candidates_are_strictly_before_target() -> None:
    """평일과 공휴일 모두 생활인구 후보가 대상일 또는 미래를 포함하지 않는다."""
    for target in (date(2025, 6, 17), date(2025, 1, 1)):
        candidates = _population_candidate_dates(target)
        assert len(candidates) == 4
        assert all(candidate < target for candidate in candidates)


def test_population_nowcast_matches_four_week_weighted_average(tmp_path) -> None:
    """운영과 같은 0.4/0.3/0.2/0.1 가중치로 과거 네 주만 결합한다."""
    target = date(2025, 6, 17)
    candidates = _population_candidate_dates(target)
    for candidate, value in zip(candidates, (10, 20, 30, 40), strict=True):
        rows = ["\"시간\",\"250M격자\",\"생활인구합계\""]
        rows.extend(f'"{hour}","GRID","{value}"' for hour in range(24))
        (tmp_path / f"250_LOCAL_RESD_{candidate:%Y%m%d}.csv").write_text(
            "\n".join(rows) + "\n",
            encoding="euc-kr",
        )
    nowcast = build_population_nowcast(
        population_dir=tmp_path,
        target_dates=(target,),
        grid_ids=frozenset(("GRID",)),
    )
    assert nowcast.value(ANCHOR, "GRID") == pytest.approx(20.0)
    assert nowcast.source_dates(target) == tuple(sorted(candidates))


def test_lags_use_embargo_visibility_and_successful_returns() -> None:
    """대여 lag는 embargo와 반납 가시성을, 반납 lag는 직전 한 시간을 지킨다."""
    visible = RentalTrip(
        "visible",
        ANCHOR - timedelta(minutes=80),
        1,
        ANCHOR - timedelta(minutes=10),
        2,
    )
    not_returned_yet = RentalTrip(
        "hidden",
        ANCHOR - timedelta(minutes=70),
        1,
        ANCHOR + timedelta(minutes=10),
        2,
    )
    embargoed = RentalTrip(
        "embargoed",
        ANCHOR - timedelta(minutes=20),
        1,
        ANCHOR - timedelta(minutes=5),
        2,
    )
    rental, returned = _lag_counts(
        (visible, not_returned_yet, embargoed),
        ANCHOR,
    )
    assert rental == {1: 1}
    assert returned == {2: 2}
