"""과거 모델 입력이 anchor 이후 정보를 참조하지 않는지 검증한다."""

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from evaluation.historical_inputs import (
    HistoricalStation,
    WeatherObservation,
    _dispatch_center_lineage,
    _lag_counts,
    _population_candidate_dates,
    build_population_nowcast,
    latest_published_weather,
)
from evaluation.rebalance_backtest import RentalTrip
from evaluation.run_policy_backtest import (
    _population_required_hours,
    _select_center_stations,
)

SEOUL = ZoneInfo("Asia/Seoul")
ANCHOR = datetime(2025, 6, 17, 6, tzinfo=SEOUL)


def test_station_center_lineage_preserves_published_gold_assignment() -> None:
    """평가 station lineage가 운영 Gold에 게시된 center 배정을 보존한다."""
    lineage = {"ST-1": "hangnyeoul"}

    assert _dispatch_center_lineage(
        {"station_id": "ST-1"},
        dispatch_center_by_station_id=lineage,
    ) == "hangnyeoul"
    assert _dispatch_center_lineage(
        {"station_id": "ST-1", "dispatch_center_id": "hangnyeoul"},
        dispatch_center_by_station_id=lineage,
    ) == "hangnyeoul"


def test_station_center_lineage_rejects_missing_or_conflicting_publication() -> None:
    """평가 station은 Gold lineage 누락이나 원천과의 충돌을 허용하지 않는다."""
    with pytest.raises(ValueError, match="lineage가 없습니다"):
        _dispatch_center_lineage(
            {"station_id": "ST-1"},
            dispatch_center_by_station_id={},
        )
    with pytest.raises(ValueError, match="Gold 배정과 다릅니다"):
        _dispatch_center_lineage(
            {"station_id": "ST-1", "dispatch_center_id": "cheonho"},
            dispatch_center_by_station_id={"ST-1": "hangnyeoul"},
        )


def test_center_station_selection_uses_lineage_instead_of_nearest_distance() -> None:
    """평가 대상은 좌표 재계산 없이 HistoricalStation의 center lineage를 따른다."""
    station = HistoricalStation(
        station_id="ST-1",
        station_no=1,
        station_name="대여소",
        capacity=10,
        latitude=37.5,
        longitude=127.0,
        grid_id="GRID",
        dispatch_center_id="hangnyeoul",
    )
    scorer = SimpleNamespace(
        station_dtype=SimpleNamespace(categories=(1,)),
    )
    model = SimpleNamespace(rental=scorer, returned=scorer)

    assert _select_center_stations((station,), "hangnyeoul", model) == {1: station}
    assert _select_center_stations((station,), "cheonho", model) == {}


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


def test_population_nowcast_requires_only_inference_target_hours(tmp_path) -> None:
    """평가가 조회하지 않는 시간 결측은 막지 않고 조회 시간 결측만 차단한다."""
    target = date(2025, 6, 17)
    candidates = _population_candidate_dates(target)
    for candidate in candidates:
        rows = ["\"시간\",\"250M격자\",\"생활인구합계\""]
        rows.extend(f'"{hour}","GRID","10"' for hour in range(6, 20))
        (tmp_path / f"250_LOCAL_RESD_{candidate:%Y%m%d}.csv").write_text(
            "\n".join(rows) + "\n",
            encoding="euc-kr",
        )
    required = _population_required_hours(
        window_start=ANCHOR,
        window_end=ANCHOR + timedelta(hours=3),
        tick_minutes=5,
    )
    nowcast = build_population_nowcast(
        population_dir=tmp_path,
        target_dates=tuple(required),
        grid_ids=frozenset(("GRID", "MISSING")),
        required_hours_by_date=required,
        require_complete=False,
    )
    assert required == {target: frozenset(range(6, 20))}
    assert nowcast.complete_grid_ids(
        frozenset(("GRID", "MISSING")),
        required,
    ) == frozenset(("GRID",))
    assert nowcast.value(ANCHOR.replace(hour=19), "GRID") == pytest.approx(10)
