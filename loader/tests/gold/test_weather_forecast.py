"""Gold weather resolver의 whole-row·coverage 계약을 검증한다."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from core.gold_publication import ContractViolation
from gold.weather_forecast import (
    FORECAST_HOUR_COUNT,
    resolve_weather_forecast,
)

KST = ZoneInfo("Asia/Seoul")
RUN_DTTM = datetime(2026, 8, 20, 0, 15, tzinfo=UTC)


def _source_row(
    target: datetime,
    *,
    product: str,
    grid_id: str = "61_126",
    base: datetime | None = None,
    **overrides: object,
) -> dict[str, object]:
    """정시 target의 유효한 KMA source 행을 반환한다."""
    local_target = target.astimezone(KST)
    local_base = (base or (target - timedelta(minutes=30))).astimezone(KST)
    nx, ny = (int(part) for part in grid_id.split("_"))
    row: dict[str, object] = {
        "nx": nx,
        "ny": ny,
        "baseDate": local_base.strftime("%Y%m%d"),
        "baseTime": local_base.strftime("%H%M"),
        "fcstDate": local_target.strftime("%Y%m%d"),
        "fcstTime": local_target.strftime("%H%M"),
        "SKY": 1,
        "PTY": 0,
        "POP": 20.0,
        "REH": 55.0,
        "WSD": 2.5,
    }
    if product == "ultra_short":
        row.update({"T1H": 28.0, "RN1": "1.0mm 미만"})
    else:
        row.update({"TMP": 27.0, "PCP": "강수없음"})
    row.update(overrides)
    return row


def _targets(run: datetime = RUN_DTTM) -> tuple[datetime, ...]:
    """run 다음 정각부터 13개 target을 반환한다."""
    first = run.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return tuple(
        first + timedelta(hours=offset) for offset in range(FORECAST_HOUR_COUNT)
    )


def _complete_rows(
    product: str,
    *,
    grids: tuple[str, ...] = ("61_126",),
    run: datetime = RUN_DTTM,
) -> tuple[dict[str, object], ...]:
    """grid×13시간 complete source 행을 반환한다."""
    return tuple(
        _source_row(target, product=product, grid_id=grid_id)
        for grid_id in grids
        for target in _targets(run)
    )


def test_resolver_prefers_valid_ultra_short_whole_rows() -> None:
    """초단기 유효 행이 있으면 단기보다 행 전체를 우선한다."""
    projection = resolve_weather_forecast(
        _complete_rows("short_term"),
        _complete_rows("ultra_short"),
        active_weather_grid_ids=("61_126",),
        run_dttm=RUN_DTTM,
    )
    assert len(projection.records) == FORECAST_HOUR_COUNT
    assert {record.source_product_cd for record in projection.records} == {
        "ultra_short"
    }
    assert {record.temperature for record in projection.records} == {28.0}
    assert {record.precipitation_amount for record in projection.records} == {0.5}
    assert projection.first_forecast_dttm == _targets()[0]


def test_resolver_uses_latest_issue_within_each_source() -> None:
    """같은 source target에서 최신 base 발표만 후보로 쓴다."""
    target = _targets()[0]
    older = _source_row(
        target,
        product="ultra_short",
        base=target - timedelta(hours=1),
        T1H=20.0,
    )
    newer = _source_row(
        target,
        product="ultra_short",
        base=target - timedelta(minutes=30),
        T1H=29.0,
    )
    remaining = _complete_rows("ultra_short")[1:]
    projection = resolve_weather_forecast(
        _complete_rows("short_term"),
        (older, newer, *remaining),
        active_weather_grid_ids=("61_126",),
        run_dttm=RUN_DTTM,
    )
    assert projection.records[0].temperature == 29.0
    assert projection.records[0].base_dttm == target - timedelta(minutes=30)


def test_invalid_latest_ultra_falls_back_to_one_short_row_without_field_mix() -> None:
    """최신 초단기 행이 무효하면 단기 행 전체로 fallback한다."""
    target = _targets()[0]
    ultra_rows = list(_complete_rows("ultra_short"))
    ultra_rows[0] = _source_row(
        target,
        product="ultra_short",
        T1H=99.0,
        POP=88.0,
        REH=77.0,
    )
    short_rows = list(_complete_rows("short_term"))
    short_rows[0] = _source_row(
        target,
        product="short_term",
        TMP=21.0,
        POP=11.0,
        REH=44.0,
        PCP="강수없음",
    )
    projection = resolve_weather_forecast(
        tuple(short_rows),
        tuple(ultra_rows),
        active_weather_grid_ids=("61_126",),
        run_dttm=RUN_DTTM,
    )
    first = projection.records[0]
    assert first.source_product_cd == "short_term"
    assert first.temperature == 21.0
    assert first.precipitation_prob == 11.0
    assert first.humidity == 44.0
    assert first.precipitation_amount == 0.0


def test_invalid_product_specific_pty_falls_back_without_cross_mapping() -> None:
    """제품별 PTY allowlist를 교차 적용하지 않는다."""
    target = _targets()[0]
    ultra_rows = list(_complete_rows("ultra_short"))
    ultra_rows[0] = _source_row(target, product="ultra_short", PTY=4)
    projection = resolve_weather_forecast(
        _complete_rows("short_term"),
        tuple(ultra_rows),
        active_weather_grid_ids=("61_126",),
        run_dttm=RUN_DTTM,
    )
    assert projection.records[0].source_product_cd == "short_term"
    assert projection.records[0].precipitation_type_cd == "none"


def test_resolver_rejects_one_missing_grid_hour_without_partial_projection() -> None:
    """active grid×13 조합 하나라도 없으면 전체를 거부한다."""
    with pytest.raises(ContractViolation, match="missing=1"):
        resolve_weather_forecast(
            _complete_rows("short_term")[:-1],
            (),
            active_weather_grid_ids=("61_126",),
            run_dttm=RUN_DTTM,
        )


def test_resolver_requires_each_distinct_active_grid_for_all_hours() -> None:
    """서로 다른 active grid 두 개에 각각 13시간을 강제한다."""
    grids = ("60_125", "61_126")
    projection = resolve_weather_forecast(
        _complete_rows("short_term", grids=grids),
        _complete_rows("ultra_short", grids=grids),
        active_weather_grid_ids=grids,
        run_dttm=RUN_DTTM,
    )
    assert len(projection.records) == 2 * FORECAST_HOUR_COUNT
    assert projection.active_weather_grid_ids == grids


def test_resolver_rolls_first_hour_into_next_kst_day() -> None:
    """KST 23시대 run의 첫 target을 다음 날 00시로 rollover한다."""
    run = datetime(2026, 8, 20, 14, 45, tzinfo=UTC)
    projection = resolve_weather_forecast(
        _complete_rows("short_term", run=run),
        (),
        active_weather_grid_ids=("61_126",),
        run_dttm=run,
    )
    assert projection.first_forecast_dttm == datetime(2026, 8, 20, 15, 0, tzinfo=UTC)
    assert projection.records[-1].forecast_dttm == datetime(
        2026, 8, 21, 3, 0, tzinfo=UTC
    )


def test_non_hourly_source_target_is_excluded() -> None:
    """정시가 아닌 source target은 Gold 후보에서 제외한다."""
    rows = list(_complete_rows("short_term"))
    off_hour = dict(rows[0])
    off_hour["fcstTime"] = "1030"
    rows[0] = off_hour
    with pytest.raises(ContractViolation, match="missing=1"):
        resolve_weather_forecast(
            tuple(rows),
            (),
            active_weather_grid_ids=("61_126",),
            run_dttm=RUN_DTTM,
        )


def test_same_source_target_base_payload_collision_rejects_snapshot() -> None:
    """같은 source target·base의 다른 payload에서 임의 승자를 고르지 않는다."""
    rows = list(_complete_rows("short_term"))
    collision = dict(rows[0])
    collision["TMP"] = 30.0
    with pytest.raises(ContractViolation, match="충돌"):
        resolve_weather_forecast(
            (*rows, collision),
            (),
            active_weather_grid_ids=("61_126",),
            run_dttm=RUN_DTTM,
        )


def test_empty_active_grid_set_produces_conditional_empty() -> None:
    """active station grid가 없으면 0행 conditional EMPTY projection을 반환한다."""
    projection = resolve_weather_forecast(
        (), (), active_weather_grid_ids=(), run_dttm=RUN_DTTM
    )
    assert projection.records == ()
    assert projection.active_weather_grid_ids == ()
    assert projection.first_forecast_dttm is None
