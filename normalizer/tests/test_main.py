"""main.py: CLI 인자 파싱과 예측 시각 해석 테스트."""

from datetime import datetime, timedelta

import pyarrow as pa
import pytest
from shapely.geometry import box

import main
import poi
from tests.conftest import KST


class TestParseArgs:
    def test_requires_window_start(self):
        with pytest.raises(SystemExit):
            main.parse_args([])

    def test_takes_window_start_only(self):
        """baseline 모드 인자는 없다 — baseline은 항상 nowcaster 추정치다."""
        args = main.parse_args(["--window-start", "2026-08-12T14:05:00+09:00"])

        assert args.window_start == "2026-08-12T14:05:00+09:00"
        assert not hasattr(args, "baseline_date_mode")

    def test_rejects_removed_baseline_mode_flag(self):
        with pytest.raises(SystemExit):
            main.parse_args([
                "--window-start", "2026-08-12T14:05:00+09:00",
                "--baseline-date-mode", "latest",
            ])


def _forecast_row(area_cd: str, first_hour: int, slots: int = 12, *, fcst_yn: str = "Y") -> dict:
    """`FCST_n_*` 슬롯이 채워진 population_realtime 행 한 개를 만든다."""
    row = {
        "AREA_CD": area_cd,
        "AREA_PPLTN_MIN": 1000,
        "AREA_PPLTN_MAX": 2000,
        "MALE_PPLTN_RATE": 50.0,
        "FEMALE_PPLTN_RATE": 50.0,
        "FCST_YN": fcst_yn,
    }
    for slot in range(1, slots + 1):
        hour = first_hour + slot - 1
        stamp = datetime(2026, 8, 12, 0, 0, tzinfo=KST) + timedelta(hours=hour)
        row[f"FCST_{slot}_TIME"] = stamp.strftime("%Y-%m-%d %H:%M")
        row[f"FCST_{slot}_PPLTN_MIN"] = 100 * slot
        row[f"FCST_{slot}_PPLTN_MAX"] = 100 * slot + 500
    return row


class TestCollectForecasts:
    """슬롯 번호가 아니라 `FCST_n_TIME`이 시각을 결정한다."""

    def test_uses_declared_times_not_slot_numbers(self):
        window_start = datetime(2026, 8, 12, 20, 55, tzinfo=KST)
        # 실측과 같은 형태: 20:55 관측인데 첫 슬롯이 22:00이다(21:00이 아니다).
        table = pa.Table.from_pylist([_forecast_row("POI001", first_hour=22)])

        result = main._collect_forecasts(table, window_start)

        assert set(result) == {"POI001"}
        assert min(result["POI001"]) == datetime(2026, 8, 12, 22, 0, tzinfo=KST)
        assert result["POI001"][datetime(2026, 8, 12, 22, 0, tzinfo=KST)] == (100.0, 600.0)

    def test_drops_past_and_beyond_horizon_times(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        # 18:00부터 12슬롯 → 18/19/20시는 과거이거나 현재, 나머지는 12시간 안에 든다.
        table = pa.Table.from_pylist([_forecast_row("POI001", first_hour=18)])

        result = main._collect_forecasts(table, window_start)

        targets = sorted(result["POI001"])
        assert targets[0] == datetime(2026, 8, 12, 21, 0, tzinfo=KST)
        assert all(t <= window_start + timedelta(hours=12) for t in targets)

    def test_skips_pois_that_do_not_forecast(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        table = pa.Table.from_pylist([_forecast_row("POI001", first_hour=21, fcst_yn="N")])

        assert main._collect_forecasts(table, window_start) == {}

    def test_targets_are_the_sorted_union_across_pois(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        table = pa.Table.from_pylist([
            _forecast_row("POI001", first_hour=21, slots=3),
            _forecast_row("POI002", first_hour=22, slots=3),
        ])

        forecasts = main._collect_forecasts(table, window_start)
        targets = main._forecast_targets(forecasts)

        assert targets == [
            datetime(2026, 8, 12, 21, 0, tzinfo=KST),
            datetime(2026, 8, 12, 22, 0, tzinfo=KST),
            datetime(2026, 8, 12, 23, 0, tzinfo=KST),
            datetime(2026, 8, 13, 0, 0, tzinfo=KST),
        ]

    def test_unparsable_time_is_dropped_without_failing(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        row = _forecast_row("POI001", first_hour=21, slots=2)
        row["FCST_1_TIME"] = "어제쯤"
        table = pa.Table.from_pylist([row])

        result = main._collect_forecasts(table, window_start)

        assert sorted(result["POI001"]) == [datetime(2026, 8, 12, 22, 0, tzinfo=KST)]


class TestBuildForecastSnapshots:
    def test_uses_min_max_average_and_leaves_rates_unused(self):
        window_start = datetime(2026, 8, 12, 20, 0, tzinfo=KST)
        target = datetime(2026, 8, 12, 21, 0, tzinfo=KST)
        table = pa.Table.from_pylist([_forecast_row("POI001", first_hour=21, slots=1)])
        forecasts = main._collect_forecasts(table, window_start)
        areas = (
            poi.PoiArea(area_cd="POI001", area_nm="가", geometry=box(0, 0, 100, 100), area_m2=10000.0),
            poi.PoiArea(area_cd="POI002", area_nm="나", geometry=box(0, 0, 100, 100), area_m2=10000.0),
        )

        snapshots = main._build_forecast_snapshots(areas, forecasts, target)

        assert set(snapshots) == {"POI001"}
        assert snapshots["POI001"].pop_estimate == pytest.approx((100.0 + 600.0) / 2)
        assert snapshots["POI001"].male_rate == 0.0


class TestFilterGridRowsForHour:
    def test_keeps_only_matching_tt_and_dedupes_by_cell_id(self):
        table = pa.table({
            "CELL_ID": ["다사53815262", "다사53815262", "다사53815262"],
            "TT": ["13", "14", "14"],
            "H_DNG_CD": ["1100053", "1100053", "1100053"],
            "SPOP": [10.0, 20.0, 30.0],
            **{c: [None, None, None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert set(result.keys()) == {"다사53815262"}
        assert result["다사53815262"].spop == 30.0  # 마지막(TT=14) 중복 행이 남음

    def test_null_spop_becomes_zero(self):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": ["14"],
            "H_DNG_CD": ["1100053"],
            "SPOP": pa.array([None], type=pa.float64()),
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert result["다사53815262"].spop == 0.0

    def test_null_ages_become_zero(self):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": ["14"],
            "H_DNG_CD": ["1100053"],
            "SPOP": [10.0],
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert all(v == 0.0 for v in result["다사53815262"].ages.values())
