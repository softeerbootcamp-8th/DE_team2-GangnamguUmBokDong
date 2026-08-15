"""estimate_day.py: 격자·시간대(H_DNG_CD/CELL_ID/TT)별로 후보 주차 archive를 조인해 추정 테이블을 만든다."""

import pandas as pd
import pytest

import estimate_day


def _frame(rows):
    return pd.DataFrame(rows)


class TestBuildNowcastTable:
    def test_all_four_weeks_present_uses_weighted_avg(self):
        week1 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}])
        week2 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 200.0}])
        week3 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0}])
        week4 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 400.0}])

        result = estimate_day.build_nowcast_table([week1, week2, week3, week4])

        row = result.iloc[0]
        assert row["SPOP"] == 100.0 * 0.4 + 200.0 * 0.3 + 300.0 * 0.2 + 400.0 * 0.1
        assert row["is_estimated"] == True
        assert row["estimation_method"] == "weighted_avg"

    def test_one_week_missing_key_renormalizes(self):
        week1 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}])
        week3 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0}])
        week4 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 400.0}])

        result = estimate_day.build_nowcast_table([week1, None, week3, week4])

        row = result.iloc[0]
        expected = (100.0 * 0.4 + 300.0 * 0.2 + 400.0 * 0.1) / 0.7
        assert row["SPOP"] == pytest.approx(expected)
        assert row["estimation_method"] == "reweighted_avg"

    def test_key_absent_from_all_four_weeks_uses_extended_fallback(self):
        empty = _frame([])
        extended_hit = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 250.0}])

        result = estimate_day.build_nowcast_table(
            [empty, empty, empty, empty], extended_frames=[None, extended_hit]
        )

        row = result.iloc[0]
        assert row["SPOP"] == 250.0
        assert row["estimation_method"] == "extended_lookback_fallback"

    def test_multiple_value_columns_all_estimated_consistently(self):
        week1 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0, "M00": 10.0}])
        week2 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 200.0, "M00": 20.0}])
        week3 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0, "M00": 30.0}])
        week4 = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 400.0, "M00": 40.0}])

        result = estimate_day.build_nowcast_table([week1, week2, week3, week4])

        row = result.iloc[0]
        assert row["M00"] == 10.0 * 0.4 + 20.0 * 0.3 + 30.0 * 0.2 + 40.0 * 0.1

class TestHistoricalAverage:
    def test_averages_value_columns_grouped_by_key_across_all_frames(self):
        frame_a = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}])
        frame_b = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0}])

        result = estimate_day.historical_average([frame_a, frame_b])

        row = result.loc[("H1", "C1", "10")]
        assert row["SPOP"] == 200.0

    def test_ignores_none_frames(self):
        frame_a = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0}])

        result = estimate_day.historical_average([frame_a, None])

        assert result.loc[("H1", "C1", "10")]["SPOP"] == 100.0

    def test_returns_none_when_no_frames_available(self):
        assert estimate_day.historical_average([None, None]) is None


class TestBuildNowcastTableUsesHistoricalAverage:
    def test_falls_back_to_historical_avg_when_no_other_source(self):
        empty = _frame([])
        historical = _frame([{"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 180.0}])

        result = estimate_day.build_nowcast_table(
            [empty, empty, empty, empty],
            extended_frames=[None],
            historical_avg_frame=historical,
        )

        row = result.iloc[0]
        assert row["SPOP"] == 180.0
        assert row["estimation_method"] == "grid_historical_avg"


class TestMultipleKeysEstimatedIndependently:
    def test_multiple_keys_estimated_independently(self):
        week1 = _frame(
            [
                {"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 100.0},
                {"H_DNG_CD": "H1", "CELL_ID": "C2", "TT": "10", "SPOP": 500.0},
            ]
        )
        week2 = _frame(
            [
                {"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 200.0},
                {"H_DNG_CD": "H1", "CELL_ID": "C2", "TT": "10", "SPOP": 600.0},
            ]
        )
        week3 = _frame(
            [
                {"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 300.0},
                {"H_DNG_CD": "H1", "CELL_ID": "C2", "TT": "10", "SPOP": 700.0},
            ]
        )
        week4 = _frame(
            [
                {"H_DNG_CD": "H1", "CELL_ID": "C1", "TT": "10", "SPOP": 400.0},
                {"H_DNG_CD": "H1", "CELL_ID": "C2", "TT": "10", "SPOP": 800.0},
            ]
        )

        result = estimate_day.build_nowcast_table([week1, week2, week3, week4])

        by_cell = result.set_index("CELL_ID")
        assert by_cell.loc["C1", "SPOP"] == 100.0 * 0.4 + 200.0 * 0.3 + 300.0 * 0.2 + 400.0 * 0.1
        assert by_cell.loc["C2", "SPOP"] == 500.0 * 0.4 + 600.0 * 0.3 + 700.0 * 0.2 + 800.0 * 0.1
