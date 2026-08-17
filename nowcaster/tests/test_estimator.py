"""estimator.py: 가중평균, 결측/불일치 재조정, 폴백 체인 (순수 함수, I/O 없음)."""

import pytest

import estimator


class TestFullWeightedAverage:
    def test_all_four_candidates_present(self):
        value, method = estimator.estimate([100.0, 200.0, 300.0, 400.0])

        assert value == 100.0 * 0.4 + 200.0 * 0.3 + 300.0 * 0.2 + 400.0 * 0.1
        assert method == "weighted_avg"


class TestReweightedAverage:
    def test_one_missing_renormalizes_remaining_weights(self):
        # 2주 전(30%) 결측 -> 남은 가중치 70% 기준 재조정
        value, method = estimator.estimate([100.0, None, 300.0, 400.0])

        expected = (100.0 * 0.4 + 300.0 * 0.2 + 400.0 * 0.1) / 0.7
        assert value == pytest.approx(expected)
        assert method == "reweighted_avg"

    def test_two_missing_renormalizes_remaining_weights(self):
        value, method = estimator.estimate([100.0, None, None, 400.0])

        expected = (100.0 * 0.4 + 400.0 * 0.1) / 0.5
        assert value == pytest.approx(expected)
        assert method == "reweighted_avg"


class TestSingleWeekFallback:
    def test_three_missing_uses_remaining_one_as_is(self):
        value, method = estimator.estimate([None, None, 300.0, None])

        assert value == 300.0
        assert method == "single_week_fallback"


class TestWorstCaseFallbackChain:
    def test_all_four_missing_uses_extended_lookback_first_valid(self):
        value, method = estimator.estimate(
            [None, None, None, None], extended=[None, 250.0, 260.0]
        )

        assert value == 250.0
        assert method == "extended_lookback_fallback"

    def test_all_missing_including_extended_uses_historical_avg(self):
        value, method = estimator.estimate(
            [None, None, None, None], extended=[None, None], historical_avg=180.0
        )

        assert value == 180.0
        assert method == "grid_historical_avg"

    def test_nothing_available_falls_back_to_zero(self):
        value, method = estimator.estimate([None, None, None, None], extended=[None])

        assert value == 0.0
        assert method == "no_data"
