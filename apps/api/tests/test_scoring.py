"""scoring.py: _severity, _max_overshoot, _max_deficit, urgency_score 테스트.

PR #40 리뷰에서 지적된 대로(#55) 예측 구간 전체를 스캔하는 로직과 점근 곡선에
대한 안전망. hold_cnt=0, 정원 초과, 예측 없음 등 경계값을 우선 다룬다.
"""

from datetime import datetime

from scoring import _max_deficit, _max_overshoot, _severity, urgency_score

NOW = datetime(2024, 1, 1, 12, 0, 0)


def _point(rent: int, ret: int, predicted_bikes: int, action_type: str) -> dict:
    return {
        "predicted_rent_cnt": rent,
        "predicted_return_cnt": ret,
        "predicted_bikes": predicted_bikes,
        "action_type": action_type,
    }


class TestSeverity:
    def test_zero_ratio_is_zero_severity(self):
        assert _severity(0.0) == 0.0

    def test_ratio_equal_to_scale_matches_known_value(self):
        # SEVERITY_SCALE=1.5 -> 1 - e^(-1) ≈ 0.6321205588
        assert round(_severity(1.5), 10) == 0.6321205588

    def test_severity_increases_monotonically_with_ratio(self):
        assert _severity(0.1) < _severity(1.0) < _severity(4.0)

    def test_severity_approaches_but_never_reaches_one(self):
        assert _severity(50) < 1.0


class TestMaxOvershoot:
    def test_empty_points_uses_current_only(self):
        assert _max_overshoot(current=15, hold_cnt=10, points=[]) == 5

    def test_empty_points_under_capacity_is_zero(self):
        assert _max_overshoot(current=5, hold_cnt=10, points=[]) == 0

    def test_peak_can_come_from_a_later_point_not_current(self):
        points = [_point(0, 0, 8, "normal"), _point(0, 0, 14, "retrieval_needed")]
        assert _max_overshoot(current=5, hold_cnt=10, points=points) == 4

    def test_current_already_over_capacity_counts_even_if_points_recover(self):
        points = [_point(0, 0, 8, "normal")]
        assert _max_overshoot(current=13, hold_cnt=10, points=points) == 3


class TestMaxDeficit:
    def test_empty_points_nonnegative_current_is_zero(self):
        assert _max_deficit(current=5, points=[]) == 0

    def test_uses_unclamped_raw_deltas_not_predicted_bikes(self):
        # predicted_bikes는 0에서 클램프되지만(-5 -> 0), _max_deficit은 클램프 없이
        # 원본 대여/반납량으로 다시 누적해서 실제로 얼마나 모자랐는지 잰다.
        points = [_point(rent=8, ret=0, predicted_bikes=0, action_type="supply_needed")]
        assert _max_deficit(current=3, points=points) == 5

    def test_deepest_point_wins_even_if_later_points_recover(self):
        points = [
            _point(rent=8, ret=0, predicted_bikes=0, action_type="supply_needed"),
            _point(rent=0, ret=10, predicted_bikes=10, action_type="retrieval_needed"),
        ]
        assert _max_deficit(current=3, points=points) == 5

    def test_never_going_negative_is_zero_deficit(self):
        points = [_point(rent=1, ret=0, predicted_bikes=4, action_type="normal")]
        assert _max_deficit(current=5, points=points) == 0


class TestUrgencyScore:
    def test_current_at_or_below_threshold_is_immediate_supply_needed(self):
        # hold_cnt=10, SUPPLY_LOW_STOCK_RATIO=0.2 -> 임계값 2. current=1은 그 이하.
        score, time_to_critical, action_type = urgency_score(
            current=1, hold_cnt=10, stock_history=[], points=[], now=NOW
        )
        assert (score, time_to_critical, action_type) == (0.0, 0, "supply_needed")

    def test_current_at_or_above_capacity_is_immediate_retrieval_needed(self):
        score, time_to_critical, action_type = urgency_score(
            current=15, hold_cnt=10, stock_history=[], points=[], now=NOW
        )
        assert (score, time_to_critical, action_type) == (28.3, 0, "retrieval_needed")

    def test_hold_cnt_zero_does_not_divide_by_zero(self):
        score, time_to_critical, action_type = urgency_score(
            current=0, hold_cnt=0, stock_history=[], points=[], now=NOW
        )
        assert (score, time_to_critical, action_type) == (0.0, 0, "supply_needed")

    def test_no_signal_at_all_returns_normal_default(self):
        # 재고이력 2개 미만(추세 계산 불가) + 예측 전부 정상 -> 감지된 시급성 없음.
        score, time_to_critical, action_type = urgency_score(
            current=5, hold_cnt=10, stock_history=[], points=[], now=NOW
        )
        assert (score, time_to_critical, action_type) == (0.0, 720, "normal")

    def test_forecast_detected_deficit_scores_by_deepest_point(self):
        points = [
            _point(rent=1, ret=0, predicted_bikes=4, action_type="normal"),
            _point(rent=1, ret=0, predicted_bikes=3, action_type="normal"),
            _point(rent=8, ret=0, predicted_bikes=0, action_type="supply_needed"),
        ]
        score, time_to_critical, action_type = urgency_score(
            current=5, hold_cnt=10, stock_history=[], points=points, now=NOW
        )
        # 3번째(인덱스 2) 지점에서 처음 supply_needed -> (2+1)*60=180분 뒤.
        assert (score, time_to_critical, action_type) == (5.0, 180, "supply_needed")
