"""scoring.py: _severity, _max_overshoot, _max_deficit, urgency_score 테스트.

PR #40 리뷰에서 지적된 대로(#55) 예측 구간 전체를 스캔하는 로직과 점근 곡선에
대한 안전망. hold_cnt=0, 정원 초과, 예측 없음 등 경계값을 우선 다룬다.
"""

from datetime import UTC, datetime, timedelta

from scoring import (
    _max_deficit,
    _max_overshoot,
    _max_unmet_demand,
    _severity,
    _trend_time_to_critical,
    urgency_score,
)

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def _point(rent: int, ret: int, predicted_bikes: int, action_type: str) -> dict:
    return {
        "predicted_rent_cnt": rent,
        "predicted_return_cnt": ret,
        "predicted_bikes": predicted_bikes,
        "action_type": action_type,
    }


def _history(before: int, now_count: int) -> list[dict]:
    """10분 전 재고(before)에서 지금(now_count)까지 두 시점짜리 재고 이력을 만든다."""
    return [
        {"observed_at": NOW - timedelta(minutes=10), "parking_bike_tot_cnt": before},
        {"observed_at": NOW, "parking_bike_tot_cnt": now_count},
    ]


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


class TestMaxUnmetDemand:
    def test_flat_net_change_at_low_stock_still_counts_demand(self):
        # rent=ret=10 -> 순변화 0. _max_deficit은 이걸 0으로 보지만, 그 시간대가
        # 시작할 때 이미 재고(2)가 threshold(hold_cnt=10*0.2=2) 이하였으므로
        # _max_unmet_demand는 그때 들어온 대여 수요(10)를 그대로 잡아야 한다.
        points = [_point(rent=10, ret=10, predicted_bikes=2, action_type="normal")]
        assert _max_deficit(current=2, points=points) == 0
        assert _max_unmet_demand(current=2, hold_cnt=10, points=points) == 10

    def test_demand_above_threshold_is_ignored(self):
        # 시간대 시작 시점 재고(5)가 threshold(2) 위였다면, 그 시간대의 대여
        # 수요가 아무리 커도 이 함수가 잡을 신호가 아니다(다른 경로가 담당).
        points = [_point(rent=10, ret=10, predicted_bikes=5, action_type="normal")]
        assert _max_unmet_demand(current=5, hold_cnt=10, points=points) == 0

    def test_worst_point_wins(self):
        points = [
            _point(rent=3, ret=3, predicted_bikes=2, action_type="normal"),
            _point(rent=9, ret=9, predicted_bikes=2, action_type="normal"),
        ]
        assert _max_unmet_demand(current=2, hold_cnt=10, points=points) == 9


class TestTrendTimeToCritical:
    def test_fewer_than_two_history_points_returns_none(self):
        assert _trend_time_to_critical(current=5, hold_cnt=10, stock_history=[], now=NOW) is None
        single = [{"observed_at": NOW, "parking_bike_tot_cnt": 5}]
        assert _trend_time_to_critical(current=5, hold_cnt=10, stock_history=single, now=NOW) is None

    def test_declining_trend_detects_supply_needed(self):
        # 10분 새 10 -> 5, 분당 -0.5. 지금 재고(5) 기준 0석까지 10분.
        history = _history(before=10, now_count=5)
        assert _trend_time_to_critical(current=5, hold_cnt=10, stock_history=history, now=NOW) == (10.0, "supply_needed")

    def test_rising_trend_detects_retrieval_needed(self):
        # 10분 새 5 -> 10, 분당 +0.5. 정원(10)까지 이미 도달해 0분.
        history = _history(before=5, now_count=10)
        assert _trend_time_to_critical(current=5, hold_cnt=10, stock_history=history, now=NOW) == (10.0, "retrieval_needed")

    def test_flat_trend_returns_none(self):
        history = _history(before=5, now_count=5)
        assert _trend_time_to_critical(current=5, hold_cnt=10, stock_history=history, now=NOW) is None

    def test_trend_slower_than_first_forecast_min_is_discarded(self):
        # 분당 -0.1로 아주 완만해서 0석까지 500분 -> FIRST_FORECAST_MIN(60분) 이후라
        # 예측모델이 그 구간을 담당하므로 추세 감지는 버려야 한다.
        history = _history(before=51, now_count=50)
        assert _trend_time_to_critical(current=50, hold_cnt=100, stock_history=history, now=NOW) is None


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

    def test_unmet_demand_scores_even_when_net_change_is_flat(self):
        # PR #96 리뷰(#dragonjin520)에서 지적된 회귀 케이스: 대여/반납이 똑같이
        # 10건씩 들어와 순변화는 0(_max_deficit=0)이지만, 재고가 이미 threshold
        # 이하였던 시간대에 대여 수요가 컸다면 _max_unmet_demand가 severity를
        # 대신 끌어올려서 score가 0으로 묻히면 안 된다.
        points = [_point(rent=10, ret=10, predicted_bikes=2, action_type="normal")]
        score, time_to_critical, action_type = urgency_score(
            current=2, hold_cnt=10, stock_history=[], points=points, now=NOW
        )
        assert (score, time_to_critical, action_type) == (48.7, 0, "supply_needed")

    def test_trend_detected_sooner_than_forecast_wins_and_still_scores_severity(self):
        # 재고 이력상 추세로는 10분 뒤 위험(_trend_time_to_critical), 예측으로는
        # 180분 뒤 위험(_forecast_time_to_critical) -> 둘 다 candidates에 들어가고
        # 더 이른 추세 쪽이 채택돼야 한다(#96 리뷰: stock_history=[]만 쓰면 이 경로가
        # 전혀 실행되지 않는다는 지적). severity는 감지 경로와 무관하게 points로
        # 계산되므로 forecast만 있던 기존 케이스(180분, score 5.0)보다
        # slack이 줄어 score가 커진다.
        history = _history(before=10, now_count=5)
        points = [
            _point(rent=1, ret=0, predicted_bikes=4, action_type="normal"),
            _point(rent=1, ret=0, predicted_bikes=3, action_type="normal"),
            _point(rent=8, ret=0, predicted_bikes=0, action_type="supply_needed"),
        ]
        score, time_to_critical, action_type = urgency_score(
            current=5, hold_cnt=10, stock_history=history, points=points, now=NOW
        )
        assert (score, time_to_critical, action_type) == (28.3, 10, "supply_needed")
