import math
from datetime import datetime

from scoring_config import (
    FIRST_FORECAST_MIN,
    HALF_LIFE_MIN,
    RESPONSE_LAG_MIN,
    SEVERITY_SCALE,
    SUPPLY_LOW_STOCK_RATIO,
)


def enrich_forecast_points(current_stock: int, hold_cnt: int, raw_points: list[dict]) -> list[dict]:
    """모델이 낸 대여·반납량(원본치)에 현재 재고를 누적해 예측 재고·action_type을 계산한다.

    0 밑으로는 못 내려가게 막지만(자전거 수가 마이너스일 순 없다), 정원 위로는 막지
    않는다. 거치대에 꽂는 방식이 아니라 비콘 기반이라 반납 자체는 막히지 않아서,
    실제로 정원을 넘는 대여소가 있기 때문이다.
    """
    predicted = current_stock
    supply_threshold = SUPPLY_LOW_STOCK_RATIO * hold_cnt
    points = []
    for raw in raw_points:
        predicted = max(0, predicted + raw["predicted_return_cnt"] - raw["predicted_rent_cnt"])
        if predicted <= supply_threshold:
            action_type = "supply_needed"
        elif predicted >= hold_cnt:
            action_type = "retrieval_needed"
        else:
            action_type = "normal"
        points.append({**raw, "predicted_bikes": predicted, "action_type": action_type})
    return points


def _regression_slope(xs: list[float], ys: list[float]) -> float:
    """최소제곱법으로 (x,y) 점들에 가장 잘 맞는 직선의 기울기를 구한다."""
    n = len(xs)
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    denominator = sum((x - x_mean) ** 2 for x in xs)
    return numerator / denominator if denominator else 0.0


def _trend_time_to_critical(
    current: int, hold_cnt: int, stock_history: list[dict], now: datetime
) -> tuple[float, str] | None:
    """최근 재고 이력에 회귀선을 그어 순유출/순유입 속도를 구하고, 그 속도가
    이어진다면 몇 분 뒤 0석(또는 만재)이 되는지 추정한다. 예측 데이터가 존재하는
    시점(1시간 뒤) 이전에만 의미가 있어, 그보다 늦게 나오면 버리고 예측모델 쪽
    감지에 맡긴다."""
    if len(stock_history) < 2:
        return None
    xs = [(row["observed_at"] - now).total_seconds() / 60 for row in stock_history]
    ys = [row["parking_bike_tot_cnt"] for row in stock_history]
    slope = _regression_slope(xs, ys)  # 분당 재고 변화량(양수=채워지는 중, 음수=빠지는 중)

    if slope < 0:
        minutes, action_type = current / -slope, "supply_needed"
    elif slope > 0:
        minutes, action_type = (hold_cnt - current) / slope, "retrieval_needed"
    else:
        return None

    if minutes >= FIRST_FORECAST_MIN:
        return None
    return minutes, action_type


def _forecast_time_to_critical(points: list[dict]) -> tuple[float, int, str] | None:
    for i, point in enumerate(points):
        if point["action_type"] != "normal":
            return (i + 1) * 60, i, point["action_type"]
    return None


def _max_overshoot(current: int, hold_cnt: int, points: list[dict]) -> int:
    """지금부터 예측 구간 전체에서 정원을 가장 크게 넘는 지점을 찾는다.
    predicted_bikes는 위쪽을 막지 않으므로(비콘 기반이라 반납이 안 막힘) 그대로
    읽으면 된다. 지금 당장이 아니라 나중에 더 심해지는 경우(추세로 채워지는
    중)까지 포함하도록 현재 재고도 후보에 넣는다."""
    peak = max(current, *(p["predicted_bikes"] for p in points)) if points else current
    return max(0, peak - hold_cnt)


def _max_deficit(current: int, points: list[dict]) -> int:
    """지금부터 예측 구간 전체에서 재고가 0 밑으로 가장 깊이 내려가는 지점을
    찾는다. predicted_bikes는 0 밑을 클램프해서 못 쓰므로(재고가 실제로
    마이너스일 순 없어서), 원본 대여·반납량으로 다시 누적해서 클램프 없이 계산한다."""
    stock = current
    worst = min(current, 0)
    for point in points:
        stock += point["predicted_return_cnt"] - point["predicted_rent_cnt"]
        worst = min(worst, stock)
    return max(0, -worst)


def _max_unmet_demand(current: int, hold_cnt: int, points: list[dict]) -> int:
    """각 시간대가 시작할 때(직전 시간대가 끝난 시점) 재고가 이미 정원의
    SUPPLY_LOW_STOCK_RATIO 이하였는데, 그 시간대에 들어온 대여 수요(predicted_rent_cnt)의
    최댓값을 찾는다. _max_deficit은 그 시간대가 끝날 때의 순누적 결과만 보므로, 그
    안에서 반납이 대여만큼 들어와 재고가 그대로거나 오히려 회복된 시간대(예: 재고
    1에서 대여 10건·반납 10건 -> 재고 1 유지)는 놓친다. 하지만 그 시간대 어느
    순간 재고가 거의 바닥이었는데 대여 수요가 있었다는 사실 자체는 남아 있으므로,
    순누적과 별개로 이 신호를 따로 계산해 둘 중 더 심각한 쪽을 최종 심각도에 쓴다."""
    threshold = SUPPLY_LOW_STOCK_RATIO * hold_cnt
    prev = current
    worst = 0
    for point in points:
        if prev <= threshold:
            worst = max(worst, point["predicted_rent_cnt"])
        prev = point["predicted_bikes"]
    return worst


def _severity(ratio: float) -> float:
    """정원 대비 초과/부족 비율(ratio)을 0~1 심각도로 바꾼다. SEVERITY_SCALE 참고."""
    return 1 - math.exp(-ratio / SEVERITY_SCALE)


def urgency_score(
    current: int,
    hold_cnt: int,
    stock_history: list[dict],
    points: list[dict],
    now: datetime,
) -> tuple[float, int, str]:
    """우선순위 점수 = 시급성(언제 위험해지나) × 심각도(그때 얼마나 아프나).

    시급성: 즉시위험(지금 이미 정원의 SUPPLY_LOW_STOCK_RATIO 이하/만재)/추세감지
    (최근 재고 추세로 1시간 안에 위험)/예측감지(예측 그래프상 처음 이상해지는
    시점) 셋 중 가장 이른 시점을 쓴다. 트럭이 도착하는 데 RESPONSE_LAG_MIN이
    걸리므로, 그보다 짧게 남은 시간은 "대응 여유가 없음"으로 취급해 전부 최대
    긴급도로 묶는다.

    심각도: 지금부터 예측 구간 전체에서 가장 심해지는 지점(회수필요는 정원을
    가장 크게 넘는 지점, 공급필요는 클램프 없이 뒀을 때 가장 깊이 마이너스로
    내려가는 지점과, 재고가 거의 바닥인 채로 대여 수요가 들어온 시간대 중 더
    심각한 쪽)을 찾아 정원 대비 비율로 바꾸고, 그 비율을 `_severity`로 0~1
    사이 값으로 변환한다. 어느 시급성 경로(즉시위험/추세감지/예측감지)로
    감지됐는지와 무관하게 항상 같은 방식으로 계산해서, 두 action_type의 점수가
    같은 기준으로 비교 가능하다.
    """
    if current <= SUPPLY_LOW_STOCK_RATIO * hold_cnt:
        time_to_critical, action_type = 0.0, "supply_needed"
    elif current >= hold_cnt:
        time_to_critical, action_type = 0.0, "retrieval_needed"
    else:
        candidates = []
        trend = _trend_time_to_critical(current, hold_cnt, stock_history, now)
        if trend is not None:
            candidates.append(trend)
        forecast = _forecast_time_to_critical(points)
        if forecast is not None:
            minutes, _index, forecast_action = forecast
            candidates.append((minutes, forecast_action))
        if not candidates:
            return 0.0, 12 * 60, "normal"
        time_to_critical, action_type = min(candidates, key=lambda c: c[0])

    slack = max(0.0, time_to_critical - RESPONSE_LAG_MIN)
    time_factor = 2 ** (-slack / HALF_LIFE_MIN)

    # hold_cnt=0(신규/이상 등록 등)인 대여소가 들어오면 division by zero로 API
    # 전체가 500 에러를 내므로, 최소 1로 방어한다.
    safe_hold_cnt = max(hold_cnt, 1)
    if action_type == "retrieval_needed":
        ratio = _max_overshoot(current, hold_cnt, points) / safe_hold_cnt
    else:
        ratio = max(_max_deficit(current, points), _max_unmet_demand(current, hold_cnt, points)) / safe_hold_cnt
    impact_factor = _severity(ratio)

    score = round(100 * time_factor * impact_factor, 1)
    return score, round(time_to_critical), action_type
