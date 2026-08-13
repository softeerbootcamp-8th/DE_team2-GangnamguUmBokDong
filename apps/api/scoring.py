from datetime import datetime

# 우선순위 점수 계산용 임계값. 실측치가 나오면 팀 확인 후 확정해야 한다.
RESPONSE_LAG_MIN = 30  # 트럭 출동~도착 소요시간
HALF_LIFE_MIN = 60  # 대응 여유시간이 이만큼 늘어날 때마다 시급성 점수가 절반이 됨
FIRST_FORECAST_MIN = 60  # 예측 데이터는 1시간 뒤부터만 존재(그 이전은 추세로 메꿈)


def enrich_forecast_points(current_stock: int, hold_cnt: int, raw_points: list[dict]) -> list[dict]:
    """모델이 낸 대여·반납량(원본치)에 현재 재고를 누적해 예측 재고·action_type을 계산한다.

    0 밑으로는 못 내려가게 막지만(자전거 수가 마이너스일 순 없다), 정원 위로는 막지
    않는다. 거치대에 꽂는 방식이 아니라 비콘 기반이라 반납 자체는 막히지 않아서,
    실제로 정원을 넘는 대여소가 있기 때문이다.
    """
    predicted = current_stock
    points = []
    for raw in raw_points:
        predicted = max(0, predicted + raw["predicted_return_cnt"] - raw["predicted_rent_cnt"])
        if predicted == 0:
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


def _net_demand(point: dict, action_type: str) -> int:
    if action_type == "supply_needed":
        return point["predicted_rent_cnt"] - point["predicted_return_cnt"]
    return point["predicted_return_cnt"] - point["predicted_rent_cnt"]


def _cumulative_deficit(current_stock: int, points: list[dict], index: int) -> int:
    """공급필요 전용. 재고는 0 밑으로 못 내려가게 막혀있어서(클램프), 그 시점의
    predicted_bikes만 보면 얼마나 부족한지 크기를 알 수 없다. 막지 않았다면 얼마나
    마이너스로 내려갔을지를 다시 계산한다. 회수필요는 반대로 위쪽을 막지 않으므로
    predicted_bikes에서 바로 읽으면 된다."""
    stock = current_stock
    for i in range(index + 1):
        stock += points[i]["predicted_return_cnt"] - points[i]["predicted_rent_cnt"]
    return max(0, -stock)


def urgency_score(
    current: int,
    hold_cnt: int,
    stock_history: list[dict],
    points: list[dict],
    now: datetime,
) -> tuple[float, int, str]:
    """우선순위 점수 = 시급성(언제 위험해지나) × 심각도(그때 얼마나 아프나).

    시급성: 즉시위험(지금 이미 0석/만재)/추세감지(최근 재고 추세로 1시간 안에
    위험)/예측감지(예측 그래프상 처음 이상해지는 시점) 셋 중 가장 이른 시점을
    쓴다. 트럭이 도착하는 데 RESPONSE_LAG_MIN이 걸리므로, 그보다 짧게 남은
    시간은 "대응 여유가 없음"으로 취급해 전부 최대 긴급도로 묶는다.

    심각도: 공급필요는 재고가 0 밑으로 못 내려가게 막혀있어 실측만으로는 얼마나
    부족한지 알 수 없으므로, 즉시위험이면 가장 이른 예측 포인트의 순수요로,
    예측감지면 클램프 없이 뒀을 때의 마이너스분을 다시 계산해서 쓴다. 회수필요는
    정원 위로는 막지 않으므로(비콘 기반이라 반납 자체는 안 막힘) 실측 재고나
    predicted_bikes에서 정원을 뺀 실제 초과분을 그대로 쓴다.
    """
    if current <= 0:
        time_to_critical, source, action_type, forecast_index = 0.0, "immediate", "supply_needed", None
    elif current >= hold_cnt:
        time_to_critical, source, action_type, forecast_index = 0.0, "immediate", "retrieval_needed", None
    else:
        candidates = []
        trend = _trend_time_to_critical(current, hold_cnt, stock_history, now)
        if trend is not None:
            candidates.append((trend[0], "trend", trend[1], None))
        forecast = _forecast_time_to_critical(points)
        if forecast is not None:
            minutes, index, forecast_action = forecast
            candidates.append((minutes, "forecast", forecast_action, index))
        if not candidates:
            return 0.0, 12 * 60, "normal"
        time_to_critical, source, action_type, forecast_index = min(candidates, key=lambda c: c[0])

    slack = max(0.0, time_to_critical - RESPONSE_LAG_MIN)
    time_factor = 2 ** (-slack / HALF_LIFE_MIN)

    if source == "immediate" and action_type == "retrieval_needed":
        overshoot = current - hold_cnt  # 실측으로 바로 알 수 있는 값
        impact_factor = min(1.0, overshoot / hold_cnt)
    elif source == "forecast" and action_type == "retrieval_needed":
        overshoot = points[forecast_index]["predicted_bikes"] - hold_cnt
        impact_factor = min(1.0, overshoot / hold_cnt)
    elif source == "forecast" and action_type == "supply_needed":
        deficit = _cumulative_deficit(current, points, forecast_index)
        impact_factor = min(1.0, deficit / hold_cnt)
    else:
        # 즉시위험(공급필요)/추세감지: 위험 시점의 실측·예측 데이터가 아직 없어
        # 가장 이른 예측 포인트의 순수요로 대신한다. 예측 자체가 없으면(배치가
        # 아직 안 돌았으면) 심각도를 매길 근거가 없어 0으로 둔다.
        net = _net_demand(points[0], action_type) if points else 0
        impact_factor = min(1.0, max(0, net) / hold_cnt)

    score = round(100 * time_factor * impact_factor, 1)
    return score, round(time_to_critical), action_type
