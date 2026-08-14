import math
from datetime import datetime

# 우선순위 점수 계산용 임계값. 실측치가 나오면 팀 확인 후 확정해야 한다.
RESPONSE_LAG_MIN = 30  # 트럭 출동~도착 소요시간
HALF_LIFE_MIN = 60  # 대응 여유시간이 이만큼 늘어날 때마다 시급성 점수가 절반이 됨
FIRST_FORECAST_MIN = 60  # 예측 데이터는 1시간 뒤부터만 존재(그 이전은 추세로 메꿈)

# 심각도(정원 대비 초과/부족량 비율 -> 0~1) 변환 곡선의 스케일. 비율이 이 값만큼
# 쌓일 때마다 남은 여유가 지수적으로 줄어든다(1 - e^(-ratio/SEVERITY_SCALE)).
# 실측 데이터(서울 전역 2,746곳)로 몇 가지 값을 대입해보고, 비율 1(정원만큼
# 초과/부족)이 40점대, 비율 4 이상(정원의 4배 이상)이 90점대로 나오는 이 값을
# 골랐다. 고정 배수에서 상한을 자르는 클램프 대신 점근 곡선을 쓴 이유는, 클램프를
# 쓰면 특정 배수를 넘는 순간부터 아무리 더 심해져도 점수가 그대로라 실측에서
# 회수필요 대여소의 35%가 똑같이 100점에 뭉쳐 있었기 때문이다(정원 2배 초과 =
# 클램프 상한). 점근 곡선은 상한이 없어서 극단치(실측 최대 22배)끼리도 계속
# 구분된다.
SEVERITY_SCALE = 1.5


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

    시급성: 즉시위험(지금 이미 0석/만재)/추세감지(최근 재고 추세로 1시간 안에
    위험)/예측감지(예측 그래프상 처음 이상해지는 시점) 셋 중 가장 이른 시점을
    쓴다. 트럭이 도착하는 데 RESPONSE_LAG_MIN이 걸리므로, 그보다 짧게 남은
    시간은 "대응 여유가 없음"으로 취급해 전부 최대 긴급도로 묶는다.

    심각도: 지금부터 예측 구간 전체에서 가장 심해지는 지점(회수필요는 정원을
    가장 크게 넘는 지점, 공급필요는 클램프 없이 뒀을 때 가장 깊이 마이너스로
    내려가는 지점)을 찾아 정원 대비 비율로 바꾸고, 그 비율을 `_severity`로
    0~1 사이 값으로 변환한다. 어느 시급성 경로(즉시위험/추세감지/예측감지)로
    감지됐는지와 무관하게 항상 같은 방식으로 계산해서, 두 action_type의 점수가
    같은 기준으로 비교 가능하다.
    """
    if current <= 0:
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

    if action_type == "retrieval_needed":
        ratio = _max_overshoot(current, hold_cnt, points) / hold_cnt
    else:
        ratio = _max_deficit(current, points) / hold_cnt
    impact_factor = _severity(ratio)

    score = round(100 * time_factor * impact_factor, 1)
    return score, round(time_to_critical), action_type
