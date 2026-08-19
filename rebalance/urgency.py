"""대여소별 재배치 긴급도(urgency_score) 배치 계산.

과거 apps/api/scoring.py에 있던 로직을 그대로 이식했다(계산 로직 변경 없음) —
urgency_score는 이제 요청마다가 아니라 5분 배치로 한 번만 계산되고, 결과는
station_urgency 테이블에 적재돼 apps/api/main.py:list_alerts()가 그 결과만
조회한다. enrich_forecast_points는 apps/api(/stations/{sta_id}/forecast, 실시간)와
공유해야 해서 libs/core/src/core/forecast.py로 옮겨 거기서 가져다 쓴다.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime

import pandas as pd
import reader
from core.forecast import enrich_forecast_points
from core.scoring_config import (
    FIRST_FORECAST_MIN,
    HALF_LIFE_MIN,
    RESPONSE_LAG_MIN,
    SEVERITY_SCALE,
    SUPPLY_LOW_STOCK_RATIO,
)

logger = logging.getLogger(__name__)


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

    # hold_cnt=0(신규/이상 등록 등)인 대여소가 들어오면 division by zero로 배치가
    # 죽으므로, 최소 1로 방어한다.
    safe_hold_cnt = max(hold_cnt, 1)
    if action_type == "retrieval_needed":
        ratio = _max_overshoot(current, hold_cnt, points) / safe_hold_cnt
    else:
        ratio = max(_max_deficit(current, points), _max_unmet_demand(current, hold_cnt, points)) / safe_hold_cnt
    impact_factor = _severity(ratio)

    score = round(100 * time_factor * impact_factor, 1)
    return score, round(time_to_critical), action_type


def _predicted_points_by_station(predictions: pd.DataFrame) -> dict[str, list[dict]]:
    """예측 결과 DataFrame(station_id, date, hour, minute, horizon, rental_pred_mean,
    return_pred_mean)을 대여소별로 horizon 순 정렬된 {predicted_rent_cnt,
    predicted_return_cnt} 리스트로 바꾼다. 반올림은 loader/transform.py의
    forecast_points_from_predictions와 같은 규칙(round)을 쓴다."""
    by_station: dict[str, list[dict]] = {}
    for sta_id, group in predictions.groupby("station_id"):
        ordered = group.sort_values("horizon")
        by_station[str(sta_id)] = [
            {
                "predicted_rent_cnt": round(row.rental_pred_mean),
                "predicted_return_cnt": round(row.return_pred_mean),
            }
            for row in ordered.itertuples(index=False)
        ]
    return by_station


def compute_all(anchor: datetime) -> pd.DataFrame:
    """anchor 시점 기준 전체 대여소의 urgency_score를 계산한다.

    입력은 전부 S3(재고 이력·예측 결과)에서만 읽는다 — RDS는 이 배치가 만든 결과를
    loader가 station_urgency에 적재할 때만 쓰인다(배치 자신은 RDS를 건드리지 않음).
    """
    if anchor.minute % 5 or anchor.second or anchor.microsecond:
        raise ValueError(f"anchor must align to a 5-minute tick: {anchor}")

    stock_history_by_station = reader.read_recent_stock(anchor)
    predictions = reader.read_predictions(anchor)
    points_by_station = _predicted_points_by_station(predictions)

    # 5분 snapshot 배치의 "현재 재고"는 anchor tick에서 직접 관측된 값만 쓴다.
    # 5분 이내(예: 14:00을 14:05에 허용)로 느슨하게 잡으면 서로 다른 snapshot의
    # 재고가 한 결과에 섞일 수 있으므로, 누락 station은 이번 batch에서 제외한다.
    current_histories = {
        sta_id: history
        for sta_id, history in stock_history_by_station.items()
        if history and history[-1]["observed_at"] == anchor
    }
    unsupported_stations = set(current_histories) - set(points_by_station)
    if unsupported_stations:
        # run_inference는 학습된 model category만 예측하고, 그 집합 안에서 partial이
        # 발생하면 exit 1로 downstream을 막는다. 따라서 여기의 stock-only station은
        # 신설 등 모델 미지원 대상으로 간주해 제외하되 운영 가시성을 위해 집계한다.
        logger.warning(
            "excluding %d current-stock stations without model predictions",
            len(unsupported_stations),
        )
    computable_station_ids = set(current_histories) & set(points_by_station)

    rows = []
    for sta_id in sorted(computable_station_ids):
        history = current_histories[sta_id]
        current = history[-1]["parking_bike_tot_cnt"]
        hold_cnt = history[-1]["hold_cnt"]
        raw_points = points_by_station[sta_id]
        points = enrich_forecast_points(current, hold_cnt, raw_points)

        score, minutes, action_type = urgency_score(current, hold_cnt, history, points, anchor)
        rows.append(
            {
                "sta_id": sta_id,
                "urgency_score": score,
                "minutes_until_critical": minutes,
                "action_type": action_type,
            }
        )
    return pd.DataFrame(rows, columns=["sta_id", "urgency_score", "minutes_until_critical", "action_type"])
