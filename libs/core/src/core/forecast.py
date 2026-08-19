from core.scoring_config import SUPPLY_LOW_STOCK_RATIO


def enrich_forecast_points(current_stock: int, hold_cnt: int, raw_points: list[dict]) -> list[dict]:
    """모델이 낸 대여·반납량(원본치)에 현재 재고를 누적해 예측 재고·action_type을 계산한다.

    0 밑으로는 못 내려가게 막지만(자전거 수가 마이너스일 순 없다), 정원 위로는 막지
    않는다. 거치대에 꽂는 방식이 아니라 비콘 기반이라 반납 자체는 막히지 않아서,
    실제로 정원을 넘는 대여소가 있기 때문이다.

    apps/api(실시간, /stations/{sta_id}/forecast)와 rebalance(배치, urgency 계산)
    양쪽에서 쓰여 이 파일 하나를 공유한다.
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
