"""training(학습 중 평가)과 inference(서빙 성능 모니터링)가 공유하는 평가 지표 함수.

`training/train_common.py`(학습 직후 테스트셋 평가), `training/monitor_performance.py`
(매달 실측 성능 재평가), `ml_common/scoring.py`의 `print_metrics()`(배치 조회 결과 검증)가
전부 같은 정의의 poisson deviance/pinball loss를 써야 baseline과 최신 값을 정확히
비교할 수 있다 — 각자 재구현하면 부동소수점 수준의 미세한 차이라도 조용히 갈라질
위험이 있어 한 곳으로 모았다.
"""

import numpy as np


def pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, alpha: float) -> float:
    """quantile alpha에 대한 pinball(quantile) loss를 계산한다.

    args:
        y_true: 실제값
        y_pred: 예측한 alpha-quantile 값
        alpha: 0~1 사이 quantile 수준
    returns:
        float: 평균 pinball loss
    """
    diff = y_true - y_pred
    return float(np.mean(np.maximum(alpha * diff, (alpha - 1) * diff)))


def poisson_deviance(y_true: np.ndarray, mu: np.ndarray) -> float:
    """Poisson deviance를 계산한다 (RMSE와 함께 point 예측 평가에 사용).

    args:
        y_true: 실제 카운트
        mu: 예측한 평균(rate)
    returns:
        float: 평균 Poisson deviance
    """
    mu = np.clip(mu, 1e-6, None)
    y_true = np.asarray(y_true, dtype=float)
    y_safe = np.where(y_true > 0, y_true, 1.0)  # log(0) 회피용 — 해당 원소는 아래 where에서 버려짐
    term = np.where(y_true > 0, y_true * np.log(y_safe / mu) - (y_true - mu), mu)
    return float(2 * np.mean(term))
