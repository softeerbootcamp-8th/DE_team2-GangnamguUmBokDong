"""반납 수요 모델 학습: 순수 Poisson(거치대 상태 무관, exposure 보정 없음) + quantile P10/P50/P90.

`horizon`(몇 시간 뒤를 예측하는지)이 FEATURE_COLUMNS에 포함돼 있어, 한 모델이
horizon=1..HORIZON_COUNT 전체를 학습한다(history.md 18번 항목 — "horizon을 feature로").
"""

import json

from .train_common import load_training_table, train_target


def main() -> dict:
    """multi-horizon feature 테이블을 읽어 반납 모델을 학습하고 평가 지표를 출력한다.

    returns:
        dict: train_target()의 평가 지표
    """
    df = load_training_table()
    metrics = train_target(df, "return_count", "return", exposure_col=None)
    print(json.dumps(metrics, indent=2, ensure_ascii=False))
    return metrics


if __name__ == "__main__":
    main()
