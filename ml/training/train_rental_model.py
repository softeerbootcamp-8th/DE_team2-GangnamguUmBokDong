"""대여 수요 모델 학습: Poisson(+exposure offset, 품절 보정) + quantile P10/P50/P90."""

import json

import pandas as pd

from . import config
from .train_common import train_target


def main() -> dict:
    """feature 테이블을 읽어 대여 모델을 학습하고 평가 지표를 출력한다.

    returns:
        dict: train_target()의 평가 지표
    """
    df = pd.read_parquet(config.FEATURES_TABLE_PARQUET)
    metrics = train_target(df, "rental_count", "rental", exposure_col="rental_exposure")
    print(json.dumps(metrics, indent=2, ensure_ascii=False))
    return metrics


if __name__ == "__main__":
    main()
