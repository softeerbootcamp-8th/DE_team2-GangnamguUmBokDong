"""반납 수요 모델 학습: 순수 Poisson(거치대 상태 무관, exposure 보정 없음) + quantile P10/P50/P90.

`horizon`(몇 시간 뒤를 예측하는지)이 FEATURE_COLUMNS에 포함돼 있어, 한 모델이
horizon=1..HORIZON_COUNT 전체를 학습한다(history.md 18번 항목 — "horizon을 feature로").

**항상 아카이브에 저장한다(챔피언에 직접 쓰지 않음)** — `training/promotion.py`가
이 결과를 챔피언과 비교해 조건을 만족할 때만 챔피언 경로로 파일명 그대로 복사한다
(`training/scripts/monthly_retrain_check.py` 참고). 날짜/프로필은
`MODEL_ARCHIVE_DATE`/`ML_PROFILE` 환경변수로 정해진다(둘 다 미지정 시 오늘 날짜 /
"default" 프로필 — 수동 실행 시 그대로 씀).
"""

import json
import os

from ml_core import common_config
from ml_core.paths import archive_models_prefix

from .config import today_kst
from .train_common import load_training_table, train_target


def main() -> dict:
    """multi-horizon feature 테이블을 읽어 반납 모델을 학습하고 평가 지표를 출력한다.

    returns:
        dict: train_target()의 평가 지표
    """
    archive_date = os.environ.get("MODEL_ARCHIVE_DATE", today_kst().isoformat())
    models_prefix = archive_models_prefix(archive_date, common_config.PROFILE_NAME)

    df = load_training_table()
    metrics = train_target(df, "return_count", "return", exposure_col=None, models_prefix=models_prefix)
    print(json.dumps(metrics, indent=2, ensure_ascii=False))
    return metrics


if __name__ == "__main__":
    main()
