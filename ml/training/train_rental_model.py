"""대여 수요 모델 학습: Poisson(+exposure offset, 품절 보정) + quantile P10/P50/P90.

`horizon`(몇 시간 뒤를 예측하는지)이 RENTAL_FEATURE_COLUMNS에 포함돼 있어, 한 모델이
horizon=1..HORIZON_COUNT 전체를 학습한다(history.md 18번 항목 — "horizon을 feature로").

**항상 아카이브에 저장한다(챔피언 파일에 직접 쓰지 않음)** — 일반 재학습은
`training/promotion.py`가 이 결과를 챔피언과 비교해 조건을 만족할 때만 챔피언
포인터를 전환한다(`training/scripts/monthly_retrain_check.py` 참고). 최초 배포만
명시적 `--promote-if-no-champion`으로 같은 profile guard를 거쳐 포인터를 만들 수
있으며, 이미 대여 챔피언이 있으면 학습 시작 전에 실패해 절대 덮어쓰지 않는다.
날짜/프로필은
`MODEL_ARCHIVE_DATE`/`ML_PROFILE` 환경변수로 정해진다(둘 다 미지정 시 실행마다
유일한 날짜 / `builtin-default` 프로필 — 수동 실행 시 그대로 씀. `MODEL_ARCHIVE_DATE`
미지정 기본값이 실행마다 달라야 하는 이유는 `config.unique_archive_date()` 참고
— 같은 날 두 번 실행해도 archive_prefix가 겹치면 안 됨).
"""

import argparse
import json
import os

from ml_core import common_config
from ml_core.paths import archive_models_prefix

from .config import unique_archive_date
from .promotion import bootstrap_challenger, ensure_champion_absent
from .train_common import run_and_notify_on_failure, train_target


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """대여 모델 학습 CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(description="대여 수요 모델을 학습해 새 아카이브에 저장합니다")
    parser.add_argument(
        "--promote-if-no-champion",
        action="store_true",
        help="대여 챔피언이 아직 없을 때만 학습 결과를 최초 챔피언으로 승격합니다",
    )
    return parser.parse_args(argv)


def main(promote_if_no_champion: bool = False) -> dict:
    """multi-horizon feature 테이블을 읽어 대여 모델을 학습하고 평가 지표를 출력한다.

    args:
        promote_if_no_champion: 챔피언이 없을 때만 profile guard 후 최초 포인터 생성
    returns:
        dict: train_target()의 평가 지표
    """
    archive_date = os.environ.get("MODEL_ARCHIVE_DATE", unique_archive_date())
    models_prefix = archive_models_prefix(archive_date, common_config.PROFILE_NAME)
    if promote_if_no_champion:
        ensure_champion_absent("rental")

    metrics = train_target(
        "rental_count", "rental", exposure_col="rental_exposure", models_prefix=models_prefix
    )
    if promote_if_no_champion:
        bootstrap_challenger("rental", models_prefix)
        print(f"[train_rental_model] 최초 챔피언 승격 완료: {models_prefix}")
    print(json.dumps(metrics, indent=2, ensure_ascii=False))
    return metrics


if __name__ == "__main__":
    args = _parse_args()
    run_and_notify_on_failure(
        "train_rental_model",
        lambda: main(promote_if_no_champion=args.promote_if_no_champion),
    )
