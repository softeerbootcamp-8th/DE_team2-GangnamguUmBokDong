"""매달 챔피언 모델(대여/반납) 성능을 점검하고, 기준 미달이면 피처마트 생성부터
재학습까지 트리거한다.

**기본은 dry-run이다** — 리포트만 찍고 아무것도 바꾸지 않는다. 실제로 Spark
피처마트 생성(수십 초~수분)과 LightGBM 재학습(모델당 ~25분, 챔피언 모델 파일을
그 자리에서 덮어씀)을 트리거하려면 `--execute`를 명시해야 한다 — 매달 자동으로
돌리는 운영 환경에서는 그 스케줄러(cron/EMR step 등)가 `--execute`로 호출하면 된다.

기준(어느 정도 악화되면 재학습할지)은 [common_config.py](../../../libs/ml_common/common_config.py)에서
관리한다 — 여기서는 그 기준을 적용만 한다.

**주의**: `--execute`는 재학습된 모델로 챔피언을 즉시 덮어쓴다(챌린저를 따로 만들어
비교 후 승격하는 방식이 아니다). 승격 전 비교가 필요하면 지금은 이 스크립트
실행 전에 `models/`를 백업해두거나, 별도 challenger 워크플로를 추가해야 한다.

실행 예:
    ./.venv/bin/python -m training.scripts.monthly_retrain_check              # 점검만 (dry-run)
    ./.venv/bin/python -m training.scripts.monthly_retrain_check --execute    # 기준 미달 시 실제 재학습
"""

import argparse
import json
import subprocess

from ml_common.paths import ML_ROOT

from .. import config
from ..monitor_performance import MODEL_SPECS, check_all_models
from ..train_common import load_training_table, train_target

SPARK_PYTHON = ML_ROOT / "feature_engineering" / ".venv" / "bin" / "python"


def _print_report(results: list[dict]) -> None:
    print("=== 월별 성능 점검 ===")
    for r in results:
        status = "재학습 필요" if r["needs_retrain"] else "정상"
        print(f"[{r['model_name']}] {status} — {r['period']['start']}~{r['period']['end']} ({r['n_rows']:,}행)")
        print(
            f"    deviance: baseline={r['baseline_deviance']:.4f} 현재={r['current_deviance']:.4f} "
            f"({r['deviance_relative_change']:+.1%})"
        )
        print(
            f"    coverage: baseline={r['baseline_coverage']:.3f} 현재={r['current_coverage']:.3f} "
            f"(drift={r['coverage_drift']:.1%}p)"
        )
        for reason in r["reasons"]:
            print(f"    - {reason}")


def _trigger_feature_pipeline() -> None:
    """feature_engineering/spark의 증분 파이프라인 + multi-horizon 테이블 생성을 Spark
    전용 venv(Python 3.11)에서 실행한다.

    rental/return 두 모델이 같은 multi-horizon feature mart(파라미터 조합 하나)를 같이
    쓰므로, 어느 모델이 기준 미달이든 이 파이프라인은 한 번만 실행하면 된다.

    **주의(1단계 한계)**: `build_multi_horizon_features`는 아직 증분(watermark)을 지원하지
    않아 매번 전체를 다시 만든다(feature_engineering/spark/build_multi_horizon_features.py
    docstring 참고) — multi-horizon 테이블이 원본의 최대 HORIZON_COUNT배라 이 단계가
    월별 점검 중 가장 오래 걸리는 부분이 될 수 있다.
    """
    if not SPARK_PYTHON.exists():
        raise RuntimeError(f"{SPARK_PYTHON}가 없습니다 — feature_engineering/에서 'uv sync'를 먼저 실행해야 합니다")
    print(f"[trigger] feature_engineering.spark.run_pipeline 실행 중 ({SPARK_PYTHON})...")
    subprocess.run([str(SPARK_PYTHON), "-m", "feature_engineering.spark.run_pipeline"], cwd=ML_ROOT, check=True)
    print(f"[trigger] feature_engineering.spark.build_multi_horizon_features 실행 중 ({SPARK_PYTHON})...")
    subprocess.run(
        [str(SPARK_PYTHON), "-m", "feature_engineering.spark.build_multi_horizon_features"], cwd=ML_ROOT, check=True
    )


def _trigger_retrain(model_name: str, target_col: str, exposure_col: str | None) -> dict:
    """방금 만든 최신 multi-horizon feature mart로 해당 모델을 재학습한다 (챔피언 경로에 덮어씀).

    args:
        model_name: "rental" 또는 "return"
        target_col: "rental_count" 또는 "return_count"
        exposure_col: train_target()에 전달할 exposure 컬럼명
    returns:
        dict: train_target()이 반환한 평가 지표 (재학습 후 새 baseline이 됨 —
            train_target()이 models/{model_name}_metrics.json에 자동 저장)
    """
    print(f"[trigger] {model_name} 재학습 중 (입력: {config.MULTI_HORIZON_FEATURES_TABLE_PARQUET})...")
    df = load_training_table()
    metrics = train_target(df, target_col, model_name, exposure_col=exposure_col)
    print(f"[trigger] {model_name} 재학습 완료")
    return metrics


def main() -> list[dict]:
    parser = argparse.ArgumentParser(description="매달 챔피언 모델 성능 점검 + (옵션) 재학습 트리거")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="기준 미달 모델이 있으면 실제로 피처마트 생성+재학습을 실행한다 (기본은 리포트만 찍는 dry-run)",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="기준 날짜(YYYY-MM-DD) override — 기본은 오늘. 과거 특정 달을 다시 점검하거나 "
        "(feature mart 범위 밖인) 운영 환경 밖에서 테스트할 때 사용",
    )
    args = parser.parse_args()

    results = check_all_models(as_of=args.as_of)
    _print_report(results)

    retrain_needed = [r for r in results if r["needs_retrain"]]
    if not retrain_needed:
        print("\n모든 모델이 기준 이내 — 재학습 트리거 없음")
        return results

    if not args.execute:
        print(
            f"\n기준 미달 모델 {len(retrain_needed)}개 — 실제 재학습은 --execute로 다시 실행하세요 "
            "(지금은 dry-run이라 아무것도 바꾸지 않았습니다)"
        )
        return results

    print(f"\n=== 재학습 트리거 실행 ({len(retrain_needed)}개 모델) ===")
    _trigger_feature_pipeline()
    for r in retrain_needed:
        spec = next(s for s in MODEL_SPECS if s[0] == r["model_name"])
        new_metrics = _trigger_retrain(*spec)
        print(f"    새 metrics: {json.dumps(new_metrics, ensure_ascii=False)}")

    return results


if __name__ == "__main__":
    main()
