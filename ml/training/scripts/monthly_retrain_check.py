"""매달 챔피언 모델(대여/반납) 성능을 점검하고, 기준 미달이면 챌린저를 학습해
챔피언보다 나을 때만 교체한다.

**기본은 dry-run이다** — 리포트만 찍고 아무것도 바꾸지 않는다. 실제로 Spark
피처마트 생성(수십 초~수분)과 LightGBM 재학습(모델당 ~25분)을 트리거하려면
`--execute`를 명시해야 한다 — 매달 자동으로 돌리는 운영 환경에서는 그
스케줄러(cron/EMR step 등)가 `--execute`로 호출하면 된다.

기준(어느 정도 악화되면 재학습할지)은 [common_config.py](../../../libs/ml_core/common_config.py)에서
관리한다 — 여기서는 그 기준을 적용만 한다.

**챌린저/챔피언 흐름**: 학습은 항상 아카이브(`ml_core.paths.archive_models_prefix()`
— 날짜+프로필별로 분리)에 쓰고, 챔피언 경로(`models/`)에는 booster/JSON을 직접
쓰지 않는다. `training.promotion.should_promote()`가 챌린저(방금 학습한 것)와
챔피언(현재 챔피언 포인터가 가리키는 archive의 `metrics.json`)을 비교해, 챌린저가
기준을 만족할 때만 `promotion.promote_challenger()`로 챔피언 포인터
(`models/champion/{model_name}.json`)가 그 아카이브 prefix를 가리키도록 원자적으로
전환한다 — 파일을 복사하지 않는다(승격 도중 파일이 부분적으로만 바뀌어 서로 다른
버전이 섞이는 문제를 피하기 위함, `ml_core.paths.read_champion_prefix()` docstring
참고). 만족 못 하면 다른 프로필(S3 `profiles/*.json` — `ml_core.common_config.
list_profile_names()`가 나열, `ml_core.profile_registry.push_profile()`로 생성)로 다시 시도하고,
가진 프로필을 전부 써도 못 넘으면 챔피언을 그대로 두고 다음 달을 기약한다(정상
종료 — 예외를 던지지 않는다).

**프로필마다 별도 프로세스가 필요한 이유**: `ml_core.common_config`는 프로세스가
시작할 때 `ML_PROFILE` 환경변수로 프로필 값을 한 번만 읽어 모듈 전역 상수로
고정한다 — 같은 프로세스 안에서 프로필을 바꿔가며 반복할 수 없다. 그래서 프로필
하나를 시도할 때마다 "feature 파이프라인 subprocess + 학습 subprocess"를
`ML_PROFILE=<profile>` 환경변수와 함께 새로 띄운다(기존에 `_trigger_feature_pipeline()`
이 이미 쓰던 subprocess 패턴을 학습에도 그대로 적용한 것).

**알림**: 레포에 Slack/이메일 등 실제 알림 채널이 없어, 구조화된 콘솔 출력
(`_notify()`)으로 학습 시작/성공/실패/승격/미승격을 전부 남긴다 — 나중에 실제
채널이 생기면 `_notify()` 하나만 그 채널로 바꾸면 된다.

실행 예:
    ./.venv/bin/python -m training.scripts.monthly_retrain_check              # 점검만 (dry-run)
    ./.venv/bin/python -m training.scripts.monthly_retrain_check --execute    # 기준 미달 시 실제 재학습 시도
"""

import argparse
import os
import subprocess
import sys

from core import s3 as s3_io
from ml_core import common_config
from ml_core.paths import ML_ROOT, archive_models_prefix, model_json_key

from ..config import today_kst
from ..monitor_performance import _load_baseline_metrics, check_all_models
from ..promotion import promote_challenger, should_promote

SPARK_PYTHON = ML_ROOT / "feature_engine" / ".venv" / "bin" / "python"

_TRAIN_SCRIPTS = {"rental": "training.train_rental_model", "return": "training.train_return_model"}


def _notify(message: str) -> None:
    """학습/승격 진행 상황을 알린다 — 지금은 표준 출력뿐이지만, 나중에 실제
    알림 채널(Slack 등)이 생기면 이 함수만 바꾸면 된다."""
    print(f"[monthly_retrain] {message}", flush=True)


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


def _candidate_profiles() -> list[str]:
    """시도할 프로필 순서 — 기본 프로필을 가장 먼저, 나머지는 이름순."""
    names = common_config.list_profile_names()
    if common_config.PROFILE_NAME in names:
        names = [common_config.PROFILE_NAME] + [n for n in names if n != common_config.PROFILE_NAME]
    return names


def _trigger_feature_pipeline(profile_name: str) -> None:
    """feature_engine/spark의 증분 파이프라인 + multi-horizon 테이블 생성을 지정한
    프로필로 Spark 전용 venv(Python 3.11)에서 실행한다.

    rental/return 두 모델이 같은 multi-horizon feature mart(파라미터 조합 하나)를
    같이 쓰므로, 두 모델이 같은 프로필을 시도하면 이 파이프라인이 두 번 실행될 수
    있다 — 워터마크 덕분에 두 번째 실행은 재계산 낭비가 없고, 월 1회 배치라 비용
    문제도 아니라 단순하게 모델별로 각자 실행한다.

    **주의(1단계 한계)**: `build_multi_horizon_features`는 아직 증분(watermark)을 지원하지
    않아 매번 전체를 다시 만든다(feature_engine/spark/build_multi_horizon_features.py
    docstring 참고) — multi-horizon 테이블이 원본의 최대 HORIZON_COUNT배라 이 단계가
    가장 오래 걸리는 부분이 될 수 있다.
    """
    if not SPARK_PYTHON.exists():
        raise RuntimeError(f"{SPARK_PYTHON}가 없습니다 — feature_engine/에서 'uv sync'를 먼저 실행해야 합니다")
    env = {**os.environ, "ML_PROFILE": profile_name}
    _notify(f"'{profile_name}' 프로필로 feature_engine.spark.run_pipeline 실행 중...")
    subprocess.run([str(SPARK_PYTHON), "-m", "feature_engine.spark.run_pipeline"], cwd=ML_ROOT, check=True, env=env)
    _notify(f"'{profile_name}' 프로필로 feature_engine.spark.build_multi_horizon_features 실행 중...")
    subprocess.run(
        [str(SPARK_PYTHON), "-m", "feature_engine.spark.build_multi_horizon_features"],
        cwd=ML_ROOT,
        check=True,
        env=env,
    )


def _run_training_subprocess(model_name: str, profile_name: str, archive_date: str) -> dict:
    """`model_name`을 지정한 프로필로 학습하는 subprocess를 띄우고, 그 결과로
    아카이브에 쓰인 metrics를 다시 읽어 반환한다.

    subprocess의 표준출력을 파싱하지 않는다 — `train_target()`이 학습 과정에서
    다른 진행 로그도 같이 찍기 때문에 정확히 마지막 JSON만 골라내는 게 불안정하다.
    대신 이미 알고 있는 아카이브 경로(`archive_models_prefix()`, 학습 스크립트가
    쓰는 것과 정확히 같은 공식)에서 `metrics.json`을 S3로 직접 다시 읽는다 — 더
    견고하고, 아카이브 자체가 이미 "진실의 원천"이라 자연스럽다.

    args:
        model_name: "rental" 또는 "return"
        profile_name: 이 시도에 쓸 프로필 이름(ML_PROFILE로 subprocess에 전달)
        archive_date: "YYYY-MM-DD" — 이 시도 전체(feature 파이프라인 포함)가 공유하는
            날짜. 자정을 넘겨 실행되더라도 아카이브 경로가 어긋나지 않게 오케스트레이터가
            한 번만 계산해서 넘긴다.
    returns:
        dict: train_target()이 저장한 metrics.json
    raises:
        subprocess.CalledProcessError: 학습 자체가 실패했을 때
        RuntimeError: 학습은 성공했다고 나왔는데 metrics.json을 못 찾았을 때(버그 신호)
    """
    env = {**os.environ, "ML_PROFILE": profile_name, "MODEL_ARCHIVE_DATE": archive_date}
    _notify(f"[{model_name}] '{profile_name}' 프로필로 학습 중...")
    subprocess.run([sys.executable, "-m", _TRAIN_SCRIPTS[model_name]], cwd=ML_ROOT, check=True, env=env)

    archive_prefix = archive_models_prefix(archive_date, profile_name)
    metrics = s3_io.read_json(model_json_key(model_name, "metrics", archive_prefix))
    if metrics is None:
        raise RuntimeError(f"[{model_name}] 학습은 끝났는데 metrics를 못 찾음: {archive_prefix}")
    return metrics


def _attempt_promotion(model_name: str, champion_metrics: dict | None) -> bool:
    """프로필을 하나씩 시도해가며 챔피언을 넘어서는 챌린저가 나올 때까지 재학습한다.

    args:
        model_name: "rental" 또는 "return"
        champion_metrics: 현재 챔피언의 metrics.json (아직 챔피언이 없으면 None —
            이 경우 첫 학습 결과가 무조건 승격된다, should_promote() 참고)
    returns:
        bool: 승격이 일어났는지
    """
    archive_date = today_kst().isoformat()
    for profile_name in _candidate_profiles():
        try:
            _trigger_feature_pipeline(profile_name)
            challenger_metrics = _run_training_subprocess(model_name, profile_name, archive_date)
        except subprocess.CalledProcessError as exc:
            _notify(f"[{model_name}] '{profile_name}' 시도 실패(subprocess 오류: {exc}) — 다음 프로필로 넘어감")
            continue

        promote, reasons = should_promote(challenger_metrics, champion_metrics)
        for reason in reasons:
            _notify(f"[{model_name}] '{profile_name}' 판정 — {reason}")

        if promote:
            archive_prefix = archive_models_prefix(archive_date, profile_name)
            promote_challenger(model_name, archive_prefix)
            _notify(f"[{model_name}] '{profile_name}' 챌린저를 챔피언으로 승격 — 포인터가 {archive_prefix}를 가리키도록 전환")
            return True

        _notify(f"[{model_name}] '{profile_name}' 챌린저가 기준 미달 — 다음 프로필 시도")

    _notify(f"[{model_name}] 가능한 프로필을 모두 시도했지만 챔피언을 넘어서지 못함 — 챔피언 유지, 다음 달에 재시도")
    return False


def main() -> list[dict]:
    """월별 성능 점검 및 챌린저 모델 재학습/승격 프로세스를 실행한다."""
    parser = argparse.ArgumentParser(description="매달 챔피언 모델 성능 점검 + (옵션) 챌린저 재학습/승격 시도")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="기준 미달 모델이 있으면 실제로 챌린저 학습을 시도한다 (기본은 리포트만 찍는 dry-run)",
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
        _notify("모든 모델이 기준 이내 — 재학습 시도 없음")
        return results

    if not args.execute:
        _notify(
            f"기준 미달 모델 {len(retrain_needed)}개 — 실제 재학습은 --execute로 다시 실행하세요 "
            "(지금은 dry-run이라 아무것도 바꾸지 않았습니다)"
        )
        return results

    _notify(f"=== 챌린저 재학습 시도 시작 ({len(retrain_needed)}개 모델) ===")
    for r in retrain_needed:
        model_name = r["model_name"]
        try:
            champion_metrics = _load_baseline_metrics(model_name)
        except FileNotFoundError:
            champion_metrics = None
        _attempt_promotion(model_name, champion_metrics)

    return results


if __name__ == "__main__":
    main()
