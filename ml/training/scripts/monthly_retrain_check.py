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
참고).

**재시도 순서(`_candidate_profiles()`)**: 1차는 챔피언이 실제로 학습됐던 프로필의
하이퍼파라미터(임베고/앵커/LGB 파라미터 등)를 그대로 쓰되 학습기간만 지금의
기본 롤링 윈도우(`TRAIN_LOOKBACK_MONTHS`, 최신 증분 포함)로 갱신해서 재시도한다
— "성능이 나빠졌으니 최신 데이터로 다시 학습해보자"가 먼저지, 하이퍼파라미터
자체를 바꾸는 건 별개 문제이기 때문이다. 그래도 못 넘으면 미리 등록해둔 다른
프로필(S3 `profiles/*.json` — `ml_core.common_config.list_profile_names()`가 나열,
`ml_core.profile_registry.push_profile()`로 생성)을 이름순으로 순차 시도한다.
단, 자동 승격은 현재 서빙과 같은 피처 계약 안에서 LightGBM/학습 기간만 튜닝하는
경우에 한한다. rolling/window/horizon처럼 피처 의미가 다른 후보는 무거운 Spark·
학습을 시작하기 전에 건너뛴다. 그런 계약 변경은 feature 생성·두 모델·inference를
함께 전환하는 별도 배포가 필요하다. 호환 후보를 전부 써도 못 넘으면 챔피언을
그대로 두고 다음 달을 기약한다(정상 종료 — 예외를 던지지 않는다).

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
from ml_core import common_config, profile_contract
from ml_core.paths import (
    ML_ROOT,
    TRAINING_RUNS_PREFIX,
    archive_models_prefix,
    model_json_key,
    read_champion_prefix,
)
from ml_core.serving_contract import (
    SERVING_FEATURE_PROFILE_KEYS,
    ServingProfileContractError,
    assert_serving_profiles_compatible,
)

from ..config import MONITOR_LOOKBACK_MONTHS, unique_archive_date
from ..monitor_performance import (
    MODEL_SPECS,
    _load_baseline_metrics,
    _log_to_mlflow,
    _recent_month_range,
    check_all_models,
    combine_evaluation_shards,
    decide_retrain,
)
from ..promotion import promote_challenger, should_promote

SPARK_PYTHON = ML_ROOT / "feature_engine" / ".venv" / "bin" / "python"

_TRAIN_SCRIPTS = {"rental": "training.train_rental_model", "return": "training.train_return_model"}
_EXPLICIT_TRAIN_WINDOW_ENV = ("TRAIN_WINDOW_START", "TRAIN_WINDOW_END")

# --- LGB_NUM_MACHINES>1일 때 로컬 subprocess 대신 YARN distributed-shell로 학습을
# 띄우는 데 쓰는 값들(ADR-0007). `training.config`의 LGB_* 환경변수와 짝을 이룬다 —
# 여기서는 "몇 대"(LGB_NUM_MACHINES)가 이미 결정된 뒤 "그 대수만큼 컨테이너를 어떻게
# 띄울지"만 다룬다. `/opt/gng`는 `ops/emr/bootstrap.sh`가 레포 패키지를 푸는
# 고정 경로다 — 이 분기는 실제로 EMR 노드에서만 실행되므로(월간 재학습 DAG가
# LGB_NUM_MACHINES>1을 EMR 스텝에만 준다) 하드코딩해도 된다.
_EMR_PYTHONPATH = "/opt/gng"
_EMR_PYTHON = "python3.11"
# 비워두면(기본값) `_resolve_distributed_shell_jar()`가 실제 노드에서 find로
# 찾는다 — 이 프로젝트는 아직 실제 EMR을 한 번도 켜본 적이 없어(2026-08) EMR
# 7.2.0의 정확한 jar 경로(버전 접미사 등 릴리스마다 달라질 수 있음)를 미리
# 확인할 방법이 없었다. 확실한 경로를 알게 되면 이 환경변수로 고정해 매번
# find를 도는 비용을 없앨 수 있다.
YARN_DISTRIBUTED_SHELL_JAR = os.environ.get("YARN_DISTRIBUTED_SHELL_JAR", "")
_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS = ("/usr/lib/hadoop-yarn", "/usr/lib/hadoop", "/opt/hadoop")
# m4.large(8GB) 노드 1대에 컨테이너 1개만 배치되게 노드 용량에 가깝게 잡는다 —
# 여러 개가 배치되면 같은 LGB_LOCAL_LISTEN_PORT를 두고 충돌한다
# (`yarn_worker_bootstrap._resolve_rank_and_machines()` 중복 host 가드 참고).
YARN_CONTAINER_MEMORY_MB = int(os.environ.get("YARN_CONTAINER_MEMORY_MB", "6144"))
YARN_CONTAINER_VCORES = int(os.environ.get("YARN_CONTAINER_VCORES", "2"))
# distributed-shell의 자체 ApplicationMaster(워커 컨테이너와 별개 — 이 AM은 그냥
# 컨테이너들을 띄우고 감시만 함) 메모리를 명시하지 않으면 기본값 100MB로 뜨는데,
# 이 JVM이 클래스 로딩만 하다가도 100MB로는 부족해 OutOfMemoryError로 즉시
# 죽는다 — 겉보기 증상은 "WARNING: package javax.script not in java.base" +
# "JNI error"라 Java 17 비호환처럼 보였지만, 실제로는 순수 메모리 부족이었다
# (실제 EMR 실행에서 -master_memory 512로 재현/해결 확인, 2026-08-26).
YARN_AM_MEMORY_MB = int(os.environ.get("YARN_AM_MEMORY_MB", "1024"))
YARN_AM_VCORES = int(os.environ.get("YARN_AM_VCORES", "1"))
# 컨테이너로 반드시 넘겨야 하는 환경변수 — distributed-shell 컨테이너는 이 프로세스의
# 환경을 상속하지 않고 `-shell_env`로 명시한 것만 받는다.
_YARN_SHELL_ENV_KEYS = (
    "ML_PROFILE",
    "MODEL_ARCHIVE_DATE",
    "LGB_NUM_MACHINES",
    "LGB_TREE_LEARNER",
    "LGB_LOCAL_LISTEN_PORT",
    "LGB_TIME_OUT",
    "TRAINING_RUN_ID",
    "S3_BUCKET",
    # 빠지면 train_common.py의 mlflow_tracking.configure()가 기본값
    # (http://localhost:5000)으로 떨어져 컨테이너 안에 없는 로컬 서버에 붙으려다
    # ConnectionRefusedError로 학습 자체가 죽는다(실제 EMR 실행에서 확인,
    # 2026-08-26) — spark-submit 경로(_yarn_python_module_step)는 AM 환경변수로
    # 자동 전달되지만, distributed-shell은 -shell_env로 명시한 것만 받는다.
    "MLFLOW_TRACKING_URI",
    # `_candidate_profiles()`의 1차 후보(챔피언이 실제 학습됐던 프로필)는
    # 하이퍼파라미터는 그대로 두고 학습기간만 "지금의 기본 롤링 윈도우"로
    # 갱신하려고 이 키 하나만 env_overrides로 얹는다 — 여기서 빠지면
    # distributed-shell 워커가 프로필에 저장된 챔피언의 **원래(옛날)** lookback을
    # 그대로 써서, "최신 데이터로 다시 학습"이라는 1차 재시도의 목적 자체가
    # 조용히 무력화된다(로컬 subprocess 경로는 env 전체를 상속해 원래도 문제
    # 없었다 — distributed-shell만 화이트리스트라 새는 경로였음).
    "TRAIN_LOOKBACK_MONTHS",
)
# 분산 평가(`yarn_eval_worker.py`) 컨테이너로 넘길 환경변수. 학습과 달리
# LightGBM 소켓 설정(LGB_LOCAL_LISTEN_PORT 등)은 필요 없다 — predict()는
# embarrassingly parallel이라 워커끼리 통신하지 않는다
# (`monitor_performance.combine_evaluation_shards()` 참고).
_EVAL_SHELL_ENV_KEYS = (
    "EVAL_RUN_ID",
    "EVAL_MODEL",
    "EVAL_TARGET_COL",
    "EVAL_EXPOSURE_COL",
    "EVAL_HORIZON",
    "EVAL_WINDOW_START",
    "EVAL_WINDOW_END",
    "EVAL_NUM_WORKERS",
    "S3_BUCKET",
)


def _notify(message: str) -> None:
    """학습/승격 진행 상황을 알린다 — 지금은 표준 출력뿐이지만, 나중에 실제
    알림 채널(Slack 등)이 생기면 이 함수만 바꾸면 된다."""
    print(f"[monthly_retrain] {message}", flush=True)


def _monthly_subprocess_env(profile_name: str, env_overrides: dict[str, str]) -> dict[str, str]:
    """월별 재학습용 rolling window 환경을 만든다.

    최초 2025년 챔피언 생성 때 사용한 명시적 `TRAIN_WINDOW_START/END`가 상위
    셸이나 장기 실행 스케줄러에 남아 있어도 월별 자식 프로세스가 상속하면 안 된다.
    두 값을 제거해 `common_config.training_window()`의 최신 rolling 경로를 강제하고,
    나머지 프로필 및 시도별 override는 그대로 전달한다.
    """
    env = dict(os.environ)
    for name in _EXPLICIT_TRAIN_WINDOW_ENV:
        env.pop(name, None)
    rolling_overrides = {
        name: value
        for name, value in env_overrides.items()
        if name not in _EXPLICIT_TRAIN_WINDOW_ENV
    }
    env.update({"ML_PROFILE": profile_name, **rolling_overrides})
    return env


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


def _champion_profile_name(model_name: str) -> str | None:
    """지금 챔피언이 실제로 학습될 때 쓴 프로필 이름 — 그 프로필의 하이퍼파라미터
    (임베고/앵커/LGB 파라미터 등)를 재학습 1차 시도에서 그대로 재사용하기 위함
    (`_candidate_profiles()` 참고). `train_common.train_target()`이 학습 시점에
    저장해두는 `{model_name}_profile.json`(`profile_name` 필드 포함)에서 읽는다.

    챔피언이 아직 없거나(최초 학습 전) 그 기록을 못 찾으면 None — 호출부가
    `common_config.PROFILE_NAME`(이 프로세스의 기본 프로필)으로 대체한다.
    """
    try:
        archive_prefix = read_champion_prefix(model_name)
    except FileNotFoundError:
        return None
    payload = s3_io.read_json(model_json_key(model_name, "profile", archive_prefix))
    if payload is None:
        print(
            f"[monthly_retrain] ERROR: [{model_name}] 챔피언 프로필 기록을 못 찾음({archive_prefix}) "
            "— 현재 프로세스 기본 프로필로 대체",
            file=sys.stderr,
        )
        return None
    return payload["profile_name"]


def _candidate_profiles(model_name: str) -> list[tuple[str, dict[str, str]]]:
    """시도할 (프로필 이름, 이 시도에만 덮어쓸 환경변수) 순서.

    **1차**: 챔피언이 실제로 학습됐던 프로필을 그대로 쓰되, 학습기간만 지금
    프로세스의 기본 롤링 윈도우(`TRAIN_LOOKBACK_MONTHS`, 최신 증분 포함)로 갱신해서
    재시도한다.
    **2차**: 해당 모델 전용 프로필(`{model_name}_*` 형식, 예: `rental_embargo45`).
    **3차**: 내장 기본값(`builtin-default`) 및 일반 공통 프로필.
    단, 타 모델 전용 프로필(예: 대여 모델 시도 시 `return_*`)은 후보에서 제외한다.
    """
    primary = _champion_profile_name(model_name) or common_config.PROFILE_NAME
    other_model = "return" if model_name == "rental" else "rental"

    remote_profiles = common_config.list_profile_names()
    # 타 모델 전용 프로필 제외
    valid_remotes = [p for p in remote_profiles if not p.startswith(f"{other_model}_")]

    # 해당 모델 전용 프로필을 일반 공통 프로필보다 우선 시도
    model_specific = sorted([p for p in valid_remotes if p.startswith(f"{model_name}_")])
    general_remotes = sorted([p for p in valid_remotes if not p.startswith(f"{model_name}_")])

    ordered_names = [
        primary,
        *model_specific,
        common_config.BUILTIN_PROFILE_NAME,
        *general_remotes,
    ]
    unique_names = list(dict.fromkeys(ordered_names))
    refreshed_period = {"TRAIN_LOOKBACK_MONTHS": str(common_config.TRAIN_LOOKBACK_MONTHS)}
    return [
        (name, refreshed_period if index == 0 else {})
        for index, name in enumerate(unique_names)
    ]


def _trigger_feature_pipeline(profile_name: str, env_overrides: dict[str, str]) -> None:
    """feature_engine/spark의 증분 파이프라인 + multi-horizon 테이블 생성을 지정한
    프로필로 Spark 전용 venv(Python 3.11)에서 실행한다.

    rental/return 두 모델이 같은 multi-horizon feature mart(파라미터 조합 하나)를
    같이 쓰므로, 두 모델이 같은 프로필을 시도하면 이 파이프라인이 두 번 실행될 수
    있다 — 워터마크 덕분에 두 번째 실행은 재계산 낭비가 없고, 월 1회 배치라 비용
    문제도 아니라 단순하게 모델별로 각자 실행한다.

    **주의(1단계 한계)**: `build_multi_horizon_features`는 아직 진짜 증분(부분 재계산)은
    지원하지 않아, 실제로 다시 만들 때는 매번 전체를 처음부터 다시 만든다
    (feature_engine/spark/build_multi_horizon_features.py docstring 참고) —
    multi-horizon 테이블이 원본의 최대 HORIZON_COUNT배라 이 단계가 가장 오래 걸리는
    부분이 될 수 있다. 다만 2026-08-27부터는 소스 watermark와 학습 윈도우가 마지막
    생성 때와 동일하면(같은 profile을 대여/반납 체인이나 후보 재시도가 반복 요청하는
    경우) 재생성 자체를 건너뛴다 — 소스가 실제로 바뀐 경우에만 이 "매번 전체 재생성"
    비용이 든다.

    args:
        env_overrides: `ML_PROFILE=profile_name` 위에 이 시도에서만 덮어쓸 환경변수
            (`_candidate_profiles()` 참고 — 챔피언 프로필 재시도에서 학습기간만
            갱신할 때 씀. 빈 dict면 프로필 값 그대로).
    """
    if not SPARK_PYTHON.exists():
        raise RuntimeError(f"{SPARK_PYTHON}가 없습니다 — feature_engine/에서 'uv sync'를 먼저 실행해야 합니다")
    env = _monthly_subprocess_env(profile_name, env_overrides)
    _notify(f"'{profile_name}' 프로필로 feature_engine.spark.run_pipeline 실행 중...")
    subprocess.run([str(SPARK_PYTHON), "-m", "feature_engine.spark.run_pipeline"], cwd=ML_ROOT, check=True, env=env)
    _notify(f"'{profile_name}' 프로필로 feature_engine.spark.build_multi_horizon_features 실행 중...")
    subprocess.run(
        [str(SPARK_PYTHON), "-m", "feature_engine.spark.build_multi_horizon_features"],
        cwd=ML_ROOT,
        check=True,
        env=env,
    )


def _validate_candidate_serving_contract(profile_name: str, env_overrides: dict[str, str]) -> None:
    """후보 프로필이 현재 서빙 계약과 같은지 무거운 작업 전에 검증한다.

    feature/training subprocess는 현재 환경을 상속한 뒤 ``env_overrides``를 마지막에
    덮는다. 여기서도 같은 우선순위를 적용해야 preflight와 실제 학습 프로필이
    어긋나지 않는다. 서빙 계약 키는 모두 분 단위 또는 개수 정수다.

    raises:
        ServingProfileContractError: 후보를 읽거나 해석할 수 없거나 현재 서빙
            피처 계약과 다를 때
    """
    try:
        loaded_profile = common_config._load_profile(profile_name)
        candidate_profile = loaded_profile.copy()
        subprocess_env = _monthly_subprocess_env(profile_name, env_overrides)
        for key in SERVING_FEATURE_PROFILE_KEYS:
            if key == "TRAIN_ANCHOR_TICK_MINUTES":
                continue
            raw_value = subprocess_env.get(key)
            if raw_value is not None:
                candidate_profile[key] = int(raw_value)
        candidate_profile["TRAIN_ANCHOR_TICK_MINUTES"] = common_config._resolved_train_anchor_tick(
            loaded_profile,
            int(candidate_profile["GRID_TICK_MINUTES"]),
            env=subprocess_env,
        )
        profile_contract.validate_model_grid_contract(
            int(candidate_profile["GRID_TICK_MINUTES"]),
            int(candidate_profile["ROLLING_TICK_MINUTES"]),
            int(candidate_profile["TARGET_HORIZON_MINUTES"]),
            f"후보 {profile_name}",
        )
        profile_contract.validate_train_anchor_contract(
            int(candidate_profile["GRID_TICK_MINUTES"]),
            int(candidate_profile["TRAIN_ANCHOR_TICK_MINUTES"]),
            f"후보 {profile_name}",
        )
    # 한 후보의 S3/파싱 실패는 다음 후보로 격리하되 KeyboardInterrupt/SystemExit은
    # Exception 바깥이라 정상적으로 전파한다.
    except Exception as exc:
        raise ServingProfileContractError(
            f"후보 프로필 '{profile_name}'을 서빙 계약으로 해석할 수 없습니다: {exc}"
        ) from exc

    assert_serving_profiles_compatible(
        common_config.effective_profile(),
        candidate_profile,
        expected_source="현재 서빙",
        actual_source=f"후보 프로필 '{profile_name}'",
    )


def _resolve_distributed_shell_jar() -> str:
    """distributed-shell 예제 jar의 실제 경로를 찾는다.

    `YARN_DISTRIBUTED_SHELL_JAR` 환경변수가 있으면 그대로 쓰고, 없으면 알려진
    설치 위치 후보를 `find`로 뒤져 실제 존재하는 파일을 찾는다 — EMR 릴리스마다
    정확한 파일명(버전 접미사 등)이 다를 수 있어 경로 하나만 고정해서 믿지
    않는다.

    raises:
        RuntimeError: 어느 후보 경로에서도 jar를 못 찾음
    """
    if YARN_DISTRIBUTED_SHELL_JAR:
        return YARN_DISTRIBUTED_SHELL_JAR
    for root in _YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS:
        result = subprocess.run(
            ["find", root, "-iname", "*distributedshell*.jar"],
            capture_output=True,
            text=True,
            check=False,
        )
        candidates = [line for line in result.stdout.splitlines() if line.strip()]
        if candidates:
            return candidates[0]
    raise RuntimeError(
        f"distributed-shell jar를 찾을 수 없습니다({_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS} 아래 탐색) "
        "— YARN_DISTRIBUTED_SHELL_JAR 환경변수로 정확한 경로를 지정하세요"
    )


def _run_distributed_training_via_yarn(model_name: str, env: dict[str, str]) -> None:
    """LGB_NUM_MACHINES(>1)개 컨테이너로 YARN distributed-shell 학습 앱을 제출하고
    끝날 때까지 대기한다(ADR-0007).

    이 함수는 EMR 노드(주로 master, YARN 클라이언트가 설치된 곳)에서만 의미가
    있다 — `_run_training_subprocess()`가 `LGB_NUM_MACHINES>1`일 때만 이 경로를
    타므로, 로컬/EC2에서 이 코드가 실행될 일은 없다(월간 재학습 DAG가 EMR
    스텝에만 `LGB_NUM_MACHINES>1`을 준다). 각 컨테이너는
    `training.scripts.yarn_worker_bootstrap`으로 시작해 barrier를 거친 뒤 이
    함수와 무관하게 독립적으로 `train_rental_model`/`train_return_model`을
    실행한다 — 이 함수는 그 컨테이너들을 다 띄우고 YARN 앱이 끝날 때까지
    블로킹하는 역할만 한다(그래야 호출부가 바로 이어서 archive의 metrics.json을
    읽어도 이미 다 쓰인 뒤라 안전하다).

    args:
        model_name: "rental" 또는 "return"
        env: 컨테이너에 전달할 환경변수 원본(`_YARN_SHELL_ENV_KEYS`에 있는 키만
            실제로 `-shell_env`로 넘어간다) — 특히 `TRAINING_RUN_ID`가 이미
            채워져 있어야 한다(barrier 네임스페이스, 호출부가 시도마다 새로 만듦).
    raises:
        RuntimeError: YARN 애플리케이션이 실패로 끝남
    """
    shell_command = (
        f"cd {_EMR_PYTHONPATH} && PYTHONPATH={_EMR_PYTHONPATH} {_EMR_PYTHON} "
        f"-m training.scripts.yarn_worker_bootstrap --model {model_name}"
    )
    shell_env_args = []
    for key in _YARN_SHELL_ENV_KEYS:
        if env.get(key):
            shell_env_args += ["-shell_env", f"{key}={env[key]}"]

    num_machines = env["LGB_NUM_MACHINES"]
    cmd = [
        "yarn",
        "org.apache.hadoop.yarn.applications.distributedshell.Client",
        "-jar",
        _resolve_distributed_shell_jar(),
        "-shell_command",
        shell_command,
        "-num_containers",
        num_machines,
        "-container_memory",
        str(YARN_CONTAINER_MEMORY_MB),
        "-container_vcores",
        str(YARN_CONTAINER_VCORES),
        "-master_memory",
        str(YARN_AM_MEMORY_MB),
        "-master_vcores",
        str(YARN_AM_VCORES),
        "-timeout",
        "345600000",
        *shell_env_args,
    ]


    _notify(f"[{model_name}] YARN distributed-shell 제출 (컨테이너 {num_machines}개)...")
    subprocess.run(cmd, check=True)


def _run_distributed_evaluation_via_yarn(
    model_name: str,
    target_col: str,
    exposure_col: str | None,
    horizon: int,
    num_workers: int,
    as_of=None,
) -> dict:
    """평가(`evaluate_recent_performance()`)를 `num_workers`개 YARN
    distributed-shell 컨테이너에 날짜 기준으로 나눠 돌리고 결과를 합친다.

    학습 분산과 달리 워커끼리 소켓 통신이 필요 없는 embarrassingly parallel
    작업이라(`monitor_performance.combine_evaluation_shards()` 참고 — poisson_
    deviance/RMSE/coverage가 전부 행 단위 평균이라 부분합만 합치면 근사 없이
    정확히 같은 결과) `yarn_worker_bootstrap.py`의 barrier(IP 교환)까지는 필요
    없지만, "정확히 num_workers개 조각이 도착했는지"는 여기서도 그대로
    검증한다 — 컨테이너 하나가 조용히 유실되면 일부 날짜가 평가에서 빠진 채로
    (에러 없이) 재학습 판단이 내려질 수 있기 때문이다(fail-closed).

    args:
        model_name: "rental" 또는 "return"
        target_col, exposure_col: `evaluate_recent_performance()` 참고
        horizon: `evaluate_recent_performance()` 참고
        num_workers: 나눌 컨테이너 수(2 이상이어야 의미가 있음)
        as_of: 기준 날짜 override(테스트/CLI `--as-of`)
    returns:
        dict: `evaluate_recent_performance()`와 정확히 같은 키
    raises:
        RuntimeError: YARN 애플리케이션 실패, 또는 도착한 조각 수가 num_workers와 다름
    """
    window_start, window_end = _recent_month_range(MONITOR_LOOKBACK_MONTHS, as_of)
    run_id = f"eval-{unique_archive_date()}-{model_name}"

    env = {
        "EVAL_RUN_ID": run_id,
        "EVAL_MODEL": model_name,
        "EVAL_TARGET_COL": target_col,
        "EVAL_EXPOSURE_COL": exposure_col or "",
        "EVAL_HORIZON": str(horizon),
        "EVAL_WINDOW_START": window_start,
        "EVAL_WINDOW_END": window_end,
        "EVAL_NUM_WORKERS": str(num_workers),
        "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
    }
    shell_command = (
        f"cd {_EMR_PYTHONPATH} && PYTHONPATH={_EMR_PYTHONPATH} {_EMR_PYTHON} -m training.scripts.yarn_eval_worker"
    )
    shell_env_args = []
    for key in _EVAL_SHELL_ENV_KEYS:
        if env.get(key):
            shell_env_args += ["-shell_env", f"{key}={env[key]}"]

    cmd = [
        "yarn",
        "org.apache.hadoop.yarn.applications.distributedshell.Client",
        "-jar",
        _resolve_distributed_shell_jar(),
        "-shell_command",
        shell_command,
        "-num_containers",
        str(num_workers),
        "-container_memory",
        str(YARN_CONTAINER_MEMORY_MB),
        "-container_vcores",
        str(YARN_CONTAINER_VCORES),
        "-master_memory",
        str(YARN_AM_MEMORY_MB),
        "-master_vcores",
        str(YARN_AM_VCORES),
        "-timeout",
        "345600000",
        *shell_env_args,
    ]


    _notify(f"[{model_name}] 분산 평가 YARN distributed-shell 제출 (워커 {num_workers}개)...")
    subprocess.run(cmd, check=True)

    shard_prefix = f"{TRAINING_RUNS_PREFIX}/{run_id}/eval-shards/{model_name}/"
    shard_keys = s3_io.list_keys(shard_prefix)
    if len(shard_keys) != num_workers:
        raise RuntimeError(
            f"[{model_name}] 분산 평가 조각이 {len(shard_keys)}/{num_workers}개만 도착함(run_id={run_id})"
        )
    shards = [s3_io.read_json(key) for key in shard_keys]
    baseline = _load_baseline_metrics(model_name)
    return combine_evaluation_shards(model_name, (window_start, window_end), shards, baseline)


def _check_all_models_distributed(
    as_of, horizon: int, model_names: list[str] | None, num_workers: int
) -> list[dict]:
    """`check_all_models()`와 같은 형태의 결과를 내지만, `num_workers>1`이면
    모델마다 평가를 YARN distributed-shell로 나눠 돌린다.

    `num_workers<=1`(기본값)이면 기존 `check_all_models()`를 그대로 호출해
    동작을 바꾸지 않는다 — DAG가 명시적으로 `--eval-num-workers`를 주지 않는
    한 이 함수를 거쳐도 이전과 완전히 같다.
    """
    if num_workers <= 1:
        return check_all_models(as_of=as_of, model_names=model_names)

    specs = [spec for spec in MODEL_SPECS if spec[0] in model_names] if model_names else MODEL_SPECS
    results = []
    for model_name, target_col, exposure_col in specs:
        evaluation = _run_distributed_evaluation_via_yarn(
            model_name, target_col, exposure_col, horizon, num_workers, as_of=as_of
        )
        result = decide_retrain(evaluation)
        _log_to_mlflow(result, horizon)
        results.append(result)
    return results


def _run_training_subprocess(
    model_name: str, profile_name: str, archive_date: str, env_overrides: dict[str, str]
) -> dict:
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
        archive_date: "YYYY-MM-DD-{실행 유니크 접미사}" — 이 시도 전체(feature
            파이프라인 포함)가 공유하는 값. 자정을 넘겨 실행되더라도 아카이브
            경로가 어긋나지 않게, 그리고 같은 날 다시 실행해도 이전 시도의
            archive_prefix와 절대 안 겹치게(`_attempt_promotion()` 참고) 오케스트레이터가
            한 번만 계산해서 넘긴다. `archive_models_prefix()`는 이 문자열의 형식을
            검사하지 않으므로 순수 날짜가 아니어도 문제없다.
        env_overrides: `_trigger_feature_pipeline()` 참고 — 같은 시도 안에서 feature
            파이프라인과 반드시 같은 값을 써야 학습기간이 어긋나지 않는다.
    returns:
        dict: train_target()이 저장한 metrics.json
    raises:
        subprocess.CalledProcessError: 학습 자체가 실패했을 때(로컬 subprocess 경로)
        RuntimeError: YARN distributed-shell 제출이 실패했거나, 학습은 성공했다고
            나왔는데 metrics.json을 못 찾았을 때(버그 신호)
    """
    env = _monthly_subprocess_env(profile_name, env_overrides)
    env["MODEL_ARCHIVE_DATE"] = archive_date
    _notify(f"[{model_name}] '{profile_name}' 프로필로 학습 중...")

    if int(env.get("LGB_NUM_MACHINES", "1")) > 1:
        env["TRAINING_RUN_ID"] = f"{archive_date}-{profile_name}-{model_name}"
        _run_distributed_training_via_yarn(model_name, env)
    else:
        # ML_ROOT는 로컬/EC2 repo clone 레이아웃 기준으로 계산된다 — EMR
        # bootstrap.sh는 core/ml_core/training을 /opt/gng 바로 아래 평평하게
        # 풀어서 ML_ROOT가 존재하지 않는 경로가 된다(yarn_worker_bootstrap.
        # _launch_training() docstring 참고). 이 분기는 지금 DAG가 항상
        # LGB_NUM_MACHINES>1로 학습 스텝을 제출해 EMR에서 실제로 타지 않지만,
        # 나중에 그 값이 바뀌어도 cwd 때문에 FileNotFoundError로 죽지 않도록
        # ML_ROOT가 없는 환경에서는 상속받은 cwd를 그대로 쓴다.
        subprocess.run(
            [sys.executable, "-m", _TRAIN_SCRIPTS[model_name]],
            cwd=ML_ROOT if ML_ROOT.exists() else None,
            check=True,
            env=env,
        )

    archive_prefix = archive_models_prefix(archive_date, profile_name)
    metrics = s3_io.read_json(model_json_key(model_name, "metrics", archive_prefix))
    if metrics is None:
        raise RuntimeError(f"[{model_name}] 학습은 끝났는데 metrics를 못 찾음: {archive_prefix}")
    return metrics


def _attempt_promotion(
    model_name: str,
    champion_metrics: dict | None,
    *,
    skip_feature_pipeline: bool = False,
    target_profile: str | None = None,
    archive_date: str | None = None,
    no_promote: bool = False,
) -> bool:
    """후보 프로필을 시도하며 챔피언을 대체할 최적의 챌린저를 선정하여 승격한다.

    1. 완전 기준(Deviance 개선 및 Coverage 정상)을 충족하는 후보 중 Deviance가 가장 낮은 후보로 승격한다.
    2. 완전 기준에 미달하더라도 챔피언보다 Deviance가 우수한 후보가 있다면 그중 최선의 후보로 차선책 승격한다.
    3. 모든 후보가 챔피언보다 열세라면 챔피언을 그대로 유지하고 실패 로그를 남긴다.

    args:
        model_name: "rental" 또는 "return"
        champion_metrics: 현재 챔피언의 metrics.json (없으면 None)
        skip_feature_pipeline: True면 EMR이 이미 피처를 생성했다고 보고 Spark 생략
        target_profile: 특정 프로필만 단독 실행할 때 프로필 이름
        archive_date: 아카이브 날짜 접미사 (None이면 새로 생성)
        no_promote: True면 학습/후보 평가는 그대로 하되 실제
            `promote_challenger()` 호출(챔피언 포인터 갱신)만 건너뛴다 — 반환값은
            "승격했을지" 그대로라 호출부가 결과를 구분해서 로그로 남길 수 있다.
    returns:
        bool: 승격이 일어났는지(no_promote=True면 "일어났을지")
    """
    exec_archive_date = archive_date or unique_archive_date()
    candidates = (
        [(target_profile, {})]
        if target_profile
        else _candidate_profiles(model_name)
    )

    champion_deviance = (
        champion_metrics["poisson_deviance_test"]
        if champion_metrics and "poisson_deviance_test" in champion_metrics
        else float("inf")
    )

    evaluated_candidates: list[dict] = []

    for profile_name, env_overrides in candidates:
        try:
            _validate_candidate_serving_contract(profile_name, env_overrides)
        except ServingProfileContractError as exc:
            _notify(
                f"[{model_name}] '{profile_name}' 후보 건너뜀(현재 서빙 계약과 불일치: {exc}) "
                "— feature 생성/학습을 시작하지 않음"
            )
            continue

        try:
            if not skip_feature_pipeline:
                _trigger_feature_pipeline(profile_name, env_overrides)
            challenger_metrics = _run_training_subprocess(
                model_name, profile_name, exec_archive_date, env_overrides
            )
        except subprocess.CalledProcessError as exc:
            _notify(f"[{model_name}] '{profile_name}' 시도 실패(subprocess 오류: {exc}) — 다음 프로필로 넘어감")
            continue
        except (RuntimeError, OSError, ValueError) as exc:
            _notify(f"[{model_name}] '{profile_name}' 실행 중 오류 발생: {exc}")
            continue

        promote, reasons = should_promote(challenger_metrics, champion_metrics)
        for reason in reasons:
            _notify(f"[{model_name}] '{profile_name}' 판정 — {reason}")

        challenger_deviance = challenger_metrics.get("poisson_deviance_test", float("inf"))
        is_better = challenger_deviance < champion_deviance
        archive_prefix = archive_models_prefix(exec_archive_date, profile_name)

        evaluated_candidates.append(
            {
                "profile_name": profile_name,
                "archive_prefix": archive_prefix,
                "metrics": challenger_metrics,
                "deviance": challenger_deviance,
                "fully_qualified": promote,
                "better_than_champion": is_better,
                "reasons": reasons,
            }
        )

        # 단일 프로필 지정 모드이고 완전 충족 시 바로 승격 시도
        if target_profile and promote:
            break

    if not evaluated_candidates:
        _notify(f"[{model_name}] 실행 가능한 후보 프로필이 없음 — 챔피언 유지")
        return False

    # 1순위: 완전 충족 후보 중 최저 deviance 순서로 시도
    fully_qualified = sorted(
        [c for c in evaluated_candidates if c["fully_qualified"]],
        key=lambda c: c["deviance"],
    )
    for best in fully_qualified:
        try:
            if not no_promote:
                promote_challenger(model_name, best["archive_prefix"])
            _notify(
                f"[{model_name}] '{best['profile_name']}' 완전 승격 기준 충족 "
                f"(deviance={best['deviance']:.4f}) — "
                f"{'[DRY-RUN] 실제로는 승격 안 함' if no_promote else '챔피언으로 승격'} "
                f"({best['archive_prefix']})"
            )
            return True
        except ServingProfileContractError as exc:
            _notify(f"[{model_name}] '{best['profile_name']}' 승격 거부(서빙 계약 불일치: {exc}) — 다음 후보 시도")

    # 2순위: 완전 기준(Coverage 등)은 미달했으나 챔피언보다 Deviance가 우수한 후보 중 최저 deviance 순서로 시도
    better_candidates = sorted(
        [c for c in evaluated_candidates if c["better_than_champion"]],
        key=lambda c: c["deviance"],
    )
    for best in better_candidates:
        try:
            if not no_promote:
                promote_challenger(model_name, best["archive_prefix"])
            _notify(
                f"[{model_name}] '{best['profile_name']}' 완전 기준(Coverage 등)에는 미달했으나 "
                f"기존 챔피언보다 성능 우수 (deviance {best['deviance']:.4f} < {champion_deviance:.4f}) "
                f"— {'[DRY-RUN] 실제로는 승격 안 함' if no_promote else '차선책으로 챔피언 교체'} "
                f"({best['archive_prefix']})"
            )
            return True
        except ServingProfileContractError as exc:
            _notify(f"[{model_name}] '{best['profile_name']}' 승격 거부(서빙 계약 불일치: {exc}) — 다음 후보 시도")

    # 3순위: 챔피언보다 뛰어난 후보가 없음 -> 유지
    best_attempt = min(evaluated_candidates, key=lambda c: c["deviance"])
    _notify(
        f"[{model_name}] 가능한 프로필({len(evaluated_candidates)}개)을 모두 시도했지만 "
        f"챔피언보다 뛰어난 모델이 없음 (최선 deviance={best_attempt['deviance']:.4f} >= 챔피언 {champion_deviance:.4f}) "
        "— 기존 챔피언 유지, 다음 달에 재시도"
    )
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
        "--check-only",
        action="store_true",
        help="성능 점검만 수행하고 재학습은 일체 진행하지 않는다",
    )
    parser.add_argument(
        "--no-promote",
        action="store_true",
        help=(
            "학습/후보 평가는 그대로 진행하되 실제 챔피언 포인터(models/champion/*.json)는 "
            "건드리지 않는다 — 어느 후보가 이겼을지만 로그로 남긴다. 프로덕션 챔피언과 "
            "물리 경로가 겹칠 수 있는 테스트 프로필로 파이프라인 전체를 스모크 테스트할 "
            "때 실수로 진짜 승격이 일어나는 걸 막는 용도(airflow monthly_retrain DAG의 "
            "test_profile_only 참고)."
        ),
    )
    parser.add_argument(
        "--skip-feature-pipeline",
        action="store_true",
        help="EMR 등에서 피처마트가 이미 생성되었다고 가정하고 Spark 생성을 건너뛴다",
    )
    parser.add_argument(
        "--profile-name",
        default=None,
        help="특정 프로필 하나만 지정하여 학습/평가를 수행한다",
    )
    parser.add_argument(
        "--models",
        default=None,
        help="쉼표로 구분된 대상 모델 목록 (예: 'rental,return' 또는 'rental')",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="점검 또는 실행 결과를 JSON 문자열로 출력한다",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="기준 날짜(YYYY-MM-DD) override — 기본은 오늘",
    )
    parser.add_argument(
        "--result-s3-key",
        default=None,
        help=(
            "결과 요약을 이 S3 키에 JSON으로 써준다. EMR 스텝(command-runner.jar)으로 이 "
            "스크립트를 실행하면 SSM처럼 stdout을 바로 못 돌려받으므로, Airflow가 스텝 완료 "
            "후 이 키를 읽어 재학습 필요 여부/승격 결과를 판단한다(월간 재학습 DAG 참고)."
        ),
    )
    parser.add_argument(
        "--eval-num-workers",
        type=int,
        default=1,
        help=(
            "1보다 크면 성능 점검(evaluate_recent_performance)을 이 개수만큼 YARN "
            "distributed-shell 컨테이너에 날짜 기준으로 나눠 돌린다. 기본값 1은 기존과 "
            "동일하게 현재 프로세스에서 단일로 평가한다."
        ),
    )
    parser.add_argument(
        "--performance-already-checked",
        action="store_true",
        help=(
            "상위 오케스트레이터가 성능 점검을 완료한 경우 재평가를 건너뛰고 "
            "--models 대상을 바로 재학습한다. --execute 및 --models와 함께만 사용한다."
        ),
    )
    args = parser.parse_args()

    requested_models = (
        [m.strip() for m in args.models.split(",") if m.strip()]
        if args.models
        else None
    )
    if args.performance_already_checked:
        if not args.execute or not requested_models:
            parser.error("--performance-already-checked는 --execute 및 --models와 함께 사용해야 합니다")
        results = []
        retrain_needed = []
        target_models = requested_models
    else:
        # `check_all_models()`에 처음부터 요청받은 모델만 넘긴다 — 안 그러면
        # `--models rental`이어도 return용 feature mart까지 통째로 읽어들여
        # m4.large 컨테이너 메모리 예산에서 OOM(exitCode 137)이 난다(실제 EMR
        # 실행에서 확인, 2026-08-26).
        results = _check_all_models_distributed(args.as_of, 1, requested_models, args.eval_num_workers)
        if not args.json_output:
            _print_report(results)

        retrain_needed = [r for r in results if r["needs_retrain"]]
        target_models = [r["model_name"] for r in retrain_needed]

    if args.check_only or not args.execute:
        summary = {
            "needs_retrain": len(retrain_needed) > 0,
            "retrain_models": [r["model_name"] for r in retrain_needed],
            "candidate_profiles": list(dict.fromkeys(
                name for r in retrain_needed for name, _ in _candidate_profiles(r["model_name"])
            )) if retrain_needed else [],
            "results": results,
        }
        if args.json_output:
            import json
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            if not retrain_needed:
                _notify("모든 모델이 기준 이내 — 재학습 필요 없음")
            else:
                _notify(
                    f"기준 미달 모델 {len(retrain_needed)}개 — 실제 재학습은 --execute로 다시 실행하세요 "
                    "(지금은 dry-run/check-only라 아무것도 바꾸지 않았습니다)"
                )
        if args.result_s3_key:
            s3_io.write_json(args.result_s3_key, summary)
        return results

    if not target_models:
        _notify("재학습 대상 모델이 없음 — 종료")
        if args.result_s3_key:
            s3_io.write_json(args.result_s3_key, {"promoted": {}, "target_models": []})
        return results

    _notify(f"=== 챌린저 재학습 시도 시작 ({len(target_models)}개 모델: {target_models}) ===")
    promoted_by_model: dict[str, bool] = {}
    for model_name in target_models:
        try:
            champion_metrics = _load_baseline_metrics(model_name)
        except FileNotFoundError:
            champion_metrics = None
        promoted_by_model[model_name] = _attempt_promotion(
            model_name,
            champion_metrics,
            skip_feature_pipeline=args.skip_feature_pipeline,
            target_profile=args.profile_name,
            no_promote=args.no_promote,
        )

    if args.result_s3_key:
        s3_io.write_json(
            args.result_s3_key,
            {
                "promoted": promoted_by_model,
                "target_models": target_models,
                # no_promote=True면 위 promoted 값은 "실제 승격"이 아니라
                # "승격 기준을 충족했을지"다 — 호출부(airflow DAG)가 이 플래그로
                # 구분해서 착각하지 않게 명시적으로 같이 남긴다.
                "no_promote": args.no_promote,
            },
        )
    return results


if __name__ == "__main__":
    main()
