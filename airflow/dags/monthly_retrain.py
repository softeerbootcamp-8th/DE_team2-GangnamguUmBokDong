"""대여(rental) 및 반납(return) 모델의 월별 점검, EMR 피처마트 생성, YARN 분산 학습, 승격을 단일 EMR 클러스터에서 오케스트레이션하는 DAG."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
import subprocess
from itertools import pairwise
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import Param
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable


from config.schedules import (
    CATCHUP,
    MAX_ACTIVE_RUNS,
    MONTHLY_CLUSTER_CREATE_TIMEOUT,
    MONTHLY_EVALUATION_TIMEOUT,
    MONTHLY_FEATURE_REFRESH_TIMEOUT,
    MONTHLY_RETRAIN_CRON,
    MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    MONTHLY_RETRAIN_TOTAL_TIMEOUT,
    MONTHLY_TRAINING_TIMEOUT,
    TIMEZONE,
)
from orchestration.aws_infra_task import (
    EMR_CORE_INSTANCE_TYPE,
    EMR_MLFLOW_TRACKING_URI,
    MOCK_OVERRIDE_FORCE_MOCK,
    MOCK_OVERRIDE_FORCE_REAL,
    MODELS_PREFIX,
    S3_BUCKET,
    TRAINING_RUNS_PREFIX,
    create_emr_cluster,
    read_s3_json,
    submit_emr_step,
    terminate_emr_cluster,
)

from airflow import DAG

logger = logging.getLogger(__name__)

# 대여 -> 반납 순서로 고정 실행한다(동시 EMR 클러스터 생성 방지).
MODEL_EXECUTION_ORDER = ("rental", "return")

FEATURE_MART_CORE_INSTANCE_COUNT = 8
TRAINING_CORE_INSTANCE_COUNT = 8
# Outer AM, Outer Worker, Inner AM 노드 예약 수
_WRAPPER_NODE_RESERVATION = 3
_EMR_PYTHONPATH = "/opt/gng"


def _resolve_wrapper_worker_count(core_instance_count: int) -> int:
    """코어 인스턴스 수에서 래퍼 예약 노드를 제외한 실제 워커 수를 계산한다.

    args:
        core_instance_count: 전체 EMR 코어 노드 수
    returns:
        실제 YARN 분산 워커 수
    raises:
        ValueError: 코어 인스턴스 수가 최소 예약 수 이하일 때
    """
    worker_count = core_instance_count - _WRAPPER_NODE_RESERVATION
    if worker_count < 1:
        raise ValueError(
            f"emr_core_instance_count={core_instance_count}는 너무 작습니다 — "
            f"최소 {_WRAPPER_NODE_RESERVATION + 1}개 이상이어야 합니다."
        )
    return worker_count

# 빠른 스모크 테스트용 프로필 (격리된 S3 경로 사용)
TEST_ONLY_PROFILE_NAME = "a-test-sparse-flat"

# DAG params의 "mock_mode" 값 -> aws_infra_task override 인자 매핑
_MOCK_MODE_PARAM_TO_OVERRIDE = {
    "force_mock": MOCK_OVERRIDE_FORCE_MOCK,
    "force_real": MOCK_OVERRIDE_FORCE_REAL,
}


def _mock_override_from_params(params: dict[str, Any]) -> str | None:
    """DAG trigger 시점 params의 mock_mode 선택값을 override 인자로 변환한다."""
    return _MOCK_MODE_PARAM_TO_OVERRIDE.get(params.get("mock_mode", "auto"))


def _result_s3_key(run_id: str, name: str) -> str:
    """EMR 스텝의 결과 요약 JSON이 기록되는 S3 키 경로를 반환한다."""
    return f"{TRAINING_RUNS_PREFIX}/{run_id}/{name}.json"


def _spark_module_launcher_command(
    module: str,
    extra_args: list[str],
    app_args: list[str] | None = None,
) -> list[str]:
    """지정된 Python 모듈을 spark-submit으로 실행하는 bash 스텝 인자를 생성한다.

    args:
        module: 실행할 Python 모듈 경로 (예: 'feature_engine.spark.run_pipeline')
        extra_args: spark-submit 설정 인자 목록
        app_args: 모듈에 전달할 CLI 인자 목록
    returns:
        bash 실행 명령 리스트
    """
    launcher = (
        f"cat > /tmp/_spark_entry_{module.rsplit('.', 1)[-1]}.py <<'PYEOF'\n"
        "import runpy\n"
        f'runpy.run_module("{module}", run_name="__main__")\n'
        "PYEOF\n"
    )
    entry_path = f"/tmp/_spark_entry_{module.rsplit('.', 1)[-1]}.py"
    spark_submit = " ".join(
        ["spark-submit", "--deploy-mode", "cluster", "--master", "yarn", *extra_args, entry_path, *(app_args or [])]
    )
    return ["bash", "-c", launcher + spark_submit]


_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS = ("/usr/lib/hadoop-yarn", "/usr/lib/hadoop", "/opt/hadoop")
_YARN_AM_MEMORY_MB = 1024
_YARN_AM_VCORES = 1


def _yarn_python_module_step(
    name: str, module: str, app_args: list[str], env: dict[str, str] | None = None
) -> tuple[str, list[str]]:
    """YARN core 노드의 컨테이너에서 순수 Python 모듈을 실행하는 스텝을 생성한다.

    args:
        name: EMR 스텝 이름
        module: 실행할 Python 모듈 (예: 'training.scripts.monthly_retrain_check')
        app_args: 모듈 CLI 인자 목록
        env: 추가 환경변수 dict
    returns:
        스텝 이름과 bash 실행 인자 튜플
    """
    shell_env = {"S3_BUCKET": S3_BUCKET, "PYTHONPATH": _EMR_PYTHONPATH, **(env or {})}
    if EMR_MLFLOW_TRACKING_URI:
        shell_env["MLFLOW_TRACKING_URI"] = EMR_MLFLOW_TRACKING_URI
    shell_env_args = " ".join(f"-shell_env {key}={value}" for key, value in shell_env.items())
    module_args = " ".join(app_args)
    shell_command = f"cd {_EMR_PYTHONPATH} && PYTHONPATH={_EMR_PYTHONPATH} python3.11 -m {module} {module_args}"
    search_roots = " ".join(_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS)
    script = (
        f"JAR=$(find {search_roots} -iname '*distributedshell*.jar' 2>/dev/null | head -1); "
        'if [ -z "$JAR" ]; then echo "distributed-shell jar를 찾을 수 없습니다" >&2; exit 1; fi; '
        "yarn org.apache.hadoop.yarn.applications.distributedshell.Client "
        f'-jar "$JAR" -shell_command \'{shell_command}\' '
        "-num_containers 1 -container_memory 6144 -container_vcores 2 "
        f"-master_memory {_YARN_AM_MEMORY_MB} -master_vcores {_YARN_AM_VCORES} "
        "-timeout 345600000 "
        f"{shell_env_args}"
    )

    return name, ["bash", "-c", script]



def _feature_mart_spark_steps(
    profile: str,
    core_instance_count: int = FEATURE_MART_CORE_INSTANCE_COUNT,
    model_name: str | None = None,
    force: bool = False,
) -> tuple[tuple[str, list[str]], tuple[str, list[str]]]:
    """`profile`로 feature mart(2차 정제)를 (재)생성하는 두 Spark 스텝(name, args)을
    반환한다 — run_pipeline.py는 watermark 기반 증분이라 매번 불러도 안전하다
    (평가 전 최신화용 `refresh_feature_mart`와 재학습 루프 양쪽에서 재사용).

    args:
        profile: 대상 ML 프로필 이름
        core_instance_count: 이 스텝이 실제로 돌 클러스터의 core 노드 수 —
            `--num-executors`를 여기 맞춰 노드마다 executor 하나씩 배치한다
            (노드 수보다 하나 적게 잡아 AM 자리를 남긴다). 하드코딩된 값을
            그대로 두면 노드 수가 늘어나도 병렬도가 그대로라 유휴 노드가
            생긴다(PR #248 리뷰 지적).
        model_name: "rental", "return", 또는 None(둘 다). 지정 시 Multi-horizon
        force: True이면 워터마크 신선도를 무시하고 무조건 강제 재생성.

            확장 스텝이 해당 모델의 피처마트만 단독 생성해 연산량을 절감한다.
    """
    common_confs = [
        # feature_engine/spark/spark_session.py의 get_spark()가 "지금 진짜 yarn
        # 클러스터 실행인지"를 판단할 유일한 신호 — pyspark SparkConf()로 spark-submit
        # 인자를 되짚어 보는 시도는 SparkContext._jvm이 아직 없는 시점엔 항상 빈
        # 값을 봐서 실패했다(실제 EMR 실행으로 확인, 2026-08-26). 이 값이 있어야
        # get_spark()가 .master()를 안 건드리고 --master yarn을 그대로 살려 executor가
        # 실제로 뜬다.
        "--conf",
        "spark.yarn.appMasterEnv.SPARK_ON_YARN=1",
        "--conf",
        "spark.executorEnv.SPARK_ON_YARN=1",
        "--conf",
        f"spark.yarn.appMasterEnv.ML_PROFILE={profile}",
        "--conf",
        f"spark.executorEnv.ML_PROFILE={profile}",
        # S3_BUCKET도 ML_PROFILE과 같은 이유로 명시해야 한다 — 없으면
        # ml/feature_engine/spark/config.py가 "local-dev"로 떨어진다.
        "--conf",
        f"spark.yarn.appMasterEnv.S3_BUCKET={S3_BUCKET}",
        "--conf",
        f"spark.executorEnv.S3_BUCKET={S3_BUCKET}",
        # PYTHONPATH가 없으면 드라이버/executor가 core/ml_core/feature_engine을
        # 못 찾아 "No module named 'core'"로 즉시 죽는다(실제 EMR 실행에서 확인,
        # 2026-08-25) — `Makefile`의 `emr-features` 타겟이 이미 쓰던 값과 맞춘다.
        "--conf",
        f"spark.yarn.appMasterEnv.PYTHONPATH={_EMR_PYTHONPATH}",
        "--conf",
        f"spark.executorEnv.PYTHONPATH={_EMR_PYTHONPATH}",
        "--conf",
        "spark.pyspark.python=/usr/bin/python3.11",
        "--conf",
        "spark.pyspark.driver.python=/usr/bin/python3.11",
        # m4.large 노드당 YARN이 실제로 내주는 건 ~6GB뿐이다(EMR 자체 로그의
        # "6144 MB per container" 상한, 물리 8GB에서 OS/데몬 몫을 뺀 값). 기본
        # 드라이버/AM 힙(2g)로는 워터마크 없는 최초 실행(수개월치 전체 재생성)이
        # py4j 연결 끊김(OOM성 컨테이너 킬)으로 죽었다 — 4g로 넉넉히 올리고,
        # num-executors를 노드 수보다 적게 잡아 AM/executor가 노드마다 하나씩만
        # 배치되게 한다(실제 EMR 실행으로 검증, 2026-08-25).
        "--driver-memory",
        "4g",
        "--executor-memory",
        "4g",
        "--num-executors",
        str(max(core_instance_count - 1, 1)),
        "--conf",
        "spark.yarn.am.waitTime=1800s",
        "--conf",
        "spark.sql.shuffle.partitions=24",
        "--conf",
        "spark.hadoop.fs.s3a.buffer.dir=/mnt/tmp",
        "--conf",
        "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    ]
    run_pipeline_args = ["--force"] if force else []
    multi_horizon_args = (["--models", model_name] if model_name else []) + (["--force"] if force else [])
    return (
        (
            f"Spark-RunPipeline-{profile}",
            _spark_module_launcher_command(
                "feature_engine.spark.run_pipeline", common_confs, app_args=run_pipeline_args
            ),
        ),
        (
            f"Spark-BuildMultiHorizon-{profile}",
            _spark_module_launcher_command(
                "feature_engine.spark.build_multi_horizon_features", common_confs, app_args=multi_horizon_args
            ),
        ),
    )


def _champion_profile_name(model_name: str) -> str | None:
    """현재 활성 챔피언 모델이 학습될 때 사용된 프로필 이름을 반환한다."""
    pointer = read_s3_json(f"{MODELS_PREFIX}/champion/{model_name}.json")
    archive_prefix = (pointer or {}).get("archive_prefix")
    if not archive_prefix:
        return None
    payload = read_s3_json(f"{archive_prefix}/{model_name}_profile.json")
    return (payload or {}).get("profile_name")


def _extract_profile_feature_params(profile_data: dict) -> tuple[str, int]:
    """프로필 딕셔너리에서 피처마트 combo_id와 anchor_tick을 추출한다.

    args:
        profile_data: S3에서 로드한 프로필 딕셔너리
    returns:
        tuple[str, int]: (combo_id, anchor_tick)
    """
    window = (
        profile_data.get("ROLLING_WINDOW_MINUTES")
        or profile_data.get("rolling_window_minutes")
        or 60
    )
    embargo = (
        profile_data.get("ROLLING_EMBARGO_MINUTES")
        or profile_data.get("rolling_embargo_minutes")
        or 40  # DEFAULT_PROFILE 기준 기본 embargo는 40
    )
    tick = (
        profile_data.get("ROLLING_TICK_MINUTES")
        or profile_data.get("rolling_tick_minutes")
        or 20
    )
    anchor_tick = (
        profile_data.get("TRAIN_ANCHOR_TICK_MINUTES")
        or profile_data.get("train_anchor_tick_minutes")
        or profile_data.get("GRID_TICK_MINUTES")
        or profile_data.get("grid_tick_minutes")
        or 20
    )
    combo_id = f"w{window}_e{embargo}_t{tick}"
    return combo_id, int(anchor_tick)


def _is_feature_mart_fresh(profile: str, model_name: str, max_age_hours: float = 24.0) -> bool:
    """S3 워터마크를 확인해 피처마트가 최근 max_age_hours 이내에 이미 갱신되었는지 판정한다.

    args:
        profile: 프로필 이름 (예: "default")
        model_name: "rental" 또는 "return"
        max_age_hours: 신선하다고 판단할 최대 경과 시간 (시간 단위)
    returns:
        bool: 베이스 및 모델 전용 워터마크가 모두 존재하고 최신이면 True
    """
    try:
        profile_data = read_s3_json(f"profiles/{profile}.json")
        if not profile_data:
            profile_data = read_s3_json(f"{MODELS_PREFIX}/profiles/{profile}.json") or {}

        combo_id, anchor_tick = _extract_profile_feature_params(profile_data)

        base_wm = read_s3_json(f"processed/features/{combo_id}/_watermark.json")
        if not base_wm or not base_wm.get("updated_at"):
            return False

        model_wm_key = (
            f"processed/features/{combo_id}/training_anchor_a{anchor_tick}/_multi_horizon_{model_name}_watermark.json"
        )
        model_wm = read_s3_json(model_wm_key)
        if not model_wm or not model_wm.get("updated_at"):
            return False

        now = datetime.now(UTC)
        base_updated = datetime.fromisoformat(base_wm["updated_at"])
        model_updated = datetime.fromisoformat(model_wm["updated_at"])
        if (now - base_updated).total_seconds() > max_age_hours * 3600:
            return False
        if (now - model_updated).total_seconds() > max_age_hours * 3600:
            return False
        return base_wm.get("max_hour_ts") == model_wm.get("max_hour_ts")
    except Exception:
        return False


def _bash_step(name: str, command: str) -> tuple[str, list[str]]:
    """S3 버킷 및 MLflow 환경변수를 주입하여 실행하는 EMR bash 스텝을 생성한다."""
    exports = f"export S3_BUCKET={S3_BUCKET}"
    if EMR_MLFLOW_TRACKING_URI:
        exports += f" && export MLFLOW_TRACKING_URI={EMR_MLFLOW_TRACKING_URI}"
    return name, ["bash", "-c", f"{exports} && {command}"]


def _wait_for_yarn_nodes_step(count: int) -> tuple[str, list[str]]:
    """지정된 개수 이상의 YARN 노드가 RUNNING 상태가 될 때까지 대기하는 스텝을 생성한다."""
    return _bash_step(
        "Wait-YARN-Nodes",
        f"until [ $(yarn node -list -all 2>/dev/null | grep -c RUNNING) -ge {count} ]; do sleep 15; done",
    )


def _task_id(model_name: str, name: str) -> str:
    """모델별 태스크 체인의 고유 task_id를 반환한다."""
    return f"{name}_{model_name}"


def _get_cluster_id(ti: Any, model_name: str | None = None) -> str | None:
    """XCom에서 생성된 EMR cluster_id를 조회한다."""
    if model_name:
        cid = ti.xcom_pull(task_ids=_task_id(model_name, "create_cluster"), key="cluster_id")
        if cid:
            return cid
    return ti.xcom_pull(task_ids="create_cluster", key="cluster_id") or ti.xcom_pull(key="cluster_id")


def make_task_create_cluster(model_name: str | None = None) -> Any:
    """월간 재학습용 상시 EMR 클러스터를 생성하는 task callable을 반환한다."""

    def task_create_cluster(**context: Any) -> str:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        label = f"-{model_name}" if model_name else ""

        logger.info("[월별 재학습] 1단계: 단일 상시 EMR 클러스터 생성(피처마트/학습용 %d노드)", FEATURE_MART_CORE_INSTANCE_COUNT)
        cluster_id = create_emr_cluster(
            cluster_name=f"ml-monthly-retrain{label}",
            core_instance_count=params.get("emr_core_instance_count") or FEATURE_MART_CORE_INSTANCE_COUNT,
            core_instance_type=params.get("emr_core_instance_type") or None,
            mock_override=mock_override,
        )
        ti.xcom_push(key="cluster_id", value=cluster_id)
        return cluster_id

    return task_create_cluster


def make_task_refresh_feature_mart(model_name: str) -> Any:
    """평가 직전에 현재 챔피언 프로필 기준으로 피처마트를 최신화하는 task callable을 반환한다."""

    def task_refresh_feature_mart(**context: Any) -> None:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        cluster_id = _get_cluster_id(ti, model_name)

        params_core_count = params.get("emr_core_instance_count") or FEATURE_MART_CORE_INSTANCE_COUNT
        submit_emr_step(
            cluster_id, *_wait_for_yarn_nodes_step(params_core_count),
            timeout_seconds=1200, mock_override=mock_override,
        )

        if params.get("test_profile_only"):
            logger.info(
                "[%s 월별 재학습] test_profile_only=True — 챔피언 feature mart 갱신 스킵", model_name
            )
            return

        force_refresh = bool(params.get("force_refresh_feature_mart", False))

        profile = _champion_profile_name(model_name) or "builtin-default"
        if not force_refresh and _is_feature_mart_fresh(profile, model_name):
            logger.info(
                "[%s 월별 재학습] 1.5단계: 프로필 '%s'의 피처마트가 이미 최신(24h 이내) 상태입니다 — Spark 스텝 제출 스킵",
                model_name,
                profile,
            )
            ti.xcom_push(key="profile", value=profile)
            return

        logger.info(
            "[%s 월별 재학습] 1.5단계: 챔피언 프로필 '%s' 기준으로 [%s] feature mart %s",
            model_name,
            profile,
            model_name,
            "강제 전체 갱신" if force_refresh else "증분 갱신",
        )
        for step_name, spark_args in _feature_mart_spark_steps(
            profile, params_core_count, model_name=model_name, force=force_refresh
        ):
            submit_emr_step(cluster_id, step_name, spark_args, mock_override=mock_override)
        ti.xcom_push(key="profile", value=profile)



    return task_refresh_feature_mart


def make_task_evaluate(model_name: str) -> Any:
    """EMR 클러스터 위에서 챔피언 모델 성능 점검 스텝을 실행하는 task callable을 반환한다."""

    def task_evaluate(**context: Any) -> dict[str, Any]:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        run_id = context["run_id"]
        cluster_id = _get_cluster_id(ti, model_name)

        if params.get("test_profile_only"):
            logger.info(
                "[%s 월별 재학습] test_profile_only=True — 재평가 건너뛰고 '%s' 프로필로 바로 재학습",
                model_name,
                TEST_ONLY_PROFILE_NAME,
            )
            summary = {"needs_retrain": True, "candidate_profiles": [TEST_ONLY_PROFILE_NAME]}
            ti.xcom_push(key="needs_retrain", value=True)
            ti.xcom_push(key="candidate_profiles", value=[TEST_ONLY_PROFILE_NAME])
            return summary

        logger.info("[%s 월별 재학습] 2단계: EMR 스텝으로 %s 챔피언 성능 점검 (--check-only)", model_name, model_name)
        result_key = _result_s3_key(run_id, f"eval-{model_name}")
        core_count = params.get("emr_core_instance_count") or FEATURE_MART_CORE_INSTANCE_COUNT
        eval_num_workers = _resolve_wrapper_worker_count(core_count)
        profile = ti.xcom_pull(task_ids=_task_id(model_name, "refresh_feature_mart"), key="profile") or "builtin-default"
        name, command = _yarn_python_module_step(
            f"Evaluate-{model_name}",
            "training.scripts.monthly_retrain_check",
            [
                "--check-only", "--models", model_name, "--result-s3-key", result_key,
                "--eval-num-workers", str(eval_num_workers),
            ],
            env={"ML_PROFILE": profile},
        )
        submit_emr_step(cluster_id, name, command, mock_override=mock_override)

        summary = read_s3_json(result_key) or {
            "needs_retrain": True,
            "retrain_models": [model_name],
            "candidate_profiles": ["builtin-default"],
        }
        ti.xcom_push(key="needs_retrain", value=summary.get("needs_retrain", False))
        ti.xcom_push(
            key="candidate_profiles", value=summary.get("candidate_profiles") or ["builtin-default"]
        )
        return summary

    return task_evaluate


def make_task_check_retrain_branch(model_name: str) -> Any:
    """재학습 진행 여부를 판별하여 분기하는 task callable을 반환한다."""

    def task_check_retrain_branch(**context: Any) -> str:
        ti = context["ti"]
        needs_retrain = ti.xcom_pull(
            task_ids=_task_id(model_name, "evaluate"), key="needs_retrain"
        )
        if needs_retrain:
            logger.info("[%s 월별 재학습] 기준 미달 발견 — 재학습 오케스트레이션으로 분기", model_name)
            return _task_id(model_name, "orchestrate_retrain_loop")
        logger.info("[%s 월별 재학습] 성능 정상 — 재학습 건너뜀", model_name)
        return _task_id(model_name, "skip_monthly_retrain")

    return task_check_retrain_branch


def make_task_orchestrate_retrain_loop(model_name: str) -> Any:
    """후보 프로필을 순환하며 피처마트 생성 및 YARN 분산 학습을 실행하는 task callable을 반환한다."""

    def task_orchestrate_retrain_loop(**context: Any) -> dict[str, Any]:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        run_id = context["run_id"]
        cluster_id = _get_cluster_id(ti, model_name)
        candidate_profiles = (
            ti.xcom_pull(task_ids=_task_id(model_name, "evaluate"), key="candidate_profiles")
            or ["builtin-default"]
        )

        logger.info(
            "[%s 월별 재학습] 3단계: 챌린저 재학습 시작 (대상 모델: %s, 후보 프로필: %s)",
            model_name,
            model_name,
            candidate_profiles,
        )

        results_by_profile: dict[str, Any] = {}
        core_instance_count = params.get("emr_core_instance_count") or TRAINING_CORE_INSTANCE_COUNT
        _resolve_wrapper_worker_count(core_instance_count)

        force_refresh = bool(params.get("force_refresh_feature_mart", False))
        for profile in candidate_profiles:
            logger.info("=== [%s 프로필: %s] EMR 피처마트 스텝 제출 ===", model_name, profile)
            try:
                for step_name, spark_args in _feature_mart_spark_steps(
                    profile, core_instance_count, model_name=model_name, force=force_refresh
                ):
                    submit_emr_step(cluster_id, step_name, spark_args, mock_override=mock_override)
                logger.info("=== [%s 프로필: %s] EMR 피처마트 완료 ===", model_name, profile)

                logger.info("=== [%s 프로필: %s] YARN distributed-shell 학습 스텝 제출 ===", model_name, profile)
                train_result_key = _result_s3_key(run_id, f"train-{model_name}-{profile}")
                train_args = [
                    "--execute",
                    "--performance-already-checked",
                    "--skip-feature-pipeline",
                    "--profile-name",
                    profile,
                    "--models",
                    model_name,
                    "--result-s3-key",
                    train_result_key,
                ]
                if params.get("test_profile_only"):
                    train_args.append("--no-promote")
                train_name, train_command = _yarn_python_module_step(
                    f"Train-{model_name}-{profile}",
                    "training.scripts.monthly_retrain_check",
                    train_args,
                    env={
                        "LGB_NUM_MACHINES": str(_resolve_wrapper_worker_count(core_instance_count)),
                        "LGB_TREE_LEARNER": "data",
                    },
                )
                submit_emr_step(
                    cluster_id,
                    train_name,
                    train_command,
                    timeout_seconds=int(MONTHLY_TRAINING_TIMEOUT.total_seconds()),
                    mock_override=mock_override,
                )
                train_summary = read_s3_json(train_result_key) or {"promoted": {model_name: False}}
                promoted = bool(train_summary.get("promoted", {}).get(model_name))
                results_by_profile[profile] = {"status": "success", "promoted": promoted}
                if promoted:
                    logger.info("=== [%s 프로필: %s] 챔피언 승격 성공 — 루프 종료 ===", model_name, profile)
                    break
            except (RuntimeError, OSError, ValueError, TimeoutError) as exc:
                logger.error("[%s 프로필: %s] 재학습 루프 중 오류 발생: %s", model_name, profile, exc)
                results_by_profile[profile] = {"status": "failed", "error": str(exc)}

        return {"status": "completed", "profiles": results_by_profile}

    return task_orchestrate_retrain_loop


def make_task_terminate_emr_cluster(model_name: str | None = None) -> Any:
    """EMR 클러스터를 안전하게 종료하는 teardown task callable을 반환한다."""

    def task_terminate_emr_cluster(**context: Any) -> None:
        ti = context["ti"]
        mock_override = _mock_override_from_params(context.get("params", {}))
        cluster_id = _get_cluster_id(ti, model_name)
        if not cluster_id:
            logger.warning("[월별 재학습] 4단계: cluster_id 없음(생성 자체가 실패) — 종료할 대상 없음")
            return
        logger.info("[월별 재학습] 4단계: EMR 클러스터 '%s' 종료", cluster_id)
        terminate_emr_cluster(cluster_id, mock_override=mock_override)

    return task_terminate_emr_cluster



_MLFLOW_COMPOSE_SERVICE_LABEL = "com.docker.compose.service=mlflow"


def _docker_mlflow_container_action(action: str) -> None:
    """호스트 Docker 데몬 소켓 API를 통해 MLflow 컨테이너의 라이프사이클을 제어한다.

    args:
        action: 'start' 또는 'stop'
    """
    find_cmd = [
        "curl", "-sf", "--unix-socket", "/var/run/docker.sock",
        f"http://localhost/containers/json?all=1&filters=%7B%22label%22%3A%5B%22{_MLFLOW_COMPOSE_SERVICE_LABEL}%22%5D%7D",
    ]
    result = subprocess.run(find_cmd, capture_output=True, text=True, check=True)
    containers = json.loads(result.stdout)
    if not containers:
        logger.warning("[mlflow %s] label=%s인 컨테이너를 못 찾음 — 건너뜀", action, _MLFLOW_COMPOSE_SERVICE_LABEL)
        return
    container_id = containers[0]["Id"]
    subprocess.run(
        ["curl", "-sf", "-X", "POST", "--unix-socket", "/var/run/docker.sock", f"http://localhost/containers/{container_id}/{action}"],
        check=True,
    )
    logger.info("[mlflow %s] 컨테이너 %s에 적용 완료", action, container_id[:12])


def make_task_start_mlflow() -> Any:
    """DAG 실행 시 MLflow 컨테이너를 시작하는 task callable을 반환한다."""

    def task_start_mlflow(**context: Any) -> None:
        mock_override = _mock_override_from_params(context.get("params", {}))
        if mock_override == MOCK_OVERRIDE_FORCE_MOCK:
            logger.info("[mlflow] force_mock — 실제로 켜지 않음")
            return
        try:
            _docker_mlflow_container_action("start")
        except Exception:
            logger.exception("[mlflow] 시작 실패 — 재학습은 계속 진행")

    return task_start_mlflow


def make_task_stop_mlflow() -> Any:
    """DAG 종료 시 MLflow 컨테이너를 정지하는 task callable을 반환한다."""

    def task_stop_mlflow(**context: Any) -> None:
        mock_override = _mock_override_from_params(context.get("params", {}))
        if mock_override == MOCK_OVERRIDE_FORCE_MOCK:
            logger.info("[mlflow] force_mock — 실제로 끄지 않음")
            return
        try:
            _docker_mlflow_container_action("stop")
        except Exception:
            logger.exception("[mlflow] 정지 실패")

    return task_stop_mlflow


def build_model_task_chain(model_name: str, is_first: bool = False) -> dict[str, Any]:
    """지정된 모델의 평가 및 재학습 태스크 체인을 생성하여 반환한다.

    args:
        model_name: 'rental' 또는 'return'
        is_first: DAG 체인의 첫 번째 모델 여부
    returns:
        생성된 태스크 체인 딕셔너리
    """
    trigger_rule = TriggerRule.ALL_SUCCESS if is_first else TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    refresh_feature_mart = PythonOperator(
        task_id=_task_id(model_name, "refresh_feature_mart"),
        python_callable=make_task_refresh_feature_mart(model_name),
        execution_timeout=MONTHLY_FEATURE_REFRESH_TIMEOUT,
        trigger_rule=trigger_rule,
    )

    evaluate = PythonOperator(
        task_id=_task_id(model_name, "evaluate"),
        python_callable=make_task_evaluate(model_name),
        execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
    )

    check_retrain_branch = BranchPythonOperator(
        task_id=_task_id(model_name, "check_retrain_branch"),
        python_callable=make_task_check_retrain_branch(model_name),
    )

    skip_monthly_retrain = EmptyOperator(
        task_id=_task_id(model_name, "skip_monthly_retrain"),
    )

    orchestrate_retrain_loop = PythonOperator(
        task_id=_task_id(model_name, "orchestrate_retrain_loop"),
        python_callable=make_task_orchestrate_retrain_loop(model_name),
        execution_timeout=MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    )

    # 태스크 흐름 정의
    refresh_feature_mart >> evaluate >> check_retrain_branch
    check_retrain_branch >> [orchestrate_retrain_loop, skip_monthly_retrain]

    return {
        "start": refresh_feature_mart,
        "ends": [orchestrate_retrain_loop, skip_monthly_retrain],
        "refresh_feature_mart": refresh_feature_mart,
        "evaluate": evaluate,
        "check_retrain_branch": check_retrain_branch,
        "skip_monthly_retrain": skip_monthly_retrain,
        "orchestrate_retrain_loop": orchestrate_retrain_loop,
    }


def build_monthly_retrain_dag(cron_schedule: str) -> DAG:
    """대여 및 반납 모델의 월간 재학습 파이프라인 DAG를 생성한다.

    args:
        cron_schedule: cron 표현식 문자열
    returns:
        구성된 Airflow DAG 인스턴스
    """
    with DAG(
        dag_id="monthly_retrain",
        schedule=CronTriggerTimetable(cron_schedule, timezone=TIMEZONE),
        start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
        catchup=CATCHUP,
        max_active_runs=MAX_ACTIVE_RUNS,
        dagrun_timeout=MONTHLY_RETRAIN_TOTAL_TIMEOUT,
        tags=["ml", "monthly", "retrain", "emr", "yarn", "rental", "return"],
        params={
            "mock_mode": Param(
                "auto",
                type="string",
                enum=["auto", "force_mock", "force_real"],
                description=(
                    "auto: 환경변수로 자동 판별. "
                    "force_mock: EMR 호출 없이 mock. "
                    "force_real: 실제 AWS EMR 호출."
                ),
            ),
            "emr_core_instance_type": Param(
                EMR_CORE_INSTANCE_TYPE,
                type="string",
                description="EMR core 노드 인스턴스 타입.",
            ),
            "emr_core_instance_count": Param(
                FEATURE_MART_CORE_INSTANCE_COUNT,
                type="integer",
                minimum=_WRAPPER_NODE_RESERVATION + 1,
                maximum=10,
                description="클러스터 Core 노드 수.",
            ),
            "force_refresh_feature_mart": Param(
                False,
                type="boolean",
                description="피처마트 강제 전체 재생성 여부.",
            ),
            "test_profile_only": Param(
                False,
                type="boolean",
                description=f"스모크 테스트용 프로필({TEST_ONLY_PROFILE_NAME}) 단독 실행 여부.",
            ),
        },
    ) as dag:

        start_mlflow = PythonOperator(
            task_id="start_mlflow",
            python_callable=make_task_start_mlflow(),
        )
        stop_mlflow = PythonOperator(
            task_id="stop_mlflow",
            python_callable=make_task_stop_mlflow(),
            trigger_rule=TriggerRule.ALL_DONE,
        )
        stop_mlflow.as_teardown(setups=start_mlflow)

        create_cluster = PythonOperator(
            task_id="create_cluster",
            python_callable=make_task_create_cluster(),
            execution_timeout=MONTHLY_CLUSTER_CREATE_TIMEOUT,
        )
        terminate_cluster = PythonOperator(
            task_id="terminate_cluster",
            python_callable=make_task_terminate_emr_cluster(),
            trigger_rule=TriggerRule.ALL_DONE,
        )
        terminate_cluster.as_teardown(setups=create_cluster)

        rental_chain = build_model_task_chain("rental", is_first=True)
        return_chain = build_model_task_chain("return", is_first=False)

        start_mlflow >> create_cluster >> rental_chain["start"]
        rental_chain["ends"] >> return_chain["start"]
        return_chain["ends"] >> terminate_cluster
        terminate_cluster >> stop_mlflow

    return dag


dag: DAG = build_monthly_retrain_dag(MONTHLY_RETRAIN_CRON)

