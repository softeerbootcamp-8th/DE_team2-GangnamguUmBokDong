"""대여(rental) 및 반납(return) 모델별 월별 점검·EMR 피처마트 생성·YARN 분산
학습·승격을 단일 EMR 클러스터 생애주기 안에서 오케스트레이션하는 DAG 팩토리.

**2026-08 재설계(ADR-0007)**: 예전에는 평가/학습을 EC2(SSM)로, 피처마트만
EMR(매번 새로 만들고 자동 종료)로 실행했다. 이 계정은 SSM(SendCommand 등)이
SCP로 전면 차단돼 있어 그 경로가 실제로는 동작하지 않았을 가능성이 높고, 학습용
EC2 자체도 더 이상 쓸 수 없게 됐다. 지금은 월 1회 EMR 클러스터 하나를 띄워
(피처마트 3노드로 시작) 평가 → (필요 시) 후보 프로필 재학습 루프 → 종료까지
전부 EMR 스텝(`command-runner.jar`, 이미 실전에서 동작 중이던 유일한 원격 실행
경로)으로 실행한다. 재학습이 실제로 필요해지면 그 시점에 한 번만 8노드로
리사이즈하고, LightGBM 학습은 YARN Distributed Shell로 8개 컨테이너에 나눠
띄운다(`training/scripts/yarn_worker_bootstrap.py`).
"""

from __future__ import annotations

import logging
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
    MONTHLY_EVALUATION_TIMEOUT,
    MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    TIMEZONE,
)
from orchestration.aws_infra_task import (
    EMR_CORE_INSTANCE_TYPE,
    EMR_S3_SCRIPTS_PREFIX,
    MOCK_OVERRIDE_FORCE_MOCK,
    MOCK_OVERRIDE_FORCE_REAL,
    TRAINING_RUNS_PREFIX,
    create_emr_cluster,
    get_core_instance_group_id,
    read_s3_json,
    resize_emr_cluster,
    submit_emr_step,
    terminate_emr_cluster,
)

from airflow import DAG

logger = logging.getLogger(__name__)

# 피처마트 단계는 3노드로 충분하고(기존 EMR_CORE_INSTANCE_COUNT 기본값과 무관하게
# 이 DAG 전용 기본값을 둔다), 재학습이 실제로 필요해질 때만 학습용 8노드로 키운다
# (m4.large만 허용되는 계정 제약 — docs/adr/0007-yarn-distributed-shell-workers.md).
FEATURE_MART_CORE_INSTANCE_COUNT = 3
TRAINING_CORE_INSTANCE_COUNT = 8
_EMR_PYTHONPATH = "/opt/gng"
_EMR_PYTHON = "python3.11"

# DAG params의 "mock_mode" 값 → aws_infra_task의 override 인자로 변환하는 매핑.
# "auto"는 override 없이 환경변수 기반 자동 판별(is_mock_mode/is_emr_mock_mode)을 그대로 따른다.
_MOCK_MODE_PARAM_TO_OVERRIDE = {
    "force_mock": MOCK_OVERRIDE_FORCE_MOCK,
    "force_real": MOCK_OVERRIDE_FORCE_REAL,
}


def _mock_override_from_params(params: dict[str, Any]) -> str | None:
    """DAG trigger 시점 params의 mock_mode 선택값을 override 인자로 변환한다."""
    return _MOCK_MODE_PARAM_TO_OVERRIDE.get(params.get("mock_mode", "auto"))


def _result_s3_key(run_id: str, name: str) -> str:
    """EMR 스텝이 stdout 대신 결과를 남기는 위치 — SSM과 달리 EMR 스텝은 호출부가
    stdout을 직접 돌려받지 못하므로, `monthly_retrain_check.py --result-s3-key`가
    이 키에 요약 JSON을 쓰고 Airflow가 스텝 완료 후 다시 읽는다."""
    return f"{TRAINING_RUNS_PREFIX}/{run_id}/{name}.json"


def _bash_step(name: str, command: str) -> tuple[str, list[str]]:
    return name, ["bash", "-c", command]


def make_task_create_cluster_and_evaluate(model_name: str) -> Any:
    """월간 사이클용 상시 EMR 클러스터를 생성하고 그 위에서 챔피언 성능 점검
    스텝을 실행하는 callable을 반환한다."""

    def task_create_cluster_and_evaluate(**context: Any) -> dict[str, Any]:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        run_id = context["run_id"]

        logger.info("[%s 월별 재학습] 1단계: 상시 EMR 클러스터 생성(피처마트용 %d노드)", model_name, FEATURE_MART_CORE_INSTANCE_COUNT)
        cluster_id = create_emr_cluster(
            cluster_name=f"ml-monthly-retrain-{model_name}",
            core_instance_count=params.get("emr_core_instance_count") or FEATURE_MART_CORE_INSTANCE_COUNT,
            core_instance_type=params.get("emr_core_instance_type") or None,
            mock_override=mock_override,
        )
        ti.xcom_push(key="cluster_id", value=cluster_id)

        logger.info("[%s 월별 재학습] 2단계: EMR 스텝으로 %s 챔피언 성능 점검 (--check-only)", model_name, model_name)
        result_key = _result_s3_key(run_id, f"eval-{model_name}")
        name, command = _bash_step(
            f"Evaluate-{model_name}",
            f"cd {_EMR_PYTHONPATH} && PYTHONPATH={_EMR_PYTHONPATH} {_EMR_PYTHON} -m "
            f"training.scripts.monthly_retrain_check --check-only --models {model_name} "
            f"--result-s3-key {result_key}",
        )
        submit_emr_step(cluster_id, name, command, mock_override=mock_override)

        # mock 모드에서는 실제 스텝이 결과를 쓰지 않으므로, 기존 SSM mock과 같은 취지로
        # "재학습 필요"를 기본값으로 삼아 dry-run에서도 재학습 루프 구조를 계속 검증할 수
        # 있게 한다(운영 real 경로에서는 항상 스텝이 쓴 실제 값을 읽는다).
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

    return task_create_cluster_and_evaluate


def make_task_check_retrain_branch(model_name: str) -> Any:
    """재학습 진행 여부를 분기하는 callable을 반환한다."""

    def task_check_retrain_branch(**context: Any) -> str:
        ti = context["ti"]
        needs_retrain = ti.xcom_pull(
            task_ids="create_cluster_and_evaluate", key="needs_retrain"
        )
        if needs_retrain:
            logger.info("[%s 월별 재학습] 기준 미달 발견 — 재학습 오케스트레이션으로 분기", model_name)
            return "orchestrate_retrain_loop"
        logger.info("[%s 월별 재학습] 성능 정상 — 재학습 건너뜀", model_name)
        return "skip_monthly_retrain"

    return task_check_retrain_branch


def make_task_orchestrate_retrain_loop(model_name: str) -> Any:
    """상시 EMR 클러스터 위에서 후보 프로필을 순환하며 피처마트 → (최초 1회) 8노드
    리사이즈 → YARN distributed-shell 학습을 반복하는 callable을 반환한다."""

    def task_orchestrate_retrain_loop(**context: Any) -> dict[str, Any]:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        run_id = context["run_id"]
        cluster_id = ti.xcom_pull(task_ids="create_cluster_and_evaluate", key="cluster_id")
        candidate_profiles = (
            ti.xcom_pull(task_ids="create_cluster_and_evaluate", key="candidate_profiles")
            or ["builtin-default"]
        )

        logger.info(
            "[%s 월별 재학습] 3단계: 챌린저 재학습 시작 (대상 모델: %s, 후보 프로필: %s)",
            model_name,
            model_name,
            candidate_profiles,
        )

        results_by_profile: dict[str, Any] = {}
        resized_to_training = False

        for profile in candidate_profiles:
            logger.info("=== [%s 프로필: %s] EMR 피처마트 스텝 제출 ===", model_name, profile)
            try:
                for step_name, spark_args in (
                    (
                        f"Spark-RunPipeline-{profile}",
                        [
                            "spark-submit",
                            "--deploy-mode",
                            "cluster",
                            "--master",
                            "yarn",
                            "--conf",
                            f"spark.yarn.appMasterEnv.ML_PROFILE={profile}",
                            "--conf",
                            f"spark.executorEnv.ML_PROFILE={profile}",
                            f"{EMR_S3_SCRIPTS_PREFIX}/run_pipeline.py",
                        ],
                    ),
                    (
                        f"Spark-BuildMultiHorizon-{profile}",
                        [
                            "spark-submit",
                            "--deploy-mode",
                            "cluster",
                            "--master",
                            "yarn",
                            "--conf",
                            f"spark.yarn.appMasterEnv.ML_PROFILE={profile}",
                            "--conf",
                            f"spark.executorEnv.ML_PROFILE={profile}",
                            f"{EMR_S3_SCRIPTS_PREFIX}/build_multi_horizon_features.py",
                        ],
                    ),
                ):
                    submit_emr_step(cluster_id, step_name, spark_args, mock_override=mock_override)
                logger.info("=== [%s 프로필: %s] EMR 피처마트 완료 ===", model_name, profile)

                if not resized_to_training:
                    logger.info(
                        "=== [%s 프로필: %s] 학습용 %d노드로 리사이즈(사이클 중 최초 1회만) ===",
                        model_name,
                        profile,
                        TRAINING_CORE_INSTANCE_COUNT,
                    )
                    core_group_id = get_core_instance_group_id(cluster_id, mock_override=mock_override)
                    resize_emr_cluster(
                        cluster_id,
                        core_group_id,
                        target_core_count=TRAINING_CORE_INSTANCE_COUNT,
                        mock_override=mock_override,
                    )
                    wait_name, wait_command = _bash_step(
                        "Wait-YARN-Nodes",
                        f"until [ $(yarn node -list -all 2>/dev/null | grep -c RUNNING) "
                        f"-ge {TRAINING_CORE_INSTANCE_COUNT} ]; do sleep 15; done",
                    )
                    submit_emr_step(cluster_id, wait_name, wait_command, mock_override=mock_override)
                    resized_to_training = True

                logger.info("=== [%s 프로필: %s] YARN distributed-shell 학습 스텝 제출 ===", model_name, profile)
                train_result_key = _result_s3_key(run_id, f"train-{model_name}-{profile}")
                train_name, train_command = _bash_step(
                    f"Train-{model_name}-{profile}",
                    f"cd {_EMR_PYTHONPATH} && LGB_NUM_MACHINES={TRAINING_CORE_INSTANCE_COUNT} "
                    f"LGB_TREE_LEARNER=data PYTHONPATH={_EMR_PYTHONPATH} {_EMR_PYTHON} -m "
                    f"training.scripts.monthly_retrain_check --execute --skip-feature-pipeline "
                    f"--profile-name {profile} --models {model_name} --result-s3-key {train_result_key}",
                )
                submit_emr_step(
                    cluster_id, train_name, train_command, timeout_seconds=10800, mock_override=mock_override
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


def make_task_terminate_emr_cluster(model_name: str) -> Any:
    """사이클이 어떻게 끝났든(성공/실패/스킵) 상시 EMR 클러스터를 반드시 종료하는
    callable을 반환한다 — 이 안전망이 없으면 태스크가 kill돼도 클러스터가 계속
    과금된다(2026-08 재설계 전 실제로 없던 안전망)."""

    def task_terminate_emr_cluster(**context: Any) -> None:
        ti = context["ti"]
        mock_override = _mock_override_from_params(context.get("params", {}))
        cluster_id = ti.xcom_pull(task_ids="create_cluster_and_evaluate", key="cluster_id")
        if not cluster_id:
            logger.warning("[%s 월별 재학습] 4단계: cluster_id 없음(생성 자체가 실패) — 종료할 대상 없음", model_name)
            return
        logger.info("[%s 월별 재학습] 4단계: EMR 클러스터 '%s' 종료", model_name, cluster_id)
        terminate_emr_cluster(cluster_id, mock_override=mock_override)

    return task_terminate_emr_cluster


def build_monthly_retrain_dag(model_name: str, cron_schedule: str) -> DAG:
    """대여/반납 모델별 월별 재학습 DAG를 생성한다.

    args:
        model_name: "rental" 또는 "return"
        cron_schedule: cron 표현식 문자열
    returns:
        DAG: 구성된 Airflow DAG 객체
    """
    dag_id = f"monthly_retrain_{model_name}"

    with DAG(
        dag_id=dag_id,
        schedule=CronTriggerTimetable(cron_schedule, timezone=TIMEZONE),
        start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
        catchup=CATCHUP,
        max_active_runs=MAX_ACTIVE_RUNS,
        tags=["ml", "monthly", "retrain", "emr", "yarn", model_name],
        params={
            "mock_mode": Param(
                "auto",
                type="string",
                enum=["auto", "force_mock", "force_real"],
                description=(
                    "auto: 환경변수(AWS_ACCESS_KEY_ID 등)로 자동 판별. "
                    "force_mock: EMR 호출 없이 무조건 mock. "
                    "force_real: mock 판별을 무시하고 무조건 실제 AWS 호출."
                ),
            ),
            "emr_core_instance_type": Param(
                EMR_CORE_INSTANCE_TYPE,
                type="string",
                description=(
                    "EMR core 노드 인스턴스 타입. 이 AWS 계정은 EMR에 m4.large 외 타입을 "
                    "허용하지 않으니 변경 전 계정 제약을 먼저 확인할 것."
                ),
            ),
            "emr_core_instance_count": Param(
                FEATURE_MART_CORE_INSTANCE_COUNT,
                type="integer",
                minimum=1,
                maximum=10,
                description=(
                    "클러스터 생성 시점(피처마트 단계) core 노드 개수 — master 1대는 별도. "
                    f"재학습이 실제로 필요해지면 학습 단계에서 {TRAINING_CORE_INSTANCE_COUNT}로 자동 리사이즈된다."
                ),
            ),
        },
    ) as dag:
        create_cluster_and_evaluate = PythonOperator(
            task_id="create_cluster_and_evaluate",
            python_callable=make_task_create_cluster_and_evaluate(model_name),
            execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
        )

        check_retrain_branch = BranchPythonOperator(
            task_id="check_retrain_branch",
            python_callable=make_task_check_retrain_branch(model_name),
        )

        skip_monthly_retrain = EmptyOperator(
            task_id="skip_monthly_retrain",
        )

        orchestrate_retrain_loop = PythonOperator(
            task_id="orchestrate_retrain_loop",
            python_callable=make_task_orchestrate_retrain_loop(model_name),
            execution_timeout=MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
        )

        terminate_cluster = PythonOperator(
            task_id="terminate_cluster",
            python_callable=make_task_terminate_emr_cluster(model_name),
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # 태스크 흐름 정의 (비순환 단방향 그래프)
        create_cluster_and_evaluate >> check_retrain_branch
        check_retrain_branch >> [orchestrate_retrain_loop, skip_monthly_retrain]
        orchestrate_retrain_loop >> terminate_cluster
        skip_monthly_retrain >> terminate_cluster

    return dag
