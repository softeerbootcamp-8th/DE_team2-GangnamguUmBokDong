"""대여(rental) 및 반납(return) 모델별 월별 점검, EMR 피처마트 생성, EC2 챌린저 학습/승격을 오케스트레이션하는 DAG 팩토리."""

from __future__ import annotations

import json
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
    EMR_CORE_INSTANCE_COUNT,
    EMR_CORE_INSTANCE_TYPE,
    MOCK_OVERRIDE_FORCE_MOCK,
    MOCK_OVERRIDE_FORCE_REAL,
    run_command_on_ec2,
    run_emr_feature_mart_job,
    start_ec2_instance,
    stop_ec2_instance,
)

from airflow import DAG

logger = logging.getLogger(__name__)

# DAG params의 "mock_mode" 값 → aws_infra_task의 override 인자로 변환하는 매핑.
# "auto"는 override 없이 환경변수 기반 자동 판별(is_mock_mode/is_emr_mock_mode)을 그대로 따른다.
_MOCK_MODE_PARAM_TO_OVERRIDE = {
    "force_mock": MOCK_OVERRIDE_FORCE_MOCK,
    "force_real": MOCK_OVERRIDE_FORCE_REAL,
}


def _mock_override_from_params(params: dict[str, Any]) -> str | None:
    """DAG trigger 시점 params의 mock_mode 선택값을 override 인자로 변환한다."""
    return _MOCK_MODE_PARAM_TO_OVERRIDE.get(params.get("mock_mode", "auto"))


def make_task_start_ec2_eval(model_name: str) -> Any:
    """평가용 EC2 인스턴스를 시작하는 callable을 반환한다."""

    def task_start_ec2_eval(**context: Any) -> str:
        logger.info("[%s 월별 재학습] 1단계: 평가용 EC2 인스턴스 시작", model_name)
        mock_override = _mock_override_from_params(context.get("params", {}))
        return start_ec2_instance(mock_override=mock_override)

    return task_start_ec2_eval


def make_task_run_eval_on_ec2(model_name: str) -> Any:
    """EC2에서 대상 모델 챔피언 성능 점검을 실행하는 callable을 반환한다."""

    def task_run_eval_on_ec2(**context: Any) -> dict[str, Any]:
        logger.info("[%s 월별 재학습] 2단계: EC2에서 %s 챔피언 성능 점검 (--check-only)", model_name, model_name)
        mock_override = _mock_override_from_params(context.get("params", {}))
        cmd = (
            "uv run --frozen python -m training.scripts.monthly_retrain_check "
            f"--check-only --json-output --models {model_name}"
        )
        result = run_command_on_ec2(cmd, working_dir="/workspace/ml", mock_override=mock_override)
        stdout = result.get("StandardOutputContent", "{}")

        try:
            summary = json.loads(stdout.strip())
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.warning("[%s 월별 재학습] JSON 파싱 실패 (%s), 기본값 생성: %s", model_name, exc, stdout)
            summary = {
                "needs_retrain": False,
                "retrain_models": [model_name],
                "candidate_profiles": [],
            }

        ti = context["ti"]
        ti.xcom_push(key="needs_retrain", value=summary.get("needs_retrain", False))
        ti.xcom_push(key="retrain_models", value=summary.get("retrain_models", [model_name]))
        ti.xcom_push(
            key="candidate_profiles", value=summary.get("candidate_profiles", [])
        )
        return summary

    return task_run_eval_on_ec2


def make_task_stop_ec2_eval(model_name: str) -> Any:
    """평가 완료 후 EC2 인스턴스를 중지하는 callable을 반환한다."""

    def task_stop_ec2_eval(**context: Any) -> None:
        logger.info("[%s 월별 재학습] 3단계: 평가용 EC2 인스턴스 중지", model_name)
        stop_ec2_instance(mock_override=_mock_override_from_params(context.get("params", {})))

    return task_stop_ec2_eval


def make_task_check_retrain_branch(model_name: str) -> Any:
    """재학습 진행 여부를 분기하는 callable을 반환한다."""

    def task_check_retrain_branch(**context: Any) -> str:
        ti = context["ti"]
        needs_retrain = ti.xcom_pull(
            task_ids="run_eval_on_ec2", key="needs_retrain"
        )
        if needs_retrain:
            logger.info("[%s 월별 재학습] 기준 미달 발견 — 재학습 오케스트레이션으로 분기", model_name)
            return "orchestrate_retrain_loop"
        logger.info("[%s 월별 재학습] 성능 정상 — 재학습 건너뜀", model_name)
        return "skip_monthly_retrain"

    return task_check_retrain_branch


def make_task_orchestrate_retrain_loop(model_name: str) -> Any:
    """후보 프로필 순환 재학습 callable을 반환한다."""

    def task_orchestrate_retrain_loop(**context: Any) -> dict[str, Any]:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        emr_core_instance_type = params.get("emr_core_instance_type") or None
        emr_core_instance_count = params.get("emr_core_instance_count") or None
        candidate_profiles = (
            ti.xcom_pull(task_ids="run_eval_on_ec2", key="candidate_profiles")
            or ["builtin-default"]
        )

        logger.info(
            "[%s 월별 재학습] 4단계: 챌린저 재학습 시작 (대상 모델: %s, 후보 프로필: %s)",
            model_name,
            model_name,
            candidate_profiles,
        )

        results_by_profile: dict[str, Any] = {}

        for profile in candidate_profiles:
            logger.info(
                "=== [%s 프로필: %s] EMR 피처마트 생성 시작 (EC2는 OFF 상태) ===",
                model_name,
                profile,
            )
            try:
                # 1. EMR 클러스터 기동 & 피처마트 생성 (완료 시 자동 Terminate)
                emr_job_id = run_emr_feature_mart_job(
                    profile,
                    mock_override=mock_override,
                    core_instance_type=emr_core_instance_type,
                    core_instance_count=emr_core_instance_count,
                )
                logger.info(
                    "=== [%s 프로필: %s] EMR 피처마트 생성 완료 (%s) ===",
                    model_name,
                    profile,
                    emr_job_id,
                )

                # 2. EMR 종료 확인 후 EC2 기동 & 챌린저 학습/평가
                logger.info(
                    "=== [%s 프로필: %s] EC2 기동 & 챌린저 학습 시작 (EMR은 OFF 상태) ===",
                    model_name,
                    profile,
                )
                try:
                    start_ec2_instance(mock_override=mock_override)
                    train_cmd = (
                        "uv run --frozen python -m training.scripts.monthly_retrain_check "
                        f"--execute --skip-feature-pipeline --profile-name {profile} --models {model_name}"
                    )
                    train_result = run_command_on_ec2(
                        train_cmd, working_dir="/workspace/ml", mock_override=mock_override
                    )
                    results_by_profile[profile] = {
                        "status": "success",
                        "output": train_result.get("StandardOutputContent", "")[:500],
                    }
                finally:
                    # 성공/실패 무관하게 EC2 즉시 중지
                    logger.info("=== [%s 프로필: %s] EC2 인스턴스 중지 ===", model_name, profile)
                    stop_ec2_instance(mock_override=mock_override)
            except (RuntimeError, OSError, ValueError, TimeoutError) as exc:
                logger.error("[%s 프로필: %s] 재학습 루프 중 오류 발생: %s", model_name, profile, exc)
                results_by_profile[profile] = {"status": "failed", "error": str(exc)}

        return {"status": "completed", "profiles": results_by_profile}

    return task_orchestrate_retrain_loop


def make_task_ensure_all_instances_stopped(model_name: str) -> Any:
    """최종 자원 정리 callable을 반환한다."""

    def task_ensure_all_instances_stopped(**context: Any) -> None:
        logger.info("[%s 월별 재학습] 5단계: 최종 자원 정리 확인 (EC2 중지 보장)", model_name)
        stop_ec2_instance(mock_override=_mock_override_from_params(context.get("params", {})))

    return task_ensure_all_instances_stopped


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
        tags=["ml", "monthly", "retrain", "emr", "ec2", model_name],
        params={
            "mock_mode": Param(
                "auto",
                type="string",
                enum=["auto", "force_mock", "force_real"],
                description=(
                    "auto: 환경변수(AWS_ACCESS_KEY_ID 등)로 자동 판별. "
                    "force_mock: EC2/EMR 호출 없이 무조건 mock. "
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
                EMR_CORE_INSTANCE_COUNT,
                type="integer",
                minimum=1,
                maximum=10,
                description="EMR core 노드 개수 (master 1대는 별도, 항상 고정).",
            ),
        },
    ) as dag:
        start_ec2_eval = PythonOperator(
            task_id="start_ec2_eval",
            python_callable=make_task_start_ec2_eval(model_name),
            execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
        )

        run_eval_on_ec2 = PythonOperator(
            task_id="run_eval_on_ec2",
            python_callable=make_task_run_eval_on_ec2(model_name),
            execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
        )

        stop_ec2_eval = PythonOperator(
            task_id="stop_ec2_eval",
            python_callable=make_task_stop_ec2_eval(model_name),
            trigger_rule=TriggerRule.ALL_DONE,
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

        ensure_all_instances_stopped = PythonOperator(
            task_id="ensure_all_instances_stopped",
            python_callable=make_task_ensure_all_instances_stopped(model_name),
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # 태스크 흐름 정의 (비순환 단방향 그래프)
        start_ec2_eval >> run_eval_on_ec2 >> stop_ec2_eval
        run_eval_on_ec2 >> check_retrain_branch
        stop_ec2_eval >> check_retrain_branch
        check_retrain_branch >> [orchestrate_retrain_loop, skip_monthly_retrain]
        orchestrate_retrain_loop >> ensure_all_instances_stopped
        skip_monthly_retrain >> ensure_all_instances_stopped

    return dag
