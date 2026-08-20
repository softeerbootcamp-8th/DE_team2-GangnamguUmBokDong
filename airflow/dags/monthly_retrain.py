"""매월 챔피언 모델 점검, EMR 피처마트 생성, EC2 챌린저 학습 및 승격을 오케스트레이션하는 월별 DAG."""

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
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import (
    CATCHUP,
    MAX_ACTIVE_RUNS,
    MONTHLY_EVALUATION_TIMEOUT,
    MONTHLY_RETRAIN_CRON,
    MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    TIMEZONE,
)
from orchestration.aws_infra_task import (
    run_command_on_ec2,
    run_emr_feature_mart_job,
    start_ec2_instance,
    stop_ec2_instance,
)

from airflow import DAG

logger = logging.getLogger(__name__)


def task_start_ec2_eval(**context: Any) -> str:
    """챔피언 모델 평가를 위해 EC2 인스턴스를 기동한다."""
    logger.info("[월별 재학습] 1단계: 평가용 EC2 인스턴스 시작")
    return start_ec2_instance()


def task_run_eval_on_ec2(**context: Any) -> dict[str, Any]:
    """EC2에서 챔피언 모델 성능 점검을 실행하고 결과를 XCom에 저장한다."""
    logger.info("[월별 재학습] 2단계: EC2에서 챔피언 성능 점검 (--check-only)")
    cmd = (
        "uv run --frozen python -m training.scripts.monthly_retrain_check "
        "--check-only --json-output"
    )
    result = run_command_on_ec2(cmd, working_dir="/workspace/ml")
    stdout = result.get("StandardOutputContent", "{}")

    try:
        # stdout에서 JSON 블록 파싱
        summary = json.loads(stdout.strip())
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        logger.warning("[월별 재학습] JSON 파싱 실패 (%s), 기본값 생성: %s", exc, stdout)
        summary = {
            "needs_retrain": False,
            "retrain_models": [],
            "candidate_profiles": [],
        }

    ti = context["ti"]
    ti.xcom_push(key="needs_retrain", value=summary.get("needs_retrain", False))
    ti.xcom_push(key="retrain_models", value=summary.get("retrain_models", []))
    ti.xcom_push(
        key="candidate_profiles", value=summary.get("candidate_profiles", [])
    )
    return summary


def task_stop_ec2_eval(**context: Any) -> None:
    """평가 완료 후 EC2 인스턴스를 즉시 중지한다 (ALL_DONE으로 항상 보장)."""
    logger.info("[월별 재학습] 3단계: 평가용 EC2 인스턴스 중지")
    stop_ec2_instance()


def task_check_retrain_branch(**context: Any) -> str:
    """성능 점검 결과에 따라 재학습 진행 여부를 분기한다."""
    ti = context["ti"]
    needs_retrain = ti.xcom_pull(
        task_ids="run_eval_on_ec2", key="needs_retrain"
    )
    if needs_retrain:
        logger.info("[월별 재학습] 기준 미달 모델 발견 — 재학습 오케스트레이션으로 분기")
        return "orchestrate_retrain_loop"
    logger.info("[월별 재학습] 모든 모델 성능 정상 — 재학습 건너뜀")
    return "skip_monthly_retrain"


def task_orchestrate_retrain_loop(**context: Any) -> dict[str, Any]:
    """후보 프로필을 순회하며 EMR 피처 생성과 EC2 챌린저 학습/승격을 상호 배타적으로 실행한다."""
    ti = context["ti"]
    candidate_profiles = (
        ti.xcom_pull(task_ids="run_eval_on_ec2", key="candidate_profiles")
        or ["builtin-default"]
    )
    retrain_models = ti.xcom_pull(
        task_ids="run_eval_on_ec2", key="retrain_models"
    ) or ["rental", "return"]

    logger.info(
        "[월별 재학습] 4단계: 챌린저 재학습 시작 (대상 모델: %s, 후보 프로필: %s)",
        retrain_models,
        candidate_profiles,
    )
    models_arg = ",".join(retrain_models)

    results_by_profile: dict[str, Any] = {}

    for profile in candidate_profiles:
        logger.info(
            "=== [프로필: %s] EMR 피처마트 생성 시작 (EC2는 OFF 상태) ===",
            profile,
        )
        # 1. EMR 클러스터 기동 & 피처마트 생성 (완료 시 자동 Terminate)
        emr_job_id = run_emr_feature_mart_job(profile)
        logger.info(
            "=== [프로필: %s] EMR 피처마트 생성 완료 (%s) ===",
            profile,
            emr_job_id,
        )

        # 2. EMR 종료 확인 후 EC2 기동 & 챌린저 학습/평가
        logger.info(
            "=== [프로필: %s] EC2 기동 & 챌린저 학습 시작 (EMR은 OFF 상태) ===",
            profile,
        )
        try:
            start_ec2_instance()
            train_cmd = (
                "uv run --frozen python -m training.scripts.monthly_retrain_check "
                f"--execute --skip-feature-pipeline --profile-name {profile} --models {models_arg}"
            )
            train_result = run_command_on_ec2(
                train_cmd, working_dir="/workspace/ml"
            )
            results_by_profile[profile] = {
                "status": "success",
                "output": train_result.get("StandardOutputContent", "")[:500],
            }
        except (RuntimeError, OSError, ValueError) as exc:
            logger.error("[프로필: %s] EC2 학습 중 오류 발생: %s", profile, exc)
            results_by_profile[profile] = {"status": "failed", "error": str(exc)}
        finally:
            # 성공/실패 무관하게 EC2 즉시 중지
            logger.info("=== [프로필: %s] EC2 인스턴스 중지 ===", profile)
            stop_ec2_instance()

    return {"status": "completed", "profiles": results_by_profile}


def task_ensure_all_instances_stopped(**context: Any) -> None:
    """파이프라인 종료 시 모든 EC2 인스턴스가 중지되었는지 최종 보장한다."""
    logger.info("[월별 재학습] 5단계: 최종 자원 정리 확인 (EC2 중지 보장)")
    stop_ec2_instance()


with DAG(
    dag_id="monthly_retrain",
    schedule=CronTriggerTimetable(MONTHLY_RETRAIN_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["ml", "monthly", "retrain", "emr", "ec2"],
) as dag:
    start_ec2_eval = PythonOperator(
        task_id="start_ec2_eval",
        python_callable=task_start_ec2_eval,
        execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
    )

    run_eval_on_ec2 = PythonOperator(
        task_id="run_eval_on_ec2",
        python_callable=task_run_eval_on_ec2,
        execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
    )

    stop_ec2_eval = PythonOperator(
        task_id="stop_ec2_eval",
        python_callable=task_stop_ec2_eval,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
    )

    check_retrain_branch = BranchPythonOperator(
        task_id="check_retrain_branch",
        python_callable=task_check_retrain_branch,
    )

    skip_monthly_retrain = EmptyOperator(
        task_id="skip_monthly_retrain",
    )

    orchestrate_retrain_loop = PythonOperator(
        task_id="orchestrate_retrain_loop",
        python_callable=task_orchestrate_retrain_loop,
        execution_timeout=MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    )

    ensure_all_instances_stopped = PythonOperator(
        task_id="ensure_all_instances_stopped",
        python_callable=task_ensure_all_instances_stopped,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 태스크 흐름 정의 (비순환 단방향 그래프)
    start_ec2_eval >> run_eval_on_ec2 >> stop_ec2_eval
    run_eval_on_ec2 >> check_retrain_branch
    stop_ec2_eval >> check_retrain_branch
    check_retrain_branch >> [orchestrate_retrain_loop, skip_monthly_retrain]
    orchestrate_retrain_loop >> ensure_all_instances_stopped
    skip_monthly_retrain >> ensure_all_instances_stopped
