"""Airflow Task 상태 변경 시 공통 로그를 남기는 callback을 정의한다."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def log_task_retry(context: dict[str, Any]) -> None:
    """Task 재시도 정보를 로그에 기록한다.

    Args:
        context: Airflow가 callback에 전달하는 실행 context.
    """
    task_instance = context["task_instance"]

    logger.warning(
        "Task retry: dag_id=%s task_id=%s run_id=%s try_number=%s",
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.run_id,
        task_instance.try_number,
    )


def log_task_failure(context: dict[str, Any]) -> None:
    """Task 최종 실패 정보를 로그에 기록한다.

    Args:
        context: Airflow가 callback에 전달하는 실행 context.
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")

    logger.error(
        "Task failed: dag_id=%s task_id=%s run_id=%s try_number=%s exception=%r",
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.run_id,
        task_instance.try_number,
        exception,
    )