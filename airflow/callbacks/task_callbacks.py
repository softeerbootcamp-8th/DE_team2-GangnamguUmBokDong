"""Task 성공/실패 시 공통으로 필요한 처리의 확장 지점.

Collector의 실패 페이지 목록이나 API 오류 세부 판단은 여기서 다시 구현하지 않는다
— 그건 각 모듈의 책임이다. 지금은 구조화 로그만 남기고, 알림/메트릭은 필요해지면
이 함수들을 확장한다.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("airflow.task")


def _context_fields(context: dict) -> dict:
    task_instance = context["task_instance"]
    dag_run = context["dag_run"]
    return {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "logical_date": str(dag_run.logical_date),
        "run_id": dag_run.run_id,
        "try_number": task_instance.try_number,
    }


def on_success_callback(context: dict) -> None:
    logger.info("task succeeded", extra=_context_fields(context))


def on_failure_callback(context: dict) -> None:
    fields = _context_fields(context)
    fields["exception"] = str(context.get("exception"))
    logger.error("task failed", extra=fields)
