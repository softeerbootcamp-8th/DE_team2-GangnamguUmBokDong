"""월간 재학습용 EMR 클러스터가 예상보다 오래 살아있으면 강제로 종료하는 안전망.

`monthly_retrain_*` DAG의 `terminate_cluster`는 setup/teardown API(`is_teardown=True`)로
표시돼 있어 운영자가 DAG Run을 수동으로 "Mark Failed" 처리해도 실행될 기회를
얻는다(Airflow 3.3.1 확인, `monthly_retrain.py` 참고). 하지만 그 보장도 결국
"그 DAG 실행의 스케줄러 처리 자체는 계속된다"는 전제 위에 서 있다 — Airflow
스케줄러 프로세스가 죽거나 재시작되는 등 더 근본적인 장애가 나면 어떤 DAG의
태스크도(teardown이든 아니든) 실행되지 않는다. 이 DAG는 그 실행 그래프와
완전히 독립적으로, 주기적으로 실제 AWS EMR 상태를 직접 조회해서 방치된
클러스터를 정리한다 — "무슨 일이 있어도 EMR은 삭제되어야 한다"는 요구사항의
두 번째(그리고 마지막) 방어선이다.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from config.schedules import CATCHUP, EMR_ORPHAN_REAPER_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from orchestration.aws_infra_task import (
    EMR_ORPHAN_MAX_AGE_HOURS,
    list_active_emr_clusters,
    terminate_emr_cluster,
)

from airflow import DAG

logger = logging.getLogger(__name__)


def _reap_orphan_emr_clusters(**context: Any) -> dict[str, Any]:
    clusters = list_active_emr_clusters()
    now = datetime.now(UTC)
    reaped: list[str] = []
    still_young: list[str] = []

    for cluster in clusters:
        age = now - cluster["created_at"]
        if age > timedelta(hours=EMR_ORPHAN_MAX_AGE_HOURS):
            logger.warning(
                "[emr-reaper] 클러스터 '%s'(%s)가 %.1f시간째 살아있어(기준 %.1f시간) 강제 종료합니다",
                cluster["name"],
                cluster["id"],
                age.total_seconds() / 3600,
                EMR_ORPHAN_MAX_AGE_HOURS,
            )
            terminate_emr_cluster(cluster["id"])
            reaped.append(cluster["id"])
        else:
            still_young.append(cluster["id"])

    logger.info(
        "[emr-reaper] 점검 완료 — 전체 %d개, 종료 %d개, 정상 범위 %d개",
        len(clusters),
        len(reaped),
        len(still_young),
    )
    return {"total": len(clusters), "reaped": reaped, "still_young": still_young}


with DAG(
    dag_id="emr_orphan_reaper",
    schedule=EMR_ORPHAN_REAPER_CRON,
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["ml", "emr", "maintenance"],
) as dag:
    reap_orphan_emr_clusters = PythonOperator(
        task_id="reap_orphan_emr_clusters",
        python_callable=_reap_orphan_emr_clusters,
    )
