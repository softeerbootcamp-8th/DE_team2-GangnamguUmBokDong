"""월간 재학습용 EMR 클러스터가 실제로는 다 끝났는데도 살아있으면 강제로
종료하는 안전망 — 단, 재학습이 실제로 돌고 있는 클러스터는 절대 건드리지 않는다.

`monthly_retrain` DAG는 대여/반납 두 모델 체인 각각의 `terminate_cluster_{model}`이
setup/teardown API(`is_teardown=True`)로 표시돼 있어 운영자가 DAG Run을 수동으로
"Mark Failed" 처리해도 실행될 기회를 얻는다(Airflow 3.3.1 확인, `monthly_retrain.py`
참고). 하지만 그 보장도 결국 "그 DAG 실행의 스케줄러 처리 자체는 계속된다"는
전제 위에 서 있다 — Airflow 스케줄러 프로세스가 죽거나 재시작되는 등 더 근본적인
장애가 나면 어떤 DAG의 태스크도(teardown이든 아니든) 실행되지 않는다. 이 DAG는
그 실행 그래프와 완전히 독립적으로, 주기적으로 실제 AWS EMR 상태를 직접
조회해서 방치된 클러스터를 정리한다 — "무슨 일이 있어도 EMR은 삭제되어야
한다"는 요구사항의 두 번째(그리고 마지막) 방어선이다.

**"재학습 DAG가 실행 중인지"를 Airflow가 아니라 EMR 자신에게 묻는다**: 이
태스크 프로세스 안에서 `monthly_retrain` DAG의 실행 상태를 직접 조회하려면
원래 metadata DB를 봐야 하는데, Airflow 3.x Task SDK는
태스크를 격리된 프로세스로 돌리고 API 서버와 HTTP로만 통신하게 해서 그 경로를
지원하지 않는다(3.3.1 소스로 확인, 인증까지 갖춘 REST API 클라이언트를 새로
두는 방법도 있지만 이 배포에 아직 없는 인증 설정을 요구한다). 대신 "이 EMR
클러스터에 지금 실행 중이거나 대기 중인 스텝이 있는가"는 "그 DAG의 재학습
루프가 지금 이 클러스터를 쓰고 있는가"와 사실상 같은 질문이면서, Airflow 쪽
기록이 어떻게 꼬여있든 무관하게 항상 맞는 answer를 준다 — 그래서 이 reaper는
EMR 스텝 활동만 본다(`get_cluster_step_activity()`).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from config.schedules import CATCHUP, EMR_ORPHAN_REAPER_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from orchestration.aws_infra_task import (
    EMR_IDLE_GRACE_MINUTES,
    get_cluster_step_activity,
    list_active_emr_clusters,
    terminate_emr_cluster,
)

from airflow import DAG

logger = logging.getLogger(__name__)


def _reap_orphan_emr_clusters(**context: Any) -> dict[str, Any]:
    """활성 스텝이 있는 클러스터는 나이와 무관하게 **절대** 건드리지 않는다.

    시간 기반 절대 상한(예: "24시간 넘으면 무조건 종료")을 없앤 이유: 이
    프로젝트는 1년치 데이터를 48GB RAM 단일 머신으로 학습하는 데 실측 24시간이
    걸린 이력이 있다(2026-08) — 분산 학습으로 옮겨 빨라지길 기대하지만, 후보
    프로필을 여러 번 순차 재시도하거나 데이터가 더 늘면 정상적인 학습도 그보다
    오래 걸릴 수 있다. "활성 스텝이 있다"는 AWS 쪽 사실 그 자체보다 더 믿을 만한
    신호는 없으므로, 시간 추측으로 그걸 뒤집지 않는다 — 진짜로 멈춘 스텝(예:
    YARN 클라이언트가 영원히 응답 없음)은 이 reaper가 아니라 그 스텝을 제출하는
    쪽(`monthly_retrain_check._run_distributed_training_via_yarn()`)에
    타임아웃을 둬서 근본적으로 막아야 한다.
    """
    clusters = list_active_emr_clusters()
    now = datetime.now(UTC)
    reaped: list[str] = []
    still_active: list[str] = []  # 지금 활성 스텝이 있음 — 재학습이 실제로 돌고 있음
    still_within_grace: list[str] = []  # 유휴지만 아직 유예 시간(15분) 안 지남

    for cluster in clusters:
        activity = get_cluster_step_activity(cluster["id"])

        if activity["has_active_step"]:
            still_active.append(cluster["id"])
            continue

        idle_since = activity["last_step_completed_at"] or cluster["created_at"]
        idle_minutes = (now - idle_since).total_seconds() / 60
        if idle_minutes >= EMR_IDLE_GRACE_MINUTES:
            logger.warning(
                "[emr-reaper] 클러스터 '%s'(%s)가 활성 스텝 없이 %.1f분째 유휴 상태라 "
                "(기준 %.1f분) 강제 종료합니다",
                cluster["name"],
                cluster["id"],
                idle_minutes,
                EMR_IDLE_GRACE_MINUTES,
            )
            terminate_emr_cluster(cluster["id"])
            reaped.append(cluster["id"])
        else:
            still_within_grace.append(cluster["id"])

    logger.info(
        "[emr-reaper] 점검 완료 — 전체 %d개, 종료 %d개, 재학습 중 %d개, 유예 중 %d개",
        len(clusters),
        len(reaped),
        len(still_active),
        len(still_within_grace),
    )
    return {
        "total": len(clusters),
        "reaped": reaped,
        "still_active": still_active,
        "still_within_grace": still_within_grace,
    }


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
