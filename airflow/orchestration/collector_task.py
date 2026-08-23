"""collector CLI를 실행하는 태스크 빌더.

호출 계약(docs/airflow/implementation-plan.md 참고): `--source`/`--window-start`만
전달하고, 페이지 번호·API URL·fetch round 등 collector 내부 세부사항은 Airflow가
알지 않는다.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.task.trigger_rule import TriggerRule
from config.schedules import (
    DEFAULT_EXECUTION_TIMEOUT,
    DEFAULT_RETRIES,
    EXECUTION_TIMEOUT_OVERRIDES,
)

from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import (
    KST_WINDOW_START,
    kst_day_hour_replay_days_ago,
    kst_window_start_shifted,
)

COLLECTOR_DIR = str(REPO_ROOT / "collector")


def build_collector_task(
    dag,
    source_id: str,
    *,
    retries: int = DEFAULT_RETRIES,
    execution_timeout: timedelta | None = None,
):
    timeout = execution_timeout or EXECUTION_TIMEOUT_OVERRIDES.get(
        source_id, DEFAULT_EXECUTION_TIMEOUT
    )
    cmd = f"uv run --frozen python main.py --source {source_id} --window-start {KST_WINDOW_START}"
    return build_module_task(
        dag,
        f"collect_{source_id}",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=timeout,
        retries=retries,
    )


def build_collector_replay_task(dag, source_id: str, hours_back: int):
    """`hours_back`시간 전 윈도우를 `--force`로 다시 수집하는 태스크.

    반납이 완료돼야 목록에 나타나는 API(tbCycleRentData)에서, 대여 시간대가 끝난 뒤
    반납된 기록을 회수하는 용도다. 자세한 배경과 실측 수치는
    `config.sources.RENTAL_HISTORY_LOOKBACK_HOURS` 주석에 있다.

    `--backfill`이 아니라 `--force`인 이유: backfill은 실패한 조각만 채우는데
    (`collector/pipeline.py`의 분기 4가 `skip=have_parts`로 부른다) 여기서 놓치는 것은
    실패가 아니라 "그때는 아직 존재하지 않았던 데이터"다. 조각이 전부 성공했으므로
    재시도 마커도 남지 않고, 그냥 재실행하면 완결된 윈도우라 SKIPPED가 된다.

    `trigger_rule=ALL_DONE`인 이유: 과거를 보강하는 일이라 현재 tick의 수집이
    실패했어도 시도할 가치가 있고, 반대로 이 태스크가 실패해도 현재 tick을 막아선
    안 된다(그래서 DAG에서 run_inference의 상위로 두지 않는다).
    """
    timeout = EXECUTION_TIMEOUT_OVERRIDES.get(source_id, DEFAULT_EXECUTION_TIMEOUT)
    window_start = kst_window_start_shifted(hours_back)
    cmd = (
        f"uv run --frozen python main.py --source {source_id} "
        f"--window-start {window_start} --force"
    )
    return build_module_task(
        dag,
        f"collect_{source_id}_replay_{hours_back}h",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=timeout,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def build_daily_history_replay_task(dag, hour: int, days_back: int):
    """과거 날짜의 대여이력 한 시간대를 실패 전파 없이 전체 재수집한다.

    시간별 태스크는 API 동시 요청을 제한하려고 순차 연결하지만, 한 시간대의 최종
    실패가 나머지 23개 시간대까지 막아서는 안 되므로 ``ALL_DONE``으로 실행한다.
    """
    source_id = "bike_rental_history"
    window_start = kst_day_hour_replay_days_ago(days_back, hour)
    cmd = (
        f"uv run --frozen python main.py --source {source_id} "
        f"--window-start {window_start} --force"
    )
    return build_module_task(
        dag,
        f"replay_{source_id}_{hour:02d}h",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=EXECUTION_TIMEOUT_OVERRIDES.get(
            source_id, DEFAULT_EXECUTION_TIMEOUT
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )
