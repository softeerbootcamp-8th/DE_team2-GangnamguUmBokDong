"""collector CLI를 실행하는 태스크 빌더.

호출 계약(docs/airflow/implementation-plan.md 참고): `--source`/`--window-start`만
전달하고, 페이지 번호·API URL·fetch round 등 collector 내부 세부사항은 Airflow가
알지 않는다.
"""

from __future__ import annotations

import json
import subprocess
from datetime import timedelta

from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.task.trigger_rule import TriggerRule

from callbacks.task_callbacks import on_failure_callback, on_success_callback
from config.schedules import (
    DEFAULT_EXECUTION_TIMEOUT,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    EXECUTION_TIMEOUT_OVERRIDES,
)
from orchestration.poi_master_task import poi_master_ref_env
from orchestration.task_builder import (
    REPO_ROOT,
    build_module_task,
    module_subprocess_env,
)
from orchestration.templates import (
    KST_WINDOW_START,
    kst_day_hour_replay_days_ago,
    kst_window_start_shifted,
)

COLLECTOR_DIR = str(REPO_ROOT / "collector")
_FRESHNESS_CHECK_TIMEOUT_SECONDS = 30


def _check_source_due(source_id: str, min_interval_seconds: int) -> bool:
    """collector CLI에 수집 주기 또는 source 설정 변경으로 재수집이 필요한지 묻는다.

    실제 실행 시각(`datetime.now()`) 기준으로 판단한다 — DAG의 논리 시각
    (`logical_date`)은 이전 run이 늦게 끝나면 실제 시각보다 뒤처질 수 있어서,
    "지금 진짜로 얼마나 지났는지"를 묻는 이 판단에는 맞지 않는다. 최신 authority가
    현재 배포 YAML과 다른 config version이면 시간 간격이 짧아도 즉시 due가 된다.
    """
    env = module_subprocess_env(COLLECTOR_DIR)
    result = subprocess.run(
        [
            "uv", "run", "--frozen", "python", "main.py",
            "--source", source_id,
            "--check-due-after-seconds", str(min_interval_seconds),
        ],
        cwd=COLLECTOR_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=_FRESHNESS_CHECK_TIMEOUT_SECONDS,
        check=True,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    return bool(payload["due"])


def build_weather_freshness_gate_task(dag, source_id: str, *, min_interval: timedelta):
    """현재 설정의 성공 수집이 `min_interval` 안에 있으면 수집을 스킵하는 게이트.

    `ShortCircuitOperator`가 `False`를 반환하면 직접 하위 태스크(이 소스의
    `collect_{source_id}`)만 스킵된다 — `ignore_downstream_trigger_rules=False`를
    명시해야 한다. 기본값(`True`)이면 이 태스크 뒤에 연결된 모든 태스크(`weather_
    ready_gate`는 물론 `prepare_serving_plan` 이후 전체 체인)가 트리거룰과 무관하게
    강제로 스킵되어, 날씨 하나만 아직 안 지났어도 그 tick의 서빙 전체가 멈춘다.
    `False`로 두면 직접 하위(collect 태스크)만 스킵되고, `weather_ready_gate`
    (`ALL_DONE`)부터는 실제 상태를 보고 정상 평가된다.
    """
    return ShortCircuitOperator(
        task_id=f"freshness_gate_{source_id}",
        python_callable=lambda: _check_source_due(source_id, int(min_interval.total_seconds())),
        ignore_downstream_trigger_rules=False,
        retries=0,
        execution_timeout=timedelta(seconds=_FRESHNESS_CHECK_TIMEOUT_SECONDS),
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        dag=dag,
    )


def build_collector_task(
    dag,
    source_id: str,
    *,
    retries: int = DEFAULT_RETRIES,
    retry_delay: timedelta = DEFAULT_RETRY_DELAY,
    execution_timeout: timedelta | None = None,
):
    """소스별 retry와 timeout을 적용해 Collector 태스크를 만든다."""
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
        retry_delay=retry_delay,
    )


def build_population_collector_task(
    dag,
    *,
    poi_master_task_id: str,
    retries: int = DEFAULT_RETRIES,
):
    """Resolver가 고정한 POI Master로 실시간 인구를 수집하는 태스크를 만든다."""
    source_id = "population_realtime"
    timeout = EXECUTION_TIMEOUT_OVERRIDES.get(source_id, DEFAULT_EXECUTION_TIMEOUT)
    cmd = (
        f"uv run --frozen python main.py --source {source_id} "
        f"--window-start {KST_WINDOW_START} "
        '--poi-master-mode "$POI_MASTER_MODE" '
        '--poi-master-manifest-uri "$POI_MASTER_MANIFEST_URI" '
        '--poi-master-manifest-sha256 "$POI_MASTER_MANIFEST_SHA256"'
    )
    return build_module_task(
        dag,
        f"collect_{source_id}",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=timeout,
        retries=retries,
        env=poi_master_ref_env(poi_master_task_id),
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
