"""5분 핵심 파이프라인 DAG의 구성과 의존성을 검증한다."""

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import EXECUTION_TIMEOUT_OVERRIDES, REALTIME_5MIN_CRON, TIMEZONE
from config.sources import REALTIME_5MIN_SOURCES, RENTAL_HISTORY_LOOKBACK_HOURS
from dags.realtime_5min import dag


def test_schedule_and_run_policy():
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert REALTIME_5MIN_CRON == "*/5 * * * *"
    assert TIMEZONE == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_expected_tasks_exist():
    expected = {f"collect_{s}" for s in REALTIME_5MIN_SOURCES} | {
        "load_stations",
        "load_station_stock",
        "run_normalizer_strict",
        "run_normalizer_fallback",
        "population_normalized",
        "run_inference",
        "load_forecast_points",
    } | {
        # 대여이력 과거 시간대 재조회. 상수를 올리면 태스크가 따라 늘어난다.
        f"collect_bike_rental_history_replay_{h}h"
        for h in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    }
    assert set(dag.task_ids) == expected


def test_stations_loads_before_station_stock():
    """station_stock.sta_id가 stations.sta_id를 FK 참조하므로 순차 실행이어야 한다."""
    load_stations = dag.get_task("load_stations")
    load_station_stock = dag.get_task("load_station_stock")
    assert load_station_stock.task_id in {t.task_id for t in load_stations.downstream_list}


def test_load_stations_depends_on_bike_station_realtime():
    collect = dag.get_task("collect_bike_station_realtime")
    load_stations = dag.get_task("load_stations")
    assert load_stations.task_id in {t.task_id for t in collect.downstream_list}


def test_normalizer_strict_then_fallback():
    collect_population = dag.get_task("collect_population_realtime")
    strict = dag.get_task("run_normalizer_strict")
    fallback = dag.get_task("run_normalizer_fallback")

    assert strict.task_id in {t.task_id for t in collect_population.downstream_list}
    assert fallback.task_id in {t.task_id for t in strict.downstream_list}
    assert fallback.trigger_rule == "all_failed"
    assert "--baseline-date-mode latest" in fallback.bash_command
    assert "--baseline-date-mode strict" in strict.bash_command


def test_inference_waits_for_realtime_bikes_and_normalized_population():
    """날씨는 별도 DAG의 최신 Silver를 읽고, 이 DAG의 실시간 입력은 직접 기다린다."""
    run_inference = dag.get_task("run_inference")
    upstream_ids = {t.task_id for t in run_inference.upstream_list}

    assert upstream_ids == {
        "collect_bike_rental_history",
        "collect_bike_station_realtime",
        "population_normalized",
    }
    assert "collect_population_realtime" not in upstream_ids
    assert "collect_weather_ultra_short_live" not in dag.task_ids


def test_inference_then_load_forecast_points():
    run_inference = dag.get_task("run_inference")
    load_forecast_points = dag.get_task("load_forecast_points")
    assert load_forecast_points.task_id in {t.task_id for t in run_inference.downstream_list}


def test_collector_task_execution_contract():
    task = dag.get_task("collect_bike_station_realtime")
    assert isinstance(task, BashOperator)
    assert task.retries == 2
    assert task.retry_delay == timedelta(seconds=30)
    assert task.execution_timeout == timedelta(seconds=240)
    assert "--source bike_station_realtime" in task.bash_command
    assert "--window-start" in task.bash_command
    assert "astimezone" in task.bash_command
    assert task.bash_command.startswith("env -u VIRTUAL_ENV -u UV_PROJECT_ENVIRONMENT ")


def test_living_population_grid_timeout_override_not_used_in_this_dag():
    """living_population_grid는 daily DAG 소관이라 이 DAG에는 없다."""
    assert "collect_living_population_grid" not in dag.task_ids
    assert EXECUTION_TIMEOUT_OVERRIDES["living_population_grid"] == timedelta(seconds=1200)


# ---------------------------------------------------------------------------
# bike_rental_history 시간대 재조회 (A2)
#
# tbCycleRentData는 `RENT_DT`(대여 시각) 기준으로 한 시간치를 주지만, 목록에는
# **반납이 완료된 기록만** 나타나고 정렬도 `RTN_DT` 오름차순이다. 그래서 대여 시간대가
# 끝난 뒤에 반납된 기록은 그 시간대를 마지막으로 조회하는 윈도우 이후에 등장한다.
#
# 현행은 시간대 H를 `T ∈ [H:05, H+1:00]` 윈도우만 조회하므로 그 뒤 반납분을 영구히
# 놓친다. 실측(2026-08-18): 03/08/12/18/21시 합계 10,538/42,902 = 24.6% 누락.
# backfill은 실패한 조각만 재시도하므로(pipeline.py:197) 이걸 회수하지 못한다.
#
# 그래서 매 tick에서 과거 시간대를 `--force`로 다시 수집한다. 각 호출은 독립된
# manifest 윈도우라 어댑터·조각 키·expected_total·재시도 로직이 전부 그대로다.
# ---------------------------------------------------------------------------


def _replay_tasks():
    return [dag.get_task(f"collect_bike_rental_history_replay_{h}h")
            for h in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)]


def test_lookback_is_one_hour():
    """실측 회수율: +1h이면 누락의 86.7~90.2%, +2h이면 98.0~100%.
    요청 수는 시간대당 12 x (L+1)회이므로 L=1은 현행의 2배다."""
    assert RENTAL_HISTORY_LOOKBACK_HOURS == 1


def test_replay_tasks_exist_for_each_lookback_hour():
    assert [t.task_id for t in _replay_tasks()] == [
        f"collect_bike_rental_history_replay_{h}h"
        for h in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


def test_replay_uses_force_because_backfill_cannot_recover_late_returns():
    """--backfill은 실패한 조각만 채운다. 여기서 놓치는 것은 실패가 아니라
    '그때는 아직 존재하지 않았던 데이터'라서 윈도우 전체를 다시 받아야 한다."""
    for task in _replay_tasks():
        assert "--force" in task.bash_command
        assert "--backfill" not in task.bash_command
        assert "--source bike_rental_history" in task.bash_command


def test_replay_window_start_is_shifted_back_by_whole_hours():
    """window_start를 H시간 앞으로 당기면 path_suffix의 window_last도 같이 당겨져
    그 시간대를 조회하고, silver도 그 시간대의 dt/hh 파티션에 쓰인다."""
    for hours, task in enumerate(_replay_tasks(), start=1):
        assert f"timedelta(hours={hours})" in task.bash_command
        assert "astimezone" in task.bash_command


def test_replay_runs_sequentially_to_cap_api_concurrency():
    """한 tick에 이 소스 호출이 여러 개 뜨는데, 각 호출이 페이지를 concurrency만큼
    병렬로 받는다(bike_rental_history.yaml: 4). 동시에 띄우면 같은 API에 대한 동시
    요청이 배수로 늘어나므로 사슬로 묶어 4로 고정한다."""
    chain = [dag.get_task("collect_bike_rental_history"), *_replay_tasks()]
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream.task_id in {t.task_id for t in upstream.downstream_list}


def test_replay_failure_does_not_block_inference():
    """재조회는 과거 시간대를 보강하는 일이라 실패해도 현재 tick의 추론을 막으면 안 된다."""
    for task in _replay_tasks():
        assert task.trigger_rule == "all_done"
        downstream = {t.task_id for t in task.downstream_list}
        assert "run_inference" not in downstream


def test_inference_still_waits_only_for_the_current_window_collection():
    run_inference = dag.get_task("run_inference")
    upstream = {t.task_id for t in run_inference.upstream_list}
    assert "collect_bike_rental_history" in upstream
    for task in _replay_tasks():
        assert task.task_id not in upstream
