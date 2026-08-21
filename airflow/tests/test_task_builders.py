"""Orchestration task builder의 production CLI·JSON XCom 계약을 검증한다."""

import json
from datetime import datetime, timedelta, timezone

import jinja2
import pytest
from airflow.sdk.execution_time import macros
from airflow.task.trigger_rule import TriggerRule
from callbacks.task_callbacks import on_failure_callback, on_success_callback
from orchestration.collector_task import (
    COLLECTOR_DIR,
    build_collector_replay_task,
    build_collector_task,
    build_daily_history_replay_task,
)
from orchestration.gold_publisher_task import (
    GOLD_PUBLISHER_DIR,
    build_gold_publisher_task,
)
from orchestration.inference_task import ML_DIR, build_inference_task
from orchestration.normalizer_task import (
    NORMALIZER_DIR,
    build_normalizer_task,
    build_station_master_enrichment_task,
)
from orchestration.nowcasting_task import NOWCASTING_DIR, build_nowcasting_task
from orchestration.routes_task import build_routes_task
from orchestration.serving_task import (
    LOADER_DIR,
    build_finalize_serving_task,
    build_prepare_serving_task,
    build_weather_manifest_sensor,
)
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import (
    KST_WINDOW_START,
    kst_date_days_ago,
    kst_day_hour_replay_days_ago,
    kst_window_start_shifted,
)
from orchestration.urgency_task import build_urgency_task


def test_repo_root_resolves_to_repository_root() -> None:
    """Task cwd 기준 저장소 루트를 실제 module directory와 결합한다."""
    assert (REPO_ROOT / "collector").is_dir()
    assert (REPO_ROOT / "loader").is_dir()
    assert (REPO_ROOT / "ml" / "inference").is_dir()


def test_existing_task_keeps_provider_default_output_processor(dag) -> None:
    """JSON 출력이 없는 기존 task에 None processor를 덮어쓰지 않는다."""
    task = build_collector_task(dag, "bike_station_realtime")

    assert task.cwd == COLLECTOR_DIR
    assert task.append_env is True
    assert callable(task.output_processor)
    assert task.output_processor("plain output") == "plain output"


def test_collector_task_uses_kst_window_and_own_project_environment(dag) -> None:
    """Collector wrapper가 frozen project와 KST 5분 window 계약을 유지한다."""
    task = build_collector_task(dag, "bike_station_realtime")

    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/collector "
    )
    assert (
        "uv run --frozen python main.py --source bike_station_realtime"
        in task.bash_command
    )
    assert "astimezone" in task.bash_command
    assert "// 5" in task.bash_command
    assert task.cwd == COLLECTOR_DIR


def test_module_task_rejects_unsafe_environment_name(dag) -> None:
    """모듈 환경 이름이 고정 volume 경로를 벗어나지 못하게 한다."""
    with pytest.raises(ValueError, match="lowercase"):
        build_module_task(
            dag,
            "unsafe_environment",
            COLLECTOR_DIR,
            "true",
            uv_environment_name="../collector",
        )


def test_normalizer_task_contract(dag) -> None:
    """Population normalizer는 기존 frozen project와 all-success 계약을 유지한다."""
    task = build_normalizer_task(dag)
    assert task.task_id == "run_normalizer"
    assert task.cwd == NORMALIZER_DIR
    assert "python main.py --window-start" in task.bash_command
    assert "--baseline-date-mode" not in task.bash_command
    assert task.trigger_rule == TriggerRule.ALL_SUCCESS


def test_station_master_enrichment_builder_contract(dag) -> None:
    """미사용 enrichment builder의 기존 CLI 계약도 회귀하지 않는다."""
    task = build_station_master_enrichment_task(dag)

    assert task.cwd == NORMALIZER_DIR
    assert "python station_master.py" in task.bash_command
    assert "--baseline-date-mode" not in task.bash_command
    assert "astimezone" in task.bash_command


def test_nowcasting_task_uses_date_not_window_start(dag) -> None:
    """Nowcasting 독립 task는 기존 target-date 계약을 유지한다."""
    task = build_nowcasting_task(dag)

    assert task.cwd == NOWCASTING_DIR
    assert "main.py estimate --target-date" in task.bash_command
    assert "strftime" in task.bash_command


def test_prepare_task_emits_json_and_uses_templated_env(dag) -> None:
    """Prepare는 shell interpolation 대신 templated env로 logical time을 받는다."""
    task = build_prepare_serving_task(dag)

    assert task.cwd == LOADER_DIR
    assert "uv run --frozen python serving_cli.py prepare" in task.bash_command
    assert task.env is not None
    assert "astimezone" in task.env["SERVING_LOGICAL_DTTM"]
    assert "ti.xcom_pull" not in task.bash_command
    assert task.append_env is True
    assert task.output_processor('{"plan":{"uri":"s3://b/k"}}') == {
        "plan": {"uri": "s3://b/k"}
    }


def test_weather_sensor_uses_loader_cli_and_bounded_soft_timeout(dag) -> None:
    """날씨 Sensor는 2초 poke·30초 soft timeout으로 Loader CLI를 호출한다."""
    task = build_weather_manifest_sensor(dag)

    assert "serving_cli.py weather-ready" in task.bash_command
    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/loader "
    )
    assert f"uv run --project {LOADER_DIR}" in task.bash_command
    assert task.poke_interval == 2
    assert task.timeout == 30
    assert task.soft_fail is True
    assert task.mode == "poke"
    assert task.retries == 0
    assert "astimezone" in task.bash_command


def test_inference_task_consumes_only_plan_ref_json(dag) -> None:
    """Inference는 plan URI·SHA만 XCom으로 받고 immutable producer CLI를 실행한다."""
    task = build_inference_task(dag, plan_task_id="prepare_x")

    assert task.cwd == ML_DIR
    assert task.cwd.endswith("/ml")
    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/ml-inference "
    )
    assert "--frozen python -m inference.publication_cli" in task.bash_command
    assert "predict_single" not in task.bash_command
    assert task.env == {
        "PLAN_URI": "{{ ti.xcom_pull(task_ids='prepare_x')['plan']['uri'] }}",
        "PLAN_SHA256": (
            "{{ ti.xcom_pull(task_ids='prepare_x')['plan']['byte_sha256'] }}"
        ),
    }
    assert "ti.xcom_pull" not in task.bash_command
    assert set(task.env) == {"PLAN_URI", "PLAN_SHA256"}
    assert task.trigger_rule == TriggerRule.ALL_SUCCESS
    assert task.output_processor(json.dumps({"inference": {}})) == {"inference": {}}


def test_finalize_task_uses_exact_refs_and_disables_single_task_retry(dag) -> None:
    """Drift 실패는 같은 refs 재시도로 치유되지 않아 final 단독 retry를 금지한다."""
    task = build_finalize_serving_task(
        dag,
        plan_task_id="prepare_x",
        inference_task_id="inference_x",
    )

    assert task.cwd == LOADER_DIR
    assert "uv run --frozen python serving_cli.py finalize" in task.bash_command
    assert task.env == {
        "PLAN_URI": "{{ ti.xcom_pull(task_ids='prepare_x')['plan']['uri'] }}",
        "PLAN_SHA256": (
            "{{ ti.xcom_pull(task_ids='prepare_x')['plan']['byte_sha256'] }}"
        ),
        "INFERENCE_URI": (
            "{{ ti.xcom_pull(task_ids='inference_x')['inference']['uri'] }}"
        ),
        "INFERENCE_SHA256": (
            "{{ ti.xcom_pull(task_ids='inference_x')['inference']['byte_sha256'] }}"
        ),
    }
    assert "ti.xcom_pull" not in task.bash_command
    assert task.retries == 0
    assert callable(task.output_processor)
    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/loader "
    )


def test_urgency_and_route_use_release_refs_without_legacy_compute(dag) -> None:
    """Derived downstream task가 loader publisher만 호출하고 stale retry를 하지 않는다."""
    urgency = build_urgency_task(dag, final_task_id="final_x")
    route = build_routes_task(dag, urgency_task_id="urgency_x")

    assert urgency.cwd == route.cwd == LOADER_DIR
    assert "uv run --frozen python serving_cli.py urgency" in urgency.bash_command
    assert "uv run --frozen python serving_cli.py route" in route.bash_command
    assert "rebalance" not in urgency.cwd
    assert urgency.retries == route.retries == 0
    for task in (urgency, route):
        assert task.bash_command.startswith(
            "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/loader "
        )
    assert urgency.env == {
        "STATION_URI": "{{ ti.xcom_pull(task_ids='final_x')['station']['uri'] }}",
        "STATION_SHA256": (
            "{{ ti.xcom_pull(task_ids='final_x')['station']['byte_sha256'] }}"
        ),
        "DEMAND_URI": (
            "{{ ti.xcom_pull(task_ids='final_x')['station_demand_forecast']['uri'] }}"
        ),
        "DEMAND_SHA256": (
            "{{ ti.xcom_pull(task_ids='final_x')"
            "['station_demand_forecast']['byte_sha256'] }}"
        ),
        "STOCK_URI": ("{{ ti.xcom_pull(task_ids='final_x')['station_stock']['uri'] }}"),
        "STOCK_SHA256": (
            "{{ ti.xcom_pull(task_ids='final_x')['station_stock']['byte_sha256'] }}"
        ),
    }
    assert route.env == {
        "URGENCY_URI": (
            "{{ ti.xcom_pull(task_ids='urgency_x')['station_urgency']['uri'] }}"
        ),
        "URGENCY_SHA256": (
            "{{ ti.xcom_pull(task_ids='urgency_x')['station_urgency']['byte_sha256'] }}"
        ),
    }
    assert "ti.xcom_pull" not in urgency.bash_command
    assert "ti.xcom_pull" not in route.bash_command


def test_gold_builder_allows_events_but_rejects_retired_station_weather(dag) -> None:
    """Standalone Gold builder에는 seed/event 외 source authority가 남지 않는다."""
    task = build_gold_publisher_task(dag, "event:cultural_event")
    assert task.cwd == GOLD_PUBLISHER_DIR == LOADER_DIR
    assert "uv run --frozen python gold_cli.py" in task.bash_command
    assert "--publication event:cultural_event" in task.bash_command
    assert "--window-start" in task.bash_command
    assert "astimezone" in task.bash_command
    assert task.trigger_rule == TriggerRule.ALL_SUCCESS
    with pytest.raises(ValueError, match="지원하지 않는"):
        build_gold_publisher_task(dag, "station-release")
    with pytest.raises(ValueError, match="지원하지 않는"):
        build_gold_publisher_task(dag, "station-master-correction")
    with pytest.raises(ValueError, match="지원하지 않는"):
        build_gold_publisher_task(dag, "weather-forecast")
    with pytest.raises(ValueError, match="지원하지 않는"):
        build_gold_publisher_task(dag, "station; unsafe")


def test_replay_collector_remains_nonblocking_side_chain(dag) -> None:
    """과거 rental 보강은 force/all-done 계약을 유지한다."""
    task = build_collector_replay_task(dag, "bike_rental_history", 1)
    assert task.task_id == "collect_bike_rental_history_replay_1h"
    assert task.cwd == COLLECTOR_DIR
    assert "--source bike_rental_history" in task.bash_command
    assert "--force" in task.bash_command
    assert "timedelta(hours=1)" in task.bash_command
    assert task.trigger_rule == TriggerRule.ALL_DONE


def test_module_wrappers_attach_success_and_failure_callbacks(dag) -> None:
    """공용 wrapper가 운영 성공·실패 callback을 유지한다."""
    task = build_collector_task(dag, "bike_station_realtime")

    assert on_success_callback in task.on_success_callback
    assert on_failure_callback in task.on_failure_callback


def test_plan_and_source_tasks_share_same_five_minute_template(dag) -> None:
    """Plan logical time과 source task가 동일한 5분 KST floor를 사용한다."""
    collector = build_collector_task(dag, "bike_station_realtime")
    normalizer = build_normalizer_task(dag, "normalize_for_template")
    prepare = build_prepare_serving_task(dag)

    for task in (collector, normalizer):
        assert "// 5" in task.bash_command
        assert "second=0" in task.bash_command
    assert prepare.env == {"SERVING_LOGICAL_DTTM": KST_WINDOW_START}


def test_replay_template_renders_to_a_whole_hour_earlier() -> None:
    """Replay template가 5분 floor 뒤 정확히 whole hour만큼 이동한다."""
    kst = timezone(timedelta(hours=9))
    context = {
        "dag_run": type(
            "R",
            (),
            {
                "logical_date": datetime(2026, 8, 18, 19, 33, 12, tzinfo=kst),
                "start_date": None,
            },
        )(),
        "macros": macros,
    }
    environment = jinja2.Environment()

    base = environment.from_string(KST_WINDOW_START).render(context)
    shifted = environment.from_string(kst_window_start_shifted(1)).render(context)

    assert datetime.fromisoformat(base) == datetime(2026, 8, 18, 19, 30, tzinfo=kst)
    assert datetime.fromisoformat(shifted) == datetime(2026, 8, 18, 18, 30, tzinfo=kst)
    assert datetime.fromisoformat(
        environment.from_string(kst_window_start_shifted(2)).render(context)
    ) == datetime(2026, 8, 18, 17, 30, tzinfo=kst)


def test_replay_template_rejects_non_positive_hours() -> None:
    """Replay offset은 양의 시간만 허용한다."""
    with pytest.raises(ValueError):
        kst_window_start_shifted(0)


def test_daily_replay_templates_render_d_minus_six_boundaries() -> None:
    """D-6의 첫·마지막 API window boundary를 정확히 렌더링한다."""
    kst = timezone(timedelta(hours=9))
    context = {
        "dag_run": type(
            "R",
            (),
            {
                "logical_date": datetime(2026, 8, 19, 4, 30, tzinfo=kst),
                "start_date": None,
            },
        )(),
        "macros": macros,
    }
    environment = jinja2.Environment()

    target_date = environment.from_string(kst_date_days_ago(6)).render(context)
    first_end = environment.from_string(kst_day_hour_replay_days_ago(6, 0)).render(
        context
    )
    last_end = environment.from_string(kst_day_hour_replay_days_ago(6, 23)).render(
        context
    )

    assert target_date == "2026-08-13"
    assert datetime.fromisoformat(first_end) == datetime(2026, 8, 13, 0, 55, tzinfo=kst)
    assert datetime.fromisoformat(last_end) == datetime(2026, 8, 13, 23, 55, tzinfo=kst)


def test_daily_history_replay_task_contract(dag) -> None:
    """D-6 시간대 끝 시각의 rental history를 force 수집한다."""
    task = build_daily_history_replay_task(dag, 23, 6)

    assert task.task_id == "replay_bike_rental_history_23h"
    assert "--source bike_rental_history" in task.bash_command
    assert "--force" in task.bash_command
    assert "macros.timedelta(days=6)" in task.bash_command
    assert "hour=23, minute=55" in task.bash_command
