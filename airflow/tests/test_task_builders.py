"""DAG 없이 orchestration/*.py의 태스크 빌더를 단독으로 검증한다."""

from orchestration.collector_task import COLLECTOR_DIR, build_collector_task
from orchestration.db_loader_task import DB_LOADER_DIR, build_db_loader_task
from orchestration.inference_task import ML_DIR, build_inference_task
from orchestration.normalizer_task import (
    NORMALIZER_DIR,
    build_normalizer_task,
    build_station_master_enrichment_task,
)
from orchestration.nowcasting_task import NOWCASTING_DIR, build_nowcasting_task
from orchestration.task_builder import REPO_ROOT
from orchestration.urgency_task import REBALANCE_DIR, build_urgency_task


def test_repo_root_resolves_to_repository_root():
    assert (REPO_ROOT / "collector").is_dir()
    assert (REPO_ROOT / "loader").is_dir()
    assert (REPO_ROOT / "ml" / "inference").is_dir()


def test_collector_task_uses_kst_window_start_and_own_project_environment(dag):
    task = build_collector_task(dag, "bike_station_realtime")
    assert task.bash_command.startswith("env -u VIRTUAL_ENV -u UV_PROJECT_ENVIRONMENT ")
    assert "uv run --frozen python main.py --source bike_station_realtime" in task.bash_command
    assert "astimezone" in task.bash_command
    assert task.cwd == COLLECTOR_DIR


def test_normalizer_task_cwd_and_flags(dag):
    task = build_normalizer_task(dag)
    assert task.task_id == "run_normalizer"
    assert task.cwd == NORMALIZER_DIR
    assert "python main.py --window-start" in task.bash_command
    # baseline은 항상 nowcaster 추정치라 모드 선택 인자가 없다.
    assert "--baseline-date-mode" not in task.bash_command


def test_station_master_enrichment_task_contract(dag):
    task = build_station_master_enrichment_task(dag)
    assert task.cwd == NORMALIZER_DIR
    assert "python station_master.py" in task.bash_command
    assert "--baseline-date-mode" not in task.bash_command
    assert "astimezone" in task.bash_command


def test_nowcasting_task_uses_date_not_window_start(dag):
    task = build_nowcasting_task(dag)
    assert task.cwd == NOWCASTING_DIR
    assert "main.py estimate --target-date" in task.bash_command
    assert "strftime" in task.bash_command


def test_inference_task_cwd_is_ml_not_ml_inference(dag):
    """`-m inference.predict_single`가 resolve되려면 cwd가 ml/ 이어야 한다(ml/inference가
    아니다) — ml/inference/README.md가 검증한 호출 패턴."""
    task = build_inference_task(dag)
    assert task.cwd == ML_DIR
    assert task.cwd.endswith("/ml")
    assert "uv --project inference run python -m inference.predict_single" in task.bash_command
    assert "--all-stations" in task.bash_command
    assert "--n-hours 12" in task.bash_command
    assert "// 5" in task.bash_command
    assert ".replace(" in task.bash_command


def test_urgency_task_cwd_and_flags(dag):
    """rebalance는 loader/nowcaster처럼 flat 레이아웃이라 -m 실행이 필요 없다 —
    ml/inference와 달리 uv --project가 아니라 uv run --frozen을 쓴다."""
    task = build_urgency_task(dag)
    assert task.cwd == REBALANCE_DIR
    assert task.cwd.endswith("/rebalance")
    assert "uv run --frozen python main.py" in task.bash_command
    assert "--date" in task.bash_command
    assert "--hour" in task.bash_command
    assert "--minute" in task.bash_command


def test_all_pipeline_tasks_floor_manual_run_to_same_five_minute_window(dag):
    """수동 trigger의 19:33도 모든 모듈에서 동일하게 19:30으로 내림한다."""
    tasks = [
        build_collector_task(dag, "bike_station_realtime"),
        build_normalizer_task(dag, "normalize"),
        build_inference_task(dag),
        build_db_loader_task(dag, "station_stock"),
        build_urgency_task(dag),
    ]

    for task in tasks:
        assert "// 5" in task.bash_command
        assert "second=0" in task.bash_command


def test_db_loader_task_table_flag(dag):
    task = build_db_loader_task(dag, "forecast_points")
    assert task.cwd == DB_LOADER_DIR
    assert "--table forecast_points" in task.bash_command
    assert "--window-start" in task.bash_command


def test_all_module_wrappers_attach_success_and_failure_callbacks(dag):
    from callbacks.task_callbacks import on_failure_callback, on_success_callback

    task = build_collector_task(dag, "bike_station_realtime")
    assert on_success_callback in task.on_success_callback
    assert on_failure_callback in task.on_failure_callback


def test_replay_template_renders_to_a_whole_hour_earlier():
    """`kst_window_start_shifted`가 실제로 Jinja에서 렌더링되는지 확인한다.
    상수를 문자열로만 검사하면 `macros.timedelta`가 없어도 테스트가 통과해버린다."""
    from datetime import datetime, timedelta, timezone

    import jinja2
    from airflow.sdk.execution_time import macros
    from orchestration.templates import KST_WINDOW_START, kst_window_start_shifted

    kst = timezone(timedelta(hours=9))
    context = {
        # 5분 경계가 아닌 수동 trigger 시각. 19:33 -> 19:30으로 내림된 뒤 이동해야 한다.
        "dag_run": type("R", (), {
            "logical_date": datetime(2026, 8, 18, 19, 33, 12, tzinfo=kst),
            "start_date": None,
        })(),
        "macros": macros,
    }
    env = jinja2.Environment()

    base = env.from_string(KST_WINDOW_START).render(context)
    shifted = env.from_string(kst_window_start_shifted(1)).render(context)

    assert datetime.fromisoformat(base) == datetime(2026, 8, 18, 19, 30, tzinfo=kst)
    assert datetime.fromisoformat(shifted) == datetime(2026, 8, 18, 18, 30, tzinfo=kst)
    assert datetime.fromisoformat(
        env.from_string(kst_window_start_shifted(2)).render(context)
    ) == datetime(2026, 8, 18, 17, 30, tzinfo=kst)


def test_replay_template_rejects_non_positive_hours():
    import pytest

    from orchestration.templates import kst_window_start_shifted

    with pytest.raises(ValueError):
        kst_window_start_shifted(0)


def test_replay_collector_task_contract(dag):
    from airflow.task.trigger_rule import TriggerRule
    from orchestration.collector_task import build_collector_replay_task

    task = build_collector_replay_task(dag, "bike_rental_history", 1)

    assert task.task_id == "collect_bike_rental_history_replay_1h"
    assert task.cwd == COLLECTOR_DIR
    assert "--source bike_rental_history" in task.bash_command
    assert "--force" in task.bash_command
    assert task.trigger_rule == TriggerRule.ALL_DONE
