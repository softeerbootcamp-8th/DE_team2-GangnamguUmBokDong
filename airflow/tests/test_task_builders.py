"""DAG 없이 orchestration/*.py의 태스크 빌더를 단독으로 검증한다."""

from orchestration.collector_task import COLLECTOR_DIR, build_collector_task
from orchestration.db_loader_task import DB_LOADER_DIR, build_db_loader_task
from orchestration.inference_task import ML_DIR, build_inference_task
from orchestration.normalizer_task import NORMALIZER_DIR, build_normalizer_task
from orchestration.nowcasting_task import NOWCASTING_DIR, build_nowcasting_task
from orchestration.task_builder import REPO_ROOT


def test_repo_root_resolves_to_repository_root():
    assert (REPO_ROOT / "collector").is_dir()
    assert (REPO_ROOT / "db-loader").is_dir()
    assert (REPO_ROOT / "ml" / "inference").is_dir()


def test_collector_task_uses_kst_window_start_and_no_virtual_env(dag):
    task = build_collector_task(dag, "bike_station_realtime")
    assert task.bash_command.startswith("env -u VIRTUAL_ENV ")
    assert "uv run python main.py --source bike_station_realtime" in task.bash_command
    assert "in_timezone" in task.bash_command
    assert task.cwd == COLLECTOR_DIR


def test_normalizer_task_cwd_and_flags(dag):
    task = build_normalizer_task(dag, "run_normalizer_strict", "strict")
    assert task.cwd == NORMALIZER_DIR
    assert "--baseline-date-mode strict" in task.bash_command


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
    assert "./inference/.venv/bin/python -m inference.predict_single" in task.bash_command
    assert "--all-stations" in task.bash_command


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
