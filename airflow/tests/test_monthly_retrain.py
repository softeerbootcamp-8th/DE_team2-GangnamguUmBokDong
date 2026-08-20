"""monthly_retrain DAG 구조, 스케줄 및 인프라 수명주기 태스크를 검증한다."""

from unittest.mock import MagicMock

import dags.monthly_retrain as monthly_dag
from airflow.task.trigger_rule import TriggerRule


def test_monthly_retrain_dag_structure() -> None:
    """monthly_retrain DAG의 ID, 태스크 목록 및 의존성 구조를 검증한다."""
    dag = monthly_dag.dag
    assert dag.dag_id == "monthly_retrain"

    expected_tasks = {
        "start_ec2_eval",
        "run_eval_on_ec2",
        "stop_ec2_eval",
        "check_retrain_branch",
        "orchestrate_retrain_loop",
        "skip_monthly_retrain",
        "ensure_all_instances_stopped",
    }
    assert set(dag.task_ids) == expected_tasks


def test_monthly_retrain_fail_safe_trigger_rules() -> None:
    """인스턴스 정리 태스크는 상위 태스크 실패 시에도 ALL_DONE으로 반드시 실행된다."""
    dag = monthly_dag.dag

    stop_eval = dag.get_task("stop_ec2_eval")
    assert stop_eval.trigger_rule == TriggerRule.ALL_DONE

    ensure_stopped = dag.get_task("ensure_all_instances_stopped")
    assert ensure_stopped.trigger_rule == TriggerRule.ALL_DONE


def test_check_retrain_branch_decisions() -> None:
    """needs_retrain 값에 따라 올바른 다운스트림 태스크로 분기한다."""
    mock_ti = MagicMock()

    # Case 1: 재학습 필요 시 orchestrate_retrain_loop로 분기
    mock_ti.xcom_pull.return_value = True
    branch = monthly_dag.task_check_retrain_branch(ti=mock_ti)
    assert branch == "orchestrate_retrain_loop"

    # Case 2: 재학습 불필요 시 skip_monthly_retrain으로 분기
    mock_ti.xcom_pull.return_value = False
    branch = monthly_dag.task_check_retrain_branch(ti=mock_ti)
    assert branch == "skip_monthly_retrain"


def test_orchestrate_retrain_loop_executes_emr_then_ec2(monkeypatch) -> None:
    """재학습 오케스트레이션이 EMR 피처마트 생성 후 EC2 학습/평가를 순차 실행하고 EC2를 중지한다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        "candidate_profiles": ["test-profile-1"],
        "retrain_models": ["rental"],
    }.get(key, [])

    emr_calls = []
    ec2_starts = []
    ec2_stops = []
    commands_run = []

    monkeypatch.setattr(
        monthly_dag, "run_emr_feature_mart_job", lambda p: emr_calls.append(p) or "job-123"
    )
    monkeypatch.setattr(
        monthly_dag, "start_ec2_instance", lambda: ec2_starts.append(1) or "i-123"
    )
    monkeypatch.setattr(
        monthly_dag, "stop_ec2_instance", lambda: ec2_stops.append(1)
    )
    monkeypatch.setattr(
        monthly_dag,
        "run_command_on_ec2",
        lambda cmd, working_dir=None: commands_run.append(cmd) or {"StandardOutputContent": "success"},
    )

    result = monthly_dag.task_orchestrate_retrain_loop(ti=mock_ti)

    assert result["status"] == "completed"
    assert emr_calls == ["test-profile-1"]
    assert len(ec2_starts) == 1
    assert len(ec2_stops) == 1
    assert any("--profile-name test-profile-1" in cmd for cmd in commands_run)
