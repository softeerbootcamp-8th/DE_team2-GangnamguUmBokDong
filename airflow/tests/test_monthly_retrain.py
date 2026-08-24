"""monthly_retrain DAG 구조, 스케줄 및 인프라 수명주기 태스크를 검증한다."""

from unittest.mock import MagicMock

import dags.monthly_retrain as monthly_dag
import dags.monthly_retrain_rental as monthly_rental_dag
import dags.monthly_retrain_return as monthly_return_dag
from airflow.task.trigger_rule import TriggerRule


def test_monthly_retrain_dags_structure() -> None:
    """대여 및 반납 monthly_retrain DAG의 ID, 태스크 목록 및 의존성 구조를 검증한다."""
    rental_dag = monthly_rental_dag.dag
    assert rental_dag.dag_id == "monthly_retrain_rental"

    return_dag = monthly_return_dag.dag
    assert return_dag.dag_id == "monthly_retrain_return"

    expected_tasks = {
        "start_ec2_eval",
        "run_eval_on_ec2",
        "stop_ec2_eval",
        "check_retrain_branch",
        "orchestrate_retrain_loop",
        "skip_monthly_retrain",
        "ensure_all_instances_stopped",
    }
    assert set(rental_dag.task_ids) == expected_tasks
    assert set(return_dag.task_ids) == expected_tasks


def test_monthly_retrain_fail_safe_trigger_rules() -> None:
    """인스턴스 정리 태스크는 상위 태스크 실패 시에도 ALL_DONE으로 반드시 실행된다."""
    for dag in (monthly_rental_dag.dag, monthly_return_dag.dag):
        stop_eval = dag.get_task("stop_ec2_eval")
        assert stop_eval.trigger_rule == TriggerRule.ALL_DONE

        ensure_stopped = dag.get_task("ensure_all_instances_stopped")
        assert ensure_stopped.trigger_rule == TriggerRule.ALL_DONE


def test_check_retrain_branch_decisions() -> None:
    """needs_retrain 값에 따라 올바른 다운스트림 태스크로 분기한다."""
    mock_ti = MagicMock()
    branch_fn = monthly_dag.make_task_check_retrain_branch("rental")

    # Case 1: 재학습 필요 시 orchestrate_retrain_loop로 분기
    mock_ti.xcom_pull.return_value = True
    branch = branch_fn(ti=mock_ti)
    assert branch == "orchestrate_retrain_loop"

    # Case 2: 재학습 불필요 시 skip_monthly_retrain으로 분기
    mock_ti.xcom_pull.return_value = False
    branch = branch_fn(ti=mock_ti)
    assert branch == "skip_monthly_retrain"


def test_orchestrate_retrain_loop_executes_emr_then_ec2(monkeypatch) -> None:
    """재학습 오케스트레이션이 EMR 피처마트 생성 후 EC2 학습/평가를 순차 실행하고 EC2를 중지한다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        "candidate_profiles": ["rental-profile-1"],
    }.get(key, [])

    emr_calls = []
    ec2_starts = []
    ec2_stops = []
    commands_run = []

    monkeypatch.setattr(
        monthly_dag,
        "run_emr_feature_mart_job",
        lambda p, **kwargs: emr_calls.append(p) or "job-123",
    )
    monkeypatch.setattr(
        monthly_dag, "start_ec2_instance", lambda **kwargs: ec2_starts.append(1) or "i-123"
    )
    monkeypatch.setattr(
        monthly_dag, "stop_ec2_instance", lambda **kwargs: ec2_stops.append(1)
    )
    monkeypatch.setattr(
        monthly_dag,
        "run_command_on_ec2",
        lambda cmd, working_dir=None, **kwargs: commands_run.append(cmd)
        or {"StandardOutputContent": "success"},
    )

    loop_fn = monthly_dag.make_task_orchestrate_retrain_loop("rental")
    result = loop_fn(ti=mock_ti, params={})

    assert result["status"] == "completed"
    assert emr_calls == ["rental-profile-1"]
    assert len(ec2_starts) == 1
    assert len(ec2_stops) == 1
    assert any("--profile-name rental-profile-1" in cmd and "--models rental" in cmd for cmd in commands_run)
