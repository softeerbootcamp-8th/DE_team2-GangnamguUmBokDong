"""monthly_retrain DAG 구조, 스케줄 및 단일 EMR 클러스터 생애주기 오케스트레이션을
검증한다(2026-08 재설계 — ADR-0007, EC2/SSM 경로를 EMR 스텝으로 통합)."""

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
        "create_cluster_and_evaluate",
        "check_retrain_branch",
        "orchestrate_retrain_loop",
        "skip_monthly_retrain",
        "terminate_cluster",
    }
    assert set(rental_dag.task_ids) == expected_tasks
    assert set(return_dag.task_ids) == expected_tasks


def test_monthly_retrain_fail_safe_trigger_rule() -> None:
    """EMR 클러스터 종료 태스크는 상위 태스크 실패 시에도 ALL_DONE으로 반드시 실행된다
    — 이 안전망이 없으면 태스크가 kill돼도 클러스터가 계속 과금된다."""
    for dag in (monthly_rental_dag.dag, monthly_return_dag.dag):
        terminate_cluster = dag.get_task("terminate_cluster")
        assert terminate_cluster.trigger_rule == TriggerRule.ALL_DONE


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


def test_create_cluster_and_evaluate_pushes_cluster_id_and_needs_retrain(monkeypatch) -> None:
    """클러스터 생성 후 평가 스텝 결과(S3 JSON)를 읽어 needs_retrain/candidate_profiles를
    xcom에 남긴다."""
    mock_ti = MagicMock()
    monkeypatch.setattr(monthly_dag, "create_emr_cluster", lambda **kwargs: "j-created")
    monkeypatch.setattr(monthly_dag, "submit_emr_step", lambda *args, **kwargs: {"StepId": "s-1", "State": "COMPLETED"})
    monkeypatch.setattr(
        monthly_dag,
        "read_s3_json",
        lambda key: {"needs_retrain": True, "candidate_profiles": ["rental-profile-1"]},
    )

    task_fn = monthly_dag.make_task_create_cluster_and_evaluate("rental")
    result = task_fn(ti=mock_ti, params={}, run_id="run-1")

    assert result["needs_retrain"] is True
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["cluster_id"] == "j-created"
    assert pushed["needs_retrain"] is True
    assert pushed["candidate_profiles"] == ["rental-profile-1"]


def test_create_cluster_and_evaluate_defaults_to_needs_retrain_when_no_result(monkeypatch) -> None:
    """스텝이 결과를 못 남겼을 때(주로 mock 모드) 재학습 루프 구조가 계속 검증되도록
    needs_retrain=True로 기본값을 잡는다."""
    mock_ti = MagicMock()
    monkeypatch.setattr(monthly_dag, "create_emr_cluster", lambda **kwargs: "mock-j-1")
    monkeypatch.setattr(monthly_dag, "submit_emr_step", lambda *args, **kwargs: {"StepId": "mock-s-1", "State": "COMPLETED"})
    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: None)

    task_fn = monthly_dag.make_task_create_cluster_and_evaluate("rental")
    result = task_fn(ti=mock_ti, params={}, run_id="run-1")

    assert result["needs_retrain"] is True
    assert result["candidate_profiles"] == ["builtin-default"]


def test_orchestrate_retrain_loop_submits_feature_mart_then_resizes_then_trains(monkeypatch) -> None:
    """재학습 루프가 프로필마다 피처마트 스텝을 제출하고, 최초 1회만 8노드로
    리사이즈한 뒤 YARN distributed-shell 학습 스텝을 제출한다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        "cluster_id": "j-1",
        "candidate_profiles": ["profile-a", "profile-b"],
    }.get(key)

    submitted_steps = []
    resize_calls = []

    monkeypatch.setattr(
        monthly_dag,
        "submit_emr_step",
        lambda cluster_id, name, command, **kwargs: submitted_steps.append(name)
        or {"StepId": "s", "State": "COMPLETED"},
    )
    monkeypatch.setattr(monthly_dag, "get_core_instance_group_id", lambda cluster_id, **kwargs: "ig-core")
    monkeypatch.setattr(
        monthly_dag,
        "resize_emr_cluster",
        lambda cluster_id, group_id, **kwargs: resize_calls.append(kwargs["target_core_count"]),
    )
    # 첫 프로필은 승격 실패, 두 번째 프로필은 승격 성공 -> 루프가 거기서 멈춰야 한다.
    monkeypatch.setattr(
        monthly_dag,
        "read_s3_json",
        lambda key: {"promoted": {"rental": "profile-b" in key}},
    )

    loop_fn = monthly_dag.make_task_orchestrate_retrain_loop("rental")
    result = loop_fn(ti=mock_ti, params={}, run_id="run-1")

    assert result["status"] == "completed"
    assert result["profiles"]["profile-a"]["promoted"] is False
    assert result["profiles"]["profile-b"]["promoted"] is True
    # 리사이즈는 최초 1회만 (8노드로)
    assert resize_calls == [monthly_dag.TRAINING_CORE_INSTANCE_COUNT]
    assert any("Spark-RunPipeline-profile-a" in s for s in submitted_steps)
    assert any("Spark-RunPipeline-profile-b" in s for s in submitted_steps)
    assert any(s == "Wait-YARN-Nodes" for s in submitted_steps)
    assert submitted_steps.count("Wait-YARN-Nodes") == 1
    assert any("Train-rental-profile-a" in s for s in submitted_steps)
    assert any("Train-rental-profile-b" in s for s in submitted_steps)


def test_terminate_cluster_task_calls_terminate_when_cluster_exists(monkeypatch) -> None:
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-1"
    terminated = []
    monkeypatch.setattr(monthly_dag, "terminate_emr_cluster", lambda cluster_id, **kwargs: terminated.append(cluster_id))

    task_fn = monthly_dag.make_task_terminate_emr_cluster("rental")
    task_fn(ti=mock_ti, params={})

    assert terminated == ["j-1"]


def test_terminate_cluster_task_no_ops_when_cluster_id_missing(monkeypatch) -> None:
    """클러스터 생성 자체가 실패해 cluster_id가 없으면 종료를 시도하지 않는다
    (터미네이트 대상이 아예 없으므로 에러가 아니라 정상 no-op)."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = None
    terminated = []
    monkeypatch.setattr(monthly_dag, "terminate_emr_cluster", lambda cluster_id, **kwargs: terminated.append(cluster_id))

    task_fn = monthly_dag.make_task_terminate_emr_cluster("rental")
    task_fn(ti=mock_ti, params={})

    assert terminated == []
