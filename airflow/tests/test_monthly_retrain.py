"""monthly_retrain DAG 구조, 스케줄 및 단일 EMR 클러스터 생애주기 오케스트레이션을
검증한다(2026-08 재설계 — ADR-0007, EC2/SSM 경로를 EMR 스텝으로 통합. 이후
대여/반납 동시 실행을 막기 위해 한 DAG 안에서 순차 실행하도록 재통합)."""

from datetime import timedelta
from unittest.mock import MagicMock

import dags.monthly_retrain as monthly_dag
from airflow.task.trigger_rule import TriggerRule

_DAG = monthly_dag.dag


def test_monthly_retrain_is_a_single_combined_dag() -> None:
    """대여/반납이 각자 최대 8노드 EMR 클러스터를 띄우므로, 동시에 두 개가 뜨는
    걸 막기 위해 하나의 DAG 안에 두 모델 체인이 모두 있어야 한다."""
    assert _DAG.dag_id == "monthly_retrain"
    expected_tasks = {
        f"{name}_{model}"
        for model in ("rental", "return")
        for name in (
            "create_cluster",
            "refresh_feature_mart",
            "evaluate",
            "check_retrain_branch",
            "orchestrate_retrain_loop",
            "skip_monthly_retrain",
            "terminate_cluster",
        )
    }
    assert set(_DAG.task_ids) == expected_tasks


def test_monthly_retrain_runs_rental_fully_before_return_starts() -> None:
    """대여 사이클이 클러스터 종료까지 완전히 끝난 뒤에만 반납 사이클이 시작돼야
    한다 — 두 EMR 클러스터가 동시에 뜨지 않게 하는 핵심 보장."""
    assert monthly_dag.MODEL_EXECUTION_ORDER == ("rental", "return")
    rental_terminate = _DAG.get_task("terminate_cluster_rental")
    return_create = _DAG.get_task("create_cluster_return")
    assert return_create.upstream_task_ids == {"terminate_cluster_rental"}
    assert "create_cluster_return" in rental_terminate.downstream_task_ids
    # 반납 쪽이 대여 쪽으로 역방향 의존을 만들지는 않는지도 확인.
    assert _DAG.get_task("create_cluster_rental").upstream_task_ids == set()


def test_monthly_retrain_total_timeout_covers_both_models_sequentially() -> None:
    """두 모델을 순차 실행하므로 전체 DAG Run 타임아웃은 모델 하나 몫(120시간)의
    2배(240시간)로 잡아야 한다 — 반납이 대여 완료를 기다리다 총 타임아웃에
    걸리면 안 되므로."""
    assert _DAG.dagrun_timeout == timedelta(hours=240)
    assert _DAG.dagrun_timeout >= 2 * monthly_dag.MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT


def test_monthly_retrain_terminate_cluster_is_a_real_teardown() -> None:
    """EMR 클러스터 종료 태스크는 trigger_rule=ALL_DONE만으로는 부족하다 —
    운영자가 DAG Run 전체를 수동으로 "Mark Failed" 처리하면 Airflow는 아직 실행
    안 된 일반 태스크를 trigger_rule 평가 없이 그냥 SKIPPED로 강제 전환하고
    끝내버린다(Airflow 3.3.1 `_set_dag_run_terminal_state()` 실측 확인). 오직
    `is_teardown=True`인 태스크만 이 강제 skip에서 예외로 남아 실제로 실행될
    기회를 얻으므로, setup/teardown API로 표시돼 있는지까지 확인해야 한다."""
    for model_name in ("rental", "return"):
        terminate_cluster = _DAG.get_task(f"terminate_cluster_{model_name}")
        create_cluster = _DAG.get_task(f"create_cluster_{model_name}")
        assert terminate_cluster.is_teardown is True
        assert terminate_cluster.trigger_rule == TriggerRule.ALL_DONE_SETUP_SUCCESS
        assert create_cluster.is_setup is True
        assert f"create_cluster_{model_name}" in terminate_cluster.upstream_task_ids


def test_monthly_retrain_evaluate_is_not_a_setup() -> None:
    """`evaluate`가 teardown의 setup에 같이 들어가면 안 된다 — 평가 스텝이
    EMR 쪽에서 멈추거나 실패해도(RUNNING에서 안 끝남 등) "클러스터 생성은
    성공했다"는 사실만으로 `terminate_cluster`가 반드시 실행돼야 한다(PR 리뷰
    지적, 2026-08). evaluate까지 setup에 포함되면 evaluate가 실패하는 순간
    teardown 조건(ALL_DONE_SETUP_SUCCESS)을 못 만족해 클러스터가 orphan으로
    남는다."""
    for model_name in ("rental", "return"):
        evaluate = _DAG.get_task(f"evaluate_{model_name}")
        assert evaluate.is_setup is False


def test_refresh_feature_mart_runs_between_create_cluster_and_evaluate() -> None:
    """evaluate가 최근 구간을 읽기 전에 feature mart를 먼저 최신화해야 한다
    (2026-08 발견 — evaluate는 스스로 feature mart를 최신화하지 않는데, 그걸
    만드는 유일한 경로가 원래 재학습 필요 판정 *뒤*에만 있어 순환 참조였다)."""
    for model_name in ("rental", "return"):
        refresh = _DAG.get_task(f"refresh_feature_mart_{model_name}")
        assert refresh.upstream_task_ids == {f"create_cluster_{model_name}"}
        assert refresh.downstream_task_ids == {f"evaluate_{model_name}"}


def test_check_retrain_branch_decisions() -> None:
    """needs_retrain 값에 따라 올바른(모델 접미사가 붙은) 다운스트림 태스크로 분기한다."""
    mock_ti = MagicMock()
    branch_fn = monthly_dag.make_task_check_retrain_branch("rental")

    # Case 1: 재학습 필요 시 orchestrate_retrain_loop_rental로 분기
    mock_ti.xcom_pull.return_value = True
    branch = branch_fn(ti=mock_ti)
    assert branch == "orchestrate_retrain_loop_rental"

    # Case 2: 재학습 불필요 시 skip_monthly_retrain_rental로 분기
    mock_ti.xcom_pull.return_value = False
    branch = branch_fn(ti=mock_ti)
    assert branch == "skip_monthly_retrain_rental"


def test_create_cluster_pushes_cluster_id(monkeypatch) -> None:
    """클러스터를 생성하고 cluster_id만 xcom에 남긴다 — 평가는 별도 태스크(evaluate)다."""
    mock_ti = MagicMock()
    monkeypatch.setattr(monthly_dag, "create_emr_cluster", lambda **kwargs: "j-created")

    task_fn = monthly_dag.make_task_create_cluster("rental")
    result = task_fn(ti=mock_ti, params={})

    assert result == "j-created"
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["cluster_id"] == "j-created"


def test_champion_profile_name_reads_pointer_then_profile_json(monkeypatch) -> None:
    """챔피언 포인터의 archive_prefix를 따라가 그 안의 {model}_profile.json에서
    profile_name을 읽는다 — training.scripts.monthly_retrain_check의 같은 이름
    함수와 같은 S3 키 형식을 airflow venv에서 boto3만으로 재현한 것."""
    reads = {}

    def fake_read_s3_json(key):
        reads[key] = True
        if key == "models/champion/rental.json":
            return {"archive_prefix": "models/archive/2026-01-01/leaves127"}
        if key == "models/archive/2026-01-01/leaves127/rental_profile.json":
            return {"profile_name": "leaves127"}
        return None

    monkeypatch.setattr(monthly_dag, "read_s3_json", fake_read_s3_json)

    assert monthly_dag._champion_profile_name("rental") == "leaves127"
    assert "models/champion/rental.json" in reads


def test_champion_profile_name_none_when_no_champion_yet(monkeypatch) -> None:
    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: None)
    assert monthly_dag._champion_profile_name("rental") is None


def test_refresh_feature_mart_submits_feature_mart_steps_for_champion_profile(monkeypatch) -> None:
    """create_cluster가 남긴 cluster_id 위에, 챔피언이 실제로 학습된 프로필로
    feature mart 스텝(run_pipeline + build_multi_horizon)을 제출해야 한다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "_champion_profile_name", lambda model_name: "leaves127")

    submitted_steps = []
    monkeypatch.setattr(
        monthly_dag,
        "submit_emr_step",
        lambda cluster_id, name, command, **kwargs: submitted_steps.append((cluster_id, name)),
    )

    task_fn = monthly_dag.make_task_refresh_feature_mart("rental")
    task_fn(ti=mock_ti, params={})

    assert ("j-created", "Spark-RunPipeline-leaves127") in submitted_steps
    assert ("j-created", "Spark-BuildMultiHorizon-leaves127") in submitted_steps
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["profile"] == "leaves127"


def test_refresh_feature_mart_falls_back_to_builtin_default_when_no_champion(monkeypatch) -> None:
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "_champion_profile_name", lambda model_name: None)

    submitted_steps = []
    monkeypatch.setattr(
        monthly_dag,
        "submit_emr_step",
        lambda cluster_id, name, command, **kwargs: submitted_steps.append(name),
    )

    task_fn = monthly_dag.make_task_refresh_feature_mart("rental")
    task_fn(ti=mock_ti, params={})

    assert any("builtin-default" in s for s in submitted_steps)


def test_evaluate_pushes_needs_retrain(monkeypatch) -> None:
    """create_cluster가 xcom에 남긴 cluster_id로 평가 스텝을 제출하고, 결과(S3 JSON)를
    읽어 needs_retrain/candidate_profiles를 xcom에 남긴다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "submit_emr_step", lambda *args, **kwargs: {"StepId": "s-1", "State": "COMPLETED"})
    monkeypatch.setattr(
        monthly_dag,
        "read_s3_json",
        lambda key: {"needs_retrain": True, "candidate_profiles": ["rental-profile-1"]},
    )

    task_fn = monthly_dag.make_task_evaluate("rental")
    result = task_fn(ti=mock_ti, params={}, run_id="run-1")

    assert result["needs_retrain"] is True
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["needs_retrain"] is True
    assert pushed["candidate_profiles"] == ["rental-profile-1"]


def test_evaluate_defaults_to_needs_retrain_when_no_result(monkeypatch) -> None:
    """스텝이 결과를 못 남겼을 때(주로 mock 모드) 재학습 루프 구조가 계속 검증되도록
    needs_retrain=True로 기본값을 잡는다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "mock-j-1"
    monkeypatch.setattr(monthly_dag, "submit_emr_step", lambda *args, **kwargs: {"StepId": "mock-s-1", "State": "COMPLETED"})
    monkeypatch.setattr(monthly_dag, "read_s3_json", lambda key: None)

    task_fn = monthly_dag.make_task_evaluate("rental")
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


def test_feature_mart_spark_steps_use_runpy_launcher_not_raw_script_path() -> None:
    """run_pipeline.py/build_multi_horizon_features.py는 패키지 내부 상대 import를
    쓴다 — spark-submit에 파일 경로를 그대로 넘기면 `__main__`으로 실행돼
    "attempted relative import with no known parent package"로 즉시 죽는다(PR
    리뷰 지적, 2026-08 — 최초 구현은 디버그 클러스터에서만 이 fix를 검증하고 실제
    코드에는 반영하지 않는 실수를 했다). `python -m`과 동등한 `runpy.run_module()`
    launcher를 통해서만 제출해야 한다."""
    run_pipeline_step, multi_horizon_step = monthly_dag._feature_mart_spark_steps("some-profile")

    for _name, command in (run_pipeline_step, multi_horizon_step):
        assert command[:2] == ["bash", "-c"]
        script = command[2]
        # spark-submit의 마지막 인자(primary python file)는 launcher(/tmp/_spark_entry*.py)여야
        # 한다 — feature_engine 안의 실제 스크립트 경로를 직접 넘기면 안 된다.
        primary_file = script.strip().splitlines()[-1].split()[-1]
        assert primary_file.startswith("/tmp/_spark_entry")
        assert "feature_engine/spark/run_pipeline.py" not in script
        assert "feature_engine/spark/build_multi_horizon_features.py" not in script

    assert 'runpy.run_module("feature_engine.spark.run_pipeline"' in run_pipeline_step[1][2]
    assert 'runpy.run_module("feature_engine.spark.build_multi_horizon_features"' in multi_horizon_step[1][2]


def test_bash_step_exports_s3_bucket_always() -> None:
    _name, command = monthly_dag._bash_step("Test", "echo hi")
    assert "export S3_BUCKET=" in command[2]


def test_bash_step_exports_mlflow_uri_when_configured(monkeypatch) -> None:
    """학습 스텝(train_common.py의 mlflow.start_run())이 EMR에서 docker 네트워크
    이름("mlflow")을 못 풀어 죽는 걸 막기 위해, terraform이 채운 상시 EC2 사설 IP
    기반 URI를 명시적으로 주입해야 한다(PR 리뷰 지적, 2026-08)."""
    monkeypatch.setattr(monthly_dag, "EMR_MLFLOW_TRACKING_URI", "http://10.0.0.5:5000/mlflow")
    _name, command = monthly_dag._bash_step("Test", "echo hi")
    assert "export MLFLOW_TRACKING_URI=http://10.0.0.5:5000/mlflow" in command[2]


def test_bash_step_skips_mlflow_uri_when_not_configured(monkeypatch) -> None:
    monkeypatch.setattr(monthly_dag, "EMR_MLFLOW_TRACKING_URI", "")
    _name, command = monthly_dag._bash_step("Test", "echo hi")
    assert "MLFLOW_TRACKING_URI" not in command[2]
