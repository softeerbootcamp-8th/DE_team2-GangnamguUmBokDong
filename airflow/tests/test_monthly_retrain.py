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
    } | {"start_mlflow", "stop_mlflow"}
    assert set(_DAG.task_ids) == expected_tasks


def test_monthly_retrain_runs_rental_fully_before_return_starts() -> None:
    """대여 사이클이 클러스터 종료까지 완전히 끝난 뒤에만 반납 사이클이 시작돼야
    한다 — 두 EMR 클러스터가 동시에 뜨지 않게 하는 핵심 보장."""
    assert monthly_dag.MODEL_EXECUTION_ORDER == ("rental", "return")
    rental_terminate = _DAG.get_task("terminate_cluster_rental")
    return_create = _DAG.get_task("create_cluster_return")
    assert return_create.upstream_task_ids == {"terminate_cluster_rental"}
    assert "create_cluster_return" in rental_terminate.downstream_task_ids
    # 반납 쪽이 대여 쪽으로 역방향 의존을 만들지는 않는지도 확인(start_mlflow는
    # DAG 전체를 감싸는 태스크라 예외로 허용).
    assert _DAG.get_task("create_cluster_rental").upstream_task_ids == {"start_mlflow"}


def test_start_stop_mlflow_wrap_the_whole_dag() -> None:
    """mlflow는 DAG 실행 구간에만 켜져 있어야 한다 — 대여 체인 시작 전에 켜고,
    반납 체인(마지막 모델)의 클러스터 종료 뒤에 끈다. stop_mlflow는
    terminate_cluster와 같은 이유로 teardown이어야 운영자가 DAG Run을 수동으로
    실패 처리해도 실행된다."""
    start_mlflow = _DAG.get_task("start_mlflow")
    stop_mlflow = _DAG.get_task("stop_mlflow")
    # setup/teardown 쌍(as_teardown(setups=...))이 start_mlflow -> stop_mlflow
    # 직접 엣지를 암묵적으로 추가하므로, create_cluster_rental과 stop_mlflow
    # 둘 다 downstream에 있어야 한다.
    assert start_mlflow.downstream_task_ids == {"create_cluster_rental", "stop_mlflow"}
    assert stop_mlflow.upstream_task_ids == {"terminate_cluster_return", "start_mlflow"}
    assert stop_mlflow.is_teardown is True


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


def test_refresh_feature_mart_waits_for_yarn_nodes_before_spark_steps(monkeypatch) -> None:
    """클러스터가 막 WAITING이 된 직후 YARN이 AM 등록을 받을 준비가 안 됐을 수
    있어(실제 EMR 실행에서 재현, 2026-08-25 — 서로 다른 두 클러스터에서 첫
    스텝의 AM이 등록 타임아웃으로 죽었다), 첫 Spark 스텝 전에 반드시
    Wait-YARN-Nodes로 대기해야 한다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    monkeypatch.setattr(monthly_dag, "_champion_profile_name", lambda model_name: "leaves127")

    submitted_names = []
    monkeypatch.setattr(
        monthly_dag,
        "submit_emr_step",
        lambda cluster_id, name, command, **kwargs: submitted_names.append(name),
    )

    task_fn = monthly_dag.make_task_refresh_feature_mart("rental")
    task_fn(ti=mock_ti, params={})

    assert submitted_names[0] == "Wait-YARN-Nodes"
    assert submitted_names.index("Wait-YARN-Nodes") < submitted_names.index("Spark-RunPipeline-leaves127")


def test_wait_for_yarn_nodes_step_checks_running_count() -> None:
    name, command = monthly_dag._wait_for_yarn_nodes_step(3)
    assert name == "Wait-YARN-Nodes"
    assert command[:2] == ["bash", "-c"]
    assert "yarn node -list -all" in command[2]
    assert "-ge 3" in command[2]


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


def test_refresh_feature_mart_skips_champion_build_when_test_profile_only(monkeypatch) -> None:
    """test_profile_only=True면 챔피언 feature mart는 만들 필요가 없다 —
    orchestrate_retrain_loop가 TEST_ONLY_PROFILE_NAME으로 어차피 다시 만든다.
    다만 Wait-YARN-Nodes는 그대로 제출해야 한다 — 뒤에 오는 retrain 루프의 첫
    스텝이 AM 등록 타임아웃으로 죽지 않으려면 이 대기가 반드시 필요하다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"

    submitted_names = []
    monkeypatch.setattr(
        monthly_dag,
        "submit_emr_step",
        lambda cluster_id, name, command, **kwargs: submitted_names.append(name),
    )

    task_fn = monthly_dag.make_task_refresh_feature_mart("rental")
    task_fn(ti=mock_ti, params={"test_profile_only": True})

    assert submitted_names == ["Wait-YARN-Nodes"]
    assert not any("Spark-RunPipeline" in s for s in submitted_names)


def test_evaluate_pushes_needs_retrain(monkeypatch) -> None:
    """create_cluster가 xcom에 남긴 cluster_id로 평가 스텝을 제출하고, 결과(S3 JSON)를
    읽어 needs_retrain/candidate_profiles를 xcom에 남긴다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    submitted = {}
    monkeypatch.setattr(
        monthly_dag,
        "submit_emr_step",
        lambda cluster_id, name, command, **kwargs: submitted.update(name=name, command=command)
        or {"StepId": "s-1", "State": "COMPLETED"},
    )
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
    # master 노드 OOM(exitCode 137) 이후 evaluate는 core 노드의 spark-submit
    # YARN cluster 컨테이너에서 돌아야 한다 — _yarn_python_module_step() docstring 참고.
    assert "spark-submit --deploy-mode cluster --master yarn" in submitted["command"][2]


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


def test_evaluate_forces_test_profile_when_test_profile_only(monkeypatch) -> None:
    """test_profile_only=True면 실제 EMR 성능 점검 스텝을 아예 제출하지 않고,
    바로 TEST_ONLY_PROFILE_NAME 하나만 재학습 대상으로 xcom에 남긴다 — 재평가를
    건너뛰고 작은 프로필로 전체 파이프라인을 빠르게 스모크 테스트하는 용도."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "j-created"
    submit_calls = []
    monkeypatch.setattr(
        monthly_dag, "submit_emr_step", lambda *a, **k: submit_calls.append(a) or {"StepId": "s", "State": "COMPLETED"}
    )

    task_fn = monthly_dag.make_task_evaluate("rental")
    result = task_fn(ti=mock_ti, params={"test_profile_only": True}, run_id="run-1")

    assert submit_calls == []
    assert result["needs_retrain"] is True
    assert result["candidate_profiles"] == [monthly_dag.TEST_ONLY_PROFILE_NAME]
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in mock_ti.xcom_push.call_args_list}
    assert pushed["needs_retrain"] is True
    assert pushed["candidate_profiles"] == [monthly_dag.TEST_ONLY_PROFILE_NAME]


def test_orchestrate_retrain_loop_submits_feature_mart_then_trains_without_resize(monkeypatch) -> None:
    """재학습 루프가 프로필마다 피처마트 스텝을 제출하고 YARN distributed-shell 학습
    스텝을 제출한다 — 클러스터가 이미 학습 단계 노드 수로 생성돼 있으므로(resize
    로직이 진행 중이던 작업을 죽이거나 목표치까지 못 올라가는 사례가 의심돼
    2026-08-26에 제거) 이 루프는 더 이상 resize를 태우지 않는다."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.side_effect = lambda task_ids, key: {
        "cluster_id": "j-1",
        "candidate_profiles": ["profile-a", "profile-b"],
    }.get(key)

    submitted_steps = []
    submitted_commands = {}

    def _fake_submit_emr_step(cluster_id, name, command, **kwargs):
        submitted_steps.append(name)
        submitted_commands[name] = command
        return {"StepId": "s", "State": "COMPLETED"}

    monkeypatch.setattr(monthly_dag, "submit_emr_step", _fake_submit_emr_step)
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
    assert any("Spark-RunPipeline-profile-a" in s for s in submitted_steps)
    assert any("Spark-RunPipeline-profile-b" in s for s in submitted_steps)
    assert any("Train-rental-profile-a" in s for s in submitted_steps)
    assert any("Train-rental-profile-b" in s for s in submitted_steps)
    # 학습 오케스트레이터도 master가 아니라 core 노드 YARN 컨테이너에서 돌아야
    # 한다(evaluate와 같은 이유 — _yarn_python_module_step() docstring 참고).
    train_command = submitted_commands["Train-rental-profile-a"][2]
    assert "spark-submit --deploy-mode cluster --master yarn" in train_command
    assert "spark.yarn.appMasterEnv.LGB_NUM_MACHINES=8" in train_command


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


def test_start_mlflow_calls_docker_action(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(monthly_dag, "_docker_mlflow_container_action", lambda action: calls.append(action))

    monthly_dag.make_task_start_mlflow()(params={})

    assert calls == ["start"]


def test_start_mlflow_skips_when_force_mock(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(monthly_dag, "_docker_mlflow_container_action", lambda action: calls.append(action))

    monthly_dag.make_task_start_mlflow()(params={"mock_mode": "force_mock"})

    assert calls == []


def test_stop_mlflow_calls_docker_action(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(monthly_dag, "_docker_mlflow_container_action", lambda action: calls.append(action))

    monthly_dag.make_task_stop_mlflow()(params={})

    assert calls == ["stop"]


def test_stop_mlflow_does_not_raise_when_docker_action_fails(monkeypatch) -> None:
    """mlflow 정지 실패가 DAG teardown 체인 전체를 실패시키면 안 된다 — 부가
    기능(모니터링)일 뿐이라 실패를 삼키고 경고만 남긴다."""

    def _boom(action):
        raise RuntimeError("docker socket 없음")

    monkeypatch.setattr(monthly_dag, "_docker_mlflow_container_action", _boom)

    monthly_dag.make_task_stop_mlflow()(params={})  # raise하지 않아야 한다


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
    # 이 두 모듈은 자기 코드 안에서 get_spark()로 SparkSession을 직접 만드므로,
    # launcher가 대신 더미 SparkSession을 끼워 넣을 필요가 없다(그건
    # _yarn_python_module_step()처럼 Spark를 전혀 안 쓰는 모듈에만 필요하다).
    assert "SparkSession.builder.getOrCreate()" not in run_pipeline_step[1][2]
    assert "SparkSession.builder.getOrCreate()" not in multi_horizon_step[1][2]


def test_yarn_python_module_step_wraps_in_spark_submit_with_small_heap_large_overhead() -> None:
    """평가/학습 오케스트레이터(`training.scripts.monthly_retrain_check`)를 master
    노드에 bare bash 스텝으로 직접 올렸다가 master의 EMR 자체 데몬 메모리 압박으로
    OOM(exitCode 137)이 났다. YARN distributed-shell(-num_containers 1)로
    옮겨보니 그 예제 jar 자체가 이 EMR의 Java 17 런타임에서 "JNI error"로 죽었다
    (둘 다 실제 EMR 실행에서 확인, 2026-08-26). spark-submit(YARN cluster 모드,
    Java 17 대응이 이미 확인된 경로)으로 감싸되, Spark가 실제로는 아무 연산도
    안 하므로 JVM 힙은 최소(1g)로 두고 진짜 메모리가 필요한 py4j 파이썬
    서브프로세스 몫은 spark.driver.memoryOverhead로 크게 잡는다.

    그런데 이렇게만 하면 이 대상 모듈이 SparkContext를 전혀 안 만들기 때문에,
    YARN cluster 모드의 ApplicationMaster가 `waitForSparkContextInitialized()`
    에서 SparkContext가 생기길 `spark.yarn.am.waitTime`(1800초)까지 기다린
    "뒤에야" RM에 AM 등록을 시도한다 — 실제로 매 시도(기본 2회)가 ACCEPTED
    상태로 30분씩 멈췄다가 실패했다(실제 EMR 실행에서 확인, 2026-08-26). 아무
    일도 안 하는 `SparkSession.builder.getOrCreate()`를 launcher 맨 앞에 끼워
    넣어야 AM이 등록을 미루지 않는다."""
    name, command = monthly_dag._yarn_python_module_step(
        "Evaluate-rental",
        "training.scripts.monthly_retrain_check",
        ["--check-only", "--models", "rental", "--result-s3-key", "some/key.json"],
    )
    assert name == "Evaluate-rental"
    assert command[:2] == ["bash", "-c"]
    script = command[2]
    assert "spark-submit --deploy-mode cluster --master yarn" in script
    assert "--driver-memory 1g" in script
    assert "spark.driver.memoryOverhead=5120" in script
    assert "--driver-cores 2" in script
    assert 'runpy.run_module("training.scripts.monthly_retrain_check"' in script
    # AM이 SparkContext 없이 등록을 미루지 않도록, runpy보다 먼저 더미
    # SparkSession을 만들어야 한다.
    assert script.index("SparkSession.builder.getOrCreate()") < script.index("import runpy")
    # app 인자는 spark-submit이 entry 스크립트 뒤에 그대로 붙여줘야 sys.argv로 전달된다.
    assert script.strip().endswith("--check-only --models rental --result-s3-key some/key.json")


def test_yarn_python_module_step_passes_env_via_am_env_conf() -> None:
    """학습 스텝의 LGB_NUM_MACHINES/LGB_TREE_LEARNER는 master 셸의 bash export가
    아니라 spark.yarn.appMasterEnv.*로 AM 컨테이너 안까지 넘어가야 한다."""
    _name, command = monthly_dag._yarn_python_module_step(
        "Train-rental-profile-a",
        "training.scripts.monthly_retrain_check",
        ["--execute"],
        env={"LGB_NUM_MACHINES": "8", "LGB_TREE_LEARNER": "data"},
    )
    script = command[2]
    assert "spark.yarn.appMasterEnv.LGB_NUM_MACHINES=8" in script
    assert "spark.yarn.appMasterEnv.LGB_TREE_LEARNER=data" in script


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
