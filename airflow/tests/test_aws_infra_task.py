"""aws_infra_task.py의 mock 기본값 반전과 신규 EMR 상시 클러스터/리사이즈/스텝
함수를 검증한다.

이 레포 airflow venv에는 moto가 없어(2026-08 확인) botocore 내장
`botocore.stub.Stubber`로 실제 boto3 호출 형태를 검증한다 — `_get_boto3_client`를
monkeypatch해서 Stubber로 감싼 클라이언트를 대신 반환하게 한다.
"""

from datetime import UTC, datetime

import boto3
import orchestration.aws_infra_task as infra
import pytest
from botocore.stub import Stubber


@pytest.fixture(autouse=True)
def _no_dummy_access_key(monkeypatch):
    """`is_emr_mock_mode()`가 access key로 mock을 추론하지 않도록 더미 값이 아닌
    값으로 고정한다 — 이 파일의 목적은 "실제 호출 경로"를 Stubber로 검증하는
    것이라 자동으로 mock으로 빠지면 안 된다."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "unittest-real")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "unittest-real-secret")
    monkeypatch.delenv("MOCK_AWS_INFRA", raising=False)


def _stub_emr_client(monkeypatch) -> Stubber:
    client = boto3.client("emr", region_name="ap-northeast-2")
    stubber = Stubber(client)
    monkeypatch.setattr(infra, "_get_boto3_client", lambda service_name, region_name=None: client)
    return stubber


# --- is_mock_mode() 기본값 반전 ---


def test_is_mock_mode_defaults_to_real_when_no_override():
    assert infra.is_mock_mode(None) is False


def test_is_mock_mode_force_mock_overrides_to_mock():
    assert infra.is_mock_mode(infra.MOCK_OVERRIDE_FORCE_MOCK) is True


def test_is_mock_mode_force_real_stays_real():
    assert infra.is_mock_mode(infra.MOCK_OVERRIDE_FORCE_REAL) is False


def test_is_mock_mode_env_var_forces_mock_regardless_of_override(monkeypatch):
    monkeypatch.setenv("MOCK_AWS_INFRA", "true")
    assert infra.is_mock_mode(infra.MOCK_OVERRIDE_FORCE_REAL) is True


# --- create_emr_cluster() ---


def test_create_emr_cluster_requests_persistent_cluster_with_required_tag(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("run_job_flow", {"JobFlowId": "j-created"})
    stubber.add_response(
        "describe_cluster",
        {"Cluster": {"Id": "j-created", "Name": "n", "Status": {"State": "WAITING"}}},
    )
    stubber.activate()

    cluster_id = infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert cluster_id == "j-created"
    stubber.assert_no_pending_responses()


def test_create_emr_cluster_sets_keep_alive_and_managed_policy_tag(monkeypatch):
    captured = {}
    client = boto3.client("emr", region_name="ap-northeast-2")

    def _capture_run_job_flow(**kwargs):
        captured.update(kwargs)
        return {"JobFlowId": "j-created", "ResponseMetadata": {}}

    monkeypatch.setattr(client, "run_job_flow", _capture_run_job_flow)
    monkeypatch.setattr(
        client,
        "describe_cluster",
        lambda **kwargs: {"Cluster": {"Status": {"State": "WAITING"}}},
    )
    monkeypatch.setattr(infra, "_get_boto3_client", lambda service_name, region_name=None: client)

    infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert captured["Instances"]["KeepJobFlowAliveWhenNoSteps"] is True
    assert {"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"} in captured["Tags"]
    assert captured["Instances"]["InstanceGroups"][1]["InstanceCount"] == 3
    assert "BootstrapActions" not in captured


def test_create_emr_cluster_adds_bootstrap_action_when_configured(monkeypatch):
    """AWS_EMR_BOOTSTRAP_SCRIPT_S3_URI가 설정되면 training 패키지를 까는
    bootstrap.sh가 BootstrapActions로 실려 가야 한다(evaluation/YARN
    distributed-shell 학습이 노드에서 직접 실행되므로)."""
    monkeypatch.setattr(infra, "EMR_BOOTSTRAP_SCRIPT_S3_URI", "s3://my-bucket/emr/bootstrap.sh")
    monkeypatch.setattr(infra, "EMR_PYFILES_S3_BUCKET", "my-bucket")
    captured = {}
    client = boto3.client("emr", region_name="ap-northeast-2")

    def _capture_run_job_flow(**kwargs):
        captured.update(kwargs)
        return {"JobFlowId": "j-created", "ResponseMetadata": {}}

    monkeypatch.setattr(client, "run_job_flow", _capture_run_job_flow)
    monkeypatch.setattr(
        client,
        "describe_cluster",
        lambda **kwargs: {"Cluster": {"Status": {"State": "WAITING"}}},
    )
    monkeypatch.setattr(infra, "_get_boto3_client", lambda service_name, region_name=None: client)

    infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    bootstrap_action = captured["BootstrapActions"][0]["ScriptBootstrapAction"]
    assert bootstrap_action["Path"] == "s3://my-bucket/emr/bootstrap.sh"
    assert bootstrap_action["Args"] == ["my-bucket"]


def test_create_emr_cluster_raises_on_early_termination(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("run_job_flow", {"JobFlowId": "j-created"})
    stubber.add_response(
        "describe_cluster",
        {"Cluster": {"Status": {"State": "TERMINATED_WITH_ERRORS"}}},
    )
    stubber.activate()

    with pytest.raises(RuntimeError, match="비정상 종료"):
        infra.create_emr_cluster(core_instance_count=3, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)


def test_create_emr_cluster_terminates_itself_on_waiting_timeout(monkeypatch):
    """WAITING 상태 도달 전에 타임아웃되면 cluster_id가 어디에도(XCom 등) 안 실린 채
    예외만 던져지므로, 이 함수가 직접 종료 요청까지 해야 한다 — 안 그러면 아무도
    이 클러스터를 못 찾아 계속 과금된다(run_emr_feature_mart_job()의 동일 패턴)."""
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("run_job_flow", {"JobFlowId": "j-created"})
    stubber.add_response("terminate_job_flows", {})
    stubber.activate()

    with pytest.raises(TimeoutError, match="WAITING 상태가 되지 못했습니다"):
        infra.create_emr_cluster(
            core_instance_count=3, timeout_seconds=0, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL
        )

    stubber.assert_no_pending_responses()


def test_create_emr_cluster_mock_mode_returns_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    cluster_id = infra.create_emr_cluster(mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK)

    assert cluster_id.startswith("mock-j-")


# --- get_core_instance_group_id() / resize_emr_cluster() ---


def test_get_core_instance_group_id_finds_core_group(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response(
        "list_instance_groups",
        {
            "InstanceGroups": [
                {"Id": "ig-master", "InstanceGroupType": "MASTER"},
                {"Id": "ig-core", "InstanceGroupType": "CORE"},
            ]
        },
    )
    stubber.activate()

    group_id = infra.get_core_instance_group_id("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert group_id == "ig-core"


def test_resize_emr_cluster_polls_until_target_running_count(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("modify_instance_groups", {})
    stubber.add_response(
        "list_instance_groups",
        {"InstanceGroups": [{"Id": "ig-core", "InstanceGroupType": "CORE", "RunningInstanceCount": 3}]},
    )
    stubber.add_response(
        "list_instance_groups",
        {"InstanceGroups": [{"Id": "ig-core", "InstanceGroupType": "CORE", "RunningInstanceCount": 8}]},
    )
    stubber.activate()
    monkeypatch.setattr(infra.time, "sleep", lambda _seconds: None)

    infra.resize_emr_cluster(
        "j-1", "ig-core", target_core_count=8, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL
    )

    stubber.assert_no_pending_responses()


def test_resize_emr_cluster_times_out_when_never_reaches_target(monkeypatch):
    """timeout_seconds=0이면 while 조건이 즉시 거짓이 되어 list_instance_groups
    호출 없이도 바로 TimeoutError가 나야 한다 — 실제 시간 경과에 의존하지 않는
    결정적(deterministic) 경계값 테스트다."""
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("modify_instance_groups", {})
    stubber.activate()

    with pytest.raises(TimeoutError, match="리사이즈되지 못했습니다"):
        infra.resize_emr_cluster(
            "j-1", "ig-core", target_core_count=8, timeout_seconds=0, mock_override=infra.MOCK_OVERRIDE_FORCE_REAL
        )


def test_resize_emr_cluster_mock_mode_returns_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    infra.resize_emr_cluster("j-1", "ig-core", target_core_count=8, mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK)


# --- submit_emr_step() ---


def test_submit_emr_step_returns_completed_state(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("add_job_flow_steps", {"StepIds": ["s-1"]})
    stubber.add_response(
        "describe_step",
        {"Step": {"Status": {"State": "COMPLETED"}}},
    )
    stubber.activate()

    result = infra.submit_emr_step(
        "j-1", "Eval", ["bash", "-c", "echo hi"], mock_override=infra.MOCK_OVERRIDE_FORCE_REAL
    )

    assert result == {"StepId": "s-1", "State": "COMPLETED"}


def test_submit_emr_step_polls_through_pending_and_running(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("add_job_flow_steps", {"StepIds": ["s-1"]})
    stubber.add_response("describe_step", {"Step": {"Status": {"State": "PENDING"}}})
    stubber.add_response("describe_step", {"Step": {"Status": {"State": "RUNNING"}}})
    stubber.add_response("describe_step", {"Step": {"Status": {"State": "COMPLETED"}}})
    stubber.activate()
    monkeypatch.setattr(infra.time, "sleep", lambda _seconds: None)

    result = infra.submit_emr_step(
        "j-1", "Eval", ["bash", "-c", "echo hi"], mock_override=infra.MOCK_OVERRIDE_FORCE_REAL
    )

    assert result["State"] == "COMPLETED"


def test_submit_emr_step_raises_on_failure(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("add_job_flow_steps", {"StepIds": ["s-1"]})
    stubber.add_response(
        "describe_step",
        {"Step": {"Status": {"State": "FAILED", "FailureDetails": {"Reason": "boom"}}}},
    )
    stubber.activate()

    with pytest.raises(RuntimeError, match="실패"):
        infra.submit_emr_step("j-1", "Eval", ["bash", "-c", "false"], mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)


def test_submit_emr_step_mock_mode_returns_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    result = infra.submit_emr_step("j-1", "Eval", ["bash", "-c", "echo hi"], mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK)

    assert result["State"] == "COMPLETED"
    assert result["StepId"].startswith("mock-s-")


# --- list_active_emr_clusters() ---


def test_list_active_emr_clusters_filters_by_name_prefix(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    created_at = datetime(2026, 8, 1, tzinfo=UTC)
    stubber.add_response(
        "list_clusters",
        {
            "Clusters": [
                {
                    "Id": "j-1",
                    "Name": "ml-monthly-retrain-rental",
                    "Status": {"State": "WAITING", "Timeline": {"CreationDateTime": created_at}},
                },
                {
                    "Id": "j-2",
                    "Name": "some-other-cluster",
                    "Status": {"State": "RUNNING", "Timeline": {"CreationDateTime": created_at}},
                },
            ]
        },
    )
    stubber.activate()

    clusters = infra.list_active_emr_clusters(mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert [c["id"] for c in clusters] == ["j-1"]
    assert clusters[0]["name"] == "ml-monthly-retrain-rental"
    assert clusters[0]["created_at"] == created_at


def test_list_active_emr_clusters_mock_mode_returns_empty_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    assert infra.list_active_emr_clusters(mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK) == []


# --- get_cluster_step_activity() ---


def test_get_cluster_step_activity_detects_active_step(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response(
        "list_steps",
        {
            "Steps": [
                {"Id": "s-old", "Status": {"State": "COMPLETED", "Timeline": {"EndDateTime": datetime(2026, 8, 25, tzinfo=UTC)}}},
                {"Id": "s-running", "Status": {"State": "RUNNING", "Timeline": {}}},
            ]
        },
    )
    stubber.activate()

    activity = infra.get_cluster_step_activity("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert activity["has_active_step"] is True


def test_get_cluster_step_activity_returns_last_completed_time_when_idle(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    earlier = datetime(2026, 8, 25, 10, 0, tzinfo=UTC)
    later = datetime(2026, 8, 25, 11, 0, tzinfo=UTC)
    stubber.add_response(
        "list_steps",
        {
            "Steps": [
                {"Id": "s-1", "Status": {"State": "COMPLETED", "Timeline": {"EndDateTime": earlier}}},
                {"Id": "s-2", "Status": {"State": "FAILED", "Timeline": {"EndDateTime": later}}},
            ]
        },
    )
    stubber.activate()

    activity = infra.get_cluster_step_activity("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert activity["has_active_step"] is False
    assert activity["last_step_completed_at"] == later


def test_get_cluster_step_activity_no_steps_yet(monkeypatch):
    stubber = _stub_emr_client(monkeypatch)
    stubber.add_response("list_steps", {"Steps": []})
    stubber.activate()

    activity = infra.get_cluster_step_activity("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_REAL)

    assert activity == {"has_active_step": False, "last_step_completed_at": None}


def test_get_cluster_step_activity_mock_mode_returns_idle_without_calling_aws(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError("mock 모드에서는 boto3 클라이언트를 만들면 안 됨")

    monkeypatch.setattr(infra, "_get_boto3_client", _fail)

    assert infra.get_cluster_step_activity("j-1", mock_override=infra.MOCK_OVERRIDE_FORCE_MOCK) == {
        "has_active_step": False,
        "last_step_completed_at": None,
    }
