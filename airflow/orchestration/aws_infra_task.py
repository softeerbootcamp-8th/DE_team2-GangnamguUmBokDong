"""AWS EC2 및 EMR 인스턴스의 라이프사이클을 제어하고 원격 작업을 실행하는 오케스트레이션 유틸리티.

EC2(학습/평가)와 EMR(Spark 피처마트 생성)의 시작, 작업 실행, 상태 대기, 중지/종료를 관리하며,
작업 실패나 예외 상황에서도 클라우드 인스턴스가 켜져 있지 않도록 확실한 자원 반환을 보장한다.
AWS 자격증명이 없는 로컬/테스트 환경에서는 드라이런(Mock) 모드로 안전하게 동작한다.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# 기본 환경변수
DEFAULT_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
EC2_TRAINING_INSTANCE_ID = os.environ.get("AWS_EC2_TRAINING_INSTANCE_ID", "")
EMR_RELEASE_LABEL = os.environ.get("AWS_EMR_RELEASE_LABEL", "emr-7.2.0")
# 이 AWS 계정은 EMR에 m4.large 외 인스턴스 타입을 허용하지 않는다.
EMR_MASTER_INSTANCE_TYPE = os.environ.get("AWS_EMR_MASTER_INSTANCE_TYPE", "m4.large")
EMR_CORE_INSTANCE_TYPE = os.environ.get("AWS_EMR_CORE_INSTANCE_TYPE", "m4.large")
EMR_CORE_INSTANCE_COUNT = int(os.environ.get("AWS_EMR_CORE_INSTANCE_COUNT", "2"))
EMR_SERVICE_ROLE = os.environ.get("AWS_EMR_SERVICE_ROLE", "EMR_DefaultRole")
EMR_JOB_FLOW_ROLE = os.environ.get("AWS_EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
EMR_S3_SCRIPTS_PREFIX = os.environ.get("AWS_EMR_S3_SCRIPTS_PREFIX", "s3://local-dev/scripts")

# is_mock_mode()/is_emr_mock_mode()의 override 인자에 허용되는 값.
MOCK_OVERRIDE_FORCE_MOCK = "force_mock"
MOCK_OVERRIDE_FORCE_REAL = "force_real"


def is_mock_mode(override: str | None = None) -> bool:
    """EC2 학습을 실제로 호출할지 확인한다 — 운영에서도 기본값은 항상 mock이다.

    AWS 키가 진짜인지(instance profile/실제 access key)로는 판단하지 않는다 —
    운영 환경도 access key를 그냥 가지고 있을 수 있어서, 키의 진위로 판단하면
    아무도 명시적으로 요청하지 않았는데 실제 EC2 학습이 조용히 시작될 수 있다.
    실제 호출은 오직 `MOCK_OVERRIDE_FORCE_REAL`을 명시했을 때만 일어난다.

    args:
        override: DAG trigger 시점 파라미터 등으로 넘긴다.
            `MOCK_OVERRIDE_FORCE_REAL`이면 실제 호출, 그 외(`MOCK_OVERRIDE_FORCE_MOCK`
            포함, `None`도 포함)에는 전부 mock.
    """
    if os.environ.get("MOCK_AWS_INFRA", "").lower() in ("1", "true", "yes"):
        return True
    return override != MOCK_OVERRIDE_FORCE_REAL


def is_emr_mock_mode(override: str | None = None) -> bool:
    """EMR 피처마트 job 전용 mock 판별.

    `is_mock_mode()`와 분리한 이유: EMR은 `EC2_TRAINING_INSTANCE_ID`와 무관하고,
    운영은 access key 없이 EC2 instance profile로 인증하므로 빈 값을 mock 신호로
    보면 실제 EMR 클러스터가 떠야 할 때도 조용히 mock으로 빠진다.

    args:
        override: `is_mock_mode()`와 동일한 의미 (`MOCK_OVERRIDE_FORCE_MOCK`/
            `MOCK_OVERRIDE_FORCE_REAL`/`None`).
    """
    if override == MOCK_OVERRIDE_FORCE_MOCK:
        return True
    if override == MOCK_OVERRIDE_FORCE_REAL:
        return False
    if os.environ.get("MOCK_AWS_INFRA", "").lower() in ("1", "true", "yes"):
        return True
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    return access_key in ("minioadmin", "test", "dummy")


def _get_boto3_client(service_name: str, region_name: str | None = None) -> Any:
    """지정된 서비스의 boto3 클라이언트를 반환한다."""
    import boto3

    region = region_name or DEFAULT_REGION
    return boto3.client(service_name, region_name=region)


# ==========================================
# EC2 인스턴스 제어 유틸리티
# ==========================================


def start_ec2_instance(
    instance_id: str | None = None,
    *,
    timeout_seconds: int = 300,
    region_name: str | None = None,
    mock_override: str | None = None,
) -> str:
    """EC2 인스턴스를 시작하고 running 상태가 될 때까지 대기한다.

    args:
        instance_id: 대상 EC2 인스턴스 ID (미지정 시 AWS_EC2_TRAINING_INSTANCE_ID 사용)
        timeout_seconds: running 상태 진입 최대 대기 시간(초)
        region_name: AWS 리전명
        mock_override: `is_mock_mode()` 참고
    returns:
        인스턴스 ID
    raises:
        TimeoutError: 대기 시간 내에 running 상태에 도달하지 못한 경우
        RuntimeError: 시작 중 오류가 발생한 경우
    """
    target_id = instance_id or EC2_TRAINING_INSTANCE_ID
    if is_mock_mode(mock_override):
        logger.info("[Mock EC2] 인스턴스 '%s' 시작 완료 (Mock)", target_id or "mock-i-12345")
        return target_id or "mock-i-12345"

    if not target_id:
        raise ValueError("EC2 인스턴스 ID가 설정되지 않았습니다 (AWS_EC2_TRAINING_INSTANCE_ID 확인).")

    ec2 = _get_boto3_client("ec2", region_name)
    logger.info("[EC2] 인스턴스 '%s' 시작 요청...", target_id)
    ec2.start_instances(InstanceIds=[target_id])

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        resp = ec2.describe_instances(InstanceIds=[target_id])
        state = resp["Reservations"][0]["Instances"][0]["State"]["Name"]
        logger.info("[EC2] 인스턴스 '%s' 상태: %s", target_id, state)
        if state == "running":
            logger.info("[EC2] 인스턴스 '%s' 시작 완료 (running)", target_id)
            return target_id
        time.sleep(10)

    raise TimeoutError(f"EC2 인스턴스 '{target_id}'가 {timeout_seconds}초 내에 running 상태가 되지 못했습니다.")


def run_command_on_ec2(
    command: str,
    *,
    instance_id: str | None = None,
    working_dir: str | None = None,
    timeout_seconds: int = 7200,
    region_name: str | None = None,
    mock_override: str | None = None,
) -> dict[str, Any]:
    """AWS Systems Manager (SSM)을 통해 EC2 인스턴스에서 쉘 명령을 실행하고 결과를 수집한다.

    args:
        command: 실행할 쉘 명령어
        instance_id: 대상 EC2 인스턴스 ID
        working_dir: 작업 디렉터리 경로
        timeout_seconds: 명령 실행 최대 대기 시간(초)
        region_name: AWS 리전명
        mock_override: `is_mock_mode()` 참고
    returns:
        명령 실행 결과 딕셔너리 (Status, StandardOutputContent, StandardErrorContent 등)
    raises:
        RuntimeError: 명령이 실패하거나 타임아웃된 경우
    """
    target_id = instance_id or EC2_TRAINING_INSTANCE_ID
    if is_mock_mode(mock_override):
        logger.info("[Mock EC2] '%s'에서 명령 실행 (Mock): %s", target_id or "mock-i-12345", command)
        # check-only 명령 시뮬레이션 — 실제 평가 없이 항상 "성능 저하"로 간주해
        # EMR 피처마트 생성까지는 실제로 진행되게 한다(EMR은 is_emr_mock_mode()로
        # 별도 판별되므로 이 mock과 무관하게 실제로 뜰 수 있다).
        if "--check-only" in command:
            mock_output = json.dumps({
                "needs_retrain": True,
                "retrain_models": ["rental", "return"],
                "candidate_profiles": ["builtin-default"],
                "results": [],
            })
            return {"Status": "Success", "StandardOutputContent": mock_output, "StandardErrorContent": ""}
        # execute(실제 학습) 명령 시뮬레이션 — EC2에서 학습을 돌리지 않고,
        # `_attempt_promotion()`의 "3순위" 결과(챔피언보다 나은 후보가 없어 기존
        # 챔피언 유지)와 동일한 결론으로 처리한다. force_real 없이는 실제 학습이
        # 절대 일어나지 않으므로 매번 이 결론으로 끝난다.
        if "--execute" in command:
            mock_output = (
                "[Mock] 챌린저 학습을 실행하지 않음 — 챔피언보다 뛰어난 모델이 없다고 "
                "간주하여 기존 챔피언 유지, 다음 달에 재시도"
            )
            return {"Status": "Success", "StandardOutputContent": mock_output, "StandardErrorContent": ""}
        return {"Status": "Success", "StandardOutputContent": "Mock execution finished successfully", "StandardErrorContent": ""}

    if not target_id:
        raise ValueError("EC2 인스턴스 ID가 설정되지 않았습니다.")

    ssm = _get_boto3_client("ssm", region_name)
    commands = []
    if working_dir:
        commands.append(f"cd {working_dir}")
    commands.append(command)

    logger.info("[EC2 SSM] '%s'에 명령 전송: %s", target_id, command)
    send_resp = ssm.send_command(
        InstanceIds=[target_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
        TimeoutSeconds=timeout_seconds,
    )
    command_id = send_resp["Command"]["CommandId"]

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        time.sleep(10)
        inv_resp = ssm.get_command_invocation(
            CommandId=command_id,
            InstanceId=target_id,
        )
        status = inv_resp["Status"]
        logger.info("[EC2 SSM] 명령 '%s' 실행 상태: %s", command_id, status)
        if status in ("Success", "Cancelled", "TimedOut", "Failed"):
            stdout = inv_resp.get("StandardOutputContent", "")
            stderr = inv_resp.get("StandardErrorContent", "")
            if status != "Success":
                raise RuntimeError(
                    f"EC2 SSM 명령 실패 ({status}):\nSTDOUT: {stdout}\nSTDERR: {stderr}"
                )
            logger.info("[EC2 SSM] 명령 성공 완료")
            return inv_resp

    raise TimeoutError(f"EC2 SSM 명령이 {timeout_seconds}초 내에 완료되지 못했습니다.")


def stop_ec2_instance(
    instance_id: str | None = None,
    *,
    wait: bool = True,
    timeout_seconds: int = 300,
    region_name: str | None = None,
    mock_override: str | None = None,
) -> None:
    """EC2 인스턴스를 중지하고 필요 시 stopped 상태까지 대기한다.

    args:
        instance_id: 대상 EC2 인스턴스 ID
        wait: stopped 상태까지 대기할지 여부
        timeout_seconds: 대기 최대 시간(초)
        region_name: AWS 리전명
        mock_override: `is_mock_mode()` 참고
    """
    target_id = instance_id or EC2_TRAINING_INSTANCE_ID
    if is_mock_mode(mock_override):
        logger.info("[Mock EC2] 인스턴스 '%s' 중지 완료 (Mock)", target_id or "mock-i-12345")
        return

    if not target_id:
        logger.warning("[EC2] 중지할 인스턴스 ID가 지정되지 않았습니다.")
        return

    ec2 = _get_boto3_client("ec2", region_name)
    try:
        logger.info("[EC2] 인스턴스 '%s' 중지 요청...", target_id)
        ec2.stop_instances(InstanceIds=[target_id])
        if not wait:
            return

        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            resp = ec2.describe_instances(InstanceIds=[target_id])
            state = resp["Reservations"][0]["Instances"][0]["State"]["Name"]
            logger.info("[EC2] 인스턴스 '%s' 상태: %s", target_id, state)
            if state in ("stopped", "terminated"):
                logger.info("[EC2] 인스턴스 '%s' 중지 완료 (%s)", target_id, state)
                return
            time.sleep(10)
        logger.warning("[EC2] 인스턴스 '%s'가 %d초 내에 완전히 중지되지 않았습니다.", target_id, timeout_seconds)
    except Exception as exc:  # noqa: BLE001 - 자원 정리 실패가 상위 태스크를 깨뜨리지 않도록 로깅만 수행
        logger.error("[EC2] 인스턴스 '%s' 중지 중 오류 발생: %s", target_id, exc)


# ==========================================
# EMR 클러스터 제어 유틸리티
# ==========================================


def run_emr_feature_mart_job(
    profile_name: str,
    *,
    cluster_name: str | None = None,
    timeout_seconds: int = 5400,
    region_name: str | None = None,
    mock_override: str | None = None,
    master_instance_type: str | None = None,
    core_instance_type: str | None = None,
    core_instance_count: int | None = None,
) -> str:
    """Spark 피처마트 생성을 위한 Transient EMR 클러스터를 생성하고 완료까지 대기한다.

    피처 파이프라인(`run_pipeline.py`) 및 Multi-horizon 확장(`build_multi_horizon_features.py`)
    두 단계를 순차 실행하며, 작업 완료 또는 실패 시 클러스터가 자동으로 종료된다.

    args:
        profile_name: 적용할 ML 프로필 이름
        cluster_name: 클러스터 명칭 접두사
        timeout_seconds: 완료 대기 최대 시간(초)
        region_name: AWS 리전명
        mock_override: `is_mock_mode()` 참고
        master_instance_type: 미지정 시 `AWS_EMR_MASTER_INSTANCE_TYPE` 사용. 이 AWS
            계정은 EMR에 m4.large 외 타입을 허용하지 않으니 변경 시 계정 제약을 먼저 확인할 것.
        core_instance_type: 미지정 시 `AWS_EMR_CORE_INSTANCE_TYPE` 사용 (위와 동일한 제약)
        core_instance_count: 미지정 시 `AWS_EMR_CORE_INSTANCE_COUNT` 사용
    returns:
        생성된 EMR 클러스터(JobFlow) ID
    raises:
        RuntimeError: EMR 단계가 실패하거나 비정상 종료된 경우
        TimeoutError: 최대 대기 시간을 초과한 경우
    """
    if is_emr_mock_mode(mock_override):
        mock_job_id = f"mock-j-{int(time.time())}"
        logger.info("[Mock EMR] 프로필 '%s' 피처마트 생성 EMR 클러스터 실행 및 완료 (Mock: %s)", profile_name, mock_job_id)
        return mock_job_id

    master_type = master_instance_type or EMR_MASTER_INSTANCE_TYPE
    core_type = core_instance_type or EMR_CORE_INSTANCE_TYPE
    core_count = core_instance_count or EMR_CORE_INSTANCE_COUNT

    emr = _get_boto3_client("emr", region_name)
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    flow_name = cluster_name or f"ml-feature-mart-{profile_name}-{now_str}"

    steps = [
        {
            "Name": f"Spark-RunPipeline-{profile_name}",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "--conf",
                    f"spark.yarn.appMasterEnv.ML_PROFILE={profile_name}",
                    "--conf",
                    f"spark.executorEnv.ML_PROFILE={profile_name}",
                    f"{EMR_S3_SCRIPTS_PREFIX}/run_pipeline.py",
                ],
            },
        },
        {
            "Name": f"Spark-BuildMultiHorizon-{profile_name}",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "--conf",
                    f"spark.yarn.appMasterEnv.ML_PROFILE={profile_name}",
                    "--conf",
                    f"spark.executorEnv.ML_PROFILE={profile_name}",
                    f"{EMR_S3_SCRIPTS_PREFIX}/build_multi_horizon_features.py",
                ],
            },
        },
    ]

    job_flow_overrides = {
        "Name": flow_name,
        "ReleaseLabel": EMR_RELEASE_LABEL,
        "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": master_type,
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": core_type,
                    "InstanceCount": core_count,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
        },
        "Steps": steps,
        "JobFlowRole": EMR_JOB_FLOW_ROLE,
        "ServiceRole": EMR_SERVICE_ROLE,
        "AutoTerminate": True,
    }

    logger.info("[EMR] Transient 클러스터 '%s' 생성 요청...", flow_name)
    run_resp = emr.run_job_flow(**job_flow_overrides)
    cluster_id = run_resp["JobFlowId"]
    logger.info("[EMR] 클러스터 생성됨: %s", cluster_id)

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        desc = emr.describe_cluster(ClusterId=cluster_id)
        status = desc["Cluster"]["Status"]
        state = status["State"]
        logger.info("[EMR] 클러스터 '%s' 상태: %s", cluster_id, state)

        if state == "TERMINATED":
            # 정상 종료 여부 확인
            state_change_reason = status.get("StateChangeReason", {}).get("Code", "")
            if state_change_reason == "ALL_STEPS_COMPLETED":
                logger.info("[EMR] 클러스터 '%s' 모든 Step 정상 완료 후 자동 종료됨", cluster_id)
                return cluster_id
            raise RuntimeError(f"EMR 클러스터 '{cluster_id}' 비정상 종료: {status}")

        if state == "TERMINATED_WITH_ERRORS":
            raise RuntimeError(f"EMR 클러스터 '{cluster_id}' 오류로 종료됨: {status}")

        time.sleep(30)

    # 타임아웃 시 강제 종료
    terminate_emr_cluster(cluster_id, region_name=region_name, mock_override=mock_override)
    raise TimeoutError(f"EMR 클러스터 '{cluster_id}'가 {timeout_seconds}초 내에 완료되지 못했습니다.")


def terminate_emr_cluster(
    cluster_id: str | None, *, region_name: str | None = None, mock_override: str | None = None
) -> None:
    """EMR 클러스터를 강제 종료한다.

    args:
        cluster_id: 대상 EMR 클러스터 ID
        region_name: AWS 리전명
        mock_override: `is_mock_mode()` 참고
    """
    if is_emr_mock_mode(mock_override):
        logger.info("[Mock EMR] 클러스터 '%s' 종료 (Mock)", cluster_id or "mock-j-12345")
        return

    if not cluster_id:
        return

    emr = _get_boto3_client("emr", region_name)
    try:
        logger.info("[EMR] 클러스터 '%s' 강제 종료 요청...", cluster_id)
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
    except Exception as exc:  # noqa: BLE001 - 자원 정리 실패가 상위 태스크를 깨뜨리지 않도록 로깅만 수행
        logger.error("[EMR] 클러스터 '%s' 종료 중 오류 발생: %s", cluster_id, exc)
