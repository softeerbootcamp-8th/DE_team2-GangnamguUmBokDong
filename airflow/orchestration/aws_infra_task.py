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
# m4.large는 EC2-Classic을 지원하지 않아 반드시 VPC 서브넷을 명시해야 한다 — 이게
# 없어서 첫 실제 실행에서 RunJobFlow가 "Subnet is required" VALIDATION_ERROR로
# 즉시 실패했다(2026-08-25, 실측). 계정/리전마다 값이 다른 실제 리소스 ID라
# 합리적인 기본값이 없으므로, terraform이 `config/prod.env`에 자동으로 채워주는
# 값(`aws_subnet.public[0].id`, `terraform/data.tf`)을 그대로 읽는다.
EMR_SUBNET_ID = os.environ.get("AWS_EMR_SUBNET_ID", "")
# Instances.EmrManagedMasterSecurityGroup/EmrManagedSlaveSecurityGroup을 안 넘기면
# EMR이 기본 보안그룹을 스스로 만들려고 하는데, 그러려면 ec2:CreateSecurityGroup을
# 호출할 VPC 자체에 `for-use-with-amazon-emr-managed-policies` 태그가 있어야 한다
# (AmazonEMRServicePolicy_v2의 조건부 권한) — 그 태그를 VPC 전체에 붙이는 대신,
# 미리 만들어 태그해둔 보안그룹 2개(terraform/emr.tf의 aws_security_group.emr_master/
# emr_core)를 명시적으로 넘겨서 이 문제 자체를 피한다(2026-08-25, 실측 확인).
EMR_MASTER_SG_ID = os.environ.get("AWS_EMR_MASTER_SG_ID", "")
EMR_CORE_SG_ID = os.environ.get("AWS_EMR_CORE_SG_ID", "")
# 기본값은 AWS CLI가 관례적으로 쓰는 이름(EMR_DefaultRole 등)이 아니라 이
# 프로젝트의 terraform(`terraform/emr.tf`)이 실제로 만드는 역할 이름
# (`${var.project}-emr-service`/`${var.project}-emr-ec2`, `variables.tf`의
# `project` 기본값 "gng-ubd" 기준)이다 — 이 계정은 공용 부트캠프 계정이라
# `aws emr create-default-roles`로 만드는 범용 기본 역할이 존재할 가능성이
# 낮고, 애초에 이 역할들을 쓰라고 terraform이 따로 만들어둔 것이기 때문이다.
# `var.project`를 다른 값으로 배포했다면 `terraform output -raw
# emr_service_role`/`emr_instance_profile` 값으로 이 두 환경변수를 override할 것.
EMR_SERVICE_ROLE = os.environ.get("AWS_EMR_SERVICE_ROLE", "gng-ubd-emr-service")
EMR_JOB_FLOW_ROLE = os.environ.get("AWS_EMR_JOB_FLOW_ROLE", "gng-ubd-emr-ec2")
EMR_S3_SCRIPTS_PREFIX = os.environ.get("AWS_EMR_S3_SCRIPTS_PREFIX", "s3://local-dev/scripts")

# `libs/ml_core/paths.py`의 MODELS_PREFIX/TRAINING_RUNS_PREFIX를 그대로 미러링한다.
# airflow venv는 lightgbm 등 무거운 의존성을 끌고 오는 ml_core/core를 설치하지
# 않으므로(`ml/feature_engine/spark/config.py`가 이미 같은 이유로 같은 상수를
# 독립적으로 다시 정의하는 것과 동일한 패턴), boto3만으로 직접 읽는다. 값이 서로
# 어긋나면 안 되므로 한쪽을 고치면 반드시 다른 쪽도 같이 고칠 것.
S3_BUCKET = os.environ.get("S3_BUCKET", "gangnamgu")
MODELS_PREFIX = os.environ.get("MODELS_PREFIX", "models")
TRAINING_RUNS_PREFIX = os.environ.get("TRAINING_RUNS_PREFIX", f"{MODELS_PREFIX}/training-runs")

# `make emr-package`가 올리는 위치(core/ml_core/feature_engine/training 번들 +
# bootstrap.sh)와 반드시 같은 값이어야 한다 — `create_emr_cluster()`의
# BootstrapActions가 이 스크립트를 실행해 상시 클러스터 노드에 `training` 패키지를
# 깐다(월간 재학습 evaluation·YARN distributed-shell 학습이 이 노드에서 직접
# 돌아야 하므로). 기본값은 `Makefile`의 `emr-package` 타겟이 실제로 업로드하는
# 경로(`s3://$BUCKET/emr/bootstrap.sh`, `s3://$BUCKET/emr/pyfiles.tar.gz`)와
# 똑같이 `S3_BUCKET`에서 유도한다 — 예전엔 빈 문자열이 기본값이라 아무도 이
# 두 환경변수를 안 채우면 BootstrapActions 없이 클러스터가 뜨고, 그러면 첫
# training 스텝이 "No module named 'training'"으로 조용히 실패했을 것이다.
EMR_BOOTSTRAP_SCRIPT_S3_URI = os.environ.get("AWS_EMR_BOOTSTRAP_SCRIPT_S3_URI", f"s3://{S3_BUCKET}/emr/bootstrap.sh")
EMR_PYFILES_S3_BUCKET = os.environ.get("AWS_EMR_PYFILES_S3_BUCKET", S3_BUCKET)

# `create_emr_cluster()`가 짓는 이름(`ml-monthly-retrain-{model_name}`)의 공통
# prefix — `list_active_emr_clusters()`가 이 값으로 "월간 재학습용" 클러스터만
# 걸러서 본다(다른 용도로 뜬 EMR까지 실수로 건드리지 않기 위함).
MONTHLY_RETRAIN_CLUSTER_NAME_PREFIX = os.environ.get(
    "AWS_MONTHLY_RETRAIN_CLUSTER_NAME_PREFIX", "ml-monthly-retrain-"
)
# `emr_orphan_reaper.py`가 종료 대상을 정하는 유일한 시간 기준: 클러스터에 지금
# 활성(PENDING/RUNNING) 스텝이 없고, 마지막 스텝이 끝난 지(스텝이 아예 없으면
# 클러스터 생성 이후) 이 시간이 지나야만 "재학습 루프가 정말 다 끝났다"고 보고
# 종료 대상으로 삼는다 — 스텝 사이 짧은 간격(리사이즈 대기 등)에 오검출로 죽이지
# 않기 위한 여유. **활성 스텝이 있는 클러스터는 나이·시간 기준과 무관하게 이
# reaper가 절대 건드리지 않는다** — 1년치 데이터를 단일 머신으로 학습하는 데
# 실측 24시간이 걸린 이력이 있어(2026-08), "N시간 넘으면 무조건 종료" 같은
# 시간 기반 절대 상한은 정상적으로 오래 걸리는 학습을 죽일 위험이 더 크다고
# 판단했다.
EMR_IDLE_GRACE_MINUTES = float(os.environ.get("AWS_EMR_IDLE_GRACE_MINUTES", "15"))

# is_mock_mode()/is_emr_mock_mode()의 override 인자에 허용되는 값.
MOCK_OVERRIDE_FORCE_MOCK = "force_mock"
MOCK_OVERRIDE_FORCE_REAL = "force_real"


def is_mock_mode(override: str | None = None) -> bool:
    """EC2 관련 호출을 실제로 할지 확인한다 — 기본값은 실제 호출이다(2026-08 반전).

    **이전에는 기본값이 mock이었다**: override 없으면 무조건 mock이라 배포된
    `monthly_retrain` DAG가 한 번도 실제로 학습/평가를 돈 적이 없었을 가능성이
    있었다(`docs/adr/0007-yarn-distributed-shell-workers.md` Context 참고). 이제는
    반대로 override 없으면 실제 AWS 호출이 나가고, `MOCK_OVERRIDE_FORCE_MOCK`을
    명시해야만(테스트/로컬 dry-run) 시뮬레이션으로 빠진다 — `is_emr_mock_mode()`와
    같은 판별 순서로 맞췄다.

    AWS 키가 진짜인지(instance profile/실제 access key)로는 판단하지 않는다 —
    운영 환경도 access key를 그냥 가지고 있을 수 있어서, 키의 진위로 판단하면
    실제 상황을 놓칠 수 있다.

    args:
        override: DAG trigger 시점 파라미터 등으로 넘긴다.
            `MOCK_OVERRIDE_FORCE_MOCK`이면 mock, 그 외(`MOCK_OVERRIDE_FORCE_REAL`
            포함, `None`도 포함)에는 전부 실제 호출.
    """
    if os.environ.get("MOCK_AWS_INFRA", "").lower() in ("1", "true", "yes"):
        return True
    return override == MOCK_OVERRIDE_FORCE_MOCK


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


def read_s3_json(key: str, *, region_name: str | None = None) -> dict | None:
    """`core.s3.read_json()`과 같은 버킷/엔드포인트 판별 규칙으로 JSON 객체를 읽는다.

    EMR 스텝(`command-runner.jar`)은 SSM과 달리 stdout을 호출부에 바로 돌려주지
    않으므로, `monthly_retrain_check.py --result-s3-key`가 이 함수가 읽는 위치에
    결과 요약을 남긴다(월간 재학습 DAG 참고). `core.s3._client()`/`get_object_bytes()`를
    그대로 재사용하지 않는 이유는 `TRAINING_RUNS_PREFIX` 주석 참고.

    args:
        key: 읽을 S3 객체의 전체 키
        region_name: AWS 리전명(엔드포인트 override가 없을 때만 의미 있음)
    returns:
        파싱된 JSON dict, 키가 없으면 None
    """
    import boto3
    from botocore.exceptions import ClientError

    endpoint_url = os.environ.get("S3_ENDPOINT_URL") or None
    if endpoint_url:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
    else:
        client = boto3.client("s3", region_name=region_name or DEFAULT_REGION)

    try:
        body = client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return json.loads(body)


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

    if not EMR_SUBNET_ID:
        raise RuntimeError(
            "AWS_EMR_SUBNET_ID가 비어 있습니다 — m4.large는 VPC 서브넷 지정 없이 못 뜹니다. "
            "terraform output -raw subnet_id 값을 config/prod.env(AWS_EMR_SUBNET_ID)에 채우세요."
        )
    if not EMR_MASTER_SG_ID or not EMR_CORE_SG_ID:
        raise RuntimeError(
            "AWS_EMR_MASTER_SG_ID/AWS_EMR_CORE_SG_ID가 비어 있습니다 — 이게 없으면 EMR이 기본 "
            "보안그룹을 스스로 만들려다 VPC 태그 조건에 막혀 실패합니다. terraform이 만든 "
            "aws_security_group.emr_master/emr_core의 ID를 config/prod.env에 채우세요."
        )

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
            "Ec2SubnetId": EMR_SUBNET_ID,
            "EmrManagedMasterSecurityGroup": EMR_MASTER_SG_ID,
            "EmrManagedSlaveSecurityGroup": EMR_CORE_SG_ID,
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
        },
        "Steps": steps,
        "JobFlowRole": EMR_JOB_FLOW_ROLE,
        "ServiceRole": EMR_SERVICE_ROLE,
        "LogUri": f"s3://{S3_BUCKET}/emr-logs/",
        "Tags": [{"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"}],
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


def list_active_emr_clusters(
    name_prefix: str = MONTHLY_RETRAIN_CLUSTER_NAME_PREFIX,
    *,
    region_name: str | None = None,
    mock_override: str | None = None,
) -> list[dict[str, Any]]:
    """`name_prefix`로 시작하는, 아직 살아있는(TERMINATED가 아닌) EMR 클러스터를 나열한다.

    `monthly_retrain` DAG 자신의 `terminate_cluster`(trigger_rule=ALL_DONE)는 그
    DAG 실행 자체가 계속 진행될 때만 보장된다 — 운영자가 DAG Run 전체를 수동으로
    "Mark Failed" 처리하면 Airflow가 이후 태스크를 더 스케줄링하지 않을 수 있어
    (버전에 따라 동작이 다르고 확실히 보장되지 않는다), 그 경우 이 안전망이 없으면
    EMR 클러스터가 아무도 모르게 계속 과금될 수 있다. 이 함수는 그 DAG 실행
    그래프와 완전히 독립적으로 실제 AWS 상태를 직접 조회하는 reaper용
    (`emr_orphan_reaper.py`)이다.

    args:
        name_prefix: 이 prefix로 시작하는 이름의 클러스터만 반환한다(다른 용도
            EMR까지 실수로 건드리지 않기 위함) — 기본값은 `create_emr_cluster()`가
            짓는 이름 규칙과 일치한다.
        region_name: AWS 리전명
        mock_override: `is_emr_mock_mode()` 참고 — mock이면 항상 빈 목록을 반환한다.
    returns:
        list[dict]: 각 원소는 {"id", "name", "state", "created_at"(tz-aware datetime)}
    """
    if is_emr_mock_mode(mock_override):
        return []

    emr = _get_boto3_client("emr", region_name)
    paginator = emr.get_paginator("list_clusters")
    clusters: list[dict[str, Any]] = []
    for page in paginator.paginate(
        ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING"]
    ):
        for cluster in page["Clusters"]:
            if not cluster["Name"].startswith(name_prefix):
                continue
            clusters.append({
                "id": cluster["Id"],
                "name": cluster["Name"],
                "state": cluster["Status"]["State"],
                "created_at": cluster["Status"]["Timeline"]["CreationDateTime"],
            })
    return clusters


def get_cluster_step_activity(
    cluster_id: str, *, region_name: str | None = None, mock_override: str | None = None
) -> dict[str, Any]:
    """이 클러스터에 지금 활성 스텝이 있는지, 마지막 스텝이 언제 끝났는지 확인한다.

    `emr_orphan_reaper.py`가 "재학습 DAG가 지금 이 클러스터를 실제로 쓰고
    있는가"를 판단하는 데 쓴다 — Airflow 자체에 물어보지 않는다. Airflow 3.x
    Task SDK는 태스크 프로세스 안에서 다른 DAG의 실행 상태를 직접(DB 조회)
    확인하는 지원 경로가 없다(태스크는 격리된 프로세스에서 API 서버와 HTTP로만
    통신 — 3.3.1 소스로 확인함, 2026-08). 대신 "이 EMR 클러스터에 지금 실행
    중이거나 대기 중인 스텝이 있는가"는 "그 DAG의 재학습 루프가 지금 이
    클러스터를 쓰고 있는가"와 사실상 같은 질문이라, EMR 쪽 실제 상태만으로
    충분하고 오히려 더 신뢰할 수 있다(Airflow 쪽 기록이 어떻게 꼬여있든 무관).

    args:
        cluster_id: 대상 EMR 클러스터 ID
        region_name: AWS 리전명
        mock_override: `is_emr_mock_mode()` 참고
    returns:
        dict: {"has_active_step": bool, "last_step_completed_at": datetime | None}
            스텝이 하나도 없거나 전부 아직 안 끝났으면 `last_step_completed_at`은
            None — 호출부가 클러스터 생성 시각으로 대신 판단해야 한다.
    """
    if is_emr_mock_mode(mock_override):
        return {"has_active_step": False, "last_step_completed_at": None}

    emr = _get_boto3_client("emr", region_name)
    steps = emr.list_steps(ClusterId=cluster_id)["Steps"]
    active_states = {"PENDING", "CANCEL_PENDING", "RUNNING"}
    has_active_step = any(step["Status"]["State"] in active_states for step in steps)
    end_times = [
        step["Status"]["Timeline"]["EndDateTime"]
        for step in steps
        if step["Status"]["Timeline"].get("EndDateTime") is not None
    ]
    return {
        "has_active_step": has_active_step,
        "last_step_completed_at": max(end_times) if end_times else None,
    }


def create_emr_cluster(
    *,
    cluster_name: str | None = None,
    core_instance_count: int | None = None,
    timeout_seconds: int = 1200,
    region_name: str | None = None,
    mock_override: str | None = None,
    master_instance_type: str | None = None,
    core_instance_type: str | None = None,
) -> str:
    """월간 재학습 사이클 전체가 공유하는 상시(long-lived) EMR 클러스터를 생성한다.

    `run_emr_feature_mart_job()`(스텝을 미리 심고 완료되면 자동 종료되는 transient
    클러스터)과 달리, 이 함수는 스텝 없이 `KeepJobFlowAliveWhenNoSteps=True`로만
    띄운다 — 평가 → (필요시) 재학습 루프 동안 `submit_emr_step()`으로 스텝을 하나씩
    얹고, 끝나면 호출부가 반드시 `terminate_emr_cluster()`를 불러야 한다(자동 종료
    없음, `docs/adr/0007-yarn-distributed-shell-workers.md` 참고).

    args:
        cluster_name: 클러스터 명칭(미지정 시 타임스탬프로 생성)
        core_instance_count: 미지정 시 `AWS_EMR_CORE_INSTANCE_COUNT` 사용 — 월간
            사이클은 피처마트 단계용으로 3개에서 시작해, 학습이 필요해지면
            `resize_emr_cluster()`로 늘리는 흐름을 가정한다.
        timeout_seconds: WAITING 상태 진입 최대 대기 시간(초)
        region_name: AWS 리전명
        mock_override: `is_emr_mock_mode()` 참고(EC2용 `is_mock_mode()`가 아니라
            EMR 전용 판별을 쓴다 — `run_emr_feature_mart_job()`과 동일한 이유)
        master_instance_type: 미지정 시 `AWS_EMR_MASTER_INSTANCE_TYPE` 사용(이 계정은
            m4.large 외 타입을 허용하지 않음)
        core_instance_type: 미지정 시 `AWS_EMR_CORE_INSTANCE_TYPE` 사용
    returns:
        생성된 EMR 클러스터(JobFlow) ID
    raises:
        RuntimeError: 클러스터가 WAITING에 도달하기 전에 종료됨
        TimeoutError: 최대 대기 시간 초과
    """
    if is_emr_mock_mode(mock_override):
        mock_cluster_id = f"mock-j-{int(time.time())}"
        logger.info("[Mock EMR] 상시 클러스터 '%s' 생성 (Mock: %s)", cluster_name or "monthly-retrain", mock_cluster_id)
        return mock_cluster_id

    if not EMR_SUBNET_ID:
        raise RuntimeError(
            "AWS_EMR_SUBNET_ID가 비어 있습니다 — m4.large는 VPC 서브넷 지정 없이 못 뜹니다. "
            "terraform output -raw subnet_id 값을 config/prod.env(AWS_EMR_SUBNET_ID)에 채우세요."
        )
    if not EMR_MASTER_SG_ID or not EMR_CORE_SG_ID:
        raise RuntimeError(
            "AWS_EMR_MASTER_SG_ID/AWS_EMR_CORE_SG_ID가 비어 있습니다 — 이게 없으면 EMR이 기본 "
            "보안그룹을 스스로 만들려다 VPC 태그 조건에 막혀 실패합니다. terraform이 만든 "
            "aws_security_group.emr_master/emr_core의 ID를 config/prod.env에 채우세요."
        )

    master_type = master_instance_type or EMR_MASTER_INSTANCE_TYPE
    core_type = core_instance_type or EMR_CORE_INSTANCE_TYPE
    core_count = core_instance_count or EMR_CORE_INSTANCE_COUNT

    emr = _get_boto3_client("emr", region_name)
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    flow_name = cluster_name or f"ml-monthly-retrain-{now_str}"

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
            "Ec2SubnetId": EMR_SUBNET_ID,
            "EmrManagedMasterSecurityGroup": EMR_MASTER_SG_ID,
            "EmrManagedSlaveSecurityGroup": EMR_CORE_SG_ID,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        "JobFlowRole": EMR_JOB_FLOW_ROLE,
        "ServiceRole": EMR_SERVICE_ROLE,
        # LogUri가 없으면 스텝이 실패해도 원인이 "Unknown Error"로만 나오고
        # "Step log files on S3 are only available for clusters which have
        # logging enabled"라고만 뜬다 — 실제 stdout/stderr을 볼 방법이 아예
        # 없다(2026-08-25, evaluate_rental 첫 실패에서 실측 확인).
        "LogUri": f"s3://{S3_BUCKET}/emr-logs/",
        "Tags": [{"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"}],
    }
    if EMR_BOOTSTRAP_SCRIPT_S3_URI:
        job_flow_overrides["BootstrapActions"] = [
            {
                "Name": "install-training-env",
                "ScriptBootstrapAction": {
                    "Path": EMR_BOOTSTRAP_SCRIPT_S3_URI,
                    "Args": [EMR_PYFILES_S3_BUCKET] if EMR_PYFILES_S3_BUCKET else [],
                },
            }
        ]

    logger.info("[EMR] 상시 클러스터 '%s' 생성 요청(core=%d)...", flow_name, core_count)
    run_resp = emr.run_job_flow(**job_flow_overrides)
    cluster_id = run_resp["JobFlowId"]
    logger.info("[EMR] 상시 클러스터 생성됨: %s", cluster_id)

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        status = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]
        state = status["State"]
        logger.info("[EMR] 클러스터 '%s' 상태: %s", cluster_id, state)
        if state == "WAITING":
            return cluster_id
        if state in ("TERMINATED", "TERMINATED_WITH_ERRORS"):
            # StateChangeReason(Code/Message)이 실제 원인(부트스트랩 실패, 용량 부족,
            # 설정 오류 등)을 담고 있는데 예전엔 이걸 버리고 상태값만 남겨서, 첫 실제
            # 실행에서 실패했을 때 콘솔을 따로 뒤져야 원인을 알 수 있었다(2026-08-25).
            reason = status.get("StateChangeReason", {})
            raise RuntimeError(
                f"EMR 클러스터 '{cluster_id}' 생성 중 비정상 종료: {state} "
                f"(code={reason.get('Code')}, message={reason.get('Message')})"
            )
        time.sleep(30)

    # 타임아웃 시 강제 종료 — 안 그러면 cluster_id가 호출부(DAG)의 XCom에 한 번도
    # 안 실리고 예외만 던져지므로, 이후 어떤 정리 태스크도 이 클러스터를 찾을 수
    # 없어 계속 과금되는 채로 방치된다(run_emr_feature_mart_job()의 동일 패턴 참고).
    terminate_emr_cluster(cluster_id, region_name=region_name, mock_override=mock_override)
    raise TimeoutError(f"EMR 클러스터 '{cluster_id}'가 {timeout_seconds}초 내에 WAITING 상태가 되지 못했습니다.")


def get_core_instance_group_id(
    cluster_id: str, *, region_name: str | None = None, mock_override: str | None = None
) -> str:
    """클러스터의 core InstanceGroup ID를 조회한다(`resize_emr_cluster()` 호출에 필요).

    args:
        cluster_id: 대상 EMR 클러스터 ID
        region_name: AWS 리전명
        mock_override: `is_emr_mock_mode()` 참고
    returns:
        core InstanceGroup ID
    """
    if is_emr_mock_mode(mock_override):
        return "mock-ig-core"

    emr = _get_boto3_client("emr", region_name)
    groups = emr.list_instance_groups(ClusterId=cluster_id)["InstanceGroups"]
    core_group = next((g for g in groups if g["InstanceGroupType"] == "CORE"), None)
    if core_group is None:
        raise RuntimeError(f"EMR 클러스터 '{cluster_id}'에 CORE 인스턴스 그룹이 없습니다: {groups}")
    return core_group["Id"]


def resize_emr_cluster(
    cluster_id: str,
    instance_group_id: str,
    *,
    target_core_count: int,
    timeout_seconds: int = 1200,
    region_name: str | None = None,
    mock_override: str | None = None,
) -> None:
    """실행 중인 EMR 클러스터의 core 인스턴스 그룹 크기를 죽이지 않고 조정한다.

    **스케일 업 전용으로 쓸 것**: 이 함수 자체는 target_core_count가 현재보다
    작아도 막지 않지만, 스케일 다운은 진행 중인 YARN 컨테이너를 강제로 죽일 수
    있어 위험하다 — 월간 재학습 DAG는 한 사이클 안에서 3→8로 한 번만 늘리고 그
    사이클이 끝날 때까지(클러스터 종료 시까지) 다시 줄이지 않는다(계획 6번 항목).

    args:
        cluster_id: 대상 EMR 클러스터(JobFlow) ID
        instance_group_id: 리사이즈할 core InstanceGroup ID(`get_core_instance_group_id()`로 조회)
        target_core_count: 목표 core 인스턴스 개수
        timeout_seconds: 목표 개수만큼 RUNNING 상태 도달 대기 최대 시간(초)
        region_name: AWS 리전명
        mock_override: `is_emr_mock_mode()` 참고
    raises:
        TimeoutError: 대기 시간 내에 목표 개수만큼 RUNNING 상태가 되지 않음
    """
    if is_emr_mock_mode(mock_override):
        logger.info(
            "[Mock EMR] 클러스터 '%s' core 그룹 '%s'를 %d개로 리사이즈 (Mock)",
            cluster_id,
            instance_group_id,
            target_core_count,
        )
        return

    emr = _get_boto3_client("emr", region_name)
    logger.info(
        "[EMR] 클러스터 '%s' core 그룹 '%s'를 %d개로 리사이즈 요청...", cluster_id, instance_group_id, target_core_count
    )
    emr.modify_instance_groups(
        InstanceGroups=[{"InstanceGroupId": instance_group_id, "InstanceCount": target_core_count}]
    )

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        groups = emr.list_instance_groups(ClusterId=cluster_id)["InstanceGroups"]
        group = next(g for g in groups if g["Id"] == instance_group_id)
        running_count = group["RunningInstanceCount"]
        logger.info("[EMR] 클러스터 '%s' core 그룹 RUNNING 인스턴스: %d/%d", cluster_id, running_count, target_core_count)
        if running_count >= target_core_count:
            logger.info("[EMR] 클러스터 '%s' 리사이즈 완료(%d개 RUNNING)", cluster_id, target_core_count)
            return
        time.sleep(30)

    raise TimeoutError(
        f"EMR 클러스터 '{cluster_id}' core 그룹이 {timeout_seconds}초 내에 "
        f"{target_core_count}개로 리사이즈되지 못했습니다."
    )


def submit_emr_step(
    cluster_id: str,
    name: str,
    command: list[str],
    *,
    action_on_failure: str = "CONTINUE",
    timeout_seconds: int = 5400,
    region_name: str | None = None,
    mock_override: str | None = None,
) -> dict[str, Any]:
    """이미 떠 있는(KeepJobFlowAliveWhenNoSteps=True) EMR 클러스터에 범용 스텝
    하나를 제출하고 완료까지 대기한다.

    `run_emr_feature_mart_job()`은 클러스터 생성+스텝+자동종료를 한 번에 묶지만,
    이 함수는 `create_emr_cluster()`로 띄워둔 상시 클러스터에 평가/피처마트/YARN
    distributed-shell 학습 스텝을 하나씩 얹는 범용 진입점이다(계획 4번 항목).

    args:
        cluster_id: 대상 EMR 클러스터 ID
        name: 스텝 이름(EMR 콘솔/로그 표시용)
        command: command-runner.jar에 넘길 인자 목록(예: `["bash", "-c", "..."]`)
        action_on_failure: 기본값 "CONTINUE" — 평가/재학습 루프 중 스텝 하나가
            실패해도 클러스터는 살려두고 다음 후보 프로필을 계속 시도해야 하므로
            `run_emr_feature_mart_job()`의 "TERMINATE_CLUSTER"와 다르게 잡았다.
        timeout_seconds: 스텝 완료 대기 최대 시간(초)
        region_name: AWS 리전명
        mock_override: `is_emr_mock_mode()` 참고
    returns:
        dict: {"StepId": ..., "State": "COMPLETED"}
    raises:
        RuntimeError: 스텝이 실패/취소됨
        TimeoutError: 대기 시간 초과
    """
    if is_emr_mock_mode(mock_override):
        mock_step_id = f"mock-s-{int(time.time())}"
        logger.info("[Mock EMR] 클러스터 '%s'에 스텝 '%s' 제출 및 완료 (Mock: %s)", cluster_id, name, mock_step_id)
        return {"StepId": mock_step_id, "State": "COMPLETED"}

    emr = _get_boto3_client("emr", region_name)
    logger.info("[EMR] 클러스터 '%s'에 스텝 '%s' 제출...", cluster_id, name)
    resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": name,
                "ActionOnFailure": action_on_failure,
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": command},
            }
        ],
    )
    step_id = resp["StepIds"][0]

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        desc = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = desc["Step"]["Status"]["State"]
        logger.info("[EMR] 스텝 '%s'(%s) 상태: %s", name, step_id, state)
        if state == "COMPLETED":
            return {"StepId": step_id, "State": state}
        if state in ("FAILED", "CANCELLED", "INTERRUPTED"):
            failure = desc["Step"]["Status"].get("FailureDetails", {})
            raise RuntimeError(f"EMR 스텝 '{name}'({step_id}) 실패({state}): {failure}")
        time.sleep(15)

    raise TimeoutError(f"EMR 스텝 '{name}'({step_id})이 {timeout_seconds}초 내에 완료되지 못했습니다.")
