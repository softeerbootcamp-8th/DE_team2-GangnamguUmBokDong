# EMR classic(on EC2) 피처마트 실행에 필요한 IAM 역할.
#
# 클러스터 자체는 Terraform으로 만들지 않는다. 1~2회만 쓰는 transient 클러스터라
# `aws emr create-cluster ... --auto-terminate`로 띄웠다 스텝이 끝나면 스스로 사라지는
# 편이 맞다. Terraform이 관리하면 상시 리소스처럼 취급되어 오히려 방치 위험이 커진다.
#
# EMR Serverless가 아니라 classic인 이유는 계정 정책상 Serverless가 거부되기 때문이지만,
# 이 코드에는 classic이 오히려 잘 맞는다 — spark_session.py가 EMR_RELEASE_LABEL을
# 전제하고(classic에서만 설정됨), 의존성을 bootstrap action에서 pip install로 넣을 수 있고,
# 마스터 노드에 접속해 디버깅할 수 있다.

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_service" {
  provider           = aws.untagged
  name               = "${var.project}-emr-service"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

# ⚠️ AmazonEMRServicePolicy_v2는 클러스터에
# `for-use-with-amazon-emr-managed-policies=true` 태그가 있어야 동작한다.
# create-cluster 시 --tags 로 반드시 붙일 것 (Makefile emr 타겟에 포함).
# Phase 4 소규모 검증에서 이 조합이 실제로 통과하는지 확인한다.
resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

# --- EMR 노드(EC2)가 쓰는 역할 ---

resource "aws_iam_role" "emr_ec2" {
  provider           = aws.untagged
  name               = "${var.project}-emr-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

# archive/silver를 읽고 피처마트를 쓴다. watermark.py가 Spark가 아니라 boto3로
# 워터마크 JSON을 읽으므로, instance profile 인증이 반드시 동작해야 한다.
resource "aws_iam_role_policy_attachment" "emr_ec2_data" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.data_access.arn
}

# 잡이 실패했을 때 마스터 노드에 들어가 확인하기 위해 붙여둔 정책이지만, 이 계정은
# SSM(StartSession·SendCommand·DescribeInstanceInformation)이 SCP로 전면 거부라
# 실제로는 동작하지 않는다(2026-08-21 실측, terraform.tfvars.example 참고). 마스터
# 노드 디버깅은 --ec2-attributes에 KeyName을 지정해 SSH로 접속하거나, 상시 EC2를
# bastion으로 한 ProxyJump를 쓸 것.
resource "aws_iam_role_policy_attachment" "emr_ec2_ssm" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "emr_ec2" {
  provider = aws.untagged
  name     = "${var.project}-emr-ec2"
  role     = aws_iam_role.emr_ec2.name
}

# --- 상시 EC2(Airflow)가 EC2/EMR을 직접 호출하는 데 필요한 권한 ---
#
# `airflow/orchestration/aws_infra_task.py`는 별도 access key 없이 상시 EC2의
# instance profile(`aws_iam_role.app`, compute_app.tf)로 boto3를 호출한다. 지금까지
# 이 role에는 S3 데이터 접근과 "이 인스턴스가 SSM으로 관리되는" 권한만 있었고,
# ec2:Start/StopInstances·elasticmapreduce:* 처럼 "이 인스턴스가 다른 리소스를
# 제어하는" 권한이 전혀 없었다 — 실제로 호출했다면 전부 AccessDenied였을 것이다
# (SSM SendCommand는 이 권한을 줘도 별도로 SCP가 계정 전체에서 막는다, emr_ec2_ssm
# 주석 참고 — 그래도 정책 자체는 갖춰 둔다).
#
# `elasticmapreduce:RunJobFlow`가 넘기는 ServiceRole/JobFlowRole을 실제로 assume
# 하려면 호출자에게 그 두 역할에 대한 iam:PassRole도 있어야 한다 — 이 role들은
# 바로 위 emr_service/emr_ec2이므로, `AWS_EMR_SERVICE_ROLE`/`AWS_EMR_JOB_FLOW_ROLE`
# 환경변수를 `terraform output emr_service_role`/`emr_instance_profile` 값으로
# 맞춰야 한다(app EC2의 .env는 Terraform이 아니라 `make deploy-env`가 S3에서 내려받으므로
# 이 파일에서 직접 wiring할 수 없다 — 배포 설정 쪽에서 반영할 것) — aws_infra_task.py의
# 기본값(`EMR_DefaultRole`/
# `EMR_EC2_DefaultRole`)은 AWS CLI `create-default-roles`가 만드는 별개 이름이라
# 이 계정에 그 기본 역할이 실제로 존재하는지 별도 확인이 필요하다.
data "aws_iam_policy_document" "airflow_infra_control" {
  statement {
    sid    = "Ec2TrainingLifecycle"
    effect = "Allow"
    actions = [
      "ec2:StartInstances",
      "ec2:StopInstances",
      "ec2:DescribeInstances",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "SsmRunCommand"
    effect = "Allow"
    actions = [
      "ssm:SendCommand",
      "ssm:GetCommandInvocation",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "EmrClusterLifecycle"
    effect = "Allow"
    actions = [
      "elasticmapreduce:RunJobFlow",
      # RunJobFlow 요청에 Tags를 같이 넣으면(aws_infra_task.create_emr_cluster()가
      # 항상 그렇게 함) EC2 RunInstances+CreateTags 조합과 같은 이유로 AddTags도
      # 별도로 필요하다 — 2026-08-25 첫 실제 실행에서 이게 없어서
      # AccessDeniedException으로 즉시 실패한 걸 실측으로 확인.
      "elasticmapreduce:AddTags",
      "elasticmapreduce:DescribeCluster",
      # emr_orphan_reaper.py의 list_active_emr_clusters()/get_cluster_step_activity()가
      # 쓰는 액션 — 이게 빠져있으면 15분마다 도는 안전망(reaper)이 매번 조용히
      # 실패해서 "EMR은 무슨 일이 있어도 삭제돼야 한다"는 요구사항이 실제로는
      # 전혀 보장되지 않는다(2026-08-25, 코드 대조로 발견 — 실측 에러는 아직
      # 안 남, RunJobFlow 실패가 먼저 나서 reaper 호출까지 못 가봤을 뿐).
      "elasticmapreduce:ListClusters",
      "elasticmapreduce:ListSteps",
      "elasticmapreduce:TerminateJobFlows",
      "elasticmapreduce:AddJobFlowSteps",
      "elasticmapreduce:DescribeStep",
      "elasticmapreduce:ModifyInstanceGroups",
      "elasticmapreduce:ListInstanceGroups",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "PassEmrRoles"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_service.arn, aws_iam_role.emr_ec2.arn]
  }

  # EMR은 클러스터 생성/정리에 계정 전역 서비스 연결 역할(service-linked role)
  # `AWSServiceRoleForEMRCleanup`이 필요한데, 이 계정엔 아직 없고 호출 주체에
  # 이걸 자동 생성할 권한도 없어서 RunJobFlow가 VALIDATION_ERROR로 즉시
  # TERMINATED_WITH_ERRORS 됐다(2026-08-25 첫 실제 실행, 실측 확인). AWS가
  # 권장하는 최소 권한 패턴대로 이 역할 하나만, EMR 서비스가 요청할 때만
  # 만들 수 있게 좁혀서 추가한다 — 한 번 생성되면 계정 전체에 영구히 남으므로
  # (서비스 연결 역할은 계정당 1개, 재사용됨) 이후로는 이 권한이 다시 쓰일
  # 일이 없다.
  statement {
    sid       = "EmrServiceLinkedRoleBootstrap"
    effect    = "Allow"
    actions   = ["iam:CreateServiceLinkedRole"]
    resources = ["arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com/AWSServiceRoleForEMRCleanup"]

    condition {
      test     = "StringEquals"
      variable = "iam:AWSServiceName"
      values   = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "airflow_infra_control" {
  provider = aws.untagged
  # 이름에 버전을 넣어 내용이 바뀔 때마다 IAM이 "새 정책 생성"으로 처리하게
  # 한다(2026-08-25) — 이 계정의 edu 사용자는 iam:CreatePolicy는 되지만
  # iam:CreatePolicyVersion(기존 정책 내용 수정)은 막혀있는 것으로 실측
  # 확인됨(AccessDeniedException). IAM은 정책 rename API가 아예 없어서
  # name이 바뀌면 terraform이 자동으로 삭제+재생성하므로, 내용을 고칠 때마다
  # 이 접미사 숫자를 올리면 CreatePolicyVersion을 아예 안 거치게 된다.
  name        = "${var.project}-airflow-infra-control-v3"
  description = "Airflow 상시 EC2가 학습 EC2/EMR 클러스터를 직접 제어하는 데 필요한 권한"
  policy      = data.aws_iam_policy_document.airflow_infra_control.json
}

resource "aws_iam_role_policy_attachment" "app_infra_control" {
  role       = aws_iam_role.app.name
  policy_arn = aws_iam_policy.airflow_infra_control.arn
}
