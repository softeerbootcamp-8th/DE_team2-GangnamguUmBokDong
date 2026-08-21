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
  name               = "${var.project}-emr-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

# archive/silver를 읽고 피처마트를 쓴다. watermark.py가 Spark가 아니라 boto3로
# 워터마크 JSON을 읽으므로, instance profile 인증이 반드시 동작해야 한다.
resource "aws_iam_role_policy_attachment" "emr_ec2_data" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.data_access.arn
}

# 잡이 실패했을 때 마스터 노드에 들어가 확인하기 위한 것. classic을 택한 주요 이점 중
# 하나이고, SSH 키 없이 Session Manager로 접속할 수 있다.
resource "aws_iam_role_policy_attachment" "emr_ec2_ssm" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.project}-emr-ec2"
  role = aws_iam_role.emr_ec2.name
}
