# 두 EC2가 공유하는 데이터 접근 정책.
#
# 정책만 여기서 만들고 역할(Role)은 각 컴퓨트 파일에서 만든다. assume_role_policy가
# 주체마다 다르기 때문이다(EC2는 ec2.amazonaws.com, ECS Task는 ecs-tasks.amazonaws.com,
# EMR은 elasticmapreduce.amazonaws.com). 나중에 컴퓨트를 교체할 때 이 정책은 그대로 쓴다.
#
# 액세스 키를 쓰지 않는 것이 핵심이다. libs/core/src/core/s3.py가 S3_ENDPOINT_URL이
# 없을 때 자격증명 인자를 넘기지 않도록 고쳐져 있어(커밋 3627681), boto3가 instance
# profile을 credential chain에서 집는다.

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "data_access" {
  # 버킷 자체에 대한 조회 — 이 버킷 하나로 한정한다.
  statement {
    sid    = "ListOwnBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.data.arn]
  }

  # 객체 읽기/쓰기. bronze 조각 저장, silver/archive parquet, 피처마트, 모델, 설정 객체.
  statement {
    sid    = "ReadWriteObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }

  # POI Master는 content-addressed artifact와 append-only activation history다.
  # 실행 역할의 실수나 잘못된 정리 작업이 최초 게시 이후 static fixture로 되돌리는
  # 상태를 만들지 못하도록 이 네 namespace의 삭제를 명시적으로 거부한다.
  statement {
    sid    = "DenyDeletePoiMasterHistory"
    effect = "Deny"
    actions = [
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/silver/poi_master/*",
      "${aws_s3_bucket.data.arn}/source_snapshot_manifest/poi_master/*",
      "${aws_s3_bucket.data.arn}/source_snapshot_pointer/poi_master/*",
      "${aws_s3_bucket.data.arn}/source_snapshot_raw/poi_master/*",
    ]
  }

}

resource "aws_iam_policy" "data_access" {
  provider    = aws.untagged
  name        = "${var.project}-data-access"
  description = "S3 데이터 버킷 읽기/쓰기 + 설정 객체 복호화"
  policy      = data.aws_iam_policy_document.data_access.json
}

# Amazon Linux 2023 arm64. Graviton을 쓰는 근거는 variables.tf의 app_instance_type 참고.
# 아키텍처를 바꾸면(예: t3.large) AMI도 x86_64로 바뀌어 인스턴스가 재생성된다 —
# EBS까지 새로 만들어지므로 terraform plan에서 반드시 확인할 것.
data "aws_ami" "al2023_arm64" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-arm64"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

# EC2 접속용 키페어는 terraform이 만들지 않는다. `aws ec2 create-key-pair`로 AWS가
# 개인키를 직접 생성하게 하고(`var.ssh_key_name` 참고), 그 이름을 컴퓨트 리소스가
# 직접 참조한다.
#
# key_name은 RunInstances 시점에만 지정할 수 있는 불변 속성이라(cloud-init이 첫 부팅에
# 메타데이터에서 읽어 authorized_keys에 쓴다), 나중에 붙이려면 인스턴스를 재생성해야 한다.
