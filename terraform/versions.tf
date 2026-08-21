# state 버킷은 Terraform 밖에서 CLI로 먼저 만든다 — "state를 저장할 버킷을
# Terraform으로 만들려면 state가 필요하다"는 순환을 피하기 위해서다.
#
#   aws s3 mb s3://<state-bucket> --region ap-northeast-2
#   aws s3api put-bucket-versioning --bucket <state-bucket> \
#     --versioning-configuration Status=Enabled
#
# 버저닝은 필수다. 4명이 같은 state를 쓰므로 잘못된 apply를 되돌릴 유일한 수단이다.
#
# 초기화:
#   terraform init -backend-config=backend.hcl
# 검증만 할 때(백엔드 없이):
#   terraform init -backend=false && terraform validate

terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "s3" {
    # 값은 backend.hcl로 주입한다(버킷 이름이 환경마다 다르고 커밋하지 않기 때문).
    key = "demo/terraform.tfstate"

    # DynamoDB 락 테이블 대신 S3 조건부 쓰기 기반 락을 쓴다(Terraform 1.10+).
    # 리소스 하나와 그에 딸린 부트스트랩 단계가 통째로 사라진다.
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Env       = "demo"
    }
  }
}

# 이 계정은 iam:TagRole · iam:TagPolicy · iam:TagInstanceProfile · kms:TagResource가
# 모두 거부된다(2026-08-21 실측). default_tags가 붙으면 리소스 **생성 자체가** 403으로
# 실패하므로, 태그를 달 수 없는 리소스는 이 alias를 지정해 만든다.
#
# S3·VPC·EC2·RDS 태깅은 허용되므로 그쪽은 기본 provider를 그대로 쓴다.
provider "aws" {
  alias  = "untagged"
  region = var.region
}
