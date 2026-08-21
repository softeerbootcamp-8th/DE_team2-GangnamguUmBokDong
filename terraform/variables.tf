variable "region" {
  description = "배포 리전. 부트캠프 계정이 서울로 제한되어 있다."
  type        = string
  default     = "ap-northeast-2"
}

variable "project" {
  description = "리소스 이름 접두사이자 태그. 공용 계정이라 우리 리소스를 식별할 수 있어야 한다."
  type        = string
  default     = "gng-ubd"
}

# --- 네트워크 ---

variable "vpc_cidr" {
  description = "새로 만드는 VPC의 CIDR. 기본 VPC(172.31.0.0/16)와 겹치지 않게 둔다."
  type        = string
  default     = "10.20.0.0/16"
}

variable "subnet_cidrs" {
  description = <<-EOT
    public 서브넷 2개의 CIDR. 첫 번째에만 실제 리소스가 들어가고 두 번째는 비어 있다 —
    RDS DB Subnet Group이 Single-AZ여도 서로 다른 AZ의 서브넷 2개를 요구하기 때문이다.
    빈 서브넷은 비용이 0이다.
  EOT
  type        = list(string)
  default     = ["10.20.0.0/24", "10.20.1.0/24"]

  validation {
    condition     = length(var.subnet_cidrs) == 2
    error_message = "RDS 서브넷 그룹 요건상 정확히 2개여야 합니다."
  }
}

variable "azs" {
  description = "subnet_cidrs와 순서를 맞춘 가용영역. 정찰로 2a/2b/2c/2d 가용을 확인했다."
  type        = list(string)
  default     = ["ap-northeast-2a", "ap-northeast-2c"]
}

variable "admin_cidrs" {
  description = <<-EOT
    SSH(22)를 열어줄 CIDR 목록. **비어 있으면 아무도 접속할 수 없다.**

    SSM이 이 계정에서 전면 거부되어 SSH가 유일한 접속 수단이다. 그래서 이 값은
    비워둘 수 없고, 접속하는 IP가 바뀔 때마다 갱신해야 한다:
      make allow-my-ip     # 현재 공인 IP로 admin_cidrs.auto.tfvars를 다시 쓰고 apply

    UI(8080·5000)는 여기서 열지 않는다. SSH 로컬 포트 포워딩으로 접근한다:
      ssh -N -L 8080:localhost:8080 -L 5000:localhost:5000 ec2-user@<eip>
    노출면을 22 하나로 줄이기 위해서다 — MLflow는 인증이 아예 없고, Airflow UI에서는
    DAG를 임의로 트리거해 OpenAPI 키 할당량을 소진시키거나 archive를 덮어쓸 수 있다.
  EOT
  type        = list(string)
  default     = []
}

# --- 컴퓨트 ---

variable "app_instance_type" {
  description = <<-EOT
    상시 EC2. Airflow 3종 + API + nginx + MLflow가 상주하고(~2.1GB) 그 위에
    BashOperator subprocess가 프로세스당 400MB~1.2GB로 뜬다. Graviton을 쓰는 이유는
    전 모듈 uv.lock에 aarch64 휠이 있어서다(LightGBM 포함).
    리허설에서 swap을 실제로 쓰기 시작하면 t4g.xlarge로 올린다 — 같은 계열이라 in-place다.
  EOT
  type        = string
  default     = "t4g.large"
}

variable "train_instance_type" {
  description = <<-EOT
    학습 EC2. 평소 정지 상태로 두고 학습할 때만 켠다.
    128GB는 "한 달 20분 anchor = peak 10.14GB"를 12개월로 외삽한 추정치다.
    1개월 smoke test로 실측한 뒤 r7g.2xlarge(64GB)나 r7g.8xlarge(256GB)로 조정한다.
  EOT
  type        = string
  default     = "r7g.4xlarge"
}

variable "app_root_volume_gb" {
  description = "상시 EC2 루트 볼륨. 모듈 venv 6개(~5GB) + airflow venv + 도커 이미지 + 로그."
  type        = number
  default     = 60
}

variable "train_root_volume_gb" {
  description = "학습 EC2 루트 볼륨. 정지 중에도 과금되는 유일한 항목(gp3 100GB ≈ 월 $9)."
  type        = number
  default     = 100
}

# --- 데이터 ---

variable "rds_instance_class" {
  description = <<-EOT
    app/airflow/mlflow 3개 DB를 한 인스턴스에서 돌린다. API(anyio 스레드풀 상한 ~40) +
    Airflow(~25) + MLflow(~10) + subprocess(~4)로 최악 ~79 커넥션이라 medium(상한 ~450)에
    여유가 있다. 정찰에서 db.t4g.medium 지원을 확인했다.
  EOT
  type        = string
  default     = "db.t4g.medium"
}

variable "rds_engine_version" {
  description = <<-EOT
    정찰로 확인한 사용 가능 버전은 16.9~16.14다. 어느 마이너가 PostGIS 3.5를 주는지는
    API로 알 수 없어 최신부터 시도한다 — ops/postgres/check_gold_schema.sql이 정확히
    3.5를 요구하므로, 생성 직후 pg_available_extension_versions로 확인하고 없으면
    16.13 → 16.12 순으로 내려가며 재생성한다(데이터가 없어 재생성이 싸다).
  EOT
  type        = string
  default     = "16.14"
}

variable "rds_storage_gb" {
  description = "RDS gp3 스토리지. 감소는 불가능하므로 넉넉하지 않게 시작한다."
  type        = number
  default     = 20
}

variable "s3_bucket_name" {
  description = "데이터 버킷 이름. 팀이 이미 만들어둔 버킷을 그대로 쓴다."
  type        = string
  default     = "gng-ubd-s3-bucket"
}

variable "ssh_public_key_path" {
  description = <<-EOT
    EC2에 등록할 SSH 공개키 경로.

    SSM Session Manager가 이 계정에서 전면 거부되어(StartSession·SendCommand·
    DescribeInstanceInformation 모두) SSH가 유일한 접속 수단이다. 로컬에서 만든
    키의 **공개키만** 등록하므로 개인키는 AWS를 거치지 않는다:
      ssh-keygen -t ed25519 -f ~/.ssh/gng-ubd
  EOT
  type        = string
  default     = "~/.ssh/gng-ubd.pub"
}
