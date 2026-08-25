# 데이터 계층 — S3 버킷, KMS 키, RDS, 그리고 설정 객체.
#
# 시크릿을 SSM Parameter Store나 Secrets Manager에 두지 않는다. 정찰 결과 두 서비스가
# 모두 계정 정책상 거부라 S3 객체로 대체한다. kms:CreateKey도 거부여서 고객 관리 키는
# 못 만들고, 버킷 기본 암호화(SSE-S3)를 쓴다.

# --- S3 ---

locals {
  legacy_bronze_sources = toset([
    "bike_rental_history",
    "bike_station_master",
    "bike_station_realtime",
    "cultural_event",
    "living_population_grid",
    "performance_event",
    "population_realtime",
    "weather_short_term_forecast",
    "weather_ultra_short_forecast",
    "weather_ultra_short_live",
  ])
}

resource "aws_s3_bucket" "data" {
  bucket = var.s3_bucket_name

  # 일부러 false로 둔다. true면 terraform destroy가 버킷 내용을 조용히 지운다.
  # 객체가 있으면 destroy가 실패해 사고를 막는다.
  force_destroy = false

  # raw-data/의 원본 ZIP 12GB는 팀원에게 다시 받아야 하는 데이터다. force_destroy가
  # 막지 못하는 경우(빈 버킷, 이름 변경으로 인한 재생성)까지 걸러내려고 한 겹 더 둔다.
  # destroy 계획이 세워지는 순간 apply 전에 멈춘다. 정말 지울 때는 이 블록을 지운다.
  lifecycle {
    prevent_destroy = true
  }

  tags = { Name = var.s3_bucket_name }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  # collector가 응답 도착 즉시 조각으로 쓰는 원본. 날짜 단위 Cold Bronze 생성과
  # 검증에 충분한 유예를 둔 뒤 Hot 객체만 지운다.
  rule {
    id     = "expire-bronze"
    status = "Enabled"

    filter {
      prefix = "bronze/hot/"
    }

    expiration {
      days = 30
    }
  }

  # 이관 전 legacy Bronze는 기존과 같은 30일 보존 정책을 유지한다. cold/와
  # cold_manifest/는 이 prefix에 들어오지 않아 장기 보관된다.
  dynamic "rule" {
    for_each = local.legacy_bronze_sources
    content {
      id     = "expire-legacy-bronze-${rule.value}"
      status = "Enabled"

      filter {
        prefix = "bronze/${rule.value}/"
      }

      expiration {
        days = 30
      }
    }
  }

  # 중단된 멀티파트 업로드가 과금되며 남지 않게 한다(15GB 적재 중 끊길 수 있다).
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# KMS 고객 관리 키를 만들지 않는다 — 이 계정은 kms:CreateKey가 거부된다(2026-08-21 실측).
# 설정 객체는 버킷 기본 암호화(SSE-S3, 위 server_side_encryption_configuration)로 보호되고,
# 접근 통제는 인스턴스 역할 + 버킷 정책 + Block Public Access가 담당한다.

# --- RDS ---

resource "aws_db_subnet_group" "main" {
  name = "${var.project}-db-subnet-group"
  # Single-AZ여도 서로 다른 AZ의 서브넷 2개를 요구한다. 두 번째 서브넷이 존재하는 유일한 이유다.
  subnet_ids = aws_subnet.public[*].id

  tags = { Name = "${var.project}-db-subnet-group" }
}

resource "aws_db_parameter_group" "main" {
  name   = "${var.project}-pg16"
  family = "postgres16"

  # 1초 넘는 쿼리만 기록한다. Airflow 메타DB가 5분마다 쓰기 때문에 전량 로깅은 과하다.
  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "random_password" "db" {
  # 영숫자만 쓴다(32자 ≈ 190비트로 충분히 강하다). 특수문자를 넣으면 두 곳이 깨진다:
  #   1) DATABASE_URL — #·?·& 가 URL 파싱을 깨뜨려 psycopg가 DSN을 잘못 읽는다
  #   2) /opt/app/.env — (·)·$·& 가 셸 메타문자라 `. .env`(source)가 문법 오류를 낸다
  #      (Makefile의 deploy-db-bootstrap/deploy-db-check가 이 방식으로 읽는다)
  length  = 32
  special = false
}

resource "aws_db_instance" "main" {
  identifier = "${var.project}-db"

  engine         = "postgres"
  engine_version = var.rds_engine_version
  instance_class = var.rds_instance_class

  allocated_storage = var.rds_storage_gb
  storage_type      = "gp3"
  storage_encrypted = true

  # RDS는 초기 DB를 하나만 만든다. airflow/mlflow DB는 ops/postgres/bootstrap_rds.sh가 만든다.
  db_name  = "app"
  username = "postgres"
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.main.name

  # public 서브넷에 있지만 퍼블릭 IP도 DNS도 할당하지 않는다. 인터넷에서 이름을 찾을
  # 방법 자체가 없고, 접근은 sg-rds(5432 ← sg-app)로만 가능하다.
  publicly_accessible = false

  multi_az = false
  # 0이면 실수 시 복구 수단이 아예 없다. 1일치는 이 스토리지 크기에서 사실상 무료다.
  backup_retention_period = 1
  # 데모 중 예고 없는 재시작을 막는다.
  auto_minor_version_upgrade = false
  deletion_protection        = false
  skip_final_snapshot        = true
  # PostGIS 3.5 확인 결과에 따라 엔진 버전을 바꿔야 할 수 있어 즉시 반영한다.
  apply_immediately = true

  tags = { Name = "${var.project}-db" }
}

# --- 운영 설정 객체 ---
#
# 이 객체에는 API 키를 넣지 않는다. SEOUL_OPENAPI_KEY와 KMA_APIHUB_KEY는 사람이
# config/secrets.env로 따로 올리고, render_env.sh가 둘을 합쳐 /opt/app/.env를 만든다.
# 그래야 코드·tfvars·tfstate 어디에도 API 키가 남지 않는다.

locals {
  db_url_base = "${aws_db_instance.main.address}:${aws_db_instance.main.port}"

  prod_env = <<-EOT
    # Terraform이 생성한 운영 설정. 직접 수정하지 말 것 — 다음 apply에 덮어써진다.
    # API 키는 config/secrets.env에 따로 있고 render_env.sh가 합친다.

    POSTGRES_USER=${aws_db_instance.main.username}
    POSTGRES_APP_DB=app
    POSTGRES_AIRFLOW_DB=airflow
    POSTGRES_MLFLOW_DB=mlflow

    # ops/postgres/bootstrap_rds.sh와 check_gold_schema.sh가 psql 표준 변수로 읽는다.
    # 두 스크립트는 EC2에서 도는데 그곳엔 terraform이 없으므로 여기서 넘겨준다.
    PGHOST=${aws_db_instance.main.address}
    PGPORT=${aws_db_instance.main.port}
    PGPASSWORD=${random_password.db.result}
    PGSSLMODE=require

    DATABASE_URL=postgresql://${aws_db_instance.main.username}:${random_password.db.result}@${local.db_url_base}/app?sslmode=require
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${aws_db_instance.main.username}:${random_password.db.result}@${local.db_url_base}/airflow?sslmode=require
    MLFLOW_BACKEND_STORE_URI=postgresql://${aws_db_instance.main.username}:${random_password.db.result}@${local.db_url_base}/mlflow

    AIRFLOW_ADMIN_USER=admin
    AIRFLOW_JWT_SECRET=${random_password.airflow_jwt.result}
    AIRFLOW__WEBSERVER__SECRET_KEY=${random_password.airflow_webserver.result}

    S3_BUCKET=${aws_s3_bucket.data.id}
    MODELS_PREFIX=models
    # m4.large는 VPC 서브넷 지정 없이 못 뜬다 — 계정/리전마다 다른 실제 리소스
    # ID라 aws_infra_task.py에 합리적인 기본값을 둘 수 없어 여기서 채운다
    # (2026-08-25, 첫 실제 EMR 실행에서 이게 빠져 실패한 걸 발견).
    AWS_EMR_SUBNET_ID=${aws_subnet.public[0].id}
    GOLD_STATION_MASTER_LOOKBACK_HOURS=168
    GOLD_STATION_REALTIME_LOOKBACK_HOURS=24

    # 컨테이너 안에서 mlflow 서비스는 compose 네트워크 이름으로 붙는다.
    # 학습 EC2는 이 값 대신 상시 EC2의 사설 IP를 쓴다.
    # 끝의 /mlflow는 오타가 아니다 — 트래킹 서버를 --static-prefix /mlflow로 띄워
    # UI를 web(nginx)의 /mlflow/ 아래에 붙였고, API 경로도 같은 접두를 갖는다.
    MLFLOW_TRACKING_URI=http://mlflow:5000/mlflow
  EOT
}

resource "random_password" "airflow_jwt" {
  length  = 64
  special = false
}

resource "random_password" "airflow_webserver" {
  length  = 48
  special = false
}

resource "aws_s3_object" "prod_env" {
  bucket = aws_s3_bucket.data.id
  key    = "config/prod.env"

  content_type = "text/plain"
  content      = local.prod_env

  # 버킷 기본 암호화(SSE-S3)가 적용된다.
}
