# VPC와 보안 경계.
#
# NAT Gateway를 쓰지 않는다. 두 EC2 모두 인터넷 아웃바운드가 필요하지만(상시는 수집
# API, 학습은 PyPI), NAT는 월 $45로 상시 EC2 비용과 맞먹는다. 대신 public 서브넷 +
# 퍼블릭 IP로 두고 인바운드를 보안 그룹으로 막는다.
#
# public 서브넷에 둬도 안전한 이유: 인스턴스가 인터넷에서 도달 가능하려면 ①라우팅
# ②퍼블릭 IP ③**SG 인바운드 허용** ④NACL이 모두 필요하다. SG는 기본이 전부 거부이고
# stateful이라, 인바운드 규칙이 0개면 아웃바운드 응답만 돌아온다. 학습 EC2가 정확히
# 그 상태이고, RDS는 publicly_accessible=false라 퍼블릭 DNS 자체가 없다.

resource "aws_vpc" "main" {
  cidr_block = var.vpc_cidr

  # SSM Session Manager와 RDS 엔드포인트 해석에 필요하다.
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.project}-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project}-igw" }
}

resource "aws_subnet" "public" {
  count = length(var.subnet_cidrs)

  vpc_id            = aws_vpc.main.id
  cidr_block        = var.subnet_cidrs[count.index]
  availability_zone = var.azs[count.index]

  # EC2에 퍼블릭 IP를 자동 할당한다. 노출 통제는 SG가 한다.
  map_public_ip_on_launch = true

  tags = { Name = "${var.project}-public-${var.azs[count.index]}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project}-public-rt" }
}

# 이 한 줄이 "public 서브넷"의 정의다 — AWS에 public/private이라는 설정 항목은 없다.
resource "aws_route" "default_igw" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# S3 트래픽을 IGW가 아니라 AWS 내부망으로 보낸다. Gateway 타입은 요금이 0이고,
# 나중에 private 서브넷으로 옮기면 S3 트래픽에 대한 NAT 요금을 아껴준다.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = { Name = "${var.project}-s3-endpoint" }
}

# --- 보안 그룹 ---

# AWS는 GroupDescription에 ASCII만 허용한다 — 한글 설명은 주석으로 남긴다.
# 상시 EC2: Airflow 3종 + API + nginx/web + MLflow
resource "aws_security_group" "app" {
  name        = "${var.project}-app"
  description = "Always-on EC2: Airflow, API, web, MLflow"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-app" }
}

# 학습 EC2. 인바운드 규칙이 하나도 없어 퍼블릭 IP가 있어도 도달 불가.
resource "aws_security_group" "train" {
  name        = "${var.project}-train"
  description = "Training EC2. No inbound rules; access via SSM Session Manager only"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-train" }
}

# RDS. app SG에서만 5432로 접근 가능.
resource "aws_security_group" "rds" {
  name        = "${var.project}-rds"
  description = "RDS PostgreSQL. Reachable only from the app security group"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project}-rds" }
}

# 웹 대시보드는 공개해야 한다.
resource "aws_vpc_security_group_ingress_rule" "app_http" {
  security_group_id = aws_security_group.app.id
  description       = "dashboard"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

# Airflow UI. admin_cidrs가 비어 있으면 규칙이 아예 생기지 않는다(= SSM 터널 전용).
resource "aws_vpc_security_group_ingress_rule" "app_airflow_ui" {
  for_each = toset(var.admin_cidrs)

  security_group_id = aws_security_group.app.id
  description       = "airflow ui"
  cidr_ipv4         = each.value
  from_port         = 8080
  to_port           = 8080
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "app_mlflow_ui" {
  for_each = toset(var.admin_cidrs)

  security_group_id = aws_security_group.app.id
  description       = "mlflow ui"
  cidr_ipv4         = each.value
  from_port         = 5000
  to_port           = 5000
  ip_protocol       = "tcp"
}

# SSH. SSM이 이 계정에서 전면 거부되어 유일한 접속 수단이다.
# admin_cidrs가 비어 있으면 규칙이 생기지 않으므로, 접속 전에 반드시 채워야 한다
# (make allow-my-ip가 현재 공인 IP로 갱신해준다).
resource "aws_vpc_security_group_ingress_rule" "app_ssh" {
  for_each = toset(var.admin_cidrs)

  security_group_id = aws_security_group.app.id
  description       = "ssh"
  cidr_ipv4         = each.value
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

# 학습 EC2는 인터넷에 22를 열지 않는다. 상시 EC2를 bastion으로 경유한다:
#   ssh -J ec2-user@<app-eip> ec2-user@<train-private-ip>
# ProxyJump는 각 홉의 인증을 로컬에서 하므로 개인키를 bastion에 두지 않아도 된다.
resource "aws_vpc_security_group_ingress_rule" "train_ssh_from_app" {
  security_group_id            = aws_security_group.train.id
  description                  = "ssh from bastion"
  referenced_security_group_id = aws_security_group.app.id
  from_port                    = 22
  to_port                      = 22
  ip_protocol                  = "tcp"
}

# 학습 EC2가 실험을 기록한다. 소스를 CIDR이 아니라 SG로 지정해, 그 SG를 단
# 인스턴스만 접근할 수 있게 한다(VPC 대역 전체를 여는 것과 다르다).
resource "aws_vpc_security_group_ingress_rule" "app_mlflow_from_train" {
  security_group_id            = aws_security_group.app.id
  description                  = "mlflow tracking from train instance"
  referenced_security_group_id = aws_security_group.train.id
  from_port                    = 5000
  to_port                      = 5000
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_app" {
  security_group_id            = aws_security_group.rds.id
  description                  = "postgres from app instance"
  referenced_security_group_id = aws_security_group.app.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

# 아웃바운드는 셋 다 전면 허용한다. 상시는 수집 API, 학습은 PyPI, 양쪽 모두
# SSM 엔드포인트(443)와 S3에 나가야 한다.
resource "aws_vpc_security_group_egress_rule" "app_all" {
  security_group_id = aws_security_group.app.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_egress_rule" "train_all" {
  security_group_id = aws_security_group.train.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
