# 상시 EC2 — Airflow 3종 + API + nginx/web + MLflow.

resource "aws_iam_role" "app" {
  name               = "${var.project}-app"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "app_data" {
  role       = aws_iam_role.app.name
  policy_arn = aws_iam_policy.data_access.arn
}

# SSH(22번)를 열지 않고 Session Manager로 접속하기 위해 필요하다. 인스턴스의 SSM
# 에이전트가 아웃바운드로 SSM에 연결을 걸어두고, 그 위로 세션이 중개된다 —
# 인바운드 포트가 0개여도 셸을 얻을 수 있다.
resource "aws_iam_role_policy_attachment" "app_ssm" {
  role       = aws_iam_role.app.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "app" {
  name = "${var.project}-app"
  role = aws_iam_role.app.name
}

resource "aws_instance" "app" {
  ami                    = data.aws_ami.al2023_arm64.id
  instance_type          = var.app_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.app.id]
  iam_instance_profile   = aws_iam_instance_profile.app.name

  root_block_device {
    volume_size           = var.app_root_volume_gb
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required" # IMDSv2 강제

    # 기본값 1이면 **도커 컨테이너가 메타데이터에 도달하지 못한다**(홉이 하나 더 필요).
    # 그러면 컨테이너 안의 boto3가 instance profile을 못 받아 S3 접근이 전부 실패한다.
    # Airflow subprocess·API·MLflow가 모두 컨테이너 안에서 도므로 반드시 2여야 한다.
    http_put_response_hop_limit = 2
  }

  user_data = templatefile("${path.module}/templates/user_data_app.sh.tftpl", {
    project = var.project
  })

  # user_data를 나중에 고쳐도 이미 뜬 인스턴스는 재생성하지 않는다(어차피 최초 부팅에만
  # 실행되므로 재생성해도 얻는 게 없다). 부팅 스크립트를 바꿔야 하면 SSM으로 접속해
  # 직접 실행한다.
  user_data_replace_on_change = false

  tags = { Name = "${var.project}-app" }
}

# 대시보드 주소가 재부팅에도 바뀌지 않아야 한다. 학습 EC2는 SSM으로만 접속하므로
# EIP 없이 자동 할당을 쓴다(정지 중 퍼블릭 IP 과금이 멈춘다).
resource "aws_eip" "app" {
  domain   = "vpc"
  instance = aws_instance.app.id

  tags = { Name = "${var.project}-app" }

  depends_on = [aws_internet_gateway.main]
}
