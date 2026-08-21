# 학습 EC2 — ml/training. 평소 정지 상태로 두고 학습할 때만 켠다.
#
# 생성 직후 `make train-stop`으로 즉시 정지시킬 것. 정지 중 비용은 EBS(100GB gp3 ≈
# 월 $9)뿐이다. count로 조건부 생성하지 않는 이유는, destroy/create를 반복하면 EBS가
# 함께 사라져 매번 레포 clone과 uv sync를 다시 해야 하기 때문이다 — 학습이 실패해
# 재시도할 때 환경이 보존되는 stop/start가 낫다.
#
# 인스턴스 타입 변경(r7g.4xlarge ↔ r7g.8xlarge)은 같은 Graviton 계열이라 in-place로
# 처리되고 EBS가 유지된다. terraform plan에서 `~ update in-place`인지 반드시 확인할 것.

resource "aws_iam_role" "train" {
  name               = "${var.project}-train"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

# 피처마트를 읽고 모델을 쓴다. RDS에는 접근하지 않는다 — 학습은 S3만 보고,
# 실험 기록은 상시 EC2의 MLflow 서버가 대신 DB에 쓴다.
resource "aws_iam_role_policy_attachment" "train_data" {
  role       = aws_iam_role.train.name
  policy_arn = aws_iam_policy.data_access.arn
}

resource "aws_iam_role_policy_attachment" "train_ssm" {
  role       = aws_iam_role.train.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "train" {
  name = "${var.project}-train"
  role = aws_iam_role.train.name
}

resource "aws_instance" "train" {
  ami           = data.aws_ami.al2023_arm64.id
  instance_type = var.train_instance_type
  subnet_id     = aws_subnet.public[0].id
  # sg-train에는 인바운드 규칙이 하나도 없다. 퍼블릭 IP가 있어도 외부에서 도달할 수 없고,
  # 접속은 SSM Session Manager로만 한다.
  vpc_security_group_ids = [aws_security_group.train.id]
  iam_instance_profile   = aws_iam_instance_profile.train.name

  root_block_device {
    volume_size           = var.train_root_volume_gb
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  user_data = templatefile("${path.module}/templates/user_data_train.sh.tftpl", {
    project     = var.project
    mlflow_host = aws_instance.app.private_ip
    s3_bucket   = aws_s3_bucket.data.id
    region      = var.region
  })

  user_data_replace_on_change = false

  tags = { Name = "${var.project}-train" }
}
