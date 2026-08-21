# 배포 스크립트와 Makefile 타겟이 참조하는 값들.

output "app_public_ip" {
  description = "대시보드 주소. http://<이 IP>"
  value       = aws_eip.app.public_ip
}

output "app_private_ip" {
  description = "학습 EC2가 MLflow에 붙을 때 쓰는 사설 IP."
  value       = aws_instance.app.private_ip
}

output "app_instance_id" {
  description = "SSM 접속·포트포워딩 대상. make tunnel-airflow / tunnel-mlflow가 쓴다."
  value       = aws_instance.app.id
}

output "train_instance_id" {
  description = "make train-start / train-stop 대상."
  value       = aws_instance.train.id
}

output "rds_endpoint" {
  description = "RDS 호스트. bootstrap_rds.sh가 PGHOST로 쓴다."
  value       = aws_db_instance.main.address
}

output "rds_engine_version" {
  description = "실제 적용된 엔진 버전. PostGIS 3.5 확인 실패 시 이 값을 내려가며 재시도한다."
  value       = aws_db_instance.main.engine_version
}

output "s3_bucket" {
  description = "데이터 버킷 이름."
  value       = aws_s3_bucket.data.id
}

output "config_object_uri" {
  description = "render_env.sh가 내려받는 설정 객체."
  value       = "s3://${aws_s3_bucket.data.id}/${aws_s3_object.prod_env.key}"
}

output "kms_key_alias" {
  description = "설정 객체 암호화 키."
  value       = aws_kms_alias.config.name
}

output "subnet_id" {
  description = "EMR 클러스터를 띄울 서브넷(--ec2-attributes SubnetId=...)."
  value       = aws_subnet.public[0].id
}

output "emr_service_role" {
  description = "aws emr create-cluster --service-role"
  value       = aws_iam_role.emr_service.name
}

output "emr_instance_profile" {
  description = "aws emr create-cluster --ec2-attributes InstanceProfile=..."
  value       = aws_iam_instance_profile.emr_ec2.name
}

output "db_password" {
  description = "RDS 마스터 비밀번호. tfstate에 평문으로 남는 것은 수용하기로 한 트레이드오프다."
  value       = random_password.db.result
  sensitive   = true
}
