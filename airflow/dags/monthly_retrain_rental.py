"""대여(rental) 모델 전용 월별 챔피언 점검, EMR 피처마트 생성, EC2 챌린저 학습/승격 DAG."""

from config.schedules import MONTHLY_RETRAIN_RENTAL_CRON
from dags.monthly_retrain import build_monthly_retrain_dag

dag = build_monthly_retrain_dag("rental", MONTHLY_RETRAIN_RENTAL_CRON)
