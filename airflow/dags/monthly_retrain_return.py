"""반납(return) 모델 전용 월별 챔피언 점검, EMR 피처마트 생성, EC2 챌린저 학습/승격 DAG."""

from airflow import DAG
from config.schedules import MONTHLY_RETRAIN_RETURN_CRON
from dags.monthly_retrain import build_monthly_retrain_dag

dag: DAG = build_monthly_retrain_dag("return", MONTHLY_RETRAIN_RETURN_CRON)
