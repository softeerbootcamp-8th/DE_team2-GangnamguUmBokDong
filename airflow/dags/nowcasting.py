"""seoul-pop-nowcasting 일 단위 오케스트레이션 DAG.

## 목적

collector의 `living_population_grid` 일 단위 수집이 끝난 뒤, 오늘 기준
`-3일 ~ +3일` 구간의 결측 생활인구를 과거 4주 가중평균으로 채운다.
실제 조회/추정/저장 로직은 이 DAG가 아니라 seoul-pop-nowcasting CLI가 담당한다.

## 스케줄에 대한 가정

`living_population_grid`를 실행하는 daily 수집 DAG가 현재 저장소에는 아직
없다(REALTIME_SOURCES에는 5분 주기 source만 등록되어 있음). 그 DAG가 추가되면
이 DAG는 고정 cron 대신 `ExternalTaskSensor` 등으로 실제 선행 의존성을 걸어야
한다. 지금은 수집이 안정적으로 끝나 있을 것으로 가정한 새벽 시간에 고정
cron으로 스케줄한다.

## data_interval을 쓰지 않는 이유

bronze_compaction과 달리 이 작업은 "과거 특정 기간의 데이터를 재처리"하는 게
아니라 "오늘 기준 -3~+3일"이라는 상대적 구간을 계산하는 것이 목적이므로,
data_interval이 아니라 실행 당일(logical_date)을 `--target-date`로 넘긴다.
"""

import pendulum
from airflow import DAG

from config.schedules import TIMEZONE
from orchestration.nowcasting_task import build_nowcasting_task

with DAG(
    dag_id="nowcasting",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 8, 15, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["nowcasting", "daily"],
) as dag:
    build_nowcasting_task()
