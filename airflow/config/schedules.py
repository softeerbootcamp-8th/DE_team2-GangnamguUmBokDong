"""Airflow 스케줄·재시도 관련 공통 설정. DAG 파일이 cron 문자열이나 retry/timeout
값을 직접 반복해서 적지 않도록 한 곳에 모은다.
"""

from datetime import timedelta

TIMEZONE = "Asia/Seoul"

# realtime tick은 5분마다 돈다. 한때는 날씨 필요 여부(분·시 나머지 연산으로 고정된
# 경계)에 따라 이 격자를 4개 cron으로 쪼개서, 필요한 시각에만 날씨 collector가 같은
# DAG에 존재하게 하는 방식을 썼다 — 그런데 그러면 서로 다른 DAG가 max_active_runs를
# 각자 따로 관리해서, 한 tick이 늦어져 여러 DAG의 실행 시각이 겹치면(2026-08-22
# 실측: `realtime_5min` 단일 DAG였던 시절에도 tick 하나가 530초까지 늘어나 CPU가
# 부족해진 적이 있다) 약한 인스턴스에서 CPU 경합으로 60초 타임아웃(retries=0)인 날씨
# collector가 죽을 위험이 있었다. 지금은 다시 DAG 하나로 합치고, 날씨 collector마다
# `orchestration.collector_task.build_weather_freshness_gate_task()`로 "마지막
# 성공 수집 이후 충분히 지났는지"를 실제 시각 기준으로 매 tick마다 직접 물어서
# 스킵 여부를 정한다(초단기 10분/단기 3시간 — `dags/realtime_tick.py` 참고). 이러면
# 3시간짜리 수집이 실패해도 다음 5분 tick에서 바로 재시도되어, cron 경계에 걸려
# 최대 3시간을 기다리던 예전보다 복구가 훨씬 빠르다.
REALTIME_TICK_CRON = "*/5 * * * *"
# living_population_grid는 그날 데이터를 하루 1개 파일로 발행한다 — 실제 발행 시각을
# 확인해 필요하면 조정한다.
DAILY_CRON = "0 3 * * *"

# station_master는 DAILY_CRON(03:00)을 쓰면 안 된다. realtime tick이 5분마다
# 돌아서 03:00도 그 tick 중 하나이므로 두 DAG이 동시에 시작하고, station_master가
# 약 88초 뒤 bike_station_master authority를
# 게시한다. 그 시각이 realtime tick의
# prepare_serving_plan(고정)과 finalize_serving_release(재검증) 사이에 들어가면
# "locked station master authority가 바뀌었습니다"로 그 tick의 Gold 게시가 실패한다
# (2026-08-22 실측: 06:15 tick, prepare 06:15:46 종료 -> master 게시 06:18:01 ->
# finalize 06:18:23 실패). 이건 버그가 아니라 의도된 방어라 코드로 우회하지 않고
# 시각을 비켜놓는다.
#
# 03:04를 고른 이유: tick 하나가 약 3분 45초라 03:00 tick은 03:03:45에 끝나고
# 03:05 tick은 아직 시작 전이다. 03:02는 03:00 tick의 finalize 창(03:02:20~03:03:00)과
# 여전히 겹칠 수 있어 부족하다. **tick 소요가 다시 5분에 가까워지면 이 여유가
# 사라지므로 그때 다시 계산해야 한다.**
STATION_MASTER_CRON = "4 3 * * *"

# D-6 대여이력 재수집 후 같은 날짜의 silver를 archive로 묶는 배치.
COMPACTION_CRON = "30 4 * * *"

# 매달 1일 03:00 KST 대여(Rental) 챔피언 점검 및 재학습 파이프라인.
MONTHLY_RETRAIN_RENTAL_CRON = "0 3 1 * *"

# 매달 1일 06:00 KST 반납(Return) 챔피언 점검 및 재학습 파이프라인.
MONTHLY_RETRAIN_RETURN_CRON = "0 6 1 * *"

# 하위 호환용 기본 스케줄
MONTHLY_RETRAIN_CRON = "0 4 1 * *"

DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY = timedelta(seconds=30)
DEFAULT_EXECUTION_TIMEOUT = timedelta(seconds=240)

# living_population_grid는 서울 전체 250m 격자 x 24시간을 페이지네이션으로 받아오므로
# 다른 실시간 소스보다 훨씬 오래 걸린다(로컬 테스트 실측 기준).
EXECUTION_TIMEOUT_OVERRIDES = {
    "living_population_grid": timedelta(seconds=1200),
}

NOWCASTING_EXECUTION_TIMEOUT = timedelta(seconds=600)
# normalizer는 tick 1회에 현재 + 향후 12시간, 최대 13개 시각을 보정한다. 가장 무거운
# 교차 계산은 시각과 무관해 1회만 돌므로 시각 수에 비례하지 않는다. 로컬 실측
# (2026-08-19 21:40 tick, 격자 8,564개 x 13시각, baseline 2개 날짜 = nowcast 27MB):
# 10.7초. 5분 tick에 여유가 크지만 baseline 크기가 늘 수 있어 상한은 300초로 둔다.
NORMALIZER_EXECUTION_TIMEOUT = timedelta(seconds=300)
# 변경이 없는 날짜는 LIST 한 번으로 끝나지만, 백필이 들어온 날은 하루치 parquet을
# 전부 다시 읽는다. bike_rental_history 기준 288개가 상한이다.
COMPACTION_EXECUTION_TIMEOUT = timedelta(seconds=900)
# 실측 데이터 없음(placeholder) — 로컬에서 --all-stations 1회 실행 시간을 재본 뒤 조정.
INFERENCE_EXECUTION_TIMEOUT = timedelta(seconds=300)
# AWS 실측: prepare_serving_plan(serving_cli.py prepare)이 station/stock/weather
# projection과 여러 S3 put/readback을 순차로 하는데, 로컬 MinIO가 아니라 실제 AWS
# S3·RDS 네트워크 왕복이 되니 콜드 스타트(재사용할 이전 산출물이 없는 최초 실행)
# 기준 165초가 걸렸다(2026-08-21, CPU는 21초뿐이라 대부분 네트워크 I/O 대기).
# 기존 120초는 이 실측 전의 placeholder였다 — 여유를 두고 300초로 올린다.
DB_LOADER_EXECUTION_TIMEOUT = timedelta(seconds=300)
# 실측 데이터 없음(placeholder) — S3 tick 5~6개 + 예측 결과 1개만 읽는 순수 계산이라
# 추론보다는 가볍게 잡았다. 로컬에서 1회 실행 시간을 재본 뒤 조정.
URGENCY_EXECUTION_TIMEOUT = timedelta(seconds=180)
# 실측 데이터 없음(placeholder) — urgency 계산을 다시 하고 dispatched 넷팅을 위한
# RDS 조회 하나가 추가되는 정도라 URGENCY_EXECUTION_TIMEOUT과 비슷하게 잡았다.
ROUTES_EXECUTION_TIMEOUT = timedelta(seconds=180)

# 월별 ML 재학습 타임아웃
MONTHLY_EVALUATION_TIMEOUT = timedelta(minutes=30)
EMR_FEATURE_MART_TIMEOUT = timedelta(minutes=90)
MONTHLY_TRAINING_TIMEOUT = timedelta(minutes=180)
MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT = timedelta(hours=6)

MAX_ACTIVE_RUNS = 1
CATCHUP = False
