"""Airflow 스케줄·재시도 관련 공통 설정. DAG 파일이 cron 문자열이나 retry/timeout
값을 직접 반복해서 적지 않도록 한 곳에 모은다.
"""

from datetime import timedelta

TIMEZONE = "Asia/Seoul"

REALTIME_5MIN_CRON = "*/5 * * * *"
WEATHER_10MIN_CRON = "*/10 * * * *"
WEATHER_3H_CRON = "0 */3 * * *"
# living_population_grid는 그날 데이터를 하루 1개 파일로 발행한다 — 실제 발행 시각을
# 확인해 필요하면 조정한다.
DAILY_CRON = "0 3 * * *"

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
DB_LOADER_EXECUTION_TIMEOUT = timedelta(seconds=120)
WEATHER_MANIFEST_WAIT_TIMEOUT_SECONDS = 30
WEATHER_MANIFEST_POKE_INTERVAL_SECONDS = 2
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
