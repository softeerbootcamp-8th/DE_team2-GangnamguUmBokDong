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

DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY = timedelta(seconds=30)
DEFAULT_EXECUTION_TIMEOUT = timedelta(seconds=240)

# living_population_grid는 서울 전체 250m 격자 x 24시간을 페이지네이션으로 받아오므로
# 다른 실시간 소스보다 훨씬 오래 걸린다(로컬 테스트 실측 기준).
EXECUTION_TIMEOUT_OVERRIDES = {
    "living_population_grid": timedelta(seconds=1200),
}

NOWCASTING_EXECUTION_TIMEOUT = timedelta(seconds=600)
# 실측 데이터 없음(placeholder) — 로컬에서 --all-stations 1회 실행 시간을 재본 뒤 조정.
INFERENCE_EXECUTION_TIMEOUT = timedelta(seconds=300)
DB_LOADER_EXECUTION_TIMEOUT = timedelta(seconds=120)
# 실측 데이터 없음(placeholder) — S3 tick 5~6개 + 예측 결과 1개만 읽는 순수 계산이라
# 추론보다는 가볍게 잡았다. 로컬에서 1회 실행 시간을 재본 뒤 조정.
URGENCY_EXECUTION_TIMEOUT = timedelta(seconds=180)
# 실측 데이터 없음(placeholder) — urgency 계산을 다시 하고 dispatched 넷팅을 위한
# RDS 조회 하나가 추가되는 정도라 URGENCY_EXECUTION_TIMEOUT과 비슷하게 잡았다.
ROUTES_EXECUTION_TIMEOUT = timedelta(seconds=180)

MAX_ACTIVE_RUNS = 1
CATCHUP = False
