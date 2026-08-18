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

# 하루치 silver를 archive로 묶는 배치. 일 단위 수집(03:00)이 끝난 뒤에 돈다.
COMPACTION_CRON = "30 4 * * *"

DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY = timedelta(seconds=30)
DEFAULT_EXECUTION_TIMEOUT = timedelta(seconds=240)

# living_population_grid는 서울 전체 250m 격자 x 24시간을 페이지네이션으로 받아오므로
# 다른 실시간 소스보다 훨씬 오래 걸린다(로컬 테스트 실측 기준).
EXECUTION_TIMEOUT_OVERRIDES = {
    "living_population_grid": timedelta(seconds=1200),
}

NOWCASTING_EXECUTION_TIMEOUT = timedelta(seconds=600)
# 변경이 없는 날짜는 LIST 한 번으로 끝나지만, 백필이 들어온 날은 하루치 parquet을
# 전부 다시 읽는다. bike_rental_history 기준 288개가 상한이다.
COMPACTION_EXECUTION_TIMEOUT = timedelta(seconds=900)
# 실측 데이터 없음(placeholder) — 로컬에서 --all-stations 1회 실행 시간을 재본 뒤 조정.
INFERENCE_EXECUTION_TIMEOUT = timedelta(seconds=300)
DB_LOADER_EXECUTION_TIMEOUT = timedelta(seconds=120)

MAX_ACTIVE_RUNS = 1
CATCHUP = False
