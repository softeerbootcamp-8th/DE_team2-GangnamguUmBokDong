"""DAG가 실행할 collector source_id 목록과 loader 테이블 이름을 모은다.

실제 값은 collector/sources/*.yaml의 source_id, loader/config.py의 TABLE_SPECS
키와 정확히 일치해야 한다. API 세부 설정(URL, 인증키 등)은 여기 넣지 않는다 —
그건 각 모듈의 책임이다.
"""

REALTIME_5MIN_SOURCES = ("bike_rental_history", "bike_station_realtime", "population_realtime")
WEATHER_10MIN_SOURCE = "weather_ultra_short_live"
WEATHER_ULTRA_SHORT_FORECAST_SOURCE = "weather_ultra_short_forecast"
WEATHER_3H_SOURCE = "weather_short_term_forecast"
DAILY_POPULATION_SOURCE = "living_population_grid"
DAILY_EVENT_SOURCE = "cultural_event"
PERFORMANCE_EVENT_SOURCE = "performance_event"
STATION_MASTER_SOURCE = "bike_station_master"

# 하루치 silver를 archive로 묶을 대상. 예보 2종(weather_ultra_short_forecast,
# weather_short_term_forecast)은 사후 재현이 불가해 archive 가치가 낮아 제외한다.
# cultural_event·performance_event·living_population_grid는 하루 1파일이라 묶을 것이 없다.
COMPACTION_SOURCES = (
    "bike_rental_history",
    "bike_station_realtime",
    "weather_ultra_short_live",
)

NORMALIZER_BASELINE_MODE_PRIMARY = "strict"
NORMALIZER_BASELINE_MODE_FALLBACK = "latest"
