"""DAG가 실행할 collector source_id 목록과 loader 테이블 이름을 모은다.

실제 값은 collector/sources/*.yaml의 source_id, loader/config.py의 TABLE_SPECS
키와 정확히 일치해야 한다. API 세부 설정(URL, 인증키 등)은 여기 넣지 않는다 —
그건 각 모듈의 책임이다.
"""

REALTIME_5MIN_SOURCES = ("bike_rental_history", "bike_station_realtime", "population_realtime")
WEATHER_10MIN_SOURCE = "weather_ultra_short_term"
WEATHER_3H_SOURCE = "weather_short_term_forecast"
DAILY_POPULATION_SOURCE = "living_population_grid"
DAILY_EVENT_SOURCE = "cultural_event"

NORMALIZER_BASELINE_MODE_PRIMARY = "strict"
NORMALIZER_BASELINE_MODE_FALLBACK = "latest"
