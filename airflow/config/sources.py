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

# bike_rental_history를 몇 시간 전까지 다시 수집할지.
#
# tbCycleRentData는 `RENT_DT`(대여 시각) 기준으로 한 시간치를 주지만 목록에는
# **반납이 완료된 기록만** 나타난다(정렬도 `RTN_DT` 오름차순). 그래서 대여 시간대가
# 끝난 뒤에 반납된 기록은 그 시간대를 마지막으로 조회하는 윈도우 이후에야 등장한다.
# 현행처럼 시간대 H를 `[H:05, H+1:00]` 윈도우만 조회하면 그 뒤 반납분을 영구히 놓치고,
# `--backfill`은 실패한 조각만 채우므로 회수하지 못한다.
#
# 실측(2026-08-18, 5개 시간대 42,902건): 24.6%가 대여 시간대 종료 후에 반납됐다.
# 시간대별 누락률은 13.4%(08시)~32.0%(21시)이고 긴 이용에 편향돼 있다.
#
# | L | 포착 조건 | 누락 회수 | 잔여 누락 | 요청/일 |
# | - | --------- | --------- | --------- | ------- |
# | 0 | RTN <= H+1:00 | —          | 24.6% | 1,836 |
# | 1 | RTN <= H+2:00 | 86.7~90.2% | ~3.0% | 3,672 |
# | 2 | RTN <= H+3:00 | 98.0~100%  | ~0.3% | 5,508 |
#
# 1로 시작한다. 요청 2배로 누락의 88%를 잡는다. 부족하면 2로 올린다 — 이 상수만
# 바꾸면 태스크가 따라 늘어난다.
RENTAL_HISTORY_LOOKBACK_HOURS = 1
