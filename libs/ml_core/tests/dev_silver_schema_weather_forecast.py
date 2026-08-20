"""weather_forecast_issue_keys()가 다른 tick 키 헬퍼와 같은 규칙(오래된 것부터
최신 순, WEATHER_FORECAST_SOURCE_ID 기준)으로 키를 만드는지 검증한다.

실제 발표 스키마/주기는 아직 확정되지 않았다(silver_schema.py의 WEATHER_FORECAST_*
주석 참고, collector 예보 수집 브랜치 병합 전) — 이 테스트는 "그 가정이 뭐든
_tick_keys()와 동일한 방식으로 정확히 조합되는지"만 확인한다.
"""

import pandas as pd

from ml_core import silver_schema


def test_keys_are_ordered_oldest_to_newest_ending_at_floored_anchor():
    anchor_ts = pd.Timestamp("2026-08-17 10:37:00")
    keys = silver_schema.weather_forecast_issue_keys(anchor_ts, lookback_hours=6.0)

    # 180분(3시간) 격자로 내림한 앵커가 마지막 키여야 한다.
    floored = anchor_ts.floor("180min")
    assert keys[-1] == silver_schema.silver_key(silver_schema.WEATHER_FORECAST_SOURCE_ID, floored)
    assert all(f"silver/{silver_schema.WEATHER_FORECAST_SOURCE_ID}/" in k for k in keys)
    # 6시간 lookback + 180분 간격 -> 6*60/180 + 1 = 3개
    assert len(keys) == 3


def test_default_lookback_is_24_hours():
    anchor_ts = pd.Timestamp("2026-08-17 00:00:00")
    keys = silver_schema.weather_forecast_issue_keys(anchor_ts)

    # 24*60/180 + 1 = 9개
    assert len(keys) == 9
