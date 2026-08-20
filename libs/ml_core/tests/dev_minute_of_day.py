"""minute_of_day()가 자정 기준 경과분을 정확히 계산하는지, tick(5분) 단위로
같은 hour 안의 서로 다른 시각을 실제로 구분하는지 검증한다.
"""

import pandas as pd

from ml_core.minute_of_day import MINUTES_PER_DAY, minute_of_day


def test_midnight_is_zero():
    assert minute_of_day(pd.Timestamp("2025-06-01 00:00:00")) == 0


def test_one_twenty_am_is_eighty():
    assert minute_of_day(pd.Timestamp("2025-06-01 01:20:00")) == 80


def test_last_tick_of_day_is_1439():
    assert minute_of_day(pd.Timestamp("2025-06-01 23:59:00")) == 1439


def test_distinguishes_ticks_within_same_hour():
    """17:00/17:05/17:10은 hour로는 구분이 안 되지만 minute_of_day는 셋 다 달라야 한다."""
    values = {minute_of_day(pd.Timestamp(f"2025-06-01 17:{m:02d}:00")) for m in (0, 5, 10)}
    assert len(values) == 3


def test_fits_in_int16():
    assert minute_of_day(pd.Timestamp("2025-06-01 23:59:00")) < 32767
    assert MINUTES_PER_DAY < 32767
