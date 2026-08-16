"""trip_events.normalize_station_no()의 결측/이상값 처리 검증."""

import pandas as pd

from ml_common.trip_events import normalize_station_no


def test_valid_numeric_strings_are_zero_padded():
    result = normalize_station_no(pd.Series(["123", "45", "99"]))
    assert result.tolist() == ["00123", "00045", "00099"]


def test_backslash_n_marker_becomes_missing():
    result = normalize_station_no(pd.Series(["123", "\\N", "45"]))
    assert result.iloc[0] == "00123"
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == "00045"


def test_non_numeric_garbage_becomes_missing_not_a_crash():
    # 실제 2025년 데이터에선 '\N' 외의 이상값을 본 적 없지만, 다른 소스/연도 데이터에
    # 숫자로 못 바꾸는 값(한글, 빈 문자열 등)이 섞이면 예전엔 astype("Int64")에서
    # 바로 크래시했다 — errors="coerce"로 안전하게 결측 처리되는지 확인한다.
    result = normalize_station_no(pd.Series(["123", "GARBAGE", "", None, "00099"]))
    assert result.iloc[0] == "00123"
    assert pd.isna(result.iloc[1])
    assert pd.isna(result.iloc[2])
    assert pd.isna(result.iloc[3])
    assert result.iloc[4] == "00099"
