from datetime import UTC, date, datetime

import pandas as pd

import transform
from transform import (
    _parse_precip_str,
    cultural_events_from_silver,
    forecast_points_from_predictions,
    performance_events_from_silver,
    station_stock_from_silver,
    stations_from_silver,
    weather_current_from_silver,
    weather_forecast_from_silver,
    weather_forecast_ultra_from_silver,
)

GANGNAM_STATION = {
    "stationId": "101",
    "stationName": "강남구청역 3번 출구",
    "stationLatitude": 37.5172,
    "stationLongitude": 127.0473,
    "rackTotCnt": 20,
}

SEOUL_GRID = {"nx": 60, "ny": 127}


def test_stations_from_silver_maps_columns_and_gu():
    df = pd.DataFrame([GANGNAM_STATION])

    [record] = stations_from_silver(df)

    assert record["sta_id"] == "101"
    assert record["sta_nm"] == "강남구청역 3번 출구"
    assert record["gu"] == "강남구"
    assert record["hold_cnt"] == 20


def test_stations_from_silver_skips_rows_outside_seoul_gu_boundary():
    outside = {**GANGNAM_STATION, "stationId": "999", "stationLatitude": 0.0, "stationLongitude": 0.0}
    df = pd.DataFrame([GANGNAM_STATION, outside])

    records = stations_from_silver(df)

    assert [r["sta_id"] for r in records] == ["101"]


def test_stations_from_silver_includes_nearest_grid():
    df = pd.DataFrame([GANGNAM_STATION])

    [record] = stations_from_silver(df)

    assert record["grid_nx"] == 61
    assert record["grid_ny"] == 126


def test_station_stock_from_silver_uses_given_observed_at():
    df = pd.DataFrame([{**GANGNAM_STATION, "parkingBikeTotCnt": 15}])
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)

    [record] = station_stock_from_silver(df, observed_at=observed_at)

    assert record == {"sta_id": "101", "observed_at": observed_at, "parking_bike_tot_cnt": 15}


def test_station_stock_from_silver_skips_rows_outside_seoul_gu_boundary():
    outside = {**GANGNAM_STATION, "stationId": "999", "stationLatitude": 0.0, "stationLongitude": 0.0, "parkingBikeTotCnt": 3}
    df = pd.DataFrame([outside])
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)

    records = station_stock_from_silver(df, observed_at=observed_at)

    assert records == []


def test_weather_current_from_silver_keeps_latest_per_gu():
    df = pd.DataFrame(
        [
            {**SEOUL_GRID, "baseDate": "20260816", "baseTime": "0900", "T1H": "28.5", "REH": "55", "WSD": "2.1", "RN1": "0", "PTY": "0"},
            {**SEOUL_GRID, "baseDate": "20260816", "baseTime": "1000", "T1H": "29.0", "REH": "50", "WSD": "2.5", "RN1": "0", "PTY": "0"},
        ]
    )

    records = weather_current_from_silver(df)

    assert len(records) == 1
    assert records[0]["gu"] == "종로구"
    assert records[0]["nx"] == 60
    assert records[0]["ny"] == 127
    assert records[0]["temperature"] == 29.0


def test_weather_forecast_from_silver_keeps_latest_issued():
    df = pd.DataFrame(
        [
            {**SEOUL_GRID, "baseDate": "20260816", "baseTime": "0800", "fcstDate": "20260816", "fcstTime": "1200", "TMP": "27", "POP": "20", "SKY": "1", "PTY": "0"},
            {**SEOUL_GRID, "baseDate": "20260816", "baseTime": "1100", "fcstDate": "20260816", "fcstTime": "1200", "TMP": "28", "POP": "30", "SKY": "3", "PTY": "0"},
        ]
    )

    records = weather_forecast_from_silver(df)

    assert len(records) == 1
    assert records[0]["nx"] == 60
    assert records[0]["ny"] == 127
    assert records[0]["temperature"] == 28.0
    assert records[0]["precip_prob"] == 30.0


def test_weather_forecast_ultra_from_silver_maps_pop_to_precip_prob():
    df = pd.DataFrame(
        [
            {**SEOUL_GRID, "baseDate": "20260816", "baseTime": "0930", "fcstDate": "20260816", "fcstTime": "1000", "T1H": "27", "POP": "40", "RN1": "강수없음", "SKY": "1", "PTY": "0"},
        ]
    )

    [record] = weather_forecast_ultra_from_silver(df)

    assert record["precip_prob"] == 40.0
    assert record["nx"] == 60
    assert record["ny"] == 127


def test_cultural_events_from_silver_filters_ended_events():
    df = pd.DataFrame(
        [
            {
                "TITLE": "여름 재즈 페스티벌",
                "CODENAME": "공연",
                "GUNAME": "강남구",
                "PLACE": "코엑스",
                "STRTDATE": "2026-08-01",
                "END_DATE": "2026-08-20",
                "IS_FREE": "N",
                "LAT": 37.5115,
                "LOT": 127.0605,
            },
            {
                "TITLE": "지난 봄꽃 축제",
                "CODENAME": "축제",
                "GUNAME": "종로구",
                "PLACE": "경복궁",
                "STRTDATE": "2026-04-01",
                "END_DATE": "2026-04-10",
                "IS_FREE": "Y",
                "LAT": 37.5796,
                "LOT": 126.9770,
            },
        ]
    )

    records = cultural_events_from_silver(df, today=date(2026, 8, 16))

    assert len(records) == 1
    assert records[0]["title"] == "여름 재즈 페스티벌"
    assert records[0]["end_date"] == date(2026, 8, 20)


# UTC 2026-08-15 16:30 = KST 2026-08-16 01:30. UTC와 KST의 날짜(date)가 갈라지는
# 자정 근방 시각을 골라야, today를 UTC로 잘못 계산하는 회귀를 테스트가 실제로 잡아낸다.
_FIXED_INSTANT_UTC = datetime(2026, 8, 15, 16, 30, tzinfo=UTC)


def _fixed_now_datetime():
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return _FIXED_INSTANT_UTC.astimezone(tz) if tz else _FIXED_INSTANT_UTC

    return FixedDatetime


def test_cultural_events_from_silver_defaults_today_to_kst_now(monkeypatch):
    monkeypatch.setattr(transform, "datetime", _fixed_now_datetime())
    df = pd.DataFrame(
        [
            {
                "TITLE": "종료 임박 행사",
                "CODENAME": "공연",
                "GUNAME": "강남구",
                "PLACE": "코엑스",
                "STRTDATE": "2026-08-01",
                "END_DATE": "2026-08-15",
                "IS_FREE": "N",
                "LAT": 37.5115,
                "LOT": 127.0605,
            }
        ]
    )

    records = cultural_events_from_silver(df)

    # KST 기준 today는 2026-08-16이므로 end_date(08-15)는 이미 종료되어 제외된다.
    # today를 UTC로 잘못 계산하면 today가 08-15가 되어 이 행사가 남아버린다.
    assert records == []


def test_performance_events_from_silver_defaults_today_to_kst_now(monkeypatch):
    monkeypatch.setattr(transform, "datetime", _fixed_now_datetime())
    df = pd.DataFrame(
        [
            {
                "SVCID": "S001",
                "SVCNM": "종료 임박 공연",
                "MINCLASSNM": "공연",
                "AREANM": "강남구",
                "PLACENM": "코엑스",
                "SVCOPNBGNDT": "2026-08-01",
                "SVCOPNENDDT": "2026-08-15",
                "PAYATNM": "유료",
                "Y": 37.5115,
                "X": 127.0605,
            }
        ]
    )

    records = performance_events_from_silver(df)

    assert records == []


def test_forecast_points_from_predictions_maps_columns_and_converts_kst_to_utc():
    df = pd.DataFrame(
        [
            {
                "station_id": "101",
                "date": "2026-08-16",
                "hour": 14,
                "minute": 5,
                "horizon": 1,
                "rental_pred_mean": 3.6,
                "return_pred_mean": 2.4,
            }
        ]
    )
    batch_run_at = datetime(2026, 8, 16, 5, 5, tzinfo=UTC)

    [record] = forecast_points_from_predictions(df, batch_run_at=batch_run_at)

    assert record["sta_id"] == "101"
    assert record["predicted_dttm"] == datetime(2026, 8, 16, 5, 5, tzinfo=UTC)
    assert record["predicted_rent_cnt"] == 4
    assert record["predicted_return_cnt"] == 2
    assert record["batch_run_at"] == batch_run_at


def test_forecast_points_from_predictions_rounds_half_to_even():
    df = pd.DataFrame(
        [
            {
                "station_id": "101",
                "date": "2026-08-16",
                "hour": 15,
                "minute": 0,
                "horizon": 2,
                "rental_pred_mean": 2.5,
                "return_pred_mean": 1.5,
            }
        ]
    )

    [record] = forecast_points_from_predictions(df, batch_run_at=datetime(2026, 8, 16, 6, 0, tzinfo=UTC))

    assert record["predicted_rent_cnt"] == round(2.5)
    assert record["predicted_return_cnt"] == round(1.5)


class TestParsePrecipStr:
    """강수량 변환 규칙 자체는 `core.precip`이 갖는다 — collector가 silver에 쓸 때와
    같은 값이 나와야 하기 때문이다. 여기서 보는 것은 loader 쪽 껍데기의 계약이다:
    해석할 수 없는 값은 예외가 아니라 None(= 해당 컬럼 결측)이어야 한다."""

    def test_uses_the_shared_rule(self):
        assert _parse_precip_str("30.0~50.0mm") == 30.0
        assert _parse_precip_str("강수없음") == 0.0
        assert _parse_precip_str("1.0mm 미만") == 0.5

    def test_at_least_is_not_dropped(self):
        """상한 없는 표기가 None으로 떨어지면 폭우 예보가 통째로 결측이 된다."""
        assert _parse_precip_str("50.0mm 이상") == 50.0

    def test_numeric_silver_passes_through(self):
        """collector가 이미 숫자로 저장한 silver를 읽는 경로."""
        assert _parse_precip_str(2.0) == 2.0

    def test_missing_values_become_none(self):
        assert _parse_precip_str(None) is None
        assert _parse_precip_str("") is None

    def test_nan_becomes_none(self):
        """숫자 컬럼의 결측은 pandas에서 NaN으로 온다. float()가 통과시켜 버리므로
        따로 막지 않으면 NaN이 그대로 RDB로 간다."""
        assert _parse_precip_str(float("nan")) is None

    def test_unparseable_becomes_none(self):
        assert _parse_precip_str("맑음") is None
