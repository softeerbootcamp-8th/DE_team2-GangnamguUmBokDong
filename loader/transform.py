"""Silver 계층 DataFrame 및 ML 예측 결과를 Gold 테이블 규격 레코드로 변환한다."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from gu_mapping import grid_to_gu, latlon_to_gu

_KST = timedelta(hours=9)


def _kst_to_utc(date_str: str, time_str: str) -> datetime:
    """기상청 일자(YYYYMMDD) 및 시각(HHMM, KST) 문자열을 UTC datetime으로 변환한다."""
    naive_kst = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M").replace(tzinfo=UTC)
    return naive_kst - _KST


def _kst_date_hm_to_utc(date_str: str, hour: int, minute: int) -> datetime:
    """ML 추론 결과 일자('YYYY-MM-DD', KST)와 시·분 정수를 UTC datetime으로 변환한다."""
    naive_kst = datetime.strptime(f"{date_str} {hour:02d}{minute:02d}", "%Y-%m-%d %H%M").replace(tzinfo=UTC)
    return naive_kst - _KST


def stations_from_silver(df: pd.DataFrame) -> list[dict]:
    """따릉이 실시간 대여소 Silver 데이터를 stations 테이블 레코드 목록으로 변환한다.

    args:
        df: bike_station_realtime Silver DataFrame
    returns:
        stations 테이블 적재용 레코드 딕셔너리 목록 (서울 경계 밖 제외)
    """
    records = []
    for row in df.to_dict("records"):
        lat = float(row["stationLatitude"])
        lon = float(row["stationLongitude"])
        gu = latlon_to_gu(lat, lon)
        if gu is None:
            continue  # 서울 자치구 경계 밖(인접 도시 접경) 정거장은 제외
        records.append(
            {
                "sta_id": str(row["stationId"]),
                "sta_nm": row["stationName"],
                "gu": gu,
                "sta_addr": row["stationName"],
                "lat": lat,
                "lon": lon,
                "hold_cnt": int(row["rackTotCnt"]),
            }
        )
    return records


def station_stock_from_silver(df: pd.DataFrame, observed_at: datetime) -> list[dict]:
    """따릉이 실시간 대여소 Silver 데이터를 station_stock 테이블 레코드 목록으로 변환한다.

    args:
        df: bike_station_realtime Silver DataFrame
        observed_at: 실측 기준 일시 (KST)
    returns:
        station_stock 테이블 적재용 레코드 딕셔너리 목록
    """
    records = []
    for row in df.to_dict("records"):
        gu = latlon_to_gu(float(row["stationLatitude"]), float(row["stationLongitude"]))
        if gu is None:
            continue
        records.append(
            {
                "sta_id": str(row["stationId"]),
                "observed_at": observed_at,
                "parking_bike_tot_cnt": int(row["parkingBikeTotCnt"]),
            }
        )
    return records


def weather_current_from_silver(df: pd.DataFrame) -> list[dict]:
    """기상청 초단기실황 Silver 데이터를 weather_current 테이블 레코드 목록으로 변환한다.

    args:
        df: weather_ultra_short_live Silver DataFrame
    returns:
        weather_current 테이블 적재용 자치구별 최신 실황 레코드 목록
    """
    by_gu: dict[str, dict] = {}
    for row in df.to_dict("records"):
        gu = grid_to_gu(row["nx"], row["ny"])
        if gu is None:
            continue
        observed_at = _kst_to_utc(str(row["baseDate"]), str(row["baseTime"]))

        # 최신 데이터 보장: 이미 담긴 구의 데이터보다 과거 시간이면 무시한다
        existing = by_gu.get(gu)
        if existing is not None and existing["observed_at"] >= observed_at:
            continue
        by_gu[gu] = {
            "gu": gu,
            "observed_at": observed_at,
            "temperature": _to_float(row.get("T1H")),  # T1H: 기온(°C)
            "humidity": _to_float(row.get("REH")),     # REH: 습도(%)
            "wind_speed": _to_float(row.get("WSD")),   # WSD: 풍속(m/s)
            "rainfall": _to_float(row.get("RN1")),     # RN1: 1시간 강수량(mm)
            "pty_type": _to_int(row.get("PTY")),       # PTY: 강수형태 코드
        }
    return list(by_gu.values())


def weather_forecast_from_silver(df: pd.DataFrame) -> list[dict]:
    """기상청 단기예보 Silver 데이터를 weather_forecast 테이블 레코드 목록으로 변환한다.

    args:
        df: weather_short_term_forecast Silver DataFrame
    returns:
        weather_forecast 테이블 적재용 예보 레코드 목록
    """
    by_key: dict[tuple[str, datetime], dict] = {}
    for row in df.to_dict("records"):
        gu = grid_to_gu(row["nx"], row["ny"])
        if gu is None:
            continue
        forecast_dttm = _kst_to_utc(str(row["fcstDate"]), str(row["fcstTime"]))
        base_dttm = _kst_to_utc(str(row["baseDate"]), str(row["baseTime"]))
        key = (gu, forecast_dttm)
        existing = by_key.get(key)
        if existing is not None and existing["base_dttm"] >= base_dttm:
            continue
        by_key[key] = {
            "gu": gu,
            "forecast_dttm": forecast_dttm,
            "sky_cond": _to_int(row.get("SKY")),       # SKY: 하늘상태 코드(1:맑음, 3:구름많음, 4:흐림)
            "pty_type": _to_int(row.get("PTY")),       # PTY: 강수형태 코드
            "temperature": _to_float(row.get("TMP")),  # TMP: 1시간 기온(°C)
            "precip_prob": _to_float(row.get("POP")),  # POP: 강수확률(%)
            "precip_amount": _parse_precip_str(row.get("PCP")),  # PCP: 1시간 강수량(mm)
            "humidity": _to_float(row.get("REH")),     # REH: 습도(%)
            "wind_speed": _to_float(row.get("WSD")),   # WSD: 풍속(m/s)
            "base_dttm": base_dttm,
        }
    return list(by_key.values())


def _parse_precip_str(value) -> float | None:
    """기상청 강수량 문자열(예: '강수없음', '1.0mm 미만', '30.0~50.0mm')을 실수값(float)으로 변환한다."""
    # 1. 빈 값이면 None 반환
    if value is None or value == "":
        return None
    text = str(value).strip()
    # 2. 비/눈이 안 오는 경우 0.0으로 변환
    if text in ("강수없음", "적설없음"):
        return 0.0
    # 3. "1.0mm 미만" 형태는 소량의 강수를 의미하는 0.5로 변환
    if "미만" in text:
        return 0.5
    # 4. "mm" 단위를 제거하고, "30.0~50.0" 같은 범위는 앞의 하한값(30.0)만 추출
    text = text.replace("mm", "").strip()
    if "~" in text:
        text = text.split("~")[0]
    # 5. 최종 숫자로 변환 (예외 발생 시 None 반환)
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def weather_forecast_ultra_from_silver(df: pd.DataFrame) -> list[dict]:
    """기상청 초단기예보 Silver 데이터를 weather_forecast 테이블 레코드 목록으로 변환한다.

    args:
        df: weather_ultra_short_forecast Silver DataFrame
    returns:
        weather_forecast 테이블 적재용 초단기예보 레코드 목록
    """
    by_key: dict[tuple[str, datetime], dict] = {}
    for row in df.to_dict("records"):
        gu = grid_to_gu(row["nx"], row["ny"])
        if gu is None:
            continue
        forecast_dttm = _kst_to_utc(str(row["fcstDate"]), str(row["fcstTime"]))
        base_dttm = _kst_to_utc(str(row["baseDate"]), str(row["baseTime"]))
        key = (gu, forecast_dttm)
        existing = by_key.get(key)
        if existing is not None and existing["base_dttm"] >= base_dttm:
            continue
        by_key[key] = {
            "gu": gu,
            "forecast_dttm": forecast_dttm,
            "sky_cond": _to_int(row.get("SKY")),       # SKY: 하늘상태 코드(1:맑음, 3:구름많음, 4:흐림)
            "pty_type": _to_int(row.get("PTY")),       # PTY: 강수형태 코드
            "temperature": _to_float(row.get("T1H")),  # T1H: 기온(°C)
            "precip_prob": None,                       # 초단기예보에는 강수확률(POP)이 없음
            "precip_amount": _parse_precip_str(row.get("RN1")),  # RN1: 1시간 강수량(mm)
            "humidity": _to_float(row.get("REH")),     # REH: 습도(%)
            "wind_speed": _to_float(row.get("WSD")),   # WSD: 풍속(m/s)
            "base_dttm": base_dttm,
        }
    return list(by_key.values())


def cultural_events_from_silver(df: pd.DataFrame, today: date | None = None) -> list[dict]:
    """서울시 문화행사 Silver 데이터를 cultural_events 테이블 레코드 목록으로 변환한다.

    args:
        df: cultural_event Silver DataFrame
        today: 행사 유효성 검사용 기준 일자 (KST, 기본값: 오늘)
    returns:
        cultural_events 테이블 적재용 레코드 목록 (종료된 행사 제외)
    """
    today = today or datetime.now(ZoneInfo("Asia/Seoul")).date()
    records = []
    for row in df.to_dict("records"):
        end_date = _parse_date(row["END_DATE"])
        # 이미 종료된 행사는 제외
        if end_date is not None and end_date < today:
            continue
        title = row["TITLE"]
        place = row["PLACE"]
        # 제목 + 장소 + 시작일을 조합하여 고유 SHA256 event_id 생성
        event_id = hashlib.sha256(f"{title}{place}{row.get('STRTDATE', '')}".encode()).hexdigest()
        records.append(
            {
                "event_id": event_id,
                "title": title,
                "category": row.get("CODENAME"),
                "gu": row.get("GUNAME"),
                "place": place,
                "start_date": _parse_date(row["STRTDATE"]),
                "end_date": end_date,
                "is_free": row.get("IS_FREE"),
                "lat": _to_float(row.get("LAT")),
                "lon": _to_float(row.get("LOT")),
            }
        )
    return records


def performance_events_from_silver(df: pd.DataFrame, today: date | None = None) -> list[dict]:
    """서울시 공공서비스예약(공연) Silver 데이터를 cultural_events 테이블 레코드 목록으로 변환한다.

    args:
        df: performance_event Silver DataFrame
        today: 행사 유효성 검사용 기준 일자 (KST, 기본값: 오늘)
    returns:
        cultural_events 테이블 적재용 레코드 목록 (종료된 행사 제외)
    """
    today = today or datetime.now(ZoneInfo("Asia/Seoul")).date()
    records = []
    for row in df.to_dict("records"):
        end_date = _parse_date(row.get("SVCOPNENDDT"))
        # 1. 이미 종료된 행사는 제외
        if end_date is not None and end_date < today:
            continue
        title = row.get("SVCNM", "")
        place = row.get("PLACENM", "")
        svcid = row.get("SVCID")

        # 2. 서울시 서비스ID(SVCID)가 있으면 사용하고, 없으면 제목+장소+시작일 해시로 event_id 생성
        event_id = str(svcid) if svcid else hashlib.sha256(f"{title}{place}{row.get('SVCOPNBGNDT', '')}".encode()).hexdigest()

        # 3. 유/무료 여부 정규화
        is_free_val = row.get("PAYATNM")
        is_free = "무료" if is_free_val == "무료" else "유료"

        # 4. cultural_events 공통 스키마에 맞게 매핑 (Y: 위도, X: 경도)
        records.append(
            {
                "event_id": event_id,
                "title": title,
                "category": row.get("MINCLASSNM"),
                "gu": row.get("AREANM"),
                "place": place,
                "start_date": _parse_date(row.get("SVCOPNBGNDT")),
                "end_date": end_date,
                "is_free": is_free,
                "lat": _to_float(row.get("Y")),
                "lon": _to_float(row.get("X")),
            }
        )
    return records


def forecast_points_from_predictions(df: pd.DataFrame, batch_run_at: datetime) -> list[dict]:
    """ML 추론 결과 DataFrame을 forecast_points 테이블 레코드 목록으로 변환한다.

    args:
        df: ML 추론 결과 DataFrame
        batch_run_at: 배치 실행 시각 (KST)
    returns:
        forecast_points 테이블 적재용 예측 레코드 목록
    """
    records = []
    for row in df.to_dict("records"):
        sta_id = str(row["station_id"])
        predicted_dttm = _kst_date_hm_to_utc(row["date"], int(row["hour"]), int(row["minute"]))
        records.append(
            {
                "sta_id": sta_id,
                "predicted_dttm": predicted_dttm,
                "predicted_rent_cnt": round(row["rental_pred_mean"]),
                "predicted_return_cnt": round(row["return_pred_mean"]),
                "batch_run_at": batch_run_at,
            }
        )
    return records


def _to_float(value) -> float | None:
    """문자열 또는 숫자 값을 float으로 변환한다 (변환 불가 시 None)."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    """문자열 또는 숫자 값을 int로 변환한다 (변환 불가 시 None)."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_date(value) -> date | None:
    """다양한 형식의 일자 문자열을 KST date 객체로 파싱한다."""
    if value is None or value == "":
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=ZoneInfo("Asia/Seoul")).date()
        except ValueError:
            continue
    return None
