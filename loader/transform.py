"""Silver DataFrame을 Gold 테이블 upsert용 dict 레코드로 변환한다.

대상 테이블이 5개로 고정돼 있어 collector 같은 YAML+정책 기반 범용 프레임워크는
쓰지 않는다. 테이블마다 명시적인 순수 함수 하나씩 두는 편이 더 읽기 쉽다.
`loader/implementation-plan.md` 1·2절의 컬럼 매핑을 그대로 구현한다.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from gu_mapping import grid_to_gu, latlon_to_gu

_KST = timedelta(hours=9)


def _kst_to_utc(date_str: str, time_str: str) -> datetime:
    """기상청 baseDate/fcstDate(YYYYMMDD) + baseTime/fcstTime(HHMM, KST)을 UTC로 변환한다."""
    naive_kst = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M").replace(tzinfo=UTC)
    return naive_kst - _KST


def _kst_date_hm_to_utc(date_str: str, hour: int, minute: int) -> datetime:
    """predict_single.py 출력의 date("YYYY-MM-DD", KST) + hour/minute(정수)을 UTC로 변환한다.

    기상청 baseDate/fcstDate(YYYYMMDD, 대시 없음)와 형식이 달라 `_kst_to_utc`를
    재사용하지 않고 별도 헬퍼로 둔다.
    """
    naive_kst = datetime.strptime(f"{date_str} {hour:02d}{minute:02d}", "%Y-%m-%d %H%M").replace(tzinfo=UTC)
    return naive_kst - _KST


def stations_from_silver(df: pd.DataFrame) -> list[dict]:
    """`bike_station_realtime` silver -> stations 레코드."""
    records = []
    for row in df.to_dict("records"):
        lat = float(row["stationLatitude"])
        lon = float(row["stationLongitude"])
        gu = latlon_to_gu(lat, lon)
        if gu is None:
            continue  # 서울 자치구 경계 밖(인접 도시 접경) 정거장은 gu 스코프 테이블에서 제외
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
    """`bike_station_realtime` silver -> station_stock 레코드. observed_at은 호출자가 결정한다.

    `stations`에 없는 sta_id는 FK 위반이 나므로, `stations_from_silver`와 동일하게
    서울 자치구 경계 밖 정거장은 제외한다.
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
    """`weather_ultra_short_live` silver -> weather_current 레코드.

    격자(nx, ny)를 gu로 변환한 뒤, 같은 gu에 여러 격자가 매핑되면 가장 최근 관측
    (baseDate·baseTime 기준) 1건만 남긴다.
    """
    by_gu: dict[str, dict] = {}
    for row in df.to_dict("records"):
        gu = grid_to_gu(row["nx"], row["ny"])
        if gu is None:
            continue
        observed_at = _kst_to_utc(str(row["baseDate"]), str(row["baseTime"]))
        existing = by_gu.get(gu)
        if existing is not None and existing["observed_at"] >= observed_at:
            continue
        by_gu[gu] = {
            "gu": gu,
            "observed_at": observed_at,
            "temperature": _to_float(row.get("T1H")),
            "humidity": _to_float(row.get("REH")),
            "wind_speed": _to_float(row.get("WSD")),
            "rainfall": _to_float(row.get("RN1")),
            "pty_type": _to_int(row.get("PTY")),
        }
    return list(by_gu.values())


def weather_forecast_from_silver(df: pd.DataFrame) -> list[dict]:
    """`weather_short_term_forecast` silver -> weather_forecast 레코드.

    동일 (gu, forecast_dttm)에 대해 가장 최근에 발표된(base_dttm이 가장 늦은) 예보만 남긴다.
    base_dttm은 대시보드에서 '(기상청 XX:XX 발표 기준)' 표시를 위해 보존한다.
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
            "sky_cond": _to_int(row.get("SKY")),
            "pty_type": _to_int(row.get("PTY")),
            "temperature": _to_float(row.get("TMP")),
            "precip_prob": _to_float(row.get("POP")),
            "precip_amount": _parse_precip_str(row.get("PCP")),
            "humidity": _to_float(row.get("REH")),
            "wind_speed": _to_float(row.get("WSD")),
            "base_dttm": base_dttm,
        }
    return list(by_key.values())


def _parse_precip_str(value) -> float | None:
    """기상청 강수량 문자열('강수없음', '1.0mm 미만', '30.0~50.0mm' 등)을 float으로 변환한다.

    단기예보(PCP)와 초단기예보(RN1)는 숫자가 아닌 범위 문자열을 반환하는 경우가 있다.
    범위인 경우 하한값을 사용하고, '강수없음'은 0.0으로 변환한다.
    """
    if value is None or value == "":
        return None
    text = str(value).strip()
    if text in ("강수없음", "적설없음"):
        return 0.0
    if "미만" in text:
        return 0.5  # "1.0mm 미만" → 0에 가까운 값
    # "30.0~50.0mm" → 30.0
    text = text.replace("mm", "").strip()
    if "~" in text:
        text = text.split("~")[0]
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def weather_forecast_ultra_from_silver(df: pd.DataFrame) -> list[dict]:
    """`weather_ultra_short_forecast` silver -> weather_forecast 레코드.

    초단기예보는 단기예보와 컬럼명이 다르다(T1H/RN1 vs TMP/PCP, POP 없음).
    동일 (gu, forecast_dttm)에 대해 가장 최근에 발표된 예보만 남긴다.
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
            "sky_cond": _to_int(row.get("SKY")),
            "pty_type": _to_int(row.get("PTY")),
            "temperature": _to_float(row.get("T1H")),
            "precip_prob": None,  # 초단기예보에는 강수확률(POP)이 없다
            "precip_amount": _parse_precip_str(row.get("RN1")),
            "humidity": _to_float(row.get("REH")),
            "wind_speed": _to_float(row.get("WSD")),
            "base_dttm": base_dttm,
        }
    return list(by_key.values())


def cultural_events_from_silver(df: pd.DataFrame, today: date | None = None) -> list[dict]:
    """`cultural_event` silver -> cultural_events 레코드. 종료일이 지난 행사는 제외한다."""
    today = today or datetime.now(UTC).date()
    records = []
    for row in df.to_dict("records"):
        end_date = _parse_date(row["END_DATE"])
        if end_date is not None and end_date < today:
            continue
        title = row["TITLE"]
        place = row["PLACE"]
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
    """`performance_event` silver -> cultural_events 레코드. 종료일이 지난 행사는 제외한다."""
    today = today or datetime.now(UTC).date()
    records = []
    for row in df.to_dict("records"):
        end_date = _parse_date(row.get("SVCOPNENDDT"))
        if end_date is not None and end_date < today:
            continue
        title = row.get("SVCNM", "")
        place = row.get("PLACENM", "")
        svcid = row.get("SVCID")
        
        event_id = str(svcid) if svcid else hashlib.sha256(f"{title}{place}{row.get('SVCOPNBGNDT', '')}".encode()).hexdigest()
        
        is_free_val = row.get("PAYATNM")
        is_free = "무료" if is_free_val == "무료" else "유료"
        
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
    """ml/inference의 predict_single.py --all-stations 출력 -> forecast_points 레코드.

    각 행의 date/hour/minute는 이미 그 horizon의 목표 시각이다
    (predict_demand_multi_hour_all_stations()가 target_ts = anchor_ts + (horizon-1)h로
    계산해서 채워 넣는다) — horizon을 여기서 다시 더하지 않는다.

    station_id("ST-101" 등)와 stations.sta_id("101" 등, bike_station_realtime의
    raw stationId를 그대로 씀)가 같은 값 공간인지는 실제 데이터로 아직 확정되지
    않았다 — libs/ml_common/silver_schema.py의 컬럼 매핑이 둘 다 raw stationId를
    그대로 통과시키는 것처럼 보이지만, 실제 Seoul OpenAPI 응답으로 검증 전까지는
    가정으로만 남겨둔다.
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
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_date(value) -> date | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=UTC).date()
        except ValueError:
            continue
    return None
