"""Silver DataFrame을 Gold 테이블 upsert용 dict 레코드로 변환한다.

대상 테이블이 5개로 고정돼 있어 collector 같은 YAML+정책 기반 범용 프레임워크는
쓰지 않는다. 테이블마다 명시적인 순수 함수 하나씩 두는 편이 더 읽기 쉽다.
`db-loader/implementation-plan.md` 1·2절의 컬럼 매핑을 그대로 구현한다.
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
    """`weather_ultra_short_term` silver -> weather_current 레코드.

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
        if existing is not None and existing["_base_dttm"] >= base_dttm:
            continue
        by_key[key] = {
            "gu": gu,
            "forecast_dttm": forecast_dttm,
            "temperature": _to_float(row.get("TMP")),
            "precip_prob": _to_float(row.get("POP")),
            "sky_cond": _to_int(row.get("SKY")),
            "pty_type": _to_int(row.get("PTY")),
            "_base_dttm": base_dttm,
        }
    for record in by_key.values():
        del record["_base_dttm"]
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
        event_id = hashlib.sha256(f"{title}{place}".encode()).hexdigest()
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
