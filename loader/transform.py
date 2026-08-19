"""Silver 계층 DataFrame 및 ML 예측 결과를 Gold 테이블 규격 레코드로 변환한다."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from gu_mapping import grid_to_gu, latlon_to_gu

logger = logging.getLogger(__name__)

_STADIUM_COORDS_PATH = Path(__file__).parent / "assets" / "stadium_coords.json"

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
    """서울시 체육시설 공연행사 Silver 데이터를 cultural_events 테이블 레코드 목록으로 변환한다.

    args:
        df: performance_event Silver DataFrame
        today: 행사 유효성 검사용 기준 일자 (KST, 기본값: 오늘)
    returns:
        cultural_events 테이블 적재용 레코드 목록 (종료된 행사 제외)
    """
    today = today or datetime.now(ZoneInfo("Asia/Seoul")).date()
    records = []
    unmapped_codes: set[str] = set()
    for row in df.to_dict("records"):
        end_date = _parse_date(row.get("EDATE"))
        # 1. 이미 종료된 행사는 제외
        if end_date is not None and end_date < today:
            continue
        # 2. 제목이 없는 행은 건너뛴다 — 스키마 변경 이전 Silver 파티션을 백필로
        #    다시 읽으면 컬럼명이 전부 달라 title이 비고, event_id가 sha256("")로
        #    모든 행에서 같아져 한 행으로 뭉개진다.
        title = row.get("TITLE") or ""
        if not str(title).strip():
            continue
        # SCH_CODE_A/B는 숫자 코드라 화면에 그대로 쓸 수 없다. 사람이 읽는 이름은
        # CODE_TITLE_A/B로 따로 오므로 표시용 필드에는 그쪽을 쓴다.
        place = row.get("CODE_TITLE_B", "")
        schedule_id = row.get("SCH_SEQ")

        # 3. 일정 순번이 있으면 사용하고, 없으면 제목+시설+시작일 해시로 event_id를 생성한다.
        event_id = (
            str(schedule_id)
            if schedule_id
            else hashlib.sha256(f"{title}{place}{row.get('SDATE', '')}".encode()).hexdigest()
        )

        # 4. USE_PAY는 자유 텍스트(가격표·안내 URL·"없음" 등)라 유/무료로 정규화하지
        #    않고 원문을 그대로 싣는다.
        is_free = row.get("USE_PAY")

        # 5. 원본 API가 좌표를 주지 않으므로 시설 코드로 좌표 마스터를 조회해 채운다.
        #    마스터에 없는 코드(시설 신설 등)는 좌표 없이 적재하고 경고만 남긴다 —
        #    행을 버리는 것보다 낫지만, 좌표가 없으면 반경 조회에는 잡히지 않는다.
        stadium_code = str(row.get("SCH_CODE_B") or "")
        coords = _stadium_coords().get(stadium_code)
        if coords is None:
            unmapped_codes.add(stadium_code)
        lat, lon, gu = coords if coords else (None, None, None)

        records.append(
            {
                "event_id": event_id,
                "title": title,
                "category": row.get("CODE_TITLE_A"),
                "gu": gu,
                "place": place,
                "start_date": _parse_date(row.get("SDATE")),
                "end_date": end_date,
                "is_free": is_free,
                "lat": lat,
                "lon": lon,
            }
        )

    if unmapped_codes:
        logger.warning(
            "stadium_coords에 없는 시설 코드 %s — 해당 행사는 좌표 없이 적재되어 "
            "주변 행사 조회에 잡히지 않는다. assets/stadium_coords.json에 추가가 필요하다.",
            sorted(unmapped_codes),
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


@lru_cache(maxsize=1)
def _stadium_coords() -> dict[str, tuple[float, float, str | None]]:
    """시설 코드(SCH_CODE_B) → (위도, 경도, 자치구) 조회 테이블을 반환한다.

    자치구는 좌표에서 도출하므로(gu_mapping) 마스터 파일에는 좌표만 둔다 —
    자치구 경계와 좌표가 따로 갱신되며 어긋나는 일을 막는다.
    """
    raw = json.loads(_STADIUM_COORDS_PATH.read_text(encoding="utf-8"))
    return {
        code: (entry["lat"], entry["lon"], latlon_to_gu(entry["lat"], entry["lon"]))
        for code, entry in raw.items()
        if not code.startswith("_")
    }


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
