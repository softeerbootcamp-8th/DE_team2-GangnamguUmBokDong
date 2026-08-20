"""authoritative 따릉이 realtime candidate를 Gold station_stock으로 변환한다."""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.gold_publication import ContractViolation, format_utc_dttm

STATION_STOCK_POLICY_VERSION = "gold-station-stock-policy-v1"
_STATION_ID = re.compile(r"ST-[0-9]+\Z")


@dataclass(frozen=True, slots=True)
class StationStockRecord:
    """Gold station_stock의 source-window 전체 교체 행을 표현한다."""

    sta_id: str
    base_dttm: datetime
    parking_bike_tot_cnt: int

    def __post_init__(self) -> None:
        """ID·시각·비음수 재고 계약을 검증한다."""
        _station_id(self.sta_id)
        if type(self.base_dttm) is not datetime:
            raise ContractViolation("station stock base_dttm은 datetime이어야 합니다.")
        format_utc_dttm(self.base_dttm)
        if type(self.parking_bike_tot_cnt) is not int or self.parking_bike_tot_cnt < 0:
            raise ContractViolation("parking_bike_tot_cnt는 0 이상 integer여야 합니다.")


@dataclass(frozen=True, slots=True)
class StationStockProjection:
    """realtime candidate와 같은 base의 전체 station_stock projection을 담는다."""

    records: tuple[StationStockRecord, ...]
    base_dttm: datetime
    excluded_missing_or_invalid_count: int

    def __post_init__(self) -> None:
        """projection의 exact base·정렬·중복 계약을 검증한다."""
        if type(self.records) is not tuple or any(
            type(record) is not StationStockRecord for record in self.records
        ):
            raise ContractViolation(
                "station stock records는 StationStockRecord tuple이어야 합니다."
            )
        base = _utc_dttm(self.base_dttm, "base_dttm")
        ids = tuple(record.sta_id for record in self.records)
        if ids != tuple(sorted(ids, key=lambda value: value.encode("utf-8"))):
            raise ContractViolation(
                "station stock records는 sta_id UTF-8 순이어야 합니다."
            )
        if len(ids) != len(set(ids)):
            raise ContractViolation(
                "station stock projection에 중복 sta_id가 있습니다."
            )
        if any(record.base_dttm != base for record in self.records):
            raise ContractViolation(
                "station stock 모든 행은 같은 candidate base여야 합니다."
            )
        if (
            type(self.excluded_missing_or_invalid_count) is not int
            or self.excluded_missing_or_invalid_count < 0
        ):
            raise ContractViolation(
                "station stock exclusion count는 0 이상 integer여야 합니다."
            )


def build_station_stock_projection(
    realtime_rows: tuple[Mapping[str, Any], ...],
    *,
    published_station_ids: tuple[str, ...],
    candidate_logical_dttm: datetime,
) -> StationStockProjection:
    """현재 candidate의 Gold station ID에 대한 재고만 전체 projection으로 만든다."""
    if type(realtime_rows) is not tuple:
        raise ContractViolation("station realtime rows는 tuple이어야 합니다.")
    if type(published_station_ids) is not tuple:
        raise ContractViolation("published station IDs는 tuple이어야 합니다.")
    normalized_published = tuple(
        sorted(
            {_station_id(station_id) for station_id in published_station_ids},
            key=lambda value: value.encode("utf-8"),
        )
    )
    if len(normalized_published) != len(published_station_ids):
        raise ContractViolation("published station ID에 중복이 있습니다.")
    published = set(normalized_published)
    base = _utc_dttm(candidate_logical_dttm, "candidate_logical_dttm")
    seen: set[str] = set()
    records: list[StationStockRecord] = []
    excluded = 0
    for row in realtime_rows:
        if not isinstance(row, Mapping):
            raise ContractViolation("station realtime row는 mapping이어야 합니다.")
        station_id = _station_id(row.get("stationId"))
        if station_id in seen:
            raise ContractViolation(
                f"station realtime candidate에 중복 ID가 있습니다: {station_id}"
            )
        seen.add(station_id)
        if station_id not in published:
            continue
        parking = _nonnegative_int_or_none(row.get("parkingBikeTotCnt"))
        if parking is None:
            excluded += 1
            continue
        records.append(StationStockRecord(station_id, base, parking))
    records.sort(key=lambda record: record.sta_id.encode("utf-8"))
    return StationStockProjection(tuple(records), base, excluded)


def _station_id(value: Any) -> str:
    """대여소 ID를 NFC nonblank 문자열로 검증한다."""
    if type(value) is not str:
        raise ContractViolation("station ID는 문자열이어야 합니다.")
    normalized = unicodedata.normalize("NFC", value.strip())
    if normalized != value or _STATION_ID.fullmatch(normalized) is None:
        raise ContractViolation("station ID는 canonical ST-숫자 형식이어야 합니다.")
    return normalized


def _nonnegative_int_or_none(value: Any) -> int | None:
    """재고가 결측·무효하면 None, 비음수 integer면 값을 반환한다."""
    if value is None or value == "" or type(value) is bool:
        return None
    try:
        converted = int(value)
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric != converted or converted < 0:
        return None
    return converted


def _utc_dttm(value: Any, name: str) -> datetime:
    """exact aware datetime을 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise ContractViolation(f"{name}은 datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)
