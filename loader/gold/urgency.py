"""계산 완료 urgency 행을 Gold projection과 route용 artifact로 고정한다."""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
from core.gold_publication import ContractViolation, format_utc_dttm

from .common import parquet_bytes, read_parquet_bytes

_STATION_ID = re.compile(r"ST-[0-9]+\Z")
_NEED_TYPES = frozenset({"normal", "supply_needed", "retrieval_needed"})
_POSTGRES_INTEGER_MAX = 2_147_483_647
_URGENCY_SCHEMA = pa.schema(
    (
        pa.field("sta_id", pa.string(), nullable=False),
        pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("urgency_score", pa.float64(), nullable=False),
        pa.field("critical_remaining_min", pa.int32(), nullable=False),
        pa.field("rebalance_need_type_cd", pa.string(), nullable=False),
        pa.field("bike_qty", pa.int32(), nullable=False),
    )
)


@dataclass(frozen=True, slots=True)
class UrgencyRecord:
    """Gold urgency 행과 route용 이동 수량을 함께 표현한다."""

    sta_id: str
    base_dttm: datetime
    urgency_score: float
    critical_remaining_min: int
    rebalance_need_type_cd: str
    bike_qty: int

    def __post_init__(self) -> None:
        """DDL 값 범위와 route artifact 수량 계약을 검증한다."""
        _station_id(self.sta_id)
        object.__setattr__(self, "base_dttm", _utc_dttm(self.base_dttm, "base_dttm"))
        if type(self.urgency_score) is not float or not math.isfinite(
            self.urgency_score
        ):
            raise ContractViolation("urgency_score는 finite float여야 합니다.")
        if not 0.0 <= self.urgency_score <= 100.0:
            raise ContractViolation("urgency_score는 0..100이어야 합니다.")
        _postgres_nonnegative_integer(
            self.critical_remaining_min,
            "critical_remaining_min",
        )
        if (
            type(self.rebalance_need_type_cd) is not str
            or self.rebalance_need_type_cd not in _NEED_TYPES
        ):
            raise ContractViolation("rebalance_need_type_cd가 SSOT allowlist 밖입니다.")
        _postgres_nonnegative_integer(self.bike_qty, "bike_qty")


@dataclass(frozen=True, slots=True)
class StationUrgencyRecord:
    """RDS station_urgency에 게시할 bike_qty 없는 행을 표현한다."""

    sta_id: str
    base_dttm: datetime
    urgency_score: float
    critical_remaining_min: int
    rebalance_need_type_cd: str

    def __post_init__(self) -> None:
        """공개 target record도 artifact와 같은 DDL 계약으로 검증한다."""
        validated = UrgencyRecord(
            sta_id=self.sta_id,
            base_dttm=self.base_dttm,
            urgency_score=self.urgency_score,
            critical_remaining_min=self.critical_remaining_min,
            rebalance_need_type_cd=self.rebalance_need_type_cd,
            bike_qty=0,
        )
        object.__setattr__(self, "base_dttm", validated.base_dttm)


@dataclass(frozen=True, slots=True)
class UrgencyProjection:
    """기대 station 전체의 urgency artifact와 Gold 행을 보관한다."""

    records: tuple[UrgencyRecord, ...]
    base_dttm: datetime
    expected_sta_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        """projection의 타입·정렬·중복·anchor·기대 집합을 재검증한다."""
        if type(self.records) is not tuple or any(
            type(record) is not UrgencyRecord for record in self.records
        ):
            raise ContractViolation(
                "urgency records는 UrgencyRecord tuple이어야 합니다."
            )
        base = _utc_dttm(self.base_dttm, "base_dttm")
        object.__setattr__(self, "base_dttm", base)
        expected = _station_ids(self.expected_sta_ids, "expected_sta_ids")
        if self.expected_sta_ids != expected:
            raise ContractViolation(
                "expected_sta_ids는 중복 없이 UTF-8 순이어야 합니다."
            )
        record_ids = tuple(record.sta_id for record in self.records)
        if record_ids != expected:
            raise ContractViolation(
                "urgency projection ID가 기대 집합과 exact하게 같지 않습니다."
            )
        if any(record.base_dttm != base for record in self.records):
            raise ContractViolation("urgency 모든 행은 같은 UTC anchor여야 합니다.")

    @property
    def target_records(self) -> tuple[StationUrgencyRecord, ...]:
        """route 전용 bike_qty를 제외한 Gold target 행을 반환한다."""
        return tuple(
            StationUrgencyRecord(
                sta_id=record.sta_id,
                base_dttm=record.base_dttm,
                urgency_score=record.urgency_score,
                critical_remaining_min=record.critical_remaining_min,
                rebalance_need_type_cd=record.rebalance_need_type_cd,
            )
            for record in self.records
        )


def build_urgency_projection(
    computed_records: tuple[UrgencyRecord, ...],
    *,
    base_dttm: datetime,
    active_station_ids: tuple[str, ...],
    current_stock_station_ids: tuple[str, ...],
    demand_support_station_ids: tuple[str, ...],
) -> UrgencyProjection:
    """세 authoritative 집합의 교집합을 완전한 urgency projection으로 만든다."""
    if type(computed_records) is not tuple or any(
        type(record) is not UrgencyRecord for record in computed_records
    ):
        raise ContractViolation("computed_records는 UrgencyRecord tuple이어야 합니다.")
    base = _utc_dttm(base_dttm, "base_dttm")
    active = set(_station_ids(active_station_ids, "active_station_ids"))
    stock = set(_station_ids(current_stock_station_ids, "current_stock_station_ids"))
    demand = set(_station_ids(demand_support_station_ids, "demand_support_station_ids"))
    expected = tuple(sorted(active & stock & demand, key=_utf8_key))

    by_id: dict[str, UrgencyRecord] = {}
    for record in computed_records:
        if record.sta_id in by_id:
            raise ContractViolation(
                f"urgency 계산 결과에 중복 sta_id가 있습니다: {record.sta_id}"
            )
        if record.base_dttm.astimezone(UTC) != base:
            raise ContractViolation(
                "urgency 계산 결과 anchor가 publication anchor와 다릅니다."
            )
        by_id[record.sta_id] = record
    if set(by_id) != set(expected):
        missing = sorted(set(expected) - set(by_id), key=_utf8_key)
        extra = sorted(set(by_id) - set(expected), key=_utf8_key)
        raise ContractViolation(
            f"urgency 계산 ID가 기대 집합과 다릅니다: missing={missing}, extra={extra}"
        )

    ordered = tuple(
        UrgencyRecord(
            sta_id=station_id,
            base_dttm=base,
            urgency_score=by_id[station_id].urgency_score,
            critical_remaining_min=by_id[station_id].critical_remaining_min,
            rebalance_need_type_cd=by_id[station_id].rebalance_need_type_cd,
            bike_qty=by_id[station_id].bike_qty,
        )
        for station_id in expected
    )
    return UrgencyProjection(ordered, base, expected)


def urgency_records_to_parquet(
    records: tuple[UrgencyRecord, ...],
    *,
    expected_sta_ids: tuple[str, ...],
) -> bytes:
    """기대 station 전체의 nonempty urgency를 fixed-schema Parquet으로 만든다."""
    _validate_record_sequence(records, expected_sta_ids=expected_sta_ids)
    table = pa.Table.from_pylist(
        [
            {
                "sta_id": record.sta_id,
                "base_dttm": record.base_dttm,
                "urgency_score": record.urgency_score,
                "critical_remaining_min": record.critical_remaining_min,
                "rebalance_need_type_cd": record.rebalance_need_type_cd,
                "bike_qty": record.bike_qty,
            }
            for record in records
        ],
        schema=_URGENCY_SCHEMA,
    )
    return parquet_bytes(table)


def urgency_records_from_parquet(
    payload: bytes,
    *,
    expected_base_dttm: datetime,
    expected_sta_ids: tuple[str, ...],
) -> tuple[UrgencyRecord, ...]:
    """urgency Parquet을 authoritative 기대 anchor·집합과 다시 검증한다."""
    table = read_parquet_bytes(payload)
    if table.schema != _URGENCY_SCHEMA:
        raise ContractViolation(
            "urgency output Parquet schema가 exact 계약과 다릅니다."
        )
    records = tuple(UrgencyRecord(**row) for row in table.to_pylist())
    _validate_record_sequence(
        records,
        expected_sta_ids=expected_sta_ids,
        expected_base_dttm=expected_base_dttm,
    )
    return records


def _validate_record_sequence(
    records: tuple[UrgencyRecord, ...],
    *,
    expected_sta_ids: tuple[str, ...],
    expected_base_dttm: datetime | None = None,
) -> None:
    """artifact record를 authoritative 타입·정렬·anchor·집합에 결합한다."""
    if type(records) is not tuple or any(
        type(record) is not UrgencyRecord for record in records
    ):
        raise ContractViolation("urgency records는 UrgencyRecord tuple이어야 합니다.")
    expected = _station_ids(expected_sta_ids, "expected_sta_ids")
    if expected_sta_ids != expected:
        raise ContractViolation("expected_sta_ids는 중복 없이 UTF-8 순이어야 합니다.")
    if not records:
        raise ContractViolation(
            "조건부 EMPTY urgency는 Parquet artifact가 아니라 artifacts=[]여야 합니다."
        )
    base = records[0].base_dttm
    if expected_base_dttm is not None:
        base = _utc_dttm(expected_base_dttm, "expected base_dttm")
    UrgencyProjection(records, base, expected)


def _station_ids(values: tuple[str, ...], name: str) -> tuple[str, ...]:
    """station ID tuple을 검증하고 UTF-8 정렬 canonical tuple로 반환한다."""
    if type(values) is not tuple:
        raise ContractViolation(f"{name}은 station ID tuple이어야 합니다.")
    normalized = tuple(_station_id(value) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ContractViolation(f"{name}에 중복 station ID가 있습니다.")
    return tuple(sorted(normalized, key=_utf8_key))


def _station_id(value: str) -> str:
    """대여소 ID를 DDL의 canonical ST-숫자 형식으로 검증한다."""
    if type(value) is not str:
        raise ContractViolation("station ID는 문자열이어야 합니다.")
    normalized = unicodedata.normalize("NFC", value.strip())
    if normalized != value or _STATION_ID.fullmatch(normalized) is None:
        raise ContractViolation("station ID는 canonical ST-숫자 형식이어야 합니다.")
    return normalized


def _postgres_nonnegative_integer(value: int, name: str) -> int:
    """값을 PostgreSQL INTEGER 범위의 비음수 exact integer로 검증한다."""
    if type(value) is not int or not 0 <= value <= _POSTGRES_INTEGER_MAX:
        raise ContractViolation(
            f"{name}은 PostgreSQL INTEGER 범위의 비음수여야 합니다."
        )
    return value


def _utc_dttm(value: datetime, name: str) -> datetime:
    """exact aware datetime을 검증하고 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise ContractViolation(f"{name}은 datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)


def _utf8_key(value: str) -> bytes:
    """문자열의 결정적 UTF-8 정렬 key를 반환한다."""
    return value.encode("utf-8")
