"""ML inference rows를 완전한 12시간 Gold 수요 projection으로 변환한다."""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from core.gold_publication import ContractViolation

from .common import parquet_bytes, read_parquet_bytes

HORIZON_COUNT = 12
POSTGRES_INTEGER_MAX = 2_147_483_647
_STATION_ID = re.compile(r"ST-[0-9]+\Z")
_DEMAND_FORECAST_SCHEMA = pa.schema(
    (
        pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("sta_id", pa.string(), nullable=False),
        pa.field("predicted_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("predicted_rent_cnt", pa.int32(), nullable=False),
        pa.field("predicted_rtn_cnt", pa.int32(), nullable=False),
    )
)


@dataclass(frozen=True, slots=True)
class DemandPredictionRecord:
    """Gold 변환 직전의 typed inference mean 예측 행을 표현한다."""

    base_dttm: datetime
    station_id: str
    horizon: int
    target_dttm: datetime
    rental_pred_mean: float
    return_pred_mean: float

    def __post_init__(self) -> None:
        """source의 base·horizon·구간 시작시각·float64 계약을 검증한다."""
        base = _utc_dttm(self.base_dttm, "inference base_dttm")
        target = _utc_dttm(self.target_dttm, "inference target_dttm")
        object.__setattr__(self, "base_dttm", base)
        object.__setattr__(self, "target_dttm", target)
        _station_id(self.station_id)
        if type(self.horizon) is not int or not 1 <= self.horizon <= HORIZON_COUNT:
            raise ContractViolation("demand horizon은 정확히 1..12여야 합니다.")
        expected_target = _add_hours(
            base,
            self.horizon - 1,
            "inference target",
        )
        if target != expected_target:
            raise ContractViolation(
                "inference target은 base+(horizon-1)시간이어야 합니다."
            )
        _prediction_mean(self.rental_pred_mean, "rental_pred_mean")
        _prediction_mean(self.return_pred_mean, "return_pred_mean")


@dataclass(frozen=True, slots=True)
class DemandForecastRecord:
    """Gold station_demand_forecast 전체 교체 행을 표현한다."""

    base_dttm: datetime
    sta_id: str
    predicted_dttm: datetime
    predicted_rent_cnt: int
    predicted_rtn_cnt: int

    def __post_init__(self) -> None:
        """target DDL의 key·시간·PostgreSQL INTEGER 계약을 검증한다."""
        base = _utc_dttm(self.base_dttm, "demand base_dttm")
        predicted = _utc_dttm(self.predicted_dttm, "predicted_dttm")
        object.__setattr__(self, "base_dttm", base)
        object.__setattr__(self, "predicted_dttm", predicted)
        _station_id(self.sta_id)
        if predicted <= base:
            raise ContractViolation("predicted_dttm은 base_dttm 후여야 합니다.")
        _postgres_nonnegative_integer(self.predicted_rent_cnt, "predicted_rent_cnt")
        _postgres_nonnegative_integer(self.predicted_rtn_cnt, "predicted_rtn_cnt")


@dataclass(frozen=True, slots=True)
class DemandProjection:
    """active·두 모델 공통 지원 station의 완전 12시간 projection을 표현한다."""

    base_dttm: datetime
    expected_sta_ids: tuple[str, ...]
    records: tuple[DemandForecastRecord, ...]

    def __post_init__(self) -> None:
        """expected key 집합·공통 base·canonical row 순서를 검증한다."""
        base = _utc_dttm(self.base_dttm, "projection base_dttm")
        object.__setattr__(self, "base_dttm", base)
        if type(self.expected_sta_ids) is not tuple:
            raise ContractViolation("expected station ID는 tuple이어야 합니다.")
        expected_ids = _station_id_set(self.expected_sta_ids, "expected station")
        if self.expected_sta_ids != expected_ids:
            raise ContractViolation(
                "expected station ID는 중복 없이 UTF-8 순이어야 합니다."
            )
        if type(self.records) is not tuple or any(
            type(record) is not DemandForecastRecord for record in self.records
        ):
            raise ContractViolation(
                "demand records는 DemandForecastRecord tuple이어야 합니다."
            )
        for record in self.records:
            if record.base_dttm != base:
                raise ContractViolation(
                    "demand projection에 서로 다른 base_dttm이 섞였습니다."
                )
        ordered = tuple(
            sorted(
                self.records,
                key=lambda record: (
                    record.sta_id.encode("utf-8"),
                    record.predicted_dttm,
                ),
            )
        )
        if self.records != ordered:
            raise ContractViolation(
                "demand records는 (sta_id,predicted_dttm) 순이어야 합니다."
            )
        actual_keys = tuple(
            (record.sta_id, record.predicted_dttm) for record in self.records
        )
        if len(actual_keys) != len(set(actual_keys)):
            raise ContractViolation("demand projection에 중복 target key가 있습니다.")
        expected_keys = {
            (station_id, _add_hours(base, horizon, "demand predicted_dttm"))
            for station_id in expected_ids
            for horizon in range(1, HORIZON_COUNT + 1)
        }
        if set(actual_keys) != expected_keys:
            missing = len(expected_keys - set(actual_keys))
            extra = len(set(actual_keys) - expected_keys)
            raise ContractViolation(
                "demand projection이 station×horizon 1..12로 완전하지 않습니다: "
                f"missing={missing}, extra={extra}"
            )


def build_demand_projection(
    predictions: tuple[DemandPredictionRecord, ...],
    *,
    base_dttm: datetime,
    active_station_ids: tuple[str, ...],
    rental_model_station_ids: tuple[str, ...],
    return_model_station_ids: tuple[str, ...],
) -> DemandProjection:
    """typed inference를 active·두 모델 교집합의 완전한 Gold 행으로 만든다."""
    base = _utc_dttm(base_dttm, "projection base_dttm")
    if type(predictions) is not tuple or any(
        type(record) is not DemandPredictionRecord for record in predictions
    ):
        raise ContractViolation(
            "predictions는 DemandPredictionRecord tuple이어야 합니다."
        )
    active = _station_id_set(active_station_ids, "active station")
    rental = _station_id_set(rental_model_station_ids, "rental model station")
    returned = _station_id_set(return_model_station_ids, "return model station")
    expected_ids = tuple(
        sorted(
            set(active) & set(rental) & set(returned),
            key=lambda value: value.encode("utf-8"),
        )
    )
    indexed: dict[tuple[str, int], DemandPredictionRecord] = {}
    for prediction in predictions:
        if prediction.base_dttm != base:
            raise ContractViolation(
                "inference rows에 서로 다른 base_dttm이 섞였거나 anchor와 다릅니다."
            )
        key = (prediction.station_id, prediction.horizon)
        if key in indexed:
            raise ContractViolation("inference에 중복 station·horizon이 있습니다.")
        indexed[key] = prediction
    expected_keys = {
        (station_id, horizon)
        for station_id in expected_ids
        for horizon in range(1, HORIZON_COUNT + 1)
    }
    actual_keys = set(indexed)
    if actual_keys != expected_keys:
        missing = len(expected_keys - actual_keys)
        extra = len(actual_keys - expected_keys)
        raise ContractViolation(
            "inference가 active∩rental∩return×horizon 1..12와 다릅니다: "
            f"missing={missing}, extra={extra}"
        )
    records = tuple(
        DemandForecastRecord(
            base_dttm=base,
            sta_id=prediction.station_id,
            predicted_dttm=_add_hours(
                base,
                prediction.horizon,
                "demand predicted_dttm",
            ),
            predicted_rent_cnt=_round_prediction(
                prediction.rental_pred_mean, "rental_pred_mean"
            ),
            predicted_rtn_cnt=_round_prediction(
                prediction.return_pred_mean, "return_pred_mean"
            ),
        )
        for prediction in sorted(
            predictions,
            key=lambda record: (
                record.station_id.encode("utf-8"),
                record.horizon,
            ),
        )
    )
    return DemandProjection(base, expected_ids, records)


def demand_records_to_parquet(
    records: tuple[DemandForecastRecord, ...],
    *,
    expected_sta_ids: tuple[str, ...],
) -> bytes:
    """authoritative 기대 집합의 nonempty demand를 고정 schema Parquet으로 만든다."""
    _validate_record_snapshot(records, expected_sta_ids=expected_sta_ids)
    table = pa.Table.from_pylist(
        [
            {
                "base_dttm": record.base_dttm,
                "sta_id": record.sta_id,
                "predicted_dttm": record.predicted_dttm,
                "predicted_rent_cnt": record.predicted_rent_cnt,
                "predicted_rtn_cnt": record.predicted_rtn_cnt,
            }
            for record in records
        ],
        schema=_DEMAND_FORECAST_SCHEMA,
    )
    return parquet_bytes(table)


def demand_records_from_parquet(
    payload: bytes,
    *,
    expected_base_dttm: datetime,
    expected_sta_ids: tuple[str, ...],
) -> tuple[DemandForecastRecord, ...]:
    """고정 schema demand Parquet을 기대 anchor·집합과 함께 다시 검증한다."""
    table = read_parquet_bytes(payload)
    if table.schema != _DEMAND_FORECAST_SCHEMA:
        raise ContractViolation("demand output Parquet schema가 exact 계약과 다릅니다.")
    records = tuple(DemandForecastRecord(**row) for row in table.to_pylist())
    _validate_record_snapshot(
        records,
        expected_sta_ids=expected_sta_ids,
        expected_base_dttm=expected_base_dttm,
    )
    return records


def _validate_record_snapshot(
    records: tuple[DemandForecastRecord, ...],
    *,
    expected_sta_ids: tuple[str, ...],
    expected_base_dttm: datetime | None = None,
) -> None:
    """output records를 authoritative base·station별 완전 12시간에 결합한다."""
    if type(records) is not tuple or any(
        type(record) is not DemandForecastRecord for record in records
    ):
        raise ContractViolation(
            "demand records는 DemandForecastRecord tuple이어야 합니다."
        )
    expected_ids = _station_id_set(expected_sta_ids, "expected station")
    if expected_sta_ids != expected_ids:
        raise ContractViolation(
            "expected station ID는 중복 없이 UTF-8 순이어야 합니다."
        )
    if not records:
        raise ContractViolation(
            "조건부 EMPTY demand는 Parquet artifact가 아니라 artifacts=[]여야 합니다."
        )
    base = records[0].base_dttm
    if expected_base_dttm is not None:
        base = _utc_dttm(expected_base_dttm, "expected demand base_dttm")
    DemandProjection(base, expected_ids, records)


def _station_id_set(values: tuple[str, ...], name: str) -> tuple[str, ...]:
    """station ID tuple을 검증하고 중복 없는 UTF-8 순 tuple로 반환한다."""
    if type(values) is not tuple:
        raise ContractViolation(f"{name} ID는 tuple이어야 합니다.")
    normalized = tuple(_station_id(value) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ContractViolation(f"{name} ID에 중복이 있습니다.")
    return tuple(sorted(normalized, key=lambda value: value.encode("utf-8")))


def _station_id(value: object) -> str:
    """station ID를 target DDL의 canonical ST-숫자 문자열로 검증한다."""
    if type(value) is not str:
        raise ContractViolation("station ID는 문자열이어야 합니다.")
    normalized = unicodedata.normalize("NFC", value)
    if normalized != value or _STATION_ID.fullmatch(normalized) is None:
        raise ContractViolation("station ID는 canonical ST-숫자 형식이어야 합니다.")
    return normalized


def _utc_dttm(value: object, name: str) -> datetime:
    """offset 포함 Python datetime을 UTC instant로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None:
        raise ContractViolation(f"{name}은 offset 포함 datetime이어야 합니다.")
    try:
        offset = value.utcoffset()
        if offset is None:
            raise ValueError("missing UTC offset")
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ContractViolation(
            f"{name}을 유한 UTC instant로 바꿀 수 없습니다."
        ) from exc


def _add_hours(value: datetime, hours: int, name: str) -> datetime:
    """UTC 시각에 hour를 더하고 Python datetime 범위 초과를 계약 오류로 바꾼다."""
    try:
        return value + timedelta(hours=hours)
    except OverflowError as exc:
        raise ContractViolation(f"{name}이 datetime 범위를 벗어났습니다.") from exc


def _prediction_mean(value: object, name: str) -> float:
    """inference mean을 finite·비음수 Python float64로 검증한다."""
    if type(value) is not float or not math.isfinite(value) or value < 0.0:
        raise ContractViolation(f"{name}은 finite·비음수 float64여야 합니다.")
    return value


def _round_prediction(value: float, name: str) -> int:
    """Python ties-to-even으로 수량을 반올림하고 INTEGER 범위를 확인한다."""
    rounded = round(_prediction_mean(value, name))
    if rounded > POSTGRES_INTEGER_MAX:
        raise ContractViolation(f"{name} 반올림 결과가 PostgreSQL INTEGER를 넘습니다.")
    return rounded


def _postgres_nonnegative_integer(value: object, name: str) -> int:
    """값을 target의 비음수 PostgreSQL INTEGER 범위로 검증한다."""
    if type(value) is not int or value < 0 or value > POSTGRES_INTEGER_MAX:
        raise ContractViolation(f"{name}은 비음수 PostgreSQL INTEGER여야 합니다.")
    return value
