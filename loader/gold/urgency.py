"""계산 완료 urgency 행을 Gold projection과 route용 artifact로 고정한다."""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
from core.forecast import enrich_forecast_points
from core.gold_publication import (
    ContractViolation,
    Dependency,
    ImmutableObjectStore,
    InputArtifact,
    Parameter,
    PreparedPublication,
    PublicationManifest,
    VerifiedPublicationEvidence,
    build_id_set,
    format_utc_dttm,
    parse_input_fingerprint,
    validate_id_set_parameter,
    validate_input_fingerprint,
    validate_linked_dependency_manifests,
    validate_sha256_hex,
)
from core.scoring_config import (
    FIRST_FORECAST_MIN,
    HALF_LIFE_MIN,
    RESPONSE_LAG_MIN,
    SEVERITY_SCALE,
    SUPPLY_LOW_STOCK_RATIO,
    URGENCY_SCORING_CONFIG_VERSION,
    URGENCY_STOCK_HISTORY_MIN_WINDOWS,
    URGENCY_STOCK_HISTORY_OFFSETS_MINUTES,
)
from core.source_snapshot import SourceSnapshotStatus
from psycopg import Connection, Cursor
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .common import (
    OutputObject,
    PublicationExecution,
    build_prepared_publication,
    materialize_publication,
    parquet_bytes,
    publish_verified,
    read_parquet_bytes,
    read_source_snapshot_payload,
    source_snapshot_parquet,
    store_input_payload,
)
from .demand import DemandForecastRecord, demand_records_from_parquet
from .rebalance_policy import (
    DEFAULT_REBALANCE_POLICY,
    RebalancePolicyConfig,
)
from .source_catalog import S3SourceSnapshotCatalog
from .state import (
    PublicationStateRecord,
    load_dependencies,
    load_publication_state,
    read_state_manifest,
)
from .station_release import _stock_records_from_parquet
from .station_stock import StationStockRecord
from .versioning import PublicationCandidate, allocate_revision

_STATION_ID = re.compile(r"ST-[0-9]+\Z")
_NEED_TYPES = frozenset({"normal", "supply_needed", "retrieval_needed"})
_POSTGRES_INTEGER_MAX = 2_147_483_647
_SERVING_RELEASE_KEYS = frozenset(
    {"station", "station_demand_forecast", "station_stock"}
)
BIKE_STATION_REALTIME_SOURCE_ID = "bike_station_realtime"
URGENCY_PUBLISHER_VERSION = "gold-urgency-publisher-v4-any-depletion"
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


@dataclass(frozen=True, slots=True)
class ActiveStation:
    """Urgency 계산에 고정하는 active Gold station topology를 표현한다."""

    sta_id: str
    hold_cnt: int
    longitude: float
    latitude: float
    dispatch_center_id: str

    def __post_init__(self) -> None:
        """Station ID·정원·Point·센터 필드를 target DDL 범위로 검증한다."""
        _station_id(self.sta_id)
        if (
            type(self.hold_cnt) is not int
            or not 0 < self.hold_cnt <= _POSTGRES_INTEGER_MAX
        ):
            raise ContractViolation(
                "active station hold_cnt는 양의 INTEGER여야 합니다."
            )
        if (
            type(self.longitude) is not float
            or not math.isfinite(self.longitude)
            or not 126.5 <= self.longitude <= 127.5
            or type(self.latitude) is not float
            or not math.isfinite(self.latitude)
            or not 37.0 <= self.latitude <= 38.0
        ):
            raise ContractViolation(
                "active station Point가 SRID 4326 서울 범위 밖입니다."
            )
        if (
            type(self.dispatch_center_id) is not str
            or not self.dispatch_center_id.strip()
            or unicodedata.normalize("NFC", self.dispatch_center_id)
            != self.dispatch_center_id
        ):
            raise ContractViolation("active station dispatch center ID가 잘못됐습니다.")


@dataclass(frozen=True, slots=True)
class StockHistoryPoint:
    """Authoritative realtime snapshot 한 시점의 station 재고를 표현한다."""

    sta_id: str
    observed_at: datetime
    parking_bike_tot_cnt: int

    def __post_init__(self) -> None:
        """History station·시각·재고를 scoring 입력 범위로 검증한다."""
        _station_id(self.sta_id)
        object.__setattr__(
            self,
            "observed_at",
            _utc_dttm(self.observed_at, "stock history observed_at"),
        )
        _postgres_nonnegative_integer(
            self.parking_bike_tot_cnt,
            "stock history parking_bike_tot_cnt",
        )


@dataclass(frozen=True, slots=True)
class UrgencyCalculationInputs:
    """검증된 topology·과거/현재 재고·Gold demand 계산 입력을 묶는다."""

    active_stations: tuple[ActiveStation, ...]
    history_offsets_minutes: tuple[int, ...]
    history_windows: tuple[tuple[StockHistoryPoint, ...], ...]
    current_stock: tuple[StationStockRecord, ...]
    demand: tuple[DemandForecastRecord, ...]
    base_dttm: datetime

    def __post_init__(self) -> None:
        """모든 입력의 타입·순서·window 수·anchor를 재검증한다."""
        base = _utc_dttm(self.base_dttm, "urgency input base_dttm")
        object.__setattr__(self, "base_dttm", base)
        if type(self.active_stations) is not tuple or any(
            type(item) is not ActiveStation for item in self.active_stations
        ):
            raise ContractViolation(
                "active_stations는 ActiveStation tuple이어야 합니다."
            )
        active_ids = tuple(item.sta_id for item in self.active_stations)
        if active_ids != _station_ids(active_ids, "active station IDs"):
            raise ContractViolation("active station은 sta_id UTF-8 순이어야 합니다.")
        offsets = self.history_offsets_minutes
        allowed = set(URGENCY_STOCK_HISTORY_OFFSETS_MINUTES)
        if (
            type(offsets) is not tuple
            or any(type(offset) is not int for offset in offsets)
            or not set(offsets).issubset(allowed)
            or offsets
            != tuple(
                offset
                for offset in URGENCY_STOCK_HISTORY_OFFSETS_MINUTES
                if offset in offsets
            )
        ):
            raise ContractViolation(
                "history_offsets_minutes는 scoring config offset의 "
                "중복 없는 oldest-first 부분집합이어야 합니다."
            )
        if len(offsets) < URGENCY_STOCK_HISTORY_MIN_WINDOWS:
            raise ContractViolation(
                "stock history window가 하한 미만입니다: "
                f"actual={len(offsets)} min={URGENCY_STOCK_HISTORY_MIN_WINDOWS}"
            )
        if (
            type(self.history_windows) is not tuple
            or len(self.history_windows) != len(offsets)
            or any(type(window) is not tuple for window in self.history_windows)
            or any(
                type(point) is not StockHistoryPoint
                for window in self.history_windows
                for point in window
            )
        ):
            raise ContractViolation(
                "stock history는 offset 수와 같은 point tuple window여야 합니다."
            )
        for offset, window in zip(offsets, self.history_windows, strict=True):
            ids = tuple(point.sta_id for point in window)
            if ids != _station_ids(ids, "stock history station IDs"):
                raise ContractViolation(
                    "stock history row는 sta_id UTF-8 순이어야 합니다."
                )
            expected_time = base + timedelta(minutes=offset)
            if any(point.observed_at != expected_time for point in window):
                raise ContractViolation(
                    "stock history window 시각 순서가 SSOT와 다릅니다."
                )
        if type(self.current_stock) is not tuple or any(
            type(item) is not StationStockRecord for item in self.current_stock
        ):
            raise ContractViolation(
                "current_stock은 StationStockRecord tuple이어야 합니다."
            )
        current_ids = tuple(item.sta_id for item in self.current_stock)
        if current_ids != _station_ids(current_ids, "current stock station IDs"):
            raise ContractViolation("current stock은 sta_id UTF-8 순이어야 합니다.")
        if any(
            _utc_dttm(item.base_dttm, "current stock base") != base
            for item in self.current_stock
        ):
            raise ContractViolation("current stock row가 urgency anchor와 다릅니다.")
        if type(self.demand) is not tuple or any(
            type(item) is not DemandForecastRecord for item in self.demand
        ):
            raise ContractViolation("demand는 DemandForecastRecord tuple이어야 합니다.")
        if any(item.base_dttm != base for item in self.demand):
            raise ContractViolation("demand row가 urgency anchor와 다릅니다.")


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


def compute_urgency_projection(
    inputs: UrgencyCalculationInputs,
    *,
    policy_config: RebalancePolicyConfig = DEFAULT_REBALANCE_POLICY,
) -> UrgencyProjection:
    """검증된 Gold·immutable 입력과 재배치 정책으로 urgency projection을 계산한다."""
    if type(inputs) is not UrgencyCalculationInputs:
        raise ContractViolation("inputs는 UrgencyCalculationInputs여야 합니다.")
    if type(policy_config) is not RebalancePolicyConfig:
        raise ContractViolation("policy_config는 RebalancePolicyConfig여야 합니다.")
    active_by_id = {station.sta_id: station for station in inputs.active_stations}
    current_by_id = {record.sta_id: record for record in inputs.current_stock}
    demand_by_id: dict[str, list[DemandForecastRecord]] = {}
    for record in inputs.demand:
        demand_by_id.setdefault(record.sta_id, []).append(record)
    demand_ids = tuple(sorted(demand_by_id, key=_utf8_key))
    expected = tuple(
        sorted(
            set(active_by_id) & set(current_by_id) & set(demand_ids),
            key=_utf8_key,
        )
    )
    history_by_id: dict[str, list[StockHistoryPoint]] = {}
    for window in inputs.history_windows:
        for point in window:
            if point.sta_id in expected:
                history_by_id.setdefault(point.sta_id, []).append(point)

    computed: list[UrgencyRecord] = []
    for station_id in expected:
        station = active_by_id[station_id]
        current = current_by_id[station_id].parking_bike_tot_cnt
        forecasts = demand_by_id[station_id]
        raw_points = [
            {
                "predicted_rent_cnt": record.predicted_rent_cnt,
                "predicted_return_cnt": record.predicted_rtn_cnt,
            }
            for record in forecasts
        ]
        points = enrich_forecast_points(current, station.hold_cnt, raw_points)
        history = [
            {
                "observed_at": point.observed_at,
                "parking_bike_tot_cnt": point.parking_bike_tot_cnt,
            }
            for point in history_by_id.get(station_id, ())
        ]
        history.append(
            {
                "observed_at": inputs.base_dttm,
                "parking_bike_tot_cnt": current,
            }
        )
        score, minutes, action_type = _urgency_score_v1(
            current,
            station.hold_cnt,
            history,
            points,
            inputs.base_dttm,
        )
        computed.append(
            UrgencyRecord(
                sta_id=station_id,
                base_dttm=inputs.base_dttm,
                urgency_score=score,
                critical_remaining_min=minutes,
                rebalance_need_type_cd=action_type,
                bike_qty=(
                    _bike_qty_v1(
                        current,
                        station.hold_cnt,
                        action_type,
                        points,
                    )
                    if policy_config.quantity_strategy == "legacy"
                    else _bike_qty_risk_band_v4(
                        current,
                        station.hold_cnt,
                        action_type,
                        points,
                        history,
                        inputs.base_dttm,
                        policy_config,
                    )
                ),
            )
        )
    return build_urgency_projection(
        tuple(computed),
        base_dttm=inputs.base_dttm,
        active_station_ids=tuple(active_by_id),
        current_stock_station_ids=tuple(current_by_id),
        demand_support_station_ids=demand_ids,
    )


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


def publish_station_urgency(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    source_catalog: S3SourceSnapshotCatalog,
    stock_history_manifest_refs: tuple[tuple[int, str, str], ...],
    serving_release_manifest_refs: Mapping[str, tuple[str, str]],
    object_base_uri: str,
    publisher_version: str = URGENCY_PUBLISHER_VERSION,
) -> PublicationExecution:
    """가용 history와 committed Gold stock·demand로 urgency를 원자 게시한다.

    과거 source manifest는 ``(offset분, uri, sha256)``을 ``anchor-25..-5분``
    oldest-first로 받는다. 5개 전부가 아니어도 되지만 **존재하는 window를
    빠뜨리는 것은 금지**한다 — `_validate_history_catalog`가 빠진 offset마다
    실제로 authority window가 없음을 증명하고서야 통과시키므로, 같은 anchor에서
    결과가 갈리지 않는다. 하한은 `URGENCY_STOCK_HISTORY_MIN_WINDOWS`이고 그
    미만이면 단발성 결측이 아닌 수집 장애로 보아 실패시킨다.

    현재 재고와 demand는 각각 잠길 Gold publication state가 가리키는 actual
    manifest·output을 사용한다. 계산 결과 ``urgency_output``도 immutable input으로
    먼저 고정하고 공통 evidence verifier가 재계산 결과와 대조한 뒤 target/state를
    함께 바꾼다.
    """
    if type(source_catalog) is not S3SourceSnapshotCatalog:
        raise ContractViolation("source_catalog는 S3SourceSnapshotCatalog여야 합니다.")
    release_refs = _serving_release_manifest_refs(serving_release_manifest_refs)
    history_inputs = _stock_history_input_artifacts(stock_history_manifest_refs)
    dependencies = load_dependencies(
        connection,
        ("station", "station_demand_forecast", "station_stock"),
    )
    dependency_by_key = {item.publication_key: item for item in dependencies}
    _validate_serving_release_state_refs(
        connection,
        object_store,
        dependency_by_key,
        release_refs,
    )
    anchor = dependency_by_key["station_stock"].logical_dttm
    if dependency_by_key["station_demand_forecast"].logical_dttm != anchor:
        raise ContractViolation("urgency demand와 current stock anchor가 다릅니다.")
    _validate_history_catalog(source_catalog, history_inputs, anchor)
    demand_state, demand_manifest, demand_input = _linked_state_manifest_input(
        connection,
        object_store,
        dependency_by_key["station_demand_forecast"],
        role="demand_publication_manifest",
    )
    stock_state, stock_manifest, stock_input = _linked_state_manifest_input(
        connection,
        object_store,
        dependency_by_key["station_stock"],
        role="stock_publication_manifest",
    )
    active_stations = _load_active_stations(connection)
    input_artifacts = (
        demand_input,
        *(artifact for _offset, artifact in history_inputs),
        stock_input,
    )
    input_payloads = {
        artifact.uri: object_store.read_bytes(
            artifact.uri,
            artifact.byte_sha256,
            require_canonical_json=True,
        )
        for artifact in input_artifacts
    }
    calculation = _calculation_inputs_from_manifests(
        object_store,
        active_stations=active_stations,
        anchor=anchor,
        demand_state=demand_state,
        demand_manifest=demand_manifest,
        stock_state=stock_state,
        stock_manifest=stock_manifest,
        history_inputs=history_inputs,
        payloads=input_payloads,
    )
    projection = compute_urgency_projection(calculation)
    urgency_payload = _urgency_input_parquet(projection.records)
    urgency_input = store_input_payload(
        object_store,
        base_uri=object_base_uri,
        publication_key="station_urgency",
        role="urgency_output",
        payload=urgency_payload,
        suffix="parquet",
    )
    outputs = (
        ()
        if not projection.records
        else (
            OutputObject(
                role="station_urgency",
                payload=urgency_payload,
                row_count=len(projection.records),
            ),
        )
    )
    expected_ids = build_id_set(projection.expected_sta_ids)
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station_urgency",
        dependencies=dependencies,
        input_artifacts=(*input_artifacts, urgency_input),
        parameters=(
            Parameter("expected_sta_id_sha256", expected_ids.sha256),
            Parameter(
                "rebalance_policy_config",
                DEFAULT_REBALANCE_POLICY.canonical_json,
            ),
            Parameter("scoring_config_version", URGENCY_SCORING_CONFIG_VERSION),
            # 결측 window를 허용하므로 상수가 아니라 실제로 쓴 수와 offset을
            # 남긴다 — 같은 anchor에서 degraded 게시와 완전 게시가 같은
            # fingerprint를 갖지 않게 하려면 이 값이 입력에 들어가야 한다.
            Parameter("stock_window_count", str(len(history_inputs) + 1)),
            Parameter(
                "stock_history_offsets",
                ",".join(str(offset) for offset, _ in history_inputs),
            ),
        ),
        outputs=outputs,
    )
    revision_no = allocate_revision(
        connection,
        PublicationCandidate(
            publication_key="station_urgency",
            logical_dttm=anchor,
            artifact_set_sha256=materials.artifact_set.sha256,
            input_fingerprint_sha256=materials.input_fingerprint.sha256,
            published_row_cnt=len(projection.records),
        ),
    )
    prepared = build_prepared_publication(
        base_uri=object_base_uri,
        publication_key="station_urgency",
        logical_dttm=anchor,
        publisher_version=publisher_version,
        revision_no=revision_no,
        target_row_counts={"station_urgency": len(projection.records)},
        materials=materials,
        conditional_empty_candidate=not projection.records,
    )

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """Actual manifest·transitive bytes로 urgency 계산과 sealed output을 재현한다."""
        linked = validate_linked_dependency_manifests(
            "station_urgency",
            publication.input_fingerprint,
            {
                demand_input.role: payloads[demand_input.uri],
                stock_input.role: payloads[stock_input.uri],
            },
        )
        verified_calculation = _calculation_inputs_from_manifests(
            object_store,
            active_stations=active_stations,
            anchor=anchor,
            demand_state=demand_state,
            demand_manifest=linked["demand_publication_manifest"],
            stock_state=stock_state,
            stock_manifest=linked["stock_publication_manifest"],
            history_inputs=history_inputs,
            payloads=payloads,
        )
        verified_projection = compute_urgency_projection(verified_calculation)
        _validate_urgency_payloads(
            publication,
            payloads,
            urgency_input,
            verified_projection,
        )
        return {
            "base_dttm": tuple(
                record.base_dttm for record in verified_projection.records
            )
        }

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Topology shared lock에서 target와 immutable projection 결합을 재증명한다."""
        item = _require_urgency_evidence(evidence)
        _validate_history_catalog(source_catalog, history_inputs, anchor)
        locked_active = _active_stations_locked(cursor)
        if locked_active != active_stations:
            raise ContractViolation(
                "urgency staging 이후 active topology가 바뀌었습니다."
            )
        _validate_dependency_targets_locked(
            cursor,
            calculation,
            demand_state=demand_state,
            stock_state=stock_state,
        )
        locked_projection = compute_urgency_projection(
            UrgencyCalculationInputs(
                active_stations=locked_active,
                history_offsets_minutes=calculation.history_offsets_minutes,
                history_windows=calculation.history_windows,
                current_stock=calculation.current_stock,
                demand=calculation.demand,
                base_dttm=calculation.base_dttm,
            )
        )
        if locked_projection != projection:
            raise ContractViolation(
                "locked urgency projection이 sealed 계산과 다릅니다."
            )
        validate_id_set_parameter(
            "station_urgency",
            item.input_fingerprint,
            build_id_set(locked_projection.expected_sta_ids),
        )

    def validate_conditional_empty(
        cursor: Cursor[tuple[Any, ...]],
        evidence: VerifiedPublicationEvidence,
    ) -> bool:
        """EMPTY가 locked active∩stock∩demand 기대 집합 0개인지 증명한다."""
        if evidence.manifest.publication_key != "station_urgency":
            raise ContractViolation("urgency EMPTY evidence key가 다릅니다.")
        locked = UrgencyCalculationInputs(
            active_stations=_active_stations_locked(cursor),
            history_offsets_minutes=calculation.history_offsets_minutes,
            history_windows=calculation.history_windows,
            current_stock=calculation.current_stock,
            demand=calculation.demand,
            base_dttm=calculation.base_dttm,
        )
        empty_projection = compute_urgency_projection(locked)
        validate_id_set_parameter(
            "station_urgency",
            evidence.input_fingerprint,
            build_id_set(empty_projection.expected_sta_ids),
        )
        return not empty_projection.records

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """검증된 urgency projection 전체를 claim과 같은 transaction에서 교체한다."""
        _require_urgency_evidence(evidence)
        _reconcile_urgency_records(cursor, projection.target_records)

    return publish_verified(
        connection,
        ((prepared, validate_staging),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
        validate_conditional_empty=validate_conditional_empty,
    )


def _stock_history_input_artifacts(
    references: tuple[tuple[int, str, str], ...],
) -> tuple[tuple[int, InputArtifact], ...]:
    """Oldest-first offset·URI·SHA 세 쌍을 canonical history input role로 만든다.

    offset은 `URGENCY_STOCK_HISTORY_OFFSETS_MINUTES`의 부분집합이면 되고, 실제로
    존재하지 않는 window는 빠질 수 있다. 다만 role을 위치 index가 아니라 offset에서
    유도해 어떤 window가 쓰였는지 manifest만 봐도 드러나게 한다.
    """
    if type(references) is not tuple or any(
        type(reference) is not tuple
        or len(reference) != 3
        or type(reference[0]) is not int
        or any(type(value) is not str or not value for value in reference[1:])
        for reference in references
    ):
        raise ContractViolation(
            "stock history ref는 offset·URI·SHA tuple이어야 합니다."
        )
    offsets = tuple(offset for offset, _uri, _sha in references)
    allowed = set(URGENCY_STOCK_HISTORY_OFFSETS_MINUTES)
    if not set(offsets).issubset(allowed):
        raise ContractViolation(
            "stock history offset이 scoring config 집합 밖입니다: "
            f"{sorted(set(offsets) - allowed)}"
        )
    if offsets != tuple(
        offset for offset in URGENCY_STOCK_HISTORY_OFFSETS_MINUTES if offset in offsets
    ):
        raise ContractViolation(
            "stock history ref는 중복 없이 oldest-first 순이어야 합니다."
        )
    if len(offsets) < URGENCY_STOCK_HISTORY_MIN_WINDOWS:
        raise ContractViolation(
            "stock history window가 하한 미만입니다: "
            f"actual={len(offsets)} min={URGENCY_STOCK_HISTORY_MIN_WINDOWS} "
            f"of {len(URGENCY_STOCK_HISTORY_OFFSETS_MINUTES)}"
        )
    return tuple(
        (
            offset,
            InputArtifact(
                byte_sha256=byte_sha256,
                role=f"stock_history_manifest_m{abs(offset):02d}",
                uri=uri,
            ),
        )
        for offset, uri, byte_sha256 in references
    )


def _serving_release_manifest_refs(
    values: Mapping[str, tuple[str, str]],
) -> dict[str, tuple[str, str]]:
    """Finalize가 반환한 station·demand·stock exact ref mapping을 검증한다."""
    if type(values) is not dict or frozenset(values) != _SERVING_RELEASE_KEYS:
        raise ContractViolation("urgency serving release ref key 집합이 잘못됐습니다.")
    result: dict[str, tuple[str, str]] = {}
    for key in sorted(values):
        reference = values[key]
        if (
            type(reference) is not tuple
            or len(reference) != 2
            or any(type(item) is not str or not item for item in reference)
        ):
            raise ContractViolation(
                f"{key} serving release ref는 exact URI·SHA tuple이어야 합니다."
            )
        validate_sha256_hex(reference[1])
        result[key] = reference
    return result


def _validate_serving_release_state_refs(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    dependency_by_key: Mapping[str, Dependency],
    references: Mapping[str, tuple[str, str]],
) -> None:
    """Urgency dependency state가 finalize가 넘긴 exact release인지 재검증한다."""
    for key in sorted(_SERVING_RELEASE_KEYS):
        dependency = dependency_by_key.get(key)
        state = load_publication_state(connection, key)
        uri, byte_sha256 = references[key]
        if (
            type(dependency) is not Dependency
            or state is None
            or state.dependency != dependency
            or state.manifest_uri != uri
        ):
            raise ContractViolation(
                f"{key} state가 finalize exact release ref와 다릅니다."
            )
        manifest = read_state_manifest(object_store, state)
        if manifest.sha256 != byte_sha256:
            raise ContractViolation(
                f"{key} manifest SHA가 finalize exact release ref와 다릅니다."
            )


def _validate_history_catalog(
    source_catalog: S3SourceSnapshotCatalog,
    history_inputs: tuple[tuple[int, InputArtifact], ...],
    anchor: datetime,
) -> None:
    """각 history ref가 exact window의 최신 correction인지, 결측은 실제 부재인지 본다."""
    if type(source_catalog) is not S3SourceSnapshotCatalog:
        raise ContractViolation("source_catalog는 S3SourceSnapshotCatalog여야 합니다.")
    base = _utc_dttm(anchor, "urgency anchor")
    artifact_by_offset = dict(history_inputs)
    for offset in URGENCY_STOCK_HISTORY_OFFSETS_MINUTES:
        logical = base + timedelta(minutes=offset)
        artifact = artifact_by_offset.get(offset)
        if artifact is None:
            # 빠진 window는 "실제로 없다"를 증명해야 통과시킨다. 존재하는데
            # 빠뜨린 경우를 허용하면 같은 시각에 서로 다른 결과가 나올 수 있어
            # 재현성이 깨진다.
            try:
                available = source_catalog.exact_window(
                    BIKE_STATION_REALTIME_SOURCE_ID,
                    logical,
                )
            except ContractViolation:
                continue
            raise ContractViolation(
                "존재하는 stock history window를 빠뜨렸습니다: "
                f"offset={offset} uri={available.uri}"
            )
        latest = source_catalog.exact_window(
            BIKE_STATION_REALTIME_SOURCE_ID,
            logical,
        )
        if (latest.uri, latest.byte_sha256) != (
            artifact.uri,
            artifact.byte_sha256,
        ):
            raise ContractViolation(
                "stock history ref가 exact window의 latest correction이 아닙니다: "
                f"role={artifact.role}"
            )


def _linked_state_manifest_input(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    dependency: Dependency,
    *,
    role: str,
) -> tuple[PublicationStateRecord, PublicationManifest, InputArtifact]:
    """Dependency state의 actual publication manifest를 fingerprint input으로 만든다."""
    state = load_publication_state(connection, dependency.publication_key)
    if state is None or state.dependency != dependency:
        raise ContractViolation(
            f"{dependency.publication_key} state가 dependency 준비 중 바뀌었습니다."
        )
    manifest = read_state_manifest(object_store, state)
    return (
        state,
        manifest,
        InputArtifact(
            byte_sha256=manifest.sha256,
            role=role,
            uri=state.manifest_uri,
        ),
    )


def _calculation_inputs_from_manifests(
    object_store: ImmutableObjectStore,
    *,
    active_stations: tuple[ActiveStation, ...],
    anchor: datetime,
    demand_state: PublicationStateRecord,
    demand_manifest: PublicationManifest,
    stock_state: PublicationStateRecord,
    stock_manifest: PublicationManifest,
    history_inputs: tuple[tuple[int, InputArtifact], ...],
    payloads: Mapping[str, bytes],
) -> UrgencyCalculationInputs:
    """Linked publication과 history actual bytes를 typed scoring 입력으로 연다."""
    base = _utc_dttm(anchor, "urgency anchor")
    _validate_manifest_state(demand_manifest, demand_state)
    _validate_manifest_state(stock_manifest, stock_state)
    if demand_manifest.logical_dttm != base or stock_manifest.logical_dttm != base:
        raise ContractViolation(
            "demand·stock publication manifest가 urgency anchor와 다릅니다."
        )
    demand = _demand_records_from_manifest(object_store, demand_manifest)
    current_stock = _stock_records_from_manifest(object_store, stock_manifest)
    history_windows = tuple(
        _history_window_from_manifest(
            object_store,
            artifact,
            payloads,
            expected_logical_dttm=base + timedelta(minutes=offset),
        )
        for offset, artifact in history_inputs
    )
    return UrgencyCalculationInputs(
        active_stations=active_stations,
        history_offsets_minutes=tuple(offset for offset, _ in history_inputs),
        history_windows=history_windows,
        current_stock=current_stock,
        demand=demand,
        base_dttm=base,
    )


def _validate_manifest_state(
    manifest: PublicationManifest,
    state: PublicationStateRecord,
) -> None:
    """Parsed publication manifest의 state-owned 필드를 exact하게 대조한다."""
    if (
        manifest.publication_key != state.publication_key
        or manifest.logical_dttm != state.logical_dttm
        or manifest.revision_no != state.revision_no
        or manifest.artifact_set_sha256 != state.artifact_set_sha256
        or manifest.input_fingerprint_sha256 != state.input_fingerprint_sha256
        or manifest.published_row_cnt != state.published_row_cnt
        or manifest.sha256 != _manifest_uri_sha(state.manifest_uri)
    ):
        raise ContractViolation(
            "publication state와 actual linked manifest가 다릅니다."
        )


def _manifest_uri_sha(uri: str) -> str:
    """공통 publication manifest URI에서 content SHA를 exact하게 추출한다."""
    match = re.search(r"/publication-([0-9a-f]{64})\.json\Z", uri)
    if match is None:
        raise ContractViolation(
            "publication manifest URI가 content-addressed 형식이 아닙니다."
        )
    return match.group(1)


def _demand_records_from_manifest(
    object_store: ImmutableObjectStore,
    manifest: PublicationManifest,
) -> tuple[DemandForecastRecord, ...]:
    """Demand manifest의 actual fingerprint·output bytes를 완전 projection으로 읽는다."""
    fingerprint_payload = object_store.read_bytes(
        manifest.input_fingerprint_uri,
        manifest.input_fingerprint_sha256,
        require_canonical_json=True,
    )
    fingerprint = parse_input_fingerprint(
        fingerprint_payload,
        "station_demand_forecast",
    )
    validate_input_fingerprint("station_demand_forecast", fingerprint)
    if not manifest.artifacts:
        if manifest.published_row_cnt != 0:
            raise ContractViolation("EMPTY demand manifest row count가 0이 아닙니다.")
        validate_id_set_parameter(
            "station_demand_forecast",
            fingerprint,
            build_id_set(()),
        )
        return ()
    artifact = _single_output_artifact(manifest, "station_demand_forecast")
    payload = object_store.read_bytes(artifact.uri, artifact.byte_sha256)
    table = read_parquet_bytes(payload)
    if "sta_id" not in table.column_names:
        raise ContractViolation("demand output Parquet에 sta_id가 없습니다.")
    station_ids = _station_ids(
        tuple(set(table.column("sta_id").to_pylist())),
        "demand support station IDs",
    )
    records = demand_records_from_parquet(
        payload,
        expected_base_dttm=manifest.logical_dttm,
        expected_sta_ids=station_ids,
    )
    if len(records) != artifact.row_count or len(records) != manifest.published_row_cnt:
        raise ContractViolation("demand actual output row count가 manifest와 다릅니다.")
    validate_id_set_parameter(
        "station_demand_forecast",
        fingerprint,
        build_id_set(station_ids),
    )
    return records


def _stock_records_from_manifest(
    object_store: ImmutableObjectStore,
    manifest: PublicationManifest,
) -> tuple[StationStockRecord, ...]:
    """Station stock manifest가 소유한 actual fixed-schema output을 읽는다."""
    artifact = _single_output_artifact(manifest, "station_stock")
    payload = object_store.read_bytes(artifact.uri, artifact.byte_sha256)
    records = _stock_records_from_parquet(payload)
    if len(records) != artifact.row_count or len(records) != manifest.published_row_cnt:
        raise ContractViolation(
            "station_stock actual output row count가 manifest와 다릅니다."
        )
    if not records:
        raise ContractViolation("station_stock EMPTY는 SSOT에서 금지됩니다.")
    return records


def _single_output_artifact(manifest: PublicationManifest, role: str) -> Any:
    """Publication manifest에서 기대 role의 output artifact 정확히 하나를 반환한다."""
    if len(manifest.artifacts) != 1 or manifest.artifacts[0].role != role:
        raise ContractViolation(
            f"{role} publication output artifact가 정확하지 않습니다."
        )
    return manifest.artifacts[0]


def _history_window_from_manifest(
    object_store: ImmutableObjectStore,
    artifact: InputArtifact,
    payloads: Mapping[str, bytes],
    *,
    expected_logical_dttm: datetime,
) -> tuple[StockHistoryPoint, ...]:
    """Complete realtime source manifest와 Silver rows를 한 history window로 읽는다."""
    snapshot = read_source_snapshot_payload(
        object_store,
        manifest_artifact=artifact,
        verified_payloads=payloads,
        expected_source_id=BIKE_STATION_REALTIME_SOURCE_ID,
        expected_logical_dttm=expected_logical_dttm,
    )
    if snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED:
        raise ContractViolation(
            "urgency history는 complete SUCCEEDED snapshot이어야 합니다."
        )
    table = source_snapshot_parquet(snapshot)
    required = {"stationId", "parkingBikeTotCnt"}
    if not required.issubset(table.column_names):
        raise ContractViolation(
            "realtime Silver에 stationId·parkingBikeTotCnt가 없습니다."
        )
    parsed: list[StockHistoryPoint] = []
    for row in table.select(sorted(required)).to_pylist():
        station_id = _station_id(row["stationId"])
        parking = row["parkingBikeTotCnt"]
        if parking is None:
            continue
        parsed.append(
            StockHistoryPoint(
                sta_id=station_id,
                observed_at=expected_logical_dttm,
                parking_bike_tot_cnt=parking,
            )
        )
    points = tuple(sorted(parsed, key=lambda point: _utf8_key(point.sta_id)))
    ids = tuple(point.sta_id for point in points)
    if len(ids) != len(set(ids)):
        raise ContractViolation(
            "complete realtime history snapshot에 중복 sta_id가 있습니다."
        )
    return points


def _validate_urgency_payloads(
    publication: PreparedPublication,
    payloads: Mapping[str, bytes],
    urgency_input: InputArtifact,
    projection: UrgencyProjection,
) -> None:
    """Computed input과 Gold output actual Parquet이 projection과 같은지 검증한다."""
    input_records = _urgency_input_records(
        payloads[urgency_input.uri],
        expected_base_dttm=projection.base_dttm,
        expected_sta_ids=projection.expected_sta_ids,
    )
    if input_records != projection.records:
        raise ContractViolation("urgency_output actual bytes가 재계산 결과와 다릅니다.")
    if not projection.records:
        if publication.manifest.artifacts:
            raise ContractViolation(
                "EMPTY urgency publication에 output artifact가 있습니다."
            )
        return
    artifact = _single_output_artifact(publication.manifest, "station_urgency")
    if payloads[artifact.uri] != payloads[urgency_input.uri]:
        raise ContractViolation(
            "Gold urgency output bytes가 computed input과 다릅니다."
        )
    actual = urgency_records_from_parquet(
        payloads[artifact.uri],
        expected_base_dttm=projection.base_dttm,
        expected_sta_ids=projection.expected_sta_ids,
    )
    if actual != projection.records:
        raise ContractViolation(
            "station_urgency output이 재계산 projection과 다릅니다."
        )


def _urgency_input_parquet(records: tuple[UrgencyRecord, ...]) -> bytes:
    """Computed urgency input을 nonempty/EMPTY 모두 같은 fixed schema로 직렬화한다."""
    if records:
        return urgency_records_to_parquet(
            records,
            expected_sta_ids=tuple(record.sta_id for record in records),
        )
    return parquet_bytes(pa.Table.from_pylist([], schema=_URGENCY_SCHEMA))


def _urgency_input_records(
    payload: bytes,
    *,
    expected_base_dttm: datetime,
    expected_sta_ids: tuple[str, ...],
) -> tuple[UrgencyRecord, ...]:
    """Computed input Parquet을 기대 projection과 대조하며 EMPTY input을 허용한다."""
    table = read_parquet_bytes(payload)
    if table.schema != _URGENCY_SCHEMA:
        raise ContractViolation(
            "urgency_output Parquet schema가 exact 계약과 다릅니다."
        )
    records = tuple(UrgencyRecord(**row) for row in table.to_pylist())
    if records:
        _validate_record_sequence(
            records,
            expected_sta_ids=expected_sta_ids,
            expected_base_dttm=expected_base_dttm,
        )
    elif expected_sta_ids:
        raise ContractViolation("nonempty 기대 집합에 EMPTY urgency_output이 왔습니다.")
    return records


def _load_active_stations(
    connection: Connection[Any],
) -> tuple[ActiveStation, ...]:
    """Transaction 밖의 짧은 read로 active topology 계산 필드를 읽는다."""
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation("active station loader는 idle 연결이 필요합니다.")
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        return _active_stations_locked(cursor)


def _active_stations_locked(
    cursor: Cursor[tuple[Any, ...]],
) -> tuple[ActiveStation, ...]:
    """Topology shared lock 아래 active station ID·정원·Point·센터를 읽는다."""
    cursor.execute(
        """
        SELECT sta_id,
               hold_cnt,
               ST_X(sta_point)::DOUBLE PRECISION,
               ST_Y(sta_point)::DOUBLE PRECISION,
               dispatch_center_id
          FROM station
         WHERE is_active
         ORDER BY sta_id COLLATE "C"
        """
    )
    result = tuple(ActiveStation(*row) for row in cursor.fetchall())
    ids = tuple(item.sta_id for item in result)
    if ids != _station_ids(ids, "active station IDs"):
        raise ContractViolation("DB active station이 canonical UTF-8 순이 아닙니다.")
    return result


def _validate_dependency_targets_locked(
    cursor: Cursor[tuple[Any, ...]],
    calculation: UrgencyCalculationInputs,
    *,
    demand_state: PublicationStateRecord,
    stock_state: PublicationStateRecord,
) -> None:
    """Locked Gold demand·stock targets가 immutable dependency outputs와 같은지 확인한다."""
    cursor.execute(
        """
        SELECT base_dttm,
               sta_id,
               predicted_dttm,
               predicted_rent_cnt,
               predicted_rtn_cnt
          FROM station_demand_forecast
         ORDER BY sta_id COLLATE "C", predicted_dttm
        """
    )
    demand = tuple(DemandForecastRecord(*row) for row in cursor.fetchall())
    if demand != calculation.demand or len(demand) != demand_state.published_row_cnt:
        raise ContractViolation(
            "locked Gold demand가 immutable dependency output과 다릅니다."
        )
    cursor.execute(
        """
        SELECT sta_id, base_dttm, parking_bike_tot_cnt
          FROM station_stock
         ORDER BY sta_id COLLATE "C"
        """
    )
    stock = tuple(StationStockRecord(*row) for row in cursor.fetchall())
    if (
        stock != calculation.current_stock
        or len(stock) != stock_state.published_row_cnt
    ):
        raise ContractViolation(
            "locked Gold stock이 immutable dependency output과 다릅니다."
        )


def _require_urgency_evidence(
    evidence: tuple[VerifiedPublicationEvidence, ...],
) -> VerifiedPublicationEvidence:
    """Callback evidence가 station_urgency 정확히 하나인지 검증한다."""
    if len(evidence) != 1 or evidence[0].manifest.publication_key != "station_urgency":
        raise ContractViolation("urgency publication evidence key가 잘못됐습니다.")
    return evidence[0]


def _reconcile_urgency_records(
    cursor: Cursor[tuple[Any, ...]],
    records: tuple[StationUrgencyRecord, ...],
) -> None:
    """Temp staging으로 urgency 전체를 upsert·delete하고 created 시각을 보존한다."""
    cursor.execute(
        """
        CREATE TEMP TABLE gold_urgency_staging (
            sta_id TEXT PRIMARY KEY,
            base_dttm TIMESTAMPTZ NOT NULL,
            urgency_score DOUBLE PRECISION NOT NULL,
            critical_remaining_min INTEGER NOT NULL,
            rebalance_need_type_cd TEXT NOT NULL
        ) ON COMMIT DROP
        """
    )
    if records:
        cursor.executemany(
            """
            INSERT INTO gold_urgency_staging (
                sta_id,
                base_dttm,
                urgency_score,
                critical_remaining_min,
                rebalance_need_type_cd
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (
                    record.sta_id,
                    record.base_dttm,
                    record.urgency_score,
                    record.critical_remaining_min,
                    record.rebalance_need_type_cd,
                )
                for record in records
            ],
        )
        cursor.execute(
            """
            INSERT INTO station_urgency AS current_urgency (
                sta_id,
                base_dttm,
                urgency_score,
                critical_remaining_min,
                rebalance_need_type_cd
            )
            SELECT sta_id,
                   base_dttm,
                   urgency_score,
                   critical_remaining_min,
                   rebalance_need_type_cd
              FROM gold_urgency_staging
             ORDER BY sta_id COLLATE "C"
            ON CONFLICT (sta_id) DO UPDATE
            SET base_dttm = EXCLUDED.base_dttm,
                urgency_score = EXCLUDED.urgency_score,
                critical_remaining_min = EXCLUDED.critical_remaining_min,
                rebalance_need_type_cd = EXCLUDED.rebalance_need_type_cd
            WHERE ROW(
                current_urgency.base_dttm,
                current_urgency.urgency_score,
                current_urgency.critical_remaining_min,
                current_urgency.rebalance_need_type_cd
            ) IS DISTINCT FROM ROW(
                EXCLUDED.base_dttm,
                EXCLUDED.urgency_score,
                EXCLUDED.critical_remaining_min,
                EXCLUDED.rebalance_need_type_cd
            )
            """
        )
    cursor.execute(
        """
        DELETE FROM station_urgency AS current_urgency
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM gold_urgency_staging AS staging
                    WHERE staging.sta_id = current_urgency.sta_id
               )
        """
    )
    cursor.execute(
        """
        SELECT sta_id,
               base_dttm,
               urgency_score,
               critical_remaining_min,
               rebalance_need_type_cd
          FROM station_urgency
         ORDER BY sta_id COLLATE "C"
        """
    )
    actual = tuple(StationUrgencyRecord(*row) for row in cursor.fetchall())
    if actual != records:
        raise ContractViolation(
            "station_urgency full reconcile readback이 staging과 다릅니다."
        )


def _regression_slope_v1(xs: list[float], ys: list[float]) -> float:
    """현행 최소제곱 trend 기울기를 byte-versioned scoring 의미로 계산한다."""
    count = len(xs)
    x_mean = sum(xs) / count
    y_mean = sum(ys) / count
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    denominator = sum((x - x_mean) ** 2 for x in xs)
    return numerator / denominator if denominator else 0.0


def _trend_time_to_critical_v1(
    current: int,
    hold_cnt: int,
    stock_history: list[dict[str, Any]],
    now: datetime,
) -> tuple[float, str] | None:
    """현행 1시간 이전 선형 추세 임계 시각을 계산한다."""
    if len(stock_history) < 2:
        return None
    xs = [(row["observed_at"] - now).total_seconds() / 60 for row in stock_history]
    ys = [row["parking_bike_tot_cnt"] for row in stock_history]
    slope = _regression_slope_v1(xs, ys)
    if slope < 0:
        minutes, action_type = current / -slope, "supply_needed"
    elif slope > 0:
        minutes, action_type = (hold_cnt - current) / slope, "retrieval_needed"
    else:
        return None
    if minutes >= FIRST_FORECAST_MIN:
        return None
    return minutes, action_type


def _forecast_time_to_critical_v1(
    points: list[dict[str, Any]],
) -> tuple[float, int, str] | None:
    """현행 horizon 순서에서 처음 normal이 아닌 예측 시각을 찾는다."""
    for index, point in enumerate(points):
        if point["action_type"] != "normal":
            return (index + 1) * 60, index, point["action_type"]
    return None


def _max_overshoot_v1(
    current: int,
    hold_cnt: int,
    points: list[dict[str, Any]],
) -> int:
    """현행 예측 구간의 최대 정원 초과량을 계산한다."""
    peak = (
        max(current, *(point["predicted_bikes"] for point in points))
        if points
        else current
    )
    return max(0, peak - hold_cnt)


def _max_deficit_v1(current: int, points: list[dict[str, Any]]) -> int:
    """현행 unclamped 누적 예측의 최대 재고 부족량을 계산한다."""
    stock = current
    worst = min(current, 0)
    for point in points:
        stock += point["predicted_return_cnt"] - point["predicted_rent_cnt"]
        worst = min(worst, stock)
    return max(0, -worst)


def _max_unmet_demand_v1(
    current: int,
    hold_cnt: int,
    points: list[dict[str, Any]],
) -> int:
    """현행 저재고 구간의 최대 대여 수요를 계산한다."""
    threshold = SUPPLY_LOW_STOCK_RATIO * hold_cnt
    previous = current
    worst = 0
    for point in points:
        if previous <= threshold:
            worst = max(worst, point["predicted_rent_cnt"])
        previous = point["predicted_bikes"]
    return worst


def _severity_v1(ratio: float) -> float:
    """현행 정원 대비 위험 비율을 0..1 점근 심각도로 변환한다."""
    return 1 - math.exp(-ratio / SEVERITY_SCALE)


def _severity_qty_v1(
    current: int,
    hold_cnt: int,
    action_type: str,
    points: list[dict[str, Any]],
) -> int:
    """현행 action별 ranking 심각도 수량을 계산한다."""
    if action_type == "retrieval_needed":
        return _max_overshoot_v1(current, hold_cnt, points)
    return max(
        _max_deficit_v1(current, points),
        _max_unmet_demand_v1(current, hold_cnt, points),
    )


def _bike_qty_v1(
    current: int,
    hold_cnt: int,
    action_type: str,
    points: list[dict[str, Any]],
) -> int:
    """현행 물리 한계로 clamp한 route 이동 수량을 계산한다."""
    if action_type == "retrieval_needed":
        return min(current, _severity_qty_v1(current, hold_cnt, action_type, points))
    if action_type == "supply_needed":
        return min(
            _severity_qty_v1(current, hold_cnt, action_type, points),
            max(0, hold_cnt - current),
        )
    return 0


def _bike_qty_risk_band_v4(
    current: int,
    hold_cnt: int,
    action_type: str,
    points: list[dict[str, Any]],
    stock_history: list[dict[str, Any]],
    now: datetime,
    policy_config: RebalancePolicyConfig,
) -> int:
    """현재 초과 재고만 any-depletion guard와 하방 범위 안에서 회수한다.

    대여와 반납 count를 독립 포아송 변수로 근사하면 누적 순수요의 분산은 두 count
    합이다. 최근 최소제곱 기울기나 보호 horizon의 모델 평균 경로 중 하나라도
    감소하면 donor 사용을 fail-closed한다. 둘 다 비감소할 때만 모델 하방과 최근
    기울기를 보호 horizon 및 예상 출동 지연까지 외삽한 하방 중 작은 값을 사용한다. 미래
    반납으로 생길 초과량을 현재 회수 가능량으로 빌리지 않으며, 최근 관측이 현재
    포함 세 점 미만이어도 fail-closed한다. 배치는 평균 최저 재고를 최소 재고까지
    올린다.
    """
    horizon_points = points[: policy_config.protection_horizon_hours]
    minimum_stock = math.ceil(policy_config.minimum_stock_ratio * hold_cnt)
    if action_type == "retrieval_needed":
        recent_projection = _recent_stock_projection_v3(
            current,
            stock_history,
            now,
            protection_minutes=(
                policy_config.protection_horizon_hours * 60 + RESPONSE_LAG_MIN
            ),
        )
        if recent_projection is None:
            return 0
        recent_slope, recent_lower = recent_projection
        model_mean_path = _forecast_lower_stock_path(
            current,
            horizon_points,
            uncertainty_z=0.0,
        )
        if (
            recent_slope < 0.0
            or min(model_mean_path[1:], default=float(current)) < current
        ):
            return 0
        model_lower = min(
            _forecast_lower_stock_path(
                current,
                horizon_points,
                uncertainty_z=policy_config.uncertainty_z,
            )
        )
        desired = max(0, current - hold_cnt)
        safe = max(0, math.floor(min(model_lower, recent_lower) - minimum_stock))
        concentration_limit = math.floor(
            current * policy_config.max_pickup_stock_fraction
        )
        return min(current, desired, safe, concentration_limit)
    if action_type == "supply_needed":
        lower_path = _forecast_lower_stock_path(
            current,
            horizon_points,
            uncertainty_z=0.0,
        )
        needed = max(0, math.ceil(minimum_stock - min(lower_path)))
        return min(needed, max(0, hold_cnt - current))
    return 0


def _recent_stock_projection_v3(
    current: int,
    stock_history: list[dict[str, Any]],
    now: datetime,
    *,
    protection_minutes: int,
) -> tuple[float, float] | None:
    """최근 최소제곱 기울기와 보호 구간 하방 재고를 함께 반환한다."""
    recent = sorted(
        (
            row
            for row in stock_history
            if row["observed_at"] <= now
        ),
        key=lambda row: row["observed_at"],
    )
    if (
        type(protection_minutes) is not int
        or protection_minutes <= 0
        or len({row["observed_at"] for row in recent}) < 3
        or not recent
        or recent[-1]["observed_at"] != now
        or recent[-1]["parking_bike_tot_cnt"] != current
    ):
        return None
    xs = [(row["observed_at"] - now).total_seconds() / 60.0 for row in recent]
    ys = [row["parking_bike_tot_cnt"] for row in recent]
    slope = _regression_slope_v1(xs, ys)
    lower = current + min(0.0, slope) * protection_minutes
    return slope, lower


def _forecast_lower_stock_path(
    current: int,
    points: list[dict[str, Any]],
    *,
    uncertainty_z: float,
) -> tuple[float, ...]:
    """독립 포아송 순수요 완충을 적용한 시점별 하방 재고를 반환한다."""
    mean_stock = float(current)
    cumulative_variance = 0.0
    lower = [mean_stock]
    for point in points:
        rentals = max(0, int(point["predicted_rent_cnt"]))
        returns = max(0, int(point["predicted_return_cnt"]))
        mean_stock += returns - rentals
        cumulative_variance += rentals + returns
        lower.append(mean_stock - uncertainty_z * math.sqrt(cumulative_variance))
    return tuple(lower)


def _urgency_score_v1(
    current: int,
    hold_cnt: int,
    stock_history: list[dict[str, Any]],
    points: list[dict[str, Any]],
    now: datetime,
) -> tuple[float, int, str]:
    """v4 any-depletion scoring이 보존하는 v1 score·남은 분·판단을 계산한다."""
    if current <= SUPPLY_LOW_STOCK_RATIO * hold_cnt:
        time_to_critical, action_type = 0.0, "supply_needed"
    elif current >= hold_cnt:
        time_to_critical, action_type = 0.0, "retrieval_needed"
    else:
        candidates: list[tuple[float, str]] = []
        trend = _trend_time_to_critical_v1(current, hold_cnt, stock_history, now)
        if trend is not None:
            candidates.append(trend)
        forecast = _forecast_time_to_critical_v1(points)
        if forecast is not None:
            minutes, _index, forecast_action = forecast
            candidates.append((minutes, forecast_action))
        if not candidates:
            return 0.0, 12 * 60, "normal"
        time_to_critical, action_type = min(candidates, key=lambda item: item[0])
    slack = max(0.0, time_to_critical - RESPONSE_LAG_MIN)
    time_factor = 2 ** (-slack / HALF_LIFE_MIN)
    ratio = _severity_qty_v1(current, hold_cnt, action_type, points) / max(hold_cnt, 1)
    score = round(100 * time_factor * _severity_v1(ratio), 1)
    return score, round(time_to_critical), action_type


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
