"""단기·초단기 완전 snapshot을 13시간 Gold 날씨로 resolve한다."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
from core.gold_publication import (
    ContractViolation,
    ImmutableObjectStore,
    InputArtifact,
    Parameter,
    PreparedPublication,
    VerifiedPublicationEvidence,
    format_utc_dttm,
)
from core.precip import parse_precip
from psycopg import Connection, Cursor

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
)
from .source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from .source_policy import validate_source_snapshot_policy
from .state import (
    active_weather_grid_ids_locked,
    load_active_weather_grid_ids,
    load_dependencies,
)
from .versioning import PublicationCandidate, allocate_revision

FORECAST_HOUR_COUNT = 13
RESOLVER_VERSION = "gold-weather-resolver-v1"
WEATHER_FORECAST_PUBLISHER_VERSION = "gold-weather-forecast-publisher-v1"
SHORT_TERM_SOURCE_ID = "weather_short_term_forecast"
ULTRA_SHORT_SOURCE_ID = "weather_ultra_short_forecast"
_KST = ZoneInfo("Asia/Seoul")
_SKY_CODES = {1: "clear", 3: "mostly_cloudy", 4: "cloudy"}
_COMMON_PRECIPITATION_CODES = {
    0: "none",
    1: "rain",
    2: "rain_snow",
    3: "snow",
}
_SHORT_PRECIPITATION_CODES = {**_COMMON_PRECIPITATION_CODES, 4: "shower"}
_ULTRA_PRECIPITATION_CODES = {
    **_COMMON_PRECIPITATION_CODES,
    5: "raindrop",
    6: "raindrop_snow_flurry",
    7: "snow_flurry",
}
_WEATHER_FORECAST_SCHEMA = pa.schema(
    (
        pa.field("weather_grid_id", pa.string(), nullable=False),
        pa.field("forecast_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("source_product_cd", pa.string(), nullable=False),
        pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("sky_condition_cd", pa.string(), nullable=False),
        pa.field("precipitation_type_cd", pa.string(), nullable=False),
        pa.field("temperature", pa.float64(), nullable=False),
        pa.field("precipitation_prob", pa.float64(), nullable=True),
        pa.field("precipitation_amount", pa.float64(), nullable=True),
        pa.field("humidity", pa.float64(), nullable=True),
        pa.field("wind_speed", pa.float64(), nullable=True),
    )
)


@dataclass(frozen=True, slots=True)
class WeatherForecastRecord:
    """Gold weather_forecast 전체 교체 행을 표현한다."""

    weather_grid_id: str
    forecast_dttm: datetime
    source_product_cd: str
    base_dttm: datetime
    sky_condition_cd: str
    precipitation_type_cd: str
    temperature: float
    precipitation_prob: float | None
    precipitation_amount: float | None
    humidity: float | None
    wind_speed: float | None

    def __post_init__(self) -> None:
        """DDL의 정시·lineage·수치 계약을 검증한다."""
        _parse_grid_id(self.weather_grid_id)
        forecast = _utc_dttm(self.forecast_dttm, "forecast_dttm")
        base = _utc_dttm(self.base_dttm, "base_dttm")
        if forecast.minute or forecast.second or forecast.microsecond:
            raise ContractViolation("weather forecast target은 정시여야 합니다.")
        if forecast <= base:
            raise ContractViolation(
                "weather forecast target은 base_dttm 후여야 합니다."
            )
        if self.source_product_cd not in {"short_term", "ultra_short"}:
            raise ContractViolation(
                "weather source product가 SSOT allowlist에 없습니다."
            )
        if self.sky_condition_cd not in set(_SKY_CODES.values()):
            raise ContractViolation("weather SKY 의미 코드가 잘못됐습니다.")
        precipitation_codes = (
            set(_ULTRA_PRECIPITATION_CODES.values())
            if self.source_product_cd == "ultra_short"
            else set(_SHORT_PRECIPITATION_CODES.values())
        )
        if self.precipitation_type_cd not in precipitation_codes:
            raise ContractViolation("weather PTY 의미 코드가 제품 계약과 다릅니다.")
        _bounded_float(self.temperature, "temperature", -50.0, 50.0)
        _optional_bounded_float(
            self.precipitation_prob, "precipitation_prob", 0.0, 100.0
        )
        _optional_bounded_float(
            self.precipitation_amount, "precipitation_amount", 0.0, None
        )
        _optional_bounded_float(self.humidity, "humidity", 0.0, 100.0)
        _optional_bounded_float(self.wind_speed, "wind_speed", 0.0, 50.0)


@dataclass(frozen=True, slots=True)
class WeatherForecastProjection:
    """active grid×정확히 13시간의 완전 projection을 표현한다."""

    records: tuple[WeatherForecastRecord, ...]
    active_weather_grid_ids: tuple[str, ...]
    first_forecast_dttm: datetime | None

    def __post_init__(self) -> None:
        """projection 정렬과 exact cardinality를 검증한다."""
        if type(self.records) is not tuple or any(
            type(record) is not WeatherForecastRecord for record in self.records
        ):
            raise ContractViolation(
                "weather records는 WeatherForecastRecord tuple이어야 합니다."
            )
        if type(self.active_weather_grid_ids) is not tuple:
            raise ContractViolation("active weather grid ID는 tuple이어야 합니다.")
        expected_grids = tuple(
            sorted(
                set(self.active_weather_grid_ids),
                key=lambda value: value.encode("utf-8"),
            )
        )
        if self.active_weather_grid_ids != expected_grids:
            raise ContractViolation(
                "active weather grid ID는 중복 없이 UTF-8 순이어야 합니다."
            )
        for grid_id in self.active_weather_grid_ids:
            _parse_grid_id(grid_id)
        ordered = tuple(
            sorted(
                self.records,
                key=lambda record: (
                    record.weather_grid_id.encode("utf-8"),
                    record.forecast_dttm,
                ),
            )
        )
        if self.records != ordered:
            raise ContractViolation("weather records 정렬이 exact 계약과 다릅니다.")
        expected_count = len(self.active_weather_grid_ids) * FORECAST_HOUR_COUNT
        if len(self.records) != expected_count:
            raise ContractViolation(
                "weather projection이 active grid×13시간으로 완전하지 않습니다."
            )
        if not self.active_weather_grid_ids:
            if self.first_forecast_dttm is not None:
                raise ContractViolation(
                    "weather EMPTY에는 first forecast 시각이 없어야 합니다."
                )
            return
        if type(self.first_forecast_dttm) is not datetime:
            raise ContractViolation("weather first forecast 시각이 필요합니다.")
        first = _utc_dttm(self.first_forecast_dttm, "first_forecast_dttm")
        expected_keys = {
            (grid_id, first + timedelta(hours=offset))
            for grid_id in self.active_weather_grid_ids
            for offset in range(FORECAST_HOUR_COUNT)
        }
        actual_keys = {
            (record.weather_grid_id, record.forecast_dttm) for record in self.records
        }
        if actual_keys != expected_keys:
            raise ContractViolation(
                "weather projection의 grid·시간 key 집합이 잘못됐습니다."
            )


def resolve_weather_forecast(
    short_term_rows: tuple[Mapping[str, Any], ...],
    ultra_short_rows: tuple[Mapping[str, Any], ...],
    *,
    active_weather_grid_ids: tuple[str, ...],
    run_dttm: datetime,
) -> WeatherForecastProjection:
    """최신 완전 두 snapshot을 whole-row 우선순위로 resolve한다."""
    run = _utc_dttm(run_dttm, "run_dttm")
    grids = tuple(
        sorted(set(active_weather_grid_ids), key=lambda value: value.encode("utf-8"))
    )
    if type(active_weather_grid_ids) is not tuple or len(grids) != len(
        active_weather_grid_ids
    ):
        raise ContractViolation(
            "active weather grid ID는 중복 없는 tuple이어야 합니다."
        )
    for grid_id in grids:
        _parse_grid_id(grid_id)
    if not grids:
        return WeatherForecastProjection((), (), None)
    first = run.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    required_times = tuple(
        first + timedelta(hours=offset) for offset in range(FORECAST_HOUR_COUNT)
    )
    required_keys = {
        (grid_id, forecast_dttm)
        for grid_id in grids
        for forecast_dttm in required_times
    }
    short_latest = _latest_source_rows(
        short_term_rows,
        source_product_cd="short_term",
        required_keys=required_keys,
    )
    ultra_latest = _latest_source_rows(
        ultra_short_rows,
        source_product_cd="ultra_short",
        required_keys=required_keys,
    )
    records: list[WeatherForecastRecord] = []
    missing: list[tuple[str, datetime]] = []
    for grid_id, forecast_dttm in sorted(
        required_keys, key=lambda key: (key[0].encode("utf-8"), key[1])
    ):
        key = (grid_id, forecast_dttm)
        record = _valid_record_or_none(ultra_latest.get(key), "ultra_short")
        if record is None:
            record = _valid_record_or_none(short_latest.get(key), "short_term")
        if record is None:
            missing.append(key)
        else:
            records.append(record)
    if missing:
        sample = ", ".join(
            f"{grid_id}@{format_utc_dttm(forecast)}"
            for grid_id, forecast in missing[:5]
        )
        raise ContractViolation(
            "weather active grid×13시간 coverage가 완전하지 않습니다: "
            f"missing={len(missing)}, sample=[{sample}]"
        )
    return WeatherForecastProjection(tuple(records), grids, first)


def publish_weather_forecast(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    short_term_artifact: SourceManifestArtifact,
    ultra_short_artifact: SourceManifestArtifact,
    source_catalog: S3SourceSnapshotCatalog,
    scheduled_anchor: datetime,
    short_term_lookback: timedelta,
    ultra_short_lookback: timedelta,
    object_base_uri: str,
    publisher_version: str = WEATHER_FORECAST_PUBLISHER_VERSION,
) -> PublicationExecution:
    """두 최신 완전 source snapshot을 13시간 Gold weather로 원자 게시한다."""
    anchor = _utc_dttm(scheduled_anchor, "scheduled_anchor")
    _require_source_artifact(short_term_artifact, SHORT_TERM_SOURCE_ID, anchor)
    _require_source_artifact(ultra_short_artifact, ULTRA_SHORT_SOURCE_ID, anchor)
    if type(source_catalog) is not S3SourceSnapshotCatalog:
        raise ContractViolation("weather source_catalog type이 잘못됐습니다.")
    _require_latest_source(
        source_catalog,
        short_term_artifact,
        SHORT_TERM_SOURCE_ID,
        anchor,
        short_term_lookback,
    )
    _require_latest_source(
        source_catalog,
        ultra_short_artifact,
        ULTRA_SHORT_SOURCE_ID,
        anchor,
        ultra_short_lookback,
    )
    short_input = InputArtifact(
        byte_sha256=short_term_artifact.byte_sha256,
        role="short_term_manifest",
        uri=short_term_artifact.uri,
    )
    ultra_input = InputArtifact(
        byte_sha256=ultra_short_artifact.byte_sha256,
        role="ultra_short_manifest",
        uri=ultra_short_artifact.uri,
    )
    active_grids = load_active_weather_grid_ids(connection)
    dependencies = load_dependencies(connection, ("station", "weather_grid"))
    projection = _projection_from_source_artifacts(
        object_store,
        short_artifact=short_term_artifact,
        ultra_artifact=ultra_short_artifact,
        short_input=short_input,
        ultra_input=ultra_input,
        active_grids=active_grids,
        anchor=anchor,
    )
    outputs = (
        ()
        if not projection.records
        else (
            OutputObject(
                role="weather_forecast",
                payload=_records_to_parquet(projection.records),
                row_count=len(projection.records),
            ),
        )
    )
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="weather_forecast",
        dependencies=dependencies,
        input_artifacts=(short_input, ultra_input),
        parameters=(
            Parameter("forecast_hour_count", str(FORECAST_HOUR_COUNT)),
            Parameter("resolver_version", RESOLVER_VERSION),
        ),
        outputs=outputs,
    )
    revision_no = allocate_revision(
        connection,
        PublicationCandidate(
            publication_key="weather_forecast",
            logical_dttm=anchor,
            artifact_set_sha256=materials.artifact_set.sha256,
            input_fingerprint_sha256=materials.input_fingerprint.sha256,
            published_row_cnt=len(projection.records),
        ),
    )
    prepared = build_prepared_publication(
        base_uri=object_base_uri,
        publication_key="weather_forecast",
        logical_dttm=anchor,
        publisher_version=publisher_version,
        revision_no=revision_no,
        target_row_counts={"weather_forecast": len(projection.records)},
        materials=materials,
        conditional_empty_candidate=not projection.records,
    )

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """actual source manifest·Silver·output bytes로 resolver 결과를 재구성한다."""
        if publication.manifest.publication_key != "weather_forecast":
            raise ContractViolation("weather prepared publication key가 다릅니다.")
        expected = _projection_from_verified_payloads(
            object_store,
            short_input=short_input,
            ultra_input=ultra_input,
            payloads=payloads,
            active_grids=active_grids,
            anchor=anchor,
        )
        if expected.records:
            if len(publication.manifest.artifacts) != 1:
                raise ContractViolation(
                    "nonempty weather publication에 output artifact 하나가 필요합니다."
                )
            actual = _records_from_parquet(
                payloads[publication.manifest.artifacts[0].uri]
            )
            if actual != expected.records:
                raise ContractViolation(
                    "weather output Parquet이 resolver projection과 다릅니다."
                )
        elif publication.manifest.artifacts:
            raise ContractViolation("weather EMPTY에 output artifact가 있습니다.")
        return {"base_dttm": tuple(record.base_dttm for record in expected.records)}

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """topology lock 안에서 active grid와 두 source 최신 correction을 재검증한다."""
        if (
            len(evidence) != 1
            or evidence[0].manifest.publication_key != "weather_forecast"
        ):
            raise ContractViolation("weather locked evidence key가 잘못됐습니다.")
        if active_weather_grid_ids_locked(cursor) != active_grids:
            raise ContractViolation(
                "weather resolver 이후 active weather grid 집합이 바뀌었습니다."
            )
        _require_latest_source(
            source_catalog,
            short_term_artifact,
            SHORT_TERM_SOURCE_ID,
            anchor,
            short_term_lookback,
        )
        _require_latest_source(
            source_catalog,
            ultra_short_artifact,
            ULTRA_SHORT_SOURCE_ID,
            anchor,
            ultra_short_lookback,
        )

    def validate_conditional_empty(
        cursor: Cursor[tuple[Any, ...]],
        evidence: VerifiedPublicationEvidence,
    ) -> bool:
        """weather EMPTY는 lock 안 active station grid가 0개일 때만 증명한다."""
        if evidence.manifest.publication_key != "weather_forecast":
            raise ContractViolation("weather EMPTY evidence key가 다릅니다.")
        return active_weather_grid_ids_locked(cursor) == ()

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """weather_forecast 전체 buffer를 claim과 같은 transaction에서 교체한다."""
        if (
            len(evidence) != 1
            or evidence[0].manifest.publication_key != "weather_forecast"
        ):
            raise ContractViolation("weather mutation evidence key가 잘못됐습니다.")
        _upsert_weather_forecast_records(cursor, projection.records)
        _delete_absent_weather_forecast_records(cursor, projection.records)

    return publish_verified(
        connection,
        ((prepared, validate_staging),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
        validate_conditional_empty=validate_conditional_empty,
    )


def _upsert_weather_forecast_records(
    cursor: Cursor[tuple[Any, ...]],
    records: tuple[WeatherForecastRecord, ...],
) -> None:
    """incoming weather PK를 단일 SQL로 upsert하며 최초 생성 시각을 보존한다."""
    if not records:
        return
    # Projection 계약이 grid·forecast key 중복을 차단한다. C 정렬은 ON CONFLICT가
    # 기존 forecast row를 잠그는 순서를 locale과 무관하게 고정한다.
    cursor.execute(
        """
        INSERT INTO weather_forecast AS current_forecast (
            weather_grid_id,
            forecast_dttm,
            source_product_cd,
            base_dttm,
            sky_condition_cd,
            precipitation_type_cd,
            temperature,
            precipitation_prob,
            precipitation_amount,
            humidity,
            wind_speed
        )
        SELECT incoming.weather_grid_id,
               incoming.forecast_dttm,
               incoming.source_product_cd,
               incoming.base_dttm,
               incoming.sky_condition_cd,
               incoming.precipitation_type_cd,
               incoming.temperature,
               incoming.precipitation_prob,
               incoming.precipitation_amount,
               incoming.humidity,
               incoming.wind_speed
          FROM unnest(
                   %s::TEXT[],
                   %s::TIMESTAMPTZ[],
                   %s::TEXT[],
                   %s::TIMESTAMPTZ[],
                   %s::TEXT[],
                   %s::TEXT[],
                   %s::DOUBLE PRECISION[],
                   %s::DOUBLE PRECISION[],
                   %s::DOUBLE PRECISION[],
                   %s::DOUBLE PRECISION[],
                   %s::DOUBLE PRECISION[]
               ) AS incoming(
                   weather_grid_id,
                   forecast_dttm,
                   source_product_cd,
                   base_dttm,
                   sky_condition_cd,
                   precipitation_type_cd,
                   temperature,
                   precipitation_prob,
                   precipitation_amount,
                   humidity,
                   wind_speed
               )
         ORDER BY incoming.weather_grid_id COLLATE "C", incoming.forecast_dttm
        ON CONFLICT (weather_grid_id, forecast_dttm) DO UPDATE
        SET source_product_cd = EXCLUDED.source_product_cd,
            base_dttm = EXCLUDED.base_dttm,
            sky_condition_cd = EXCLUDED.sky_condition_cd,
            precipitation_type_cd = EXCLUDED.precipitation_type_cd,
            temperature = EXCLUDED.temperature,
            precipitation_prob = EXCLUDED.precipitation_prob,
            precipitation_amount = EXCLUDED.precipitation_amount,
            humidity = EXCLUDED.humidity,
            wind_speed = EXCLUDED.wind_speed
        WHERE ROW(
            current_forecast.source_product_cd,
            current_forecast.base_dttm,
            current_forecast.sky_condition_cd,
            current_forecast.precipitation_type_cd,
            current_forecast.temperature,
            current_forecast.precipitation_prob,
            current_forecast.precipitation_amount,
            current_forecast.humidity,
            current_forecast.wind_speed
        ) IS DISTINCT FROM ROW(
            EXCLUDED.source_product_cd,
            EXCLUDED.base_dttm,
            EXCLUDED.sky_condition_cd,
            EXCLUDED.precipitation_type_cd,
            EXCLUDED.temperature,
            EXCLUDED.precipitation_prob,
            EXCLUDED.precipitation_amount,
            EXCLUDED.humidity,
            EXCLUDED.wind_speed
        )
        """,
        (
            [record.weather_grid_id for record in records],
            [record.forecast_dttm for record in records],
            [record.source_product_cd for record in records],
            [record.base_dttm for record in records],
            [record.sky_condition_cd for record in records],
            [record.precipitation_type_cd for record in records],
            [record.temperature for record in records],
            [record.precipitation_prob for record in records],
            [record.precipitation_amount for record in records],
            [record.humidity for record in records],
            [record.wind_speed for record in records],
        ),
    )


def _delete_absent_weather_forecast_records(
    cursor: Cursor[tuple[Any, ...]],
    records: tuple[WeatherForecastRecord, ...],
) -> None:
    """incoming complete buffer에 없는 weather 복합 PK만 제거한다."""
    cursor.execute(
        """
        DELETE FROM weather_forecast AS current_forecast
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM unnest(
                              %s::TEXT[],
                              %s::TIMESTAMPTZ[]
                          ) AS incoming(weather_grid_id, forecast_dttm)
                    WHERE incoming.weather_grid_id = current_forecast.weather_grid_id
                      AND incoming.forecast_dttm = current_forecast.forecast_dttm
               )
        """,
        (
            [record.weather_grid_id for record in records],
            [record.forecast_dttm for record in records],
        ),
    )


def _projection_from_source_artifacts(
    object_store: ImmutableObjectStore,
    *,
    short_artifact: SourceManifestArtifact,
    ultra_artifact: SourceManifestArtifact,
    short_input: InputArtifact,
    ultra_input: InputArtifact,
    active_grids: tuple[str, ...],
    anchor: datetime,
) -> WeatherForecastProjection:
    """catalog actual manifest payload에서 weather projection을 준비한다."""
    return _projection_from_verified_payloads(
        object_store,
        short_input=short_input,
        ultra_input=ultra_input,
        payloads={
            short_input.uri: short_artifact.payload,
            ultra_input.uri: ultra_artifact.payload,
        },
        active_grids=active_grids,
        anchor=anchor,
    )


def _projection_from_verified_payloads(
    object_store: ImmutableObjectStore,
    *,
    short_input: InputArtifact,
    ultra_input: InputArtifact,
    payloads: Mapping[str, bytes],
    active_grids: tuple[str, ...],
    anchor: datetime,
) -> WeatherForecastProjection:
    """verifier actual payload의 두 complete Silver를 whole-row resolver에 전달한다."""
    short_snapshot = read_source_snapshot_payload(
        object_store,
        manifest_artifact=short_input,
        verified_payloads=payloads,
        expected_source_id=SHORT_TERM_SOURCE_ID,
    )
    ultra_snapshot = read_source_snapshot_payload(
        object_store,
        manifest_artifact=ultra_input,
        verified_payloads=payloads,
        expected_source_id=ULTRA_SHORT_SOURCE_ID,
    )
    validate_source_snapshot_policy(short_snapshot.manifest)
    validate_source_snapshot_policy(ultra_snapshot.manifest)
    short_rows = tuple(source_snapshot_parquet(short_snapshot).to_pylist())
    ultra_rows = tuple(source_snapshot_parquet(ultra_snapshot).to_pylist())
    return resolve_weather_forecast(
        short_rows,
        ultra_rows,
        active_weather_grid_ids=active_grids,
        run_dttm=anchor,
    )


def _records_to_parquet(records: tuple[WeatherForecastRecord, ...]) -> bytes:
    """weather records를 fixed schema deterministic Parquet bytes로 만든다."""
    table = pa.Table.from_pylist(
        [
            {
                "weather_grid_id": record.weather_grid_id,
                "forecast_dttm": record.forecast_dttm,
                "source_product_cd": record.source_product_cd,
                "base_dttm": record.base_dttm,
                "sky_condition_cd": record.sky_condition_cd,
                "precipitation_type_cd": record.precipitation_type_cd,
                "temperature": record.temperature,
                "precipitation_prob": record.precipitation_prob,
                "precipitation_amount": record.precipitation_amount,
                "humidity": record.humidity,
                "wind_speed": record.wind_speed,
            }
            for record in records
        ],
        schema=_WEATHER_FORECAST_SCHEMA,
    )
    return parquet_bytes(table)


def _records_from_parquet(payload: bytes) -> tuple[WeatherForecastRecord, ...]:
    """weather output Parquet의 exact schema와 typed record를 재검증한다."""
    table = read_parquet_bytes(payload)
    if table.schema != _WEATHER_FORECAST_SCHEMA:
        raise ContractViolation(
            "weather output Parquet schema가 exact 계약과 다릅니다."
        )
    return tuple(WeatherForecastRecord(**row) for row in table.to_pylist())


def _require_source_artifact(
    artifact: SourceManifestArtifact,
    source_id: str,
    anchor: datetime,
) -> None:
    """weather source artifact의 exact source와 scheduled anchor 이전성을 검증한다."""
    if type(artifact) is not SourceManifestArtifact:
        raise ContractViolation("weather source artifact type이 잘못됐습니다.")
    if artifact.manifest.source_id != source_id:
        raise ContractViolation("weather source artifact source_id가 다릅니다.")
    if artifact.manifest.logical_dttm > anchor:
        raise ContractViolation(
            "weather source artifact가 scheduled anchor보다 미래입니다."
        )


def _require_latest_source(
    catalog: S3SourceSnapshotCatalog,
    selected: SourceManifestArtifact,
    source_id: str,
    anchor: datetime,
    lookback: timedelta,
) -> None:
    """lock 후 scheduled anchor 이전 최신 source artifact가 선택값과 같은지 검증한다."""
    latest = catalog.latest_at_or_before(
        source_id,
        anchor,
        lookback=lookback,
    )
    if (latest.uri, latest.byte_sha256) != (selected.uri, selected.byte_sha256):
        raise ContractViolation(
            f"{source_id} correction이 갱신되어 weather 준비 입력이 최신이 아닙니다."
        )


def _latest_source_rows(
    rows: tuple[Mapping[str, Any], ...],
    *,
    source_product_cd: str,
    required_keys: set[tuple[str, datetime]],
) -> dict[tuple[str, datetime], Mapping[str, Any]]:
    """source 내 exact target별 최신 발표 행 하나를 선택한다."""
    if type(rows) is not tuple:
        raise ContractViolation("weather source rows는 tuple이어야 합니다.")
    latest: dict[tuple[str, datetime], tuple[datetime, Mapping[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ContractViolation("weather source row는 mapping이어야 합니다.")
        nx = _positive_int(row.get("nx"), "nx")
        ny = _positive_int(row.get("ny"), "ny")
        grid_id = f"{nx}_{ny}"
        forecast = _kst_dttm(row.get("fcstDate"), row.get("fcstTime"), "forecast")
        if forecast.minute or forecast.second or forecast.microsecond:
            continue
        key = (grid_id, forecast)
        if key not in required_keys:
            continue
        base = _kst_dttm(row.get("baseDate"), row.get("baseTime"), "base")
        existing = latest.get(key)
        if existing is not None and existing[0] == base:
            if dict(existing[1]) != dict(row):
                raise ContractViolation(
                    f"{source_product_cd} 같은 target·base에 다른 행이 충돌합니다."
                )
            continue
        if existing is None or base > existing[0]:
            latest[key] = (base, row)
    return {key: row for key, (_, row) in latest.items()}


def _valid_record_or_none(
    row: Mapping[str, Any] | None,
    source_product_cd: str,
) -> WeatherForecastRecord | None:
    """선택 source 행 전체가 유효하면 record, 아니면 fallback용 None을 반환한다."""
    if row is None:
        return None
    try:
        nx = _positive_int(row.get("nx"), "nx")
        ny = _positive_int(row.get("ny"), "ny")
        base = _kst_dttm(row.get("baseDate"), row.get("baseTime"), "base")
        forecast = _kst_dttm(row.get("fcstDate"), row.get("fcstTime"), "forecast")
        temperature_field = "T1H" if source_product_cd == "ultra_short" else "TMP"
        precipitation_field = "RN1" if source_product_cd == "ultra_short" else "PCP"
        precipitation_codes = (
            _ULTRA_PRECIPITATION_CODES
            if source_product_cd == "ultra_short"
            else _SHORT_PRECIPITATION_CODES
        )
        return WeatherForecastRecord(
            weather_grid_id=f"{nx}_{ny}",
            forecast_dttm=forecast,
            source_product_cd=source_product_cd,
            base_dttm=base,
            sky_condition_cd=_SKY_CODES[_integer_code(row.get("SKY"), "SKY")],
            precipitation_type_cd=precipitation_codes[
                _integer_code(row.get("PTY"), "PTY")
            ],
            temperature=_bounded_float(
                row.get(temperature_field), temperature_field, -50.0, 50.0
            ),
            precipitation_prob=_optional_bounded_float(
                row.get("POP"), "POP", 0.0, 100.0
            ),
            precipitation_amount=_optional_precipitation(
                row.get(precipitation_field), precipitation_field
            ),
            humidity=_optional_bounded_float(row.get("REH"), "REH", 0.0, 100.0),
            wind_speed=_optional_bounded_float(row.get("WSD"), "WSD", 0.0, 50.0),
        )
    except (ContractViolation, KeyError, TypeError, ValueError):
        return None


def _parse_grid_id(value: Any) -> tuple[int, int]:
    """weather_grid_id를 DDL의 canonical x_y 형식으로 검증한다."""
    if type(value) is not str or value.count("_") != 1:
        raise ContractViolation("weather_grid_id는 canonical x_y 문자열이어야 합니다.")
    x_text, y_text = value.split("_")
    try:
        x_no = int(x_text)
        y_no = int(y_text)
    except ValueError as exc:
        raise ContractViolation(
            "weather_grid_id에 정수 격자번호가 필요합니다."
        ) from exc
    if x_no <= 0 or y_no <= 0 or value != f"{x_no}_{y_no}":
        raise ContractViolation("weather_grid_id가 DDL canonical 형식과 다릅니다.")
    return x_no, y_no


def _kst_dttm(date_value: Any, time_value: Any, name: str) -> datetime:
    """KMA YYYYMMDD·HHMM을 timezone-aware UTC instant로 변환한다."""
    date_text = str(date_value).strip().replace("-", "")
    time_text = str(time_value).strip().zfill(4)
    if len(date_text) != 8 or not date_text.isdigit():
        raise ContractViolation(f"{name} date가 YYYYMMDD가 아닙니다.")
    if len(time_text) != 4 or not time_text.isdigit():
        raise ContractViolation(f"{name} time이 HHMM이 아닙니다.")
    try:
        local = datetime.strptime(date_text + time_text, "%Y%m%d%H%M").replace(
            tzinfo=_KST
        )
    except ValueError as exc:
        raise ContractViolation(f"{name} KST 시각이 잘못됐습니다.") from exc
    return local.astimezone(UTC)


def _utc_dttm(value: Any, name: str) -> datetime:
    """exact aware datetime을 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise ContractViolation(f"{name}은 datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)


def _positive_int(value: Any, name: str) -> int:
    """값을 bool이 아닌 양의 정수로 검증한다."""
    if type(value) is bool:
        raise ContractViolation(f"{name}은 양의 정수여야 합니다.")
    try:
        converted = int(value)
    except (TypeError, ValueError) as exc:
        raise ContractViolation(f"{name}은 양의 정수여야 합니다.") from exc
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractViolation(f"{name}은 양의 정수여야 합니다.") from exc
    if not math.isfinite(numeric) or numeric != converted or converted <= 0:
        raise ContractViolation(f"{name}은 양의 정수여야 합니다.")
    return converted


def _integer_code(value: Any, name: str) -> int:
    """KMA 범주 코드를 exact 정수로 변환한다."""
    if type(value) is bool:
        raise ContractViolation(f"{name}은 정수 코드여야 합니다.")
    try:
        converted = int(value)
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractViolation(f"{name}은 정수 코드여야 합니다.") from exc
    if not math.isfinite(numeric) or numeric != converted:
        raise ContractViolation(f"{name}은 정수 코드여야 합니다.")
    return converted


def _bounded_float(value: Any, name: str, minimum: float, maximum: float) -> float:
    """값을 DDL 범위 안의 유한 float로 검증한다."""
    if type(value) is bool:
        raise ContractViolation(f"{name}은 유한 숫자여야 합니다.")
    try:
        converted = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractViolation(f"{name}은 유한 숫자여야 합니다.") from exc
    if not math.isfinite(converted) or not minimum <= converted <= maximum:
        raise ContractViolation(f"{name}이 DDL 범위 밖입니다.")
    return converted


def _optional_bounded_float(
    value: Any,
    name: str,
    minimum: float,
    maximum: float | None,
) -> float | None:
    """결측은 None, 값이 있으면 DDL 범위 유한 float를 반환한다."""
    if value is None or value == "" or (type(value) is float and math.isnan(value)):
        return None
    if maximum is None:
        if type(value) is bool:
            raise ContractViolation(f"{name}은 유한 숫자여야 합니다.")
        try:
            converted = float(value)
        except (TypeError, ValueError) as exc:
            raise ContractViolation(f"{name}은 유한 숫자여야 합니다.") from exc
        if not math.isfinite(converted) or converted < minimum:
            raise ContractViolation(f"{name}이 DDL 범위 밖입니다.")
        return converted
    return _bounded_float(value, name, minimum, maximum)


def _optional_precipitation(value: Any, name: str) -> float | None:
    """결측은 None, KMA 강수량 표기는 공통 mm 하한값으로 변환한다."""
    if value is None or value == "" or (type(value) is float and math.isnan(value)):
        return None
    converted = parse_precip(value)
    if not math.isfinite(converted) or converted < 0:
        raise ContractViolation(f"{name}이 DDL 범위 밖입니다.")
    return converted
