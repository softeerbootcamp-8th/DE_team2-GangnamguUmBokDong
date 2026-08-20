"""문화·공연 원천 snapshot을 Gold event projection으로 변환한다."""

from __future__ import annotations

import json
import math
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
from core.gold_publication import (
    ImmutableObjectStore,
    InputArtifact,
    Parameter,
    PreparedPublication,
    VerifiedPublicationEvidence,
    canonical_json_bytes,
    format_utc_dttm,
    point_ewkb_xdr_hex,
    sha256_hex,
)
from core.gold_publication.errors import ContractViolation
from core.source_snapshot import SourceSnapshotStatus
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
    store_input_payload,
)
from .source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from .source_policy import validate_source_snapshot_policy
from .versioning import PublicationCandidate, allocate_revision

CULTURAL_EVENT_SOURCE = "cultural_event"
PERFORMANCE_EVENT_SOURCE = "performance_event"
STADIUM_COORDINATE_VERSION = "stadium-coordinates-v1"
STADIUM_COORDINATE_SHA256 = (
    "0e0c047bd08f77e82bbccda969c0e726af6998ceaa92979081506cb2140a969b"
)
EVENT_IDENTITY_VERSION = "cultural-event-identity-v1"
EVENT_POLICY_VERSION = "gold-event-policy-v1"
EVENT_PUBLISHER_VERSION = "gold-event-publisher-v1"
_KST = ZoneInfo("Asia/Seoul")
_EVENT_SCHEMA = pa.schema(
    (
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_source_cd", pa.string(), nullable=False),
        pa.field("source_event_id", pa.string(), nullable=False),
        pa.field("event_name", pa.string(), nullable=False),
        pa.field("event_spot_nm", pa.string(), nullable=True),
        pa.field("event_point_ewkb", pa.binary(), nullable=False),
        pa.field("event_point_source_cd", pa.string(), nullable=False),
        pa.field("location_accuracy_cd", pa.string(), nullable=False),
        pa.field("event_start_dt", pa.date32(), nullable=False),
        pa.field("event_end_dt", pa.date32(), nullable=False),
        pa.field("last_seen_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
    )
)


@dataclass(frozen=True, slots=True)
class EventRecord:
    """Gold event 테이블에 source-scoped reconcile할 행을 표현한다."""

    event_id: str
    event_source_cd: str
    source_event_id: str
    event_name: str
    event_spot_nm: str | None
    longitude: float
    latitude: float
    event_point_source_cd: str
    location_accuracy_cd: str
    event_start_dt: date
    event_end_dt: date
    last_seen_dttm: datetime

    def __post_init__(self) -> None:
        """DDL이 요구하는 identity·Point·날짜·시각 계약을 검증한다."""
        source_id = _nonblank_text(self.source_event_id, "source_event_id")
        source = _nonblank_text(self.event_source_cd, "event_source_cd")
        if source not in {CULTURAL_EVENT_SOURCE, PERFORMANCE_EVENT_SOURCE}:
            raise ContractViolation("event_source_cd가 SSOT allowlist에 없습니다.")
        if self.event_id != f"{source}:{source_id}":
            raise ContractViolation("event_id가 source-qualified identity와 다릅니다.")
        _nonblank_text(self.event_name, "event_name")
        if self.event_spot_nm is not None:
            _nonblank_text(self.event_spot_nm, "event_spot_nm")
        _point(self.longitude, self.latitude)
        if self.event_end_dt < self.event_start_dt:
            raise ContractViolation(
                "event_end_dt는 event_start_dt보다 빠를 수 없습니다."
            )
        if type(self.last_seen_dttm) is not datetime:
            raise ContractViolation("last_seen_dttm은 datetime이어야 합니다.")
        format_utc_dttm(self.last_seen_dttm)


@dataclass(frozen=True, slots=True)
class EventProjection:
    """소스별 게시 행과 Silver-only serving rejection 수를 보관한다."""

    records: tuple[EventRecord, ...]
    rejected_row_count: int

    def __post_init__(self) -> None:
        """event identity 정렬·중복과 rejection count를 검증한다."""
        if type(self.records) is not tuple or any(
            type(record) is not EventRecord for record in self.records
        ):
            raise ContractViolation(
                "event projection records는 EventRecord tuple이어야 합니다."
            )
        event_ids = tuple(record.event_id for record in self.records)
        if event_ids != tuple(
            sorted(event_ids, key=lambda value: value.encode("utf-8"))
        ):
            raise ContractViolation(
                "event projection은 event_id UTF-8 순으로 정렬해야 합니다."
            )
        if len(event_ids) != len(set(event_ids)):
            raise ContractViolation("event projection에 중복 event_id가 있습니다.")
        if type(self.rejected_row_count) is not int or self.rejected_row_count < 0:
            raise ContractViolation("rejected_row_count는 0 이상 integer여야 합니다.")


@dataclass(frozen=True, slots=True)
class StadiumCoordinate:
    """검수된 공연 시설 코드·명칭·근사 Point를 표현한다."""

    code: str
    name: str
    longitude: float
    latitude: float


def cultural_source_event_id(
    title: Any,
    place: Any,
    start_date: Any,
    end_date: Any,
) -> str:
    """SSOT v1 canonical identity 배열의 source event ID를 반환한다."""
    normalized_title = _identity_text(title, "TITLE", optional=False)
    normalized_place = _identity_text(place, "PLACE", optional=True)
    normalized_start = _date_value(start_date, "STRTDATE")
    normalized_end = _date_value(end_date, "END_DATE")
    payload = canonical_json_bytes(
        [
            normalized_title,
            normalized_place,
            normalized_start.isoformat(),
            normalized_end.isoformat(),
        ]
    )
    return f"v1:{sha256_hex(payload)}"


def build_cultural_event_projection(
    rows: tuple[Mapping[str, Any], ...],
    *,
    last_seen_dttm: datetime,
    today: date,
) -> EventProjection:
    """완전 문화행사 snapshot을 현재·예정 Gold 행으로 변환한다."""
    observed_at = _utc_dttm(last_seen_dttm, "last_seen_dttm")
    records: dict[str, EventRecord] = {}
    rejected = 0
    for row in rows:
        try:
            title = _identity_text(row.get("TITLE"), "TITLE", optional=False)
            place = _identity_text(row.get("PLACE"), "PLACE", optional=True)
            start = _date_value(row.get("STRTDATE"), "STRTDATE")
            end = _date_value(row.get("END_DATE"), "END_DATE")
            longitude, latitude = _point(row.get("LOT"), row.get("LAT"))
            if end < start:
                raise ContractViolation("문화행사 종료일이 시작일보다 빠릅니다.")
            if end < today or _beyond_two_year_horizon(start, end, today):
                rejected += 1
                continue
            source_event_id = cultural_source_event_id(title, place, start, end)
            record = EventRecord(
                event_id=f"{CULTURAL_EVENT_SOURCE}:{source_event_id}",
                event_source_cd=CULTURAL_EVENT_SOURCE,
                source_event_id=source_event_id,
                event_name=title,
                event_spot_nm=place,
                longitude=longitude,
                latitude=latitude,
                event_point_source_cd="source_reported",
                location_accuracy_cd="source_reported",
                event_start_dt=start,
                event_end_dt=end,
                last_seen_dttm=observed_at,
            )
        except (ContractViolation, TypeError, ValueError):
            rejected += 1
            continue
        _add_equivalent_or_fail(records, record)
    return EventProjection(_ordered_records(records), rejected)


def load_stadium_coordinates(
    path: Path,
    *,
    expected_sha256: str = STADIUM_COORDINATE_SHA256,
) -> tuple[StadiumCoordinate, ...]:
    """검수된 stadium asset bytes와 코드·명칭·Point를 읽어 고정한다."""
    return parse_stadium_coordinates(path.read_bytes(), expected_sha256=expected_sha256)


def parse_stadium_coordinates(
    payload: bytes,
    *,
    expected_sha256: str = STADIUM_COORDINATE_SHA256,
) -> tuple[StadiumCoordinate, ...]:
    """공연 시설 asset actual bytes를 exact SHA·JSON·11개 코드로 검증한다."""
    if type(payload) is not bytes:
        raise ContractViolation("stadium coordinate asset은 bytes여야 합니다.")
    actual_sha256 = sha256_hex(payload)
    if actual_sha256 != expected_sha256:
        raise ContractViolation(
            "stadium coordinate asset SHA-256이 SSOT와 다릅니다: "
            f"expected={expected_sha256}, actual={actual_sha256}"
        )
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractViolation(
            "stadium coordinate asset이 올바른 JSON이 아닙니다."
        ) from exc
    if type(document) is not dict:
        raise ContractViolation("stadium coordinate asset root는 object여야 합니다.")
    coordinates: list[StadiumCoordinate] = []
    for code, raw in document.items():
        if code == "_comment":
            continue
        if type(code) is not str or type(raw) is not dict:
            raise ContractViolation("stadium coordinate 항목이 올바르지 않습니다.")
        if set(raw) != {"name", "lat", "lon"}:
            raise ContractViolation(
                f"stadium coordinate {code} key가 exact 계약과 다릅니다."
            )
        longitude, latitude = _point(raw["lon"], raw["lat"])
        coordinates.append(
            StadiumCoordinate(
                code=_nonblank_text(code, "stadium code"),
                name=_nonblank_text(raw["name"], "stadium name"),
                longitude=longitude,
                latitude=latitude,
            )
        )
    ordered = tuple(sorted(coordinates, key=lambda item: item.code.encode("utf-8")))
    if len(ordered) != 11 or len({item.code for item in ordered}) != len(ordered):
        raise ContractViolation(
            "stadium-coordinates-v1은 서로 다른 11개 코드여야 합니다."
        )
    return ordered


def publish_cultural_event(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    source_artifact: SourceManifestArtifact,
    source_catalog: S3SourceSnapshotCatalog,
    object_base_uri: str,
    publisher_version: str = EVENT_PUBLISHER_VERSION,
) -> PublicationExecution:
    """문화행사 authority snapshot을 source-scoped Gold event로 reconcile한다."""
    return _publish_event(
        connection,
        object_store,
        source_artifact=source_artifact,
        source_catalog=source_catalog,
        object_base_uri=object_base_uri,
        publication_key="event:cultural_event",
        source_id=CULTURAL_EVENT_SOURCE,
        input_role="cultural_event_manifest",
        publisher_version=publisher_version,
        stadium_payload=None,
    )


def publish_performance_event(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    source_artifact: SourceManifestArtifact,
    source_catalog: S3SourceSnapshotCatalog,
    stadium_asset_path: Path,
    object_base_uri: str,
    publisher_version: str = EVENT_PUBLISHER_VERSION,
) -> PublicationExecution:
    """공연행사 authority snapshot과 exact stadium asset을 Gold event로 reconcile한다."""
    stadium_payload = stadium_asset_path.read_bytes()
    parse_stadium_coordinates(stadium_payload)
    return _publish_event(
        connection,
        object_store,
        source_artifact=source_artifact,
        source_catalog=source_catalog,
        object_base_uri=object_base_uri,
        publication_key="event:performance_event",
        source_id=PERFORMANCE_EVENT_SOURCE,
        input_role="performance_event_manifest",
        publisher_version=publisher_version,
        stadium_payload=stadium_payload,
    )


def _publish_event(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    source_artifact: SourceManifestArtifact,
    source_catalog: S3SourceSnapshotCatalog,
    object_base_uri: str,
    publication_key: str,
    source_id: str,
    input_role: str,
    publisher_version: str,
    stadium_payload: bytes | None,
) -> PublicationExecution:
    """event source 공통 immutable evidence·revision·transaction 경계를 실행한다."""
    if type(source_artifact) is not SourceManifestArtifact:
        raise ContractViolation("event source_artifact type이 잘못됐습니다.")
    if type(source_catalog) is not S3SourceSnapshotCatalog:
        raise ContractViolation("event source_catalog type이 잘못됐습니다.")
    if source_artifact.manifest.source_id != source_id:
        raise ContractViolation(
            "event source artifact가 publication source와 다릅니다."
        )
    _require_latest_source_artifact(
        source_catalog,
        source_artifact,
        source_id,
    )
    manifest_artifact = InputArtifact(
        byte_sha256=source_artifact.byte_sha256,
        role=input_role,
        uri=source_artifact.uri,
    )
    inputs = [manifest_artifact]
    stadium_artifact: InputArtifact | None = None
    if stadium_payload is not None:
        stadium_artifact = store_input_payload(
            object_store,
            base_uri=object_base_uri,
            publication_key=publication_key,
            role="stadium_coordinate_seed",
            payload=stadium_payload,
            suffix="json",
        )
        inputs.append(stadium_artifact)
    projection = _projection_from_source(
        object_store,
        manifest_artifact=manifest_artifact,
        manifest_payload=source_artifact.payload,
        source_id=source_id,
        stadium_payload=stadium_payload,
    )
    output_role = (
        "event_cultural_event"
        if source_id == CULTURAL_EVENT_SOURCE
        else "event_performance_event"
    )
    outputs = (
        ()
        if not projection.records
        else (
            OutputObject(
                role=output_role,
                payload=_records_to_parquet(projection.records),
                row_count=len(projection.records),
            ),
        )
    )
    parameters = (
        (
            Parameter("event_identity_version", EVENT_IDENTITY_VERSION),
            Parameter("event_policy_version", EVENT_POLICY_VERSION),
        )
        if source_id == CULTURAL_EVENT_SOURCE
        else (
            Parameter("event_policy_version", EVENT_POLICY_VERSION),
            Parameter("stadium_coordinate_version", STADIUM_COORDINATE_VERSION),
        )
    )
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key=publication_key,
        input_artifacts=tuple(inputs),
        parameters=parameters,
        outputs=outputs,
    )
    candidate = PublicationCandidate(
        publication_key=publication_key,
        logical_dttm=source_artifact.manifest.logical_dttm,
        artifact_set_sha256=materials.artifact_set.sha256,
        input_fingerprint_sha256=materials.input_fingerprint.sha256,
        published_row_cnt=len(projection.records),
    )
    revision_no = allocate_revision(connection, candidate)
    prepared = build_prepared_publication(
        base_uri=object_base_uri,
        publication_key=publication_key,
        logical_dttm=source_artifact.manifest.logical_dttm,
        publisher_version=publisher_version,
        revision_no=revision_no,
        target_row_counts={"event": len(projection.records)},
        materials=materials,
    )

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """actual source·asset·output bytes로 event projection을 다시 만든다."""
        if publication.manifest.publication_key != publication_key:
            raise ContractViolation("event prepared publication key가 다릅니다.")
        verified_stadium = (
            None if stadium_artifact is None else payloads[stadium_artifact.uri]
        )
        expected = _projection_from_verified_payloads(
            object_store,
            manifest_artifact=manifest_artifact,
            payloads=payloads,
            source_id=source_id,
            stadium_payload=verified_stadium,
        )
        if expected.records:
            if len(publication.manifest.artifacts) != 1:
                raise ContractViolation(
                    "nonempty event publication에 output artifact 하나가 필요합니다."
                )
            actual = _records_from_parquet(
                payloads[publication.manifest.artifacts[0].uri]
            )
            if actual != expected.records:
                raise ContractViolation(
                    "event output Parquet이 source projection과 다릅니다."
                )
        elif publication.manifest.artifacts:
            raise ContractViolation(
                "EMPTY event publication에 output artifact가 있습니다."
            )
        return {
            "last_seen_dttm": tuple(
                record.last_seen_dttm for record in expected.records
            )
        }

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """DB claim lock 후 source logical window의 최신 correction을 재확인한다."""
        del cursor
        if (
            len(evidence) != 1
            or evidence[0].manifest.publication_key != publication_key
        ):
            raise ContractViolation("event locked evidence key가 잘못됐습니다.")
        _require_latest_source_artifact(
            source_catalog,
            source_artifact,
            source_id,
        )

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """event source 하나의 전체 행을 같은 transaction에서 reconcile한다."""
        if (
            len(evidence) != 1
            or evidence[0].manifest.publication_key != publication_key
        ):
            raise ContractViolation("event mutation evidence key가 잘못됐습니다.")
        _upsert_event_records(cursor, projection.records)
        _delete_absent_event_records(
            cursor,
            source_id,
            tuple(record.event_id for record in projection.records),
        )

    return publish_verified(
        connection,
        ((prepared, validate_staging),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
    )


def _upsert_event_records(
    cursor: Cursor[tuple[Any, ...]],
    records: tuple[EventRecord, ...],
) -> None:
    """incoming event PK를 upsert하며 DB 최초 생성 시각을 보존한다."""
    if not records:
        return
    cursor.executemany(
        """
        INSERT INTO event AS current_event (
            event_id,
            event_source_cd,
            source_event_id,
            event_name,
            event_spot_nm,
            event_point,
            event_point_source_cd,
            location_accuracy_cd,
            event_start_dt,
            event_end_dt,
            last_seen_dttm
        )
        VALUES (
            %s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (event_id) DO UPDATE
        SET event_source_cd = EXCLUDED.event_source_cd,
            source_event_id = EXCLUDED.source_event_id,
            event_name = EXCLUDED.event_name,
            event_spot_nm = EXCLUDED.event_spot_nm,
            event_point = EXCLUDED.event_point,
            event_point_source_cd = EXCLUDED.event_point_source_cd,
            location_accuracy_cd = EXCLUDED.location_accuracy_cd,
            event_start_dt = EXCLUDED.event_start_dt,
            event_end_dt = EXCLUDED.event_end_dt,
            last_seen_dttm = EXCLUDED.last_seen_dttm
        WHERE ROW(
            current_event.event_source_cd,
            current_event.source_event_id,
            current_event.event_name,
            current_event.event_spot_nm,
            current_event.event_point,
            current_event.event_point_source_cd,
            current_event.location_accuracy_cd,
            current_event.event_start_dt,
            current_event.event_end_dt,
            current_event.last_seen_dttm
        ) IS DISTINCT FROM ROW(
            EXCLUDED.event_source_cd,
            EXCLUDED.source_event_id,
            EXCLUDED.event_name,
            EXCLUDED.event_spot_nm,
            EXCLUDED.event_point,
            EXCLUDED.event_point_source_cd,
            EXCLUDED.location_accuracy_cd,
            EXCLUDED.event_start_dt,
            EXCLUDED.event_end_dt,
            EXCLUDED.last_seen_dttm
        )
        """,
        tuple(
            (
                record.event_id,
                record.event_source_cd,
                record.source_event_id,
                record.event_name,
                record.event_spot_nm,
                record.longitude,
                record.latitude,
                record.event_point_source_cd,
                record.location_accuracy_cd,
                record.event_start_dt,
                record.event_end_dt,
                record.last_seen_dttm,
            )
            for record in records
        ),
    )


def _delete_absent_event_records(
    cursor: Cursor[tuple[Any, ...]],
    source_id: str,
    incoming_event_ids: tuple[str, ...],
) -> None:
    """한 source의 incoming snapshot에 없는 event PK만 제거한다."""
    cursor.execute(
        """
        DELETE FROM event
         WHERE event_source_cd = %s
           AND NOT (event_id = ANY(%s::TEXT[]))
        """,
        (source_id, list(incoming_event_ids)),
    )


def _require_latest_source_artifact(
    source_catalog: S3SourceSnapshotCatalog,
    source_artifact: SourceManifestArtifact,
    source_id: str,
) -> None:
    """exact logical window의 최신 correction이 선택 artifact와 같은지 확인한다."""
    latest = source_catalog.exact_window(
        source_id,
        source_artifact.manifest.logical_dttm,
    )
    if (latest.uri, latest.byte_sha256) != (
        source_artifact.uri,
        source_artifact.byte_sha256,
    ):
        raise ContractViolation(
            "event source correction이 갱신되어 준비한 입력이 최신이 아닙니다."
        )


def _projection_from_source(
    object_store: ImmutableObjectStore,
    *,
    manifest_artifact: InputArtifact,
    manifest_payload: bytes,
    source_id: str,
    stadium_payload: bytes | None,
) -> EventProjection:
    """catalog actual manifest와 exact Silver로 event projection을 물질화한다."""
    return _projection_from_verified_payloads(
        object_store,
        manifest_artifact=manifest_artifact,
        payloads={manifest_artifact.uri: manifest_payload},
        source_id=source_id,
        stadium_payload=stadium_payload,
    )


def _projection_from_verified_payloads(
    object_store: ImmutableObjectStore,
    *,
    manifest_artifact: InputArtifact,
    payloads: Mapping[str, bytes],
    source_id: str,
    stadium_payload: bytes | None,
) -> EventProjection:
    """verifier actual payload mapping에서 source status를 보존해 event를 만든다."""
    snapshot = read_source_snapshot_payload(
        object_store,
        manifest_artifact=manifest_artifact,
        verified_payloads=payloads,
        expected_source_id=source_id,
    )
    validate_source_snapshot_policy(snapshot.manifest)
    rows: tuple[Mapping[str, Any], ...]
    if snapshot.manifest.status is SourceSnapshotStatus.EMPTY:
        rows = ()
    else:
        rows = tuple(source_snapshot_parquet(snapshot).to_pylist())
    today = snapshot.manifest.logical_dttm.astimezone(_KST).date()
    if source_id == CULTURAL_EVENT_SOURCE:
        if stadium_payload is not None:
            raise ContractViolation(
                "cultural event에 stadium asset을 추가할 수 없습니다."
            )
        return build_cultural_event_projection(
            rows,
            last_seen_dttm=snapshot.manifest.logical_dttm,
            today=today,
        )
    if source_id != PERFORMANCE_EVENT_SOURCE or stadium_payload is None:
        raise ContractViolation("performance event에 exact stadium asset이 필요합니다.")
    return build_performance_event_projection(
        rows,
        last_seen_dttm=snapshot.manifest.logical_dttm,
        today=today,
        coordinates=parse_stadium_coordinates(stadium_payload),
    )


def _records_to_parquet(records: tuple[EventRecord, ...]) -> bytes:
    """event records를 fixed schema deterministic Parquet bytes로 만든다."""
    table = pa.Table.from_pylist(
        [
            {
                "event_id": record.event_id,
                "event_source_cd": record.event_source_cd,
                "source_event_id": record.source_event_id,
                "event_name": record.event_name,
                "event_spot_nm": record.event_spot_nm,
                "event_point_ewkb": bytes.fromhex(
                    point_ewkb_xdr_hex(record.longitude, record.latitude)
                ),
                "event_point_source_cd": record.event_point_source_cd,
                "location_accuracy_cd": record.location_accuracy_cd,
                "event_start_dt": record.event_start_dt,
                "event_end_dt": record.event_end_dt,
                "last_seen_dttm": record.last_seen_dttm,
            }
            for record in records
        ],
        schema=_EVENT_SCHEMA,
    )
    return parquet_bytes(table)


def _records_from_parquet(payload: bytes) -> tuple[EventRecord, ...]:
    """fixed schema event output Parquet을 typed records로 다시 검증한다."""
    table = read_parquet_bytes(payload)
    if table.schema != _EVENT_SCHEMA:
        raise ContractViolation("event output Parquet schema가 exact 계약과 다릅니다.")
    records: list[EventRecord] = []
    for row in table.to_pylist():
        longitude, latitude = _point_from_ewkb(row["event_point_ewkb"])
        records.append(
            EventRecord(
                event_id=row["event_id"],
                event_source_cd=row["event_source_cd"],
                source_event_id=row["source_event_id"],
                event_name=row["event_name"],
                event_spot_nm=row["event_spot_nm"],
                longitude=longitude,
                latitude=latitude,
                event_point_source_cd=row["event_point_source_cd"],
                location_accuracy_cd=row["location_accuracy_cd"],
                event_start_dt=row["event_start_dt"],
                event_end_dt=row["event_end_dt"],
                last_seen_dttm=row["last_seen_dttm"],
            )
        )
    return tuple(records)


def _point_from_ewkb(payload: bytes) -> tuple[float, float]:
    """contract 25-byte XDR Point EWKB에서 longitude·latitude를 복원한다."""
    import struct

    if type(payload) is not bytes or len(payload) != 25:
        raise ContractViolation("event Point EWKB bytes가 잘못됐습니다.")
    byte_order, geometry_type, srid, longitude, latitude = struct.unpack(
        ">BIIdd", payload
    )
    if byte_order != 0 or geometry_type != 0x20000001 or srid != 4326:
        raise ContractViolation("event Point EWKB header가 잘못됐습니다.")
    return _point(longitude, latitude)


def build_performance_event_projection(
    rows: tuple[Mapping[str, Any], ...],
    *,
    last_seen_dttm: datetime,
    today: date,
    coordinates: tuple[StadiumCoordinate, ...],
) -> EventProjection:
    """완전 공연 snapshot을 검수된 시설 Point와 결합해 게시한다."""
    observed_at = _utc_dttm(last_seen_dttm, "last_seen_dttm")
    coordinate_by_code = {coordinate.code: coordinate for coordinate in coordinates}
    if len(coordinate_by_code) != len(coordinates):
        raise ContractViolation("stadium coordinate code가 중복됩니다.")
    records: dict[str, EventRecord] = {}
    rejected = 0
    for row in rows:
        source_event_id = _optional_text(row.get("SCH_SEQ"))
        code = _optional_text(row.get("SCH_CODE_B"))
        if source_event_id is None or code is None or code not in coordinate_by_code:
            rejected += 1
            continue
        coordinate = coordinate_by_code[code]
        source_name = _optional_text(row.get("CODE_TITLE_B"))
        if source_name != coordinate.name:
            raise ContractViolation(
                "공연 시설 코드·명칭이 stadium seed와 다릅니다: "
                f"code={code}, source={source_name!r}, seed={coordinate.name!r}"
            )
        try:
            title = _identity_text(row.get("TITLE"), "TITLE", optional=False)
            start = _date_value(row.get("SDATE"), "SDATE")
            end = _date_value(row.get("EDATE"), "EDATE")
            if end < start:
                raise ContractViolation("공연 종료일이 시작일보다 빠릅니다.")
            if end < today or _beyond_two_year_horizon(start, end, today):
                rejected += 1
                continue
            record = EventRecord(
                event_id=f"{PERFORMANCE_EVENT_SOURCE}:{source_event_id}",
                event_source_cd=PERFORMANCE_EVENT_SOURCE,
                source_event_id=source_event_id,
                event_name=title,
                event_spot_nm=coordinate.name,
                longitude=coordinate.longitude,
                latitude=coordinate.latitude,
                event_point_source_cd="curated_osm_nominatim",
                location_accuracy_cd="approximate",
                event_start_dt=start,
                event_end_dt=end,
                last_seen_dttm=observed_at,
            )
        except (ContractViolation, TypeError, ValueError):
            rejected += 1
            continue
        _add_equivalent_or_fail(records, record)
    return EventProjection(_ordered_records(records), rejected)


def _add_equivalent_or_fail(
    records: dict[str, EventRecord],
    incoming: EventRecord,
) -> None:
    """동일 event identity의 byte-equivalent duplicate만 dedupe한다."""
    existing = records.get(incoming.event_id)
    if existing is None:
        records[incoming.event_id] = incoming
        return
    if existing != incoming:
        raise ContractViolation(
            f"같은 event identity에 다른 payload가 충돌합니다: {incoming.event_id}"
        )


def _ordered_records(records: Mapping[str, EventRecord]) -> tuple[EventRecord, ...]:
    """event record를 event_id UTF-8 byte 순으로 반환한다."""
    return tuple(
        records[event_id]
        for event_id in sorted(records, key=lambda value: value.encode("utf-8"))
    )


def _identity_text(value: Any, name: str, *, optional: bool) -> str | None:
    """identity 문자열을 NFC·trim·연속 공백 한 칸으로 정규화한다."""
    if value is None:
        if optional:
            return None
        raise ContractViolation(f"{name}이 없습니다.")
    if type(value) is not str:
        value = str(value)
    normalized = unicodedata.normalize("NFC", " ".join(value.strip().split()))
    if not normalized:
        if optional:
            return None
        raise ContractViolation(f"{name}은 nonblank 문자열이어야 합니다.")
    return normalized


def _nonblank_text(value: Any, name: str) -> str:
    """값을 NFC nonblank 문자열로 검증해 반환한다."""
    normalized = _identity_text(value, name, optional=False)
    assert normalized is not None
    return normalized


def _optional_text(value: Any) -> str | None:
    """값이 있으면 NFC nonblank 문자열, 없으면 None을 반환한다."""
    return _identity_text(value, "optional text", optional=True)


def _date_value(value: Any, name: str) -> date:
    """source 날짜 값을 유한 Gregorian date로 변환한다."""
    if type(value) is date:
        return value
    if type(value) is datetime:
        return value.date()
    if value is None:
        raise ContractViolation(f"{name}이 없습니다.")
    text = str(value).strip()
    if not text:
        raise ContractViolation(f"{name}이 비어 있습니다.")
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ContractViolation(f"{name}이 ISO 날짜가 아닙니다: {value!r}") from exc


def _beyond_two_year_horizon(start: date, end: date, today: date) -> bool:
    """snapshot KST 날짜에서 2년을 넘는 일정인지 반환한다."""
    try:
        horizon = today.replace(year=today.year + 2)
    except ValueError:
        horizon = today.replace(year=today.year + 2, day=28)
    return start > horizon or end > horizon


def _point(longitude: Any, latitude: Any) -> tuple[float, float]:
    """좌표를 DDL 안전 box 안의 유한 WGS84 Point로 검증한다."""
    if type(longitude) not in {int, float} or type(latitude) not in {int, float}:
        raise ContractViolation("event Point 좌표는 숫자여야 합니다.")
    lon = float(longitude)
    lat = float(latitude)
    if not math.isfinite(lon) or not math.isfinite(lat):
        raise ContractViolation("event Point 좌표는 유한해야 합니다.")
    if not 126.5 <= lon <= 127.5 or not 37.0 <= lat <= 38.0:
        raise ContractViolation("event Point가 Gold DDL 안전 box 밖입니다.")
    return lon, lat


def _utc_dttm(value: datetime, name: str) -> datetime:
    """timezone-aware datetime을 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise ContractViolation(f"{name}은 datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)
