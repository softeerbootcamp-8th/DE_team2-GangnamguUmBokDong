"""Versioned dispatch center YAML을 exact Gold seed로 게시한다."""

from __future__ import annotations

import math
import re
import struct
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pyarrow as pa
from core.gold_publication import (
    ImmutableObjectStore,
    Parameter,
    PreparedPublication,
    VerifiedPublicationEvidence,
    point_ewkb_xdr_hex,
    validate_point_ewkb_xdr_hex,
    validate_sha256_hex,
)
from core.gold_publication.errors import ContractViolation, PublicationDependencyError
from psycopg import Connection, Cursor

from .common import (
    OutputObject,
    PublicationExecution,
    build_prepared_publication,
    materialize_publication,
    parquet_bytes,
    parse_yaml_mapping,
    publish_verified,
    read_parquet_bytes,
    store_input_payload,
)
from .station import DispatchCenterReference, assign_dispatch_center_id
from .versioning import PublicationCandidate, allocate_revision

DISPATCH_CENTER_SEED_VERSION = "dispatch-center-v3"
DISPATCH_CENTER_PUBLISHER_VERSION = "gold-dispatch-center-publisher-v3"
EXPECTED_DISPATCH_CENTER_COUNT = 11
DISPATCH_CENTER_SEED_PATH = "docs/gold/dispatch-center-seed.yaml"
_APPROVED_DISPATCH_TRANSITION_SHA256 = (
    "853d48c964f520a33c67a4016d1ba54dc045320675e379c4f7a6b1bc896ba733"
)

_ROOT_KEYS = frozenset(
    (
        "assignment_policy",
        "centers",
        "coordinate_order",
        "coordinate_reference_system",
        "effective_dttm",
        "schema_version",
        "seed_version",
        "source",
    )
)
_ASSIGNMENT_KEYS = frozenset(("method", "tie_breaker"))
_SOURCE_KEYS = frozenset(
    ("description", "file_sha256", "repository_path", "retrieved_dt", "source_url")
)
_CENTER_KEYS = frozenset(
    (
        "dispatch_center_id",
        "dispatch_center_nm",
        "is_active",
        "latitude",
        "location_accuracy_cd",
        "location_source_desc",
        "location_verified_dt",
        "longitude",
    )
)
_EXPECTED_CENTER_IDS = frozenset(
    (
        "cheonho",
        "cheonwang",
        "dobong",
        "gaehwa",
        "hangnyeoul",
        "hunryeonwon",
        "isu",
        "jungnang",
        "sangam",
        "sejongno",
        "yeongnam",
    )
)
_ALLOWED_ACCURACY = frozenset(
    ("verified_site", "landmark_approximation", "administrative_centroid")
)
_CENTER_ID_PATTERN = re.compile(r"[a-z0-9_]+")
_DISPATCH_CENTER_SCHEMA = pa.schema(
    (
        pa.field("dispatch_center_id", pa.string(), nullable=False),
        pa.field("dispatch_center_nm", pa.string(), nullable=False),
        pa.field("dispatch_center_point_ewkb", pa.binary(), nullable=False),
        pa.field("location_accuracy_cd", pa.string(), nullable=False),
        pa.field("location_source_desc", pa.string(), nullable=False),
        pa.field("location_verified_dt", pa.date32(), nullable=True),
        pa.field("is_active", pa.bool_(), nullable=False),
    )
)


@dataclass(frozen=True, slots=True, order=True)
class DispatchCenterRow:
    """dispatch_center target projection 한 행을 표현한다."""

    dispatch_center_id: str
    dispatch_center_nm: str
    dispatch_center_point_ewkb: str
    location_accuracy_cd: str
    location_source_desc: str
    location_verified_dt: date | None
    is_active: bool

    def __post_init__(self) -> None:
        """center target field를 DDL의 ID·Point·enum·nullable 계약으로 검증한다."""
        if (
            type(self.dispatch_center_id) is not str
            or _CENTER_ID_PATTERN.fullmatch(self.dispatch_center_id) is None
        ):
            raise ContractViolation("dispatch_center_id 형식이 올바르지 않습니다.")
        _nonblank_string(self.dispatch_center_nm, "dispatch_center_nm")
        validate_point_ewkb_xdr_hex(self.dispatch_center_point_ewkb)
        if self.location_accuracy_cd not in _ALLOWED_ACCURACY:
            raise ContractViolation("location_accuracy_cd가 허용 enum이 아닙니다.")
        _nonblank_string(self.location_source_desc, "location_source_desc")
        if (
            self.location_verified_dt is not None
            and type(self.location_verified_dt) is not date
        ):
            raise ContractViolation(
                "location_verified_dt는 date 또는 null이어야 합니다."
            )
        if type(self.is_active) is not bool:
            raise ContractViolation("dispatch center is_active는 bool이어야 합니다.")

    @property
    def longitude(self) -> float:
        """contract EWKB에서 center 경도를 정확히 복원한다."""
        return _point_coordinates(self.dispatch_center_point_ewkb)[0]

    @property
    def latitude(self) -> float:
        """contract EWKB에서 center 위도를 정확히 복원한다."""
        return _point_coordinates(self.dispatch_center_point_ewkb)[1]


@dataclass(frozen=True, slots=True)
class DispatchCenterSeed:
    """dispatch center seed YAML과 검증된 target rows를 함께 보관한다."""

    seed_version: str
    effective_dttm: datetime
    source_repository_path: str
    source_file_sha256: str
    rows: tuple[DispatchCenterRow, ...]
    yaml_bytes: bytes

    def __post_init__(self) -> None:
        """seed metadata·row count·raw YAML bytes 타입을 검증한다."""
        if self.seed_version != DISPATCH_CENTER_SEED_VERSION:
            raise ContractViolation("dispatch center seed_version이 SSOT와 다릅니다.")
        if type(self.effective_dttm) is not datetime:
            raise ContractViolation(
                "dispatch center effective_dttm은 datetime이어야 합니다."
            )
        if (
            self.effective_dttm.tzinfo is None
            or self.effective_dttm.utcoffset() is None
        ):
            raise ContractViolation(
                "dispatch center effective_dttm은 timezone-aware여야 합니다."
            )
        object.__setattr__(self, "effective_dttm", self.effective_dttm.astimezone(UTC))
        _nonblank_string(self.source_repository_path, "source repository_path")
        validate_sha256_hex(self.source_file_sha256)
        if type(self.rows) is not tuple or any(
            type(row) is not DispatchCenterRow for row in self.rows
        ):
            raise ContractViolation(
                "dispatch center rows는 DispatchCenterRow tuple이어야 합니다."
            )
        _validate_rows(self.rows)
        if type(self.yaml_bytes) is not bytes:
            raise ContractViolation("dispatch center seed YAML은 bytes여야 합니다.")


def load_dispatch_center_seed(
    repository_root: Path,
    *,
    seed_path: str = DISPATCH_CENTER_SEED_PATH,
) -> DispatchCenterSeed:
    """repository seed YAML과 그 안의 source file hash를 함께 검증해 읽는다."""
    if not isinstance(repository_root, Path):
        raise ContractViolation("repository_root는 pathlib.Path여야 합니다.")
    payload = (repository_root / seed_path).read_bytes()
    preliminary = parse_dispatch_center_seed(payload)
    source_payload = (repository_root / preliminary.source_repository_path).read_bytes()
    if validate_sha256_hex(preliminary.source_file_sha256) != _sha256(source_payload):
        raise ContractViolation(
            "dispatch center seed의 source file hash가 repository와 다릅니다."
        )
    return preliminary


def parse_dispatch_center_seed(payload: bytes) -> DispatchCenterSeed:
    """dispatch-center-v3 YAML bytes를 exact 11개 typed center로 파싱한다."""
    document = _exact_mapping(
        parse_yaml_mapping(payload), _ROOT_KEYS, "dispatch center seed"
    )
    if document["schema_version"] != 2:
        raise ContractViolation("dispatch center seed schema_version은 2여야 합니다.")
    seed_version = _nonblank_string(document["seed_version"], "seed_version")
    if seed_version != DISPATCH_CENTER_SEED_VERSION:
        raise ContractViolation("dispatch center seed_version이 SSOT와 다릅니다.")
    effective_dttm = _source_utc_datetime(document["effective_dttm"])
    if document["coordinate_reference_system"] != "EPSG:4326":
        raise ContractViolation("dispatch center 좌표계는 EPSG:4326이어야 합니다.")
    if document["coordinate_order"] != ["longitude", "latitude"]:
        raise ContractViolation("dispatch center coordinate_order가 다릅니다.")

    assignment = _exact_mapping(
        document["assignment_policy"],
        _ASSIGNMENT_KEYS,
        "assignment_policy",
    )
    if assignment != {
        "method": "documented_management_area_then_nearest_geography_distance",
        "tie_breaker": "dispatch_center_id_ascending",
    }:
        raise ContractViolation("dispatch center assignment_policy가 SSOT와 다릅니다.")

    source = _exact_mapping(document["source"], _SOURCE_KEYS, "dispatch center source")
    source_path = _nonblank_string(source["repository_path"], "source repository_path")
    if source_path != "libs/core/src/core/regions.py":
        raise ContractViolation("dispatch center source repository_path가 다릅니다.")
    source_hash = validate_sha256_hex(
        _nonblank_string(source["file_sha256"], "source file_sha256")
    )
    source_url = _nonblank_string(source["source_url"], "source source_url")
    if source_url != (
        "https://github.com/softeerbootcamp-8th/"
        "DE_team2-GangnamguUmBokDong/issues/60"
    ):
        raise ContractViolation("dispatch center source URL이 SSOT와 다릅니다.")
    _nullable_date(source["retrieved_dt"])
    _nonblank_string(source["description"], "source description")

    center_values = document["centers"]
    if type(center_values) is not list:
        raise ContractViolation("dispatch center centers는 list여야 합니다.")
    rows = tuple(
        sorted(
            (_parse_center(value) for value in center_values),
            key=lambda row: row.dispatch_center_id.encode("utf-8"),
        )
    )
    _validate_rows(rows)
    return DispatchCenterSeed(
        seed_version=seed_version,
        effective_dttm=effective_dttm,
        source_repository_path=source_path,
        source_file_sha256=source_hash,
        rows=rows,
        yaml_bytes=payload,
    )


def publish_dispatch_center(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    seed: DispatchCenterSeed,
    object_base_uri: str,
    publisher_version: str = DISPATCH_CENTER_PUBLISHER_VERSION,
) -> PublicationExecution:
    """11개 center seed를 immutable evidence와 한 DB transaction으로 reconcile한다."""
    if type(seed) is not DispatchCenterSeed:
        raise ContractViolation("seed는 DispatchCenterSeed여야 합니다.")
    seed = parse_dispatch_center_seed(seed.yaml_bytes)
    approved_dispatch_transition = (
        sha256(seed.yaml_bytes).hexdigest() == _APPROVED_DISPATCH_TRANSITION_SHA256
    )
    input_artifact = store_input_payload(
        object_store,
        base_uri=object_base_uri,
        publication_key="dispatch_center",
        role="dispatch_center_seed",
        payload=seed.yaml_bytes,
        suffix="yaml",
    )
    output_payload = _rows_to_parquet(seed.rows)
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="dispatch_center",
        input_artifacts=(input_artifact,),
        parameters=(
            Parameter("center_seed_version", seed.seed_version),
            Parameter("expected_center_count", str(EXPECTED_DISPATCH_CENTER_COUNT)),
        ),
        outputs=(
            OutputObject(
                role="dispatch_center",
                payload=output_payload,
                row_count=len(seed.rows),
            ),
        ),
    )
    candidate = PublicationCandidate(
        publication_key="dispatch_center",
        logical_dttm=seed.effective_dttm,
        artifact_set_sha256=materials.artifact_set.sha256,
        input_fingerprint_sha256=materials.input_fingerprint.sha256,
        published_row_cnt=len(seed.rows),
    )
    revision_no = allocate_revision(connection, candidate)
    prepared = build_prepared_publication(
        base_uri=object_base_uri,
        publication_key="dispatch_center",
        logical_dttm=seed.effective_dttm,
        publisher_version=publisher_version,
        revision_no=revision_no,
        target_row_counts={"dispatch_center": len(seed.rows)},
        materials=materials,
    )

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """verifier가 읽은 raw seed와 output이 같은 exact 11개 center인지 검증한다."""
        _require_prepared_key(publication, "dispatch_center")
        parsed_seed = parse_dispatch_center_seed(payloads[input_artifact.uri])
        artifact = publication.manifest.artifacts[0]
        parsed_rows = _rows_from_parquet(payloads[artifact.uri])
        if parsed_seed.rows != parsed_rows:
            raise ContractViolation("dispatch center output이 seed rows와 다릅니다.")
        return {}

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """참조 center 변경을 거부하되 승인된 11개 Point 전환만 허용한다."""
        _require_evidence_keys(evidence, ("dispatch_center",))
        candidate_by_id = {row.dispatch_center_id: row for row in seed.rows}
        cursor.execute(
            """
            SELECT dc.dispatch_center_id,
                   lower(encode(ST_AsEWKB(ST_Force2D(dc.dispatch_center_point), 'XDR'), 'hex'))
              FROM dispatch_center AS dc
             WHERE EXISTS (
                       SELECT 1
                         FROM station AS station
                        WHERE station.dispatch_center_id = dc.dispatch_center_id
                          AND station.is_active
                   )
                OR EXISTS (
                       SELECT 1
                         FROM rebalance_route AS route
                        WHERE route.dispatch_center_id = dc.dispatch_center_id
                          AND route.route_status_cd = 'proposed'
                   )
             ORDER BY dc.dispatch_center_id
            """
        )
        for center_id, current_point in cursor.fetchall():
            candidate_row = candidate_by_id.get(center_id)
            point_transition = (
                approved_dispatch_transition
                and center_id in _EXPECTED_CENTER_IDS
                and candidate_row is not None
                and candidate_row.is_active
                and candidate_row.dispatch_center_point_ewkb != current_point
            )
            if point_transition:
                continue
            if (
                candidate_row is None
                or not candidate_row.is_active
                or candidate_row.dispatch_center_point_ewkb != current_point
            ):
                raise PublicationDependencyError(
                    "참조 중인 dispatch center의 제거·비활성화·Point 변경은 station/route "
                    f"동시 전환 없이 게시할 수 없습니다: dispatch_center_id={center_id}"
                )

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """센터를 reconcile하고 승인된 좌표·관리권역 전환만 함께 적용한다."""
        _require_evidence_keys(evidence, ("dispatch_center",))
        cursor.execute(
            """
            CREATE TEMP TABLE gold_dispatch_center_staging (
                dispatch_center_id TEXT PRIMARY KEY,
                dispatch_center_nm TEXT NOT NULL UNIQUE,
                dispatch_center_point geometry(Point, 4326) NOT NULL,
                location_accuracy_cd TEXT NOT NULL,
                location_source_desc TEXT NOT NULL,
                location_verified_dt DATE,
                is_active BOOLEAN NOT NULL
            ) ON COMMIT DROP
            """
        )
        cursor.executemany(
            """
            INSERT INTO gold_dispatch_center_staging (
                dispatch_center_id,
                dispatch_center_nm,
                dispatch_center_point,
                location_accuracy_cd,
                location_source_desc,
                location_verified_dt,
                is_active
            ) VALUES (%s, %s, ST_GeomFromEWKB(%s), %s, %s, %s, %s)
            """,
            [
                (
                    row.dispatch_center_id,
                    row.dispatch_center_nm,
                    bytes.fromhex(row.dispatch_center_point_ewkb),
                    row.location_accuracy_cd,
                    row.location_source_desc,
                    row.location_verified_dt,
                    row.is_active,
                )
                for row in seed.rows
            ],
        )
        if approved_dispatch_transition:
            active_centers = tuple(
                DispatchCenterReference(
                    dispatch_center_id=row.dispatch_center_id,
                    longitude=row.longitude,
                    latitude=row.latitude,
                    is_active=True,
                )
                for row in sorted(seed.rows)
                if row.is_active
            )
            cursor.execute(
                """
                SELECT target.sta_id,
                       ST_X(target.sta_point),
                       ST_Y(target.sta_point),
                       array_agg(
                           ST_Distance(
                               target.sta_point::geography,
                               center.dispatch_center_point::geography
                           )
                           ORDER BY center.dispatch_center_id COLLATE "C"
                       )
                  FROM station AS target
                 CROSS JOIN gold_dispatch_center_staging AS center
                 WHERE center.is_active
                 GROUP BY target.sta_id, target.sta_point
                 ORDER BY target.sta_id
                """
            )
            station_assignment_rows = [
                (
                    station_id,
                    assign_dispatch_center_id(
                        station_id=station_id,
                        longitude=float(longitude),
                        latitude=float(latitude),
                        centers=active_centers,
                        meters=tuple(float(value) for value in distance_values),
                    ),
                )
                for station_id, longitude, latitude, distance_values in cursor.fetchall()
            ]
            cursor.execute(
                """
                CREATE TEMP TABLE gold_station_dispatch_assignment (
                    sta_id TEXT PRIMARY KEY,
                    dispatch_center_id TEXT NOT NULL
                ) ON COMMIT DROP
                """
            )
            cursor.executemany(
                """
                INSERT INTO gold_station_dispatch_assignment (
                    sta_id,
                    dispatch_center_id
                )
                VALUES (%s, %s)
                """,
                station_assignment_rows,
            )
            cursor.execute(
                "DELETE FROM rebalance_route WHERE route_status_cd = 'proposed'"
            )
        cursor.execute(
            """
            INSERT INTO dispatch_center (
                dispatch_center_id,
                dispatch_center_nm,
                dispatch_center_point,
                location_accuracy_cd,
                location_source_desc,
                location_verified_dt,
                is_active
            )
            SELECT dispatch_center_id,
                   dispatch_center_nm,
                   dispatch_center_point,
                   location_accuracy_cd,
                   location_source_desc,
                   location_verified_dt,
                   is_active
              FROM gold_dispatch_center_staging
             ORDER BY dispatch_center_id
            ON CONFLICT (dispatch_center_id) DO UPDATE
            SET dispatch_center_nm = EXCLUDED.dispatch_center_nm,
                dispatch_center_point = EXCLUDED.dispatch_center_point,
                location_accuracy_cd = EXCLUDED.location_accuracy_cd,
                location_source_desc = EXCLUDED.location_source_desc,
                location_verified_dt = EXCLUDED.location_verified_dt,
                is_active = EXCLUDED.is_active
            """
        )
        if approved_dispatch_transition:
            cursor.execute(
                """
                UPDATE station AS target
                   SET dispatch_center_id = assignment.dispatch_center_id
                  FROM gold_station_dispatch_assignment AS assignment
                 WHERE assignment.sta_id = target.sta_id
                   AND target.dispatch_center_id IS DISTINCT FROM
                       assignment.dispatch_center_id
                """
            )
        cursor.execute(
            """
            UPDATE dispatch_center AS target
               SET is_active = false
             WHERE target.is_active
               AND NOT EXISTS (
                       SELECT 1
                         FROM gold_dispatch_center_staging AS staging
                        WHERE staging.dispatch_center_id = target.dispatch_center_id
                   )
            """
        )
        cursor.execute(
            """
            SELECT count(*)
              FROM dispatch_center AS target
              JOIN gold_dispatch_center_staging AS staging
                ON staging.dispatch_center_id = target.dispatch_center_id
               AND staging.dispatch_center_nm = target.dispatch_center_nm
               AND ST_Equals(staging.dispatch_center_point, target.dispatch_center_point)
               AND staging.location_accuracy_cd = target.location_accuracy_cd
               AND staging.location_source_desc = target.location_source_desc
               AND staging.location_verified_dt IS NOT DISTINCT FROM target.location_verified_dt
               AND staging.is_active = target.is_active
            """
        )
        count_row = cursor.fetchone()
        if count_row is None or count_row[0] != EXPECTED_DISPATCH_CENTER_COUNT:
            raise ContractViolation(
                "dispatch_center full reconcile row count가 11이 아닙니다."
            )

    return publish_verified(
        connection,
        ((prepared, validate_staging),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
    )


def _parse_center(value: Any) -> DispatchCenterRow:
    """seed YAML center mapping을 typed target row로 바꾼다."""
    center = _exact_mapping(value, _CENTER_KEYS, "dispatch center")
    longitude = _finite_number(center["longitude"], "longitude")
    latitude = _finite_number(center["latitude"], "latitude")
    if not 126.5 <= longitude <= 127.5 or not 37.0 <= latitude <= 38.0:
        raise ContractViolation("dispatch center Point가 DDL safety box 밖입니다.")
    verified_date = _nullable_date(center["location_verified_dt"])
    return DispatchCenterRow(
        dispatch_center_id=_nonblank_string(
            center["dispatch_center_id"],
            "dispatch_center_id",
        ),
        dispatch_center_nm=_nonblank_string(
            center["dispatch_center_nm"],
            "dispatch_center_nm",
        ),
        dispatch_center_point_ewkb=point_ewkb_xdr_hex(longitude, latitude),
        location_accuracy_cd=_nonblank_string(
            center["location_accuracy_cd"],
            "location_accuracy_cd",
        ),
        location_source_desc=_nonblank_string(
            center["location_source_desc"],
            "location_source_desc",
        ),
        location_verified_dt=verified_date,
        is_active=_boolean(center["is_active"], "is_active"),
    )


def _validate_rows(rows: tuple[DispatchCenterRow, ...]) -> None:
    """center가 SSOT의 exact 11 ID이고 명칭·Point도 중복되지 않았는지 검증한다."""
    if len(rows) != EXPECTED_DISPATCH_CENTER_COUNT:
        raise ContractViolation("dispatch center seed는 정확히 11개여야 합니다.")
    identifiers = tuple(row.dispatch_center_id for row in rows)
    if frozenset(identifiers) != _EXPECTED_CENTER_IDS:
        raise ContractViolation("dispatch center seed ID 집합이 SSOT와 다릅니다.")
    names = tuple(row.dispatch_center_nm for row in rows)
    points = tuple(row.dispatch_center_point_ewkb for row in rows)
    if len(set(names)) != len(names) or len(set(points)) != len(points):
        raise ContractViolation("dispatch center seed에 중복 명칭 또는 Point가 있습니다.")
    accuracy_counts = Counter(row.location_accuracy_cd for row in rows)
    if accuracy_counts != Counter({"landmark_approximation": 11}):
        raise ContractViolation("dispatch center 좌표 정확도 분포가 SSOT와 다릅니다.")
    if any(row.location_verified_dt is None for row in rows):
        raise ContractViolation(
            "dispatch-center-v3의 좌표 대조일은 정확히 11개여야 합니다."
        )
    if any(not row.is_active for row in rows):
        raise ContractViolation("dispatch-center-v3 center는 모두 active여야 합니다.")


def _rows_to_parquet(rows: tuple[DispatchCenterRow, ...]) -> bytes:
    """dispatch center rows를 고정 schema Parquet bytes로 직렬화한다."""
    table = pa.Table.from_pylist(
        [
            {
                "dispatch_center_id": row.dispatch_center_id,
                "dispatch_center_nm": row.dispatch_center_nm,
                "dispatch_center_point_ewkb": bytes.fromhex(
                    row.dispatch_center_point_ewkb
                ),
                "location_accuracy_cd": row.location_accuracy_cd,
                "location_source_desc": row.location_source_desc,
                "location_verified_dt": row.location_verified_dt,
                "is_active": row.is_active,
            }
            for row in rows
        ],
        schema=_DISPATCH_CENTER_SCHEMA,
    )
    return parquet_bytes(table)


def _rows_from_parquet(payload: bytes) -> tuple[DispatchCenterRow, ...]:
    """dispatch center output Parquet을 exact schema와 ID 순서로 읽는다."""
    table = read_parquet_bytes(payload)
    if not table.schema.equals(_DISPATCH_CENTER_SCHEMA, check_metadata=False):
        raise ContractViolation("dispatch center output Parquet schema가 다릅니다.")
    rows = tuple(
        DispatchCenterRow(
            dispatch_center_id=_nonblank_string(
                value["dispatch_center_id"],
                "dispatch_center_id",
            ),
            dispatch_center_nm=_nonblank_string(
                value["dispatch_center_nm"],
                "dispatch_center_nm",
            ),
            dispatch_center_point_ewkb=_binary(
                value["dispatch_center_point_ewkb"],
                "dispatch_center_point_ewkb",
            ).hex(),
            location_accuracy_cd=_nonblank_string(
                value["location_accuracy_cd"],
                "location_accuracy_cd",
            ),
            location_source_desc=_nonblank_string(
                value["location_source_desc"],
                "location_source_desc",
            ),
            location_verified_dt=_nullable_date(value["location_verified_dt"]),
            is_active=_boolean(value["is_active"], "is_active"),
        )
        for value in table.to_pylist()
    )
    _validate_rows(rows)
    if rows != tuple(
        sorted(rows, key=lambda row: row.dispatch_center_id.encode("utf-8"))
    ):
        raise ContractViolation("dispatch center output rows가 ID 오름차순이 아닙니다.")
    return rows


def _source_utc_datetime(value: Any) -> datetime:
    """seed YAML의 RFC3339 Z 시각을 UTC datetime으로 파싱한다."""
    if type(value) is not str or not value.endswith("Z"):
        raise ContractViolation("dispatch center effective_dttm은 Z 시각이어야 합니다.")
    try:
        result = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ContractViolation(
            "dispatch center effective_dttm이 유효하지 않습니다."
        ) from exc
    if result.utcoffset() != UTC.utcoffset(result):
        raise ContractViolation("dispatch center effective_dttm은 UTC여야 합니다.")
    return result.astimezone(UTC)


def _nullable_date(value: Any) -> date | None:
    """YAML/Parquet 값을 exact date 또는 null로 정규화한다."""
    if value is None:
        return None
    if type(value) is date:
        return value
    if type(value) is str:
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ContractViolation(
                "location_verified_dt가 유효하지 않습니다."
            ) from exc
    raise ContractViolation("location_verified_dt는 ISO date 또는 null이어야 합니다.")


def _finite_number(value: Any, name: str) -> float:
    """값이 bool이 아닌 유한 int/float인지 확인한다."""
    if type(value) not in {int, float} or not math.isfinite(value):
        raise ContractViolation(f"{name}은 유한한 숫자여야 합니다.")
    return float(value)


def _exact_mapping(value: Any, keys: frozenset[str], name: str) -> dict[str, Any]:
    """YAML 값이 exact-key mapping인지 확인한다."""
    if type(value) is not dict or set(value) != keys:
        raise ContractViolation(f"{name} key 집합이 정확하지 않습니다.")
    return value


def _nonblank_string(value: Any, name: str) -> str:
    """값이 nonblank exact string인지 확인한다."""
    if type(value) is not str or not value.strip():
        raise ContractViolation(f"{name}은 nonblank 문자열이어야 합니다.")
    return value


def _boolean(value: Any, name: str) -> bool:
    """값이 exact bool인지 확인한다."""
    if type(value) is not bool:
        raise ContractViolation(f"{name}은 bool이어야 합니다.")
    return value


def _binary(value: Any, name: str) -> bytes:
    """값이 exact bytes인지 확인한다."""
    if type(value) is not bytes:
        raise ContractViolation(f"{name}은 bytes여야 합니다.")
    return value


def _point_coordinates(value: str) -> tuple[float, float]:
    """검증된 XDR Point EWKB에서 longitude와 latitude를 반환한다."""
    validate_point_ewkb_xdr_hex(value)
    _byte_order, _geometry_type, _srid, longitude, latitude = struct.unpack(
        ">BIIdd",
        bytes.fromhex(value),
    )
    return longitude, latitude


def _sha256(payload: bytes) -> str:
    """bytes의 lowercase SHA-256을 반환한다."""
    from core.gold_publication import sha256_hex

    return sha256_hex(payload)


def _require_prepared_key(publication: PreparedPublication, expected: str) -> None:
    """staging callback이 의도한 publication key 하나를 받았는지 확인한다."""
    if publication.manifest.publication_key != expected:
        raise ContractViolation(
            f"예상하지 않은 publication key입니다: {publication.manifest.publication_key}"
        )


def _require_evidence_keys(
    evidence: tuple[VerifiedPublicationEvidence, ...],
    expected: tuple[str, ...],
) -> None:
    """transaction callback evidence key가 정확한지 확인한다."""
    actual = tuple(item.manifest.publication_key for item in evidence)
    if actual != expected:
        raise ContractViolation(f"publication evidence key가 다릅니다: {actual}")
