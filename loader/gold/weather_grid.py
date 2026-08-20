"""세 weather source 설정에서 exact Gold weather_grid seed를 게시한다."""

from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
from core.gold_publication import (
    ImmutableObjectStore,
    Parameter,
    PreparedPublication,
    VerifiedPublicationEvidence,
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    parse_utc_dttm,
    sha256_hex,
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
from .versioning import PublicationCandidate, allocate_revision

WEATHER_GRID_SEED_SCHEMA_VERSION = "gold-weather-grid-seed-v1"
WEATHER_GRID_PUBLISHER_VERSION = "gold-weather-grid-publisher-v1"
EXPECTED_WEATHER_GRID_COUNT = 34

WEATHER_SOURCE_PATHS = (
    "collector/sources/weather_short_term_forecast.yaml",
    "collector/sources/weather_ultra_short_forecast.yaml",
)

_EXPECTED_SOURCE_IDS = {
    "collector/sources/weather_short_term_forecast.yaml": (
        "weather_short_term_forecast"
    ),
    "collector/sources/weather_ultra_short_forecast.yaml": (
        "weather_ultra_short_forecast"
    ),
}
_SEED_KEYS = frozenset(
    ("effective_dttm", "grids", "schema_version", "seed_version", "sources")
)
_GRID_KEYS = frozenset(("weather_grid_id", "weather_grid_x_no", "weather_grid_y_no"))
_SOURCE_KEYS = frozenset(("byte_sha256", "repository_path", "source_id", "yaml_base64"))
_WEATHER_GRID_SCHEMA = pa.schema(
    (
        pa.field("weather_grid_id", pa.string(), nullable=False),
        pa.field("weather_grid_x_no", pa.int16(), nullable=False),
        pa.field("weather_grid_y_no", pa.int16(), nullable=False),
    )
)


@dataclass(frozen=True, slots=True, order=True)
class WeatherGridRow:
    """weather_grid target 한 행의 자연키와 X/Y를 표현한다."""

    weather_grid_id: str
    weather_grid_x_no: int
    weather_grid_y_no: int

    def __post_init__(self) -> None:
        """ID가 정확히 X_Y이고 두 격자 번호가 양수인지 검증한다."""
        if (
            type(self.weather_grid_x_no) is not int
            or type(self.weather_grid_y_no) is not int
        ):
            raise ContractViolation("weather grid X/Y는 integer여야 합니다.")
        if self.weather_grid_x_no <= 0 or self.weather_grid_y_no <= 0:
            raise ContractViolation("weather grid X/Y는 양수여야 합니다.")
        expected_id = f"{self.weather_grid_x_no}_{self.weather_grid_y_no}"
        if type(self.weather_grid_id) is not str or self.weather_grid_id != expected_id:
            raise ContractViolation("weather_grid_id는 정확히 X_Y여야 합니다.")


@dataclass(frozen=True, slots=True)
class WeatherGridSeed:
    """두 예보 source YAML과 동일한 34개 grid를 고정한 seed manifest를 표현한다."""

    seed_version: str
    effective_dttm: datetime
    rows: tuple[WeatherGridRow, ...]
    canonical_bytes: bytes

    def __post_init__(self) -> None:
        """seed metadata·row count·canonical bytes를 다시 검증한다."""
        Parameter("grid_seed_version", self.seed_version)
        if type(self.effective_dttm) is not datetime:
            raise ContractViolation(
                "weather grid effective_dttm은 datetime이어야 합니다."
            )
        if (
            self.effective_dttm.tzinfo is None
            or self.effective_dttm.utcoffset() is None
        ):
            raise ContractViolation(
                "weather grid effective_dttm은 timezone-aware여야 합니다."
            )
        object.__setattr__(self, "effective_dttm", self.effective_dttm.astimezone(UTC))
        if type(self.rows) is not tuple or any(
            type(row) is not WeatherGridRow for row in self.rows
        ):
            raise ContractViolation(
                "weather grid rows는 WeatherGridRow tuple이어야 합니다."
            )
        _validate_rows(self.rows)
        if type(self.canonical_bytes) is not bytes:
            raise ContractViolation("weather grid seed manifest는 bytes여야 합니다.")
        if (
            canonical_json_bytes(parse_canonical_json(self.canonical_bytes))
            != self.canonical_bytes
        ):
            raise ContractViolation(
                "weather grid seed manifest는 canonical JSON이어야 합니다."
            )


def load_weather_grid_seed(
    repository_root: Path,
    *,
    seed_version: str,
    effective_dttm: datetime,
) -> WeatherGridSeed:
    """repository의 두 예보 YAML exact bytes에서 versioned seed를 만든다."""
    if not isinstance(repository_root, Path):
        raise ContractViolation("repository_root는 pathlib.Path여야 합니다.")
    source_payloads = {
        path: (repository_root / path).read_bytes() for path in WEATHER_SOURCE_PATHS
    }
    return build_weather_grid_seed(
        source_payloads,
        seed_version=seed_version,
        effective_dttm=effective_dttm,
    )


def build_weather_grid_seed(
    source_payloads: Mapping[str, bytes],
    *,
    seed_version: str,
    effective_dttm: datetime,
) -> WeatherGridSeed:
    """두 예보 YAML의 순서까지 같은 34개 grid를 canonical seed로 묶는다."""
    Parameter("grid_seed_version", seed_version)
    normalized_time = _utc_datetime(effective_dttm, "weather grid effective_dttm")
    if type(source_payloads) is not dict:
        source_payloads = dict(source_payloads)
    if set(source_payloads) != set(WEATHER_SOURCE_PATHS):
        raise ContractViolation(
            "weather grid seed는 두 예보 source YAML exact 집합이 필요합니다."
        )

    source_documents: list[dict[str, Any]] = []
    baseline_rows: tuple[WeatherGridRow, ...] | None = None
    for repository_path in WEATHER_SOURCE_PATHS:
        payload = source_payloads[repository_path]
        if type(payload) is not bytes:
            raise ContractViolation("weather source YAML payload는 bytes여야 합니다.")
        source_id, rows = _rows_from_source_yaml(payload, repository_path)
        if baseline_rows is None:
            baseline_rows = rows
        elif rows != baseline_rows:
            raise ContractViolation(
                "두 예보 source YAML의 grid 목록과 순서가 정확히 같아야 합니다."
            )
        source_documents.append(
            {
                "byte_sha256": sha256_hex(payload),
                "repository_path": repository_path,
                "source_id": source_id,
                "yaml_base64": base64.b64encode(payload).decode("ascii"),
            }
        )

    if baseline_rows is None:
        raise ContractViolation("weather source YAML이 없습니다.")
    ordered_rows = tuple(
        sorted(baseline_rows, key=lambda row: row.weather_grid_id.encode("utf-8"))
    )
    seed_document = {
        "effective_dttm": format_utc_dttm(normalized_time),
        "grids": [_row_document(row) for row in ordered_rows],
        "schema_version": WEATHER_GRID_SEED_SCHEMA_VERSION,
        "seed_version": seed_version,
        "sources": source_documents,
    }
    payload = canonical_json_bytes(seed_document)
    return WeatherGridSeed(seed_version, normalized_time, ordered_rows, payload)


def parse_weather_grid_seed(payload: bytes) -> WeatherGridSeed:
    """canonical weather grid seed를 파싱하고 내장된 두 YAML hash·grid를 재검증한다."""
    document = _exact_object(
        parse_canonical_json(payload), _SEED_KEYS, "weather grid seed"
    )
    if document["schema_version"] != WEATHER_GRID_SEED_SCHEMA_VERSION:
        raise ContractViolation("weather grid seed schema_version이 다릅니다.")
    seed_version = _string(document["seed_version"], "seed_version")
    effective_dttm = parse_utc_dttm(
        _string(document["effective_dttm"], "effective_dttm")
    )
    sources = _list(document["sources"], "sources")
    if len(sources) != len(WEATHER_SOURCE_PATHS):
        raise ContractViolation("weather grid seed sources는 정확히 두 개여야 합니다.")

    source_payloads: dict[str, bytes] = {}
    for expected_path, value in zip(WEATHER_SOURCE_PATHS, sources, strict=True):
        source = _exact_object(value, _SOURCE_KEYS, "weather grid seed source")
        repository_path = _string(source["repository_path"], "repository_path")
        if repository_path != expected_path:
            raise ContractViolation("weather grid seed source 순서나 path가 다릅니다.")
        encoded = _string(source["yaml_base64"], "yaml_base64")
        try:
            source_payload = base64.b64decode(encoded, validate=True)
        except (ValueError, base64.binascii.Error) as exc:
            raise ContractViolation(
                "weather source yaml_base64가 유효하지 않습니다."
            ) from exc
        if sha256_hex(source_payload) != _string(source["byte_sha256"], "byte_sha256"):
            raise ContractViolation("weather source YAML checksum이 다릅니다.")
        source_id, _rows = _rows_from_source_yaml(source_payload, expected_path)
        if source_id != _string(source["source_id"], "source_id"):
            raise ContractViolation("weather source_id가 내장 YAML과 다릅니다.")
        source_payloads[repository_path] = source_payload

    rebuilt = build_weather_grid_seed(
        source_payloads,
        seed_version=seed_version,
        effective_dttm=effective_dttm,
    )
    if rebuilt.canonical_bytes != payload:
        raise ContractViolation(
            "weather grid seed canonical bytes가 재구성값과 다릅니다."
        )
    document_rows = tuple(
        _row_from_document(value) for value in _list(document["grids"], "grids")
    )
    if document_rows != rebuilt.rows:
        raise ContractViolation("weather grid seed grids가 내장 YAML과 다릅니다.")
    return rebuilt


def publish_weather_grid(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    seed: WeatherGridSeed,
    object_base_uri: str,
    publisher_version: str = WEATHER_GRID_PUBLISHER_VERSION,
) -> PublicationExecution:
    """34개 grid seed를 immutable evidence와 한 DB transaction으로 reconcile한다."""
    if type(seed) is not WeatherGridSeed:
        raise ContractViolation("seed는 WeatherGridSeed여야 합니다.")
    seed = parse_weather_grid_seed(seed.canonical_bytes)
    input_artifact = store_input_payload(
        object_store,
        base_uri=object_base_uri,
        publication_key="weather_grid",
        role="weather_grid_seed",
        payload=seed.canonical_bytes,
        suffix="json",
        require_canonical_json=True,
    )
    output_payload = _rows_to_parquet(seed.rows)
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="weather_grid",
        input_artifacts=(input_artifact,),
        parameters=(
            Parameter("expected_grid_count", str(EXPECTED_WEATHER_GRID_COUNT)),
            Parameter("grid_seed_version", seed.seed_version),
        ),
        outputs=(
            OutputObject(
                role="weather_grid",
                payload=output_payload,
                row_count=len(seed.rows),
            ),
        ),
    )
    candidate = PublicationCandidate(
        publication_key="weather_grid",
        logical_dttm=seed.effective_dttm,
        artifact_set_sha256=materials.artifact_set.sha256,
        input_fingerprint_sha256=materials.input_fingerprint.sha256,
        published_row_cnt=len(seed.rows),
    )
    revision_no = allocate_revision(connection, candidate)
    prepared = build_prepared_publication(
        base_uri=object_base_uri,
        publication_key="weather_grid",
        logical_dttm=seed.effective_dttm,
        publisher_version=publisher_version,
        revision_no=revision_no,
        target_row_counts={"weather_grid": len(seed.rows)},
        materials=materials,
    )

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """verifier가 읽은 seed/output bytes가 같은 exact 34개 grid인지 검증한다."""
        _require_prepared_key(publication, "weather_grid")
        parsed_seed = parse_weather_grid_seed(payloads[input_artifact.uri])
        artifact = publication.manifest.artifacts[0]
        parsed_rows = _rows_from_parquet(payloads[artifact.uri])
        if parsed_seed.rows != parsed_rows:
            raise ContractViolation("weather grid output이 seed rows와 다릅니다.")
        return {}

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """candidate에서 빠질 grid를 참조하는 station·forecast가 없는지 잠금 안에서 확인한다."""
        _require_evidence_keys(evidence, ("weather_grid",))
        candidate_ids = [row.weather_grid_id for row in seed.rows]
        cursor.execute(
            """
            SELECT 'station', weather_grid_id
              FROM station
             WHERE NOT (weather_grid_id = ANY(%s::TEXT[]))
            UNION ALL
            SELECT 'weather_forecast', weather_grid_id
              FROM weather_forecast
             WHERE NOT (weather_grid_id = ANY(%s::TEXT[]))
            LIMIT 1
            """,
            (candidate_ids, candidate_ids),
        )
        reference = cursor.fetchone()
        if reference is not None:
            raise PublicationDependencyError(
                "참조 중인 weather grid는 standalone seed publication에서 제거할 수 "
                f"없습니다: table={reference[0]}, weather_grid_id={reference[1]}"
            )

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """검증된 34개 grid staging을 upsert하고 unreferenced 잔여 행을 제거한다."""
        _require_evidence_keys(evidence, ("weather_grid",))
        cursor.execute(
            """
            CREATE TEMP TABLE gold_weather_grid_staging (
                weather_grid_id TEXT PRIMARY KEY,
                weather_grid_x_no SMALLINT NOT NULL,
                weather_grid_y_no SMALLINT NOT NULL
            ) ON COMMIT DROP
            """
        )
        cursor.executemany(
            """
            INSERT INTO gold_weather_grid_staging (
                weather_grid_id,
                weather_grid_x_no,
                weather_grid_y_no
            ) VALUES (%s, %s, %s)
            """,
            [
                (
                    row.weather_grid_id,
                    row.weather_grid_x_no,
                    row.weather_grid_y_no,
                )
                for row in seed.rows
            ],
        )
        cursor.execute(
            """
            INSERT INTO weather_grid (
                weather_grid_id,
                weather_grid_x_no,
                weather_grid_y_no
            )
            SELECT weather_grid_id, weather_grid_x_no, weather_grid_y_no
              FROM gold_weather_grid_staging
             ORDER BY weather_grid_id
            ON CONFLICT (weather_grid_id) DO UPDATE
            SET weather_grid_x_no = EXCLUDED.weather_grid_x_no,
                weather_grid_y_no = EXCLUDED.weather_grid_y_no
            """
        )
        cursor.execute(
            """
            DELETE FROM weather_grid AS target
             WHERE NOT EXISTS (
                 SELECT 1
                   FROM gold_weather_grid_staging AS staging
                  WHERE staging.weather_grid_id = target.weather_grid_id
             )
            """
        )
        cursor.execute("SELECT count(*) FROM weather_grid")
        count_row = cursor.fetchone()
        if count_row is None or count_row[0] != EXPECTED_WEATHER_GRID_COUNT:
            raise ContractViolation(
                "weather_grid full reconcile row count가 34가 아닙니다."
            )

    return publish_verified(
        connection,
        ((prepared, validate_staging),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
    )


def _rows_from_source_yaml(
    payload: bytes,
    repository_path: str,
) -> tuple[str, tuple[WeatherGridRow, ...]]:
    """weather source YAML 하나에서 exact ordered grid 목록을 읽는다."""
    document = parse_yaml_mapping(payload)
    source_id = _string(document.get("source_id"), "source_id")
    if source_id != _EXPECTED_SOURCE_IDS[repository_path]:
        raise ContractViolation(
            f"weather source_id가 path와 다릅니다: {repository_path}"
        )
    adapter_params = document.get("adapter_params")
    if type(adapter_params) is not dict:
        raise ContractViolation("weather adapter_params는 mapping이어야 합니다.")
    grids = adapter_params.get("grids")
    if type(grids) is not list:
        raise ContractViolation("weather adapter_params.grids는 list여야 합니다.")
    rows: list[WeatherGridRow] = []
    for value in grids:
        if type(value) is not list or len(value) != 2:
            raise ContractViolation("weather grid 원소는 [nx, ny]여야 합니다.")
        x_no, y_no = value
        if type(x_no) is not int or type(y_no) is not int:
            raise ContractViolation("weather grid X/Y는 integer여야 합니다.")
        rows.append(WeatherGridRow(f"{x_no}_{y_no}", x_no, y_no))
    result = tuple(rows)
    _validate_rows(result)
    return source_id, result


def _validate_rows(rows: tuple[WeatherGridRow, ...]) -> None:
    """weather grid가 정확히 34개이고 ID·X/Y가 중복되지 않았는지 검증한다."""
    if len(rows) != EXPECTED_WEATHER_GRID_COUNT:
        raise ContractViolation("weather grid seed는 정확히 34개여야 합니다.")
    identifiers = tuple(row.weather_grid_id for row in rows)
    coordinates = tuple((row.weather_grid_x_no, row.weather_grid_y_no) for row in rows)
    if len(set(identifiers)) != len(identifiers) or len(set(coordinates)) != len(
        coordinates
    ):
        raise ContractViolation("weather grid seed에 중복 ID 또는 X/Y가 있습니다.")


def _rows_to_parquet(rows: tuple[WeatherGridRow, ...]) -> bytes:
    """weather grid rows를 고정 schema Parquet bytes로 직렬화한다."""
    table = pa.Table.from_pylist(
        [
            {
                "weather_grid_id": row.weather_grid_id,
                "weather_grid_x_no": row.weather_grid_x_no,
                "weather_grid_y_no": row.weather_grid_y_no,
            }
            for row in rows
        ],
        schema=_WEATHER_GRID_SCHEMA,
    )
    return parquet_bytes(table)


def _rows_from_parquet(payload: bytes) -> tuple[WeatherGridRow, ...]:
    """weather grid output Parquet을 exact schema와 canonical row 순서로 읽는다."""
    table = read_parquet_bytes(payload)
    if not table.schema.equals(_WEATHER_GRID_SCHEMA, check_metadata=False):
        raise ContractViolation("weather grid output Parquet schema가 다릅니다.")
    rows = tuple(
        WeatherGridRow(
            _string(value["weather_grid_id"], "weather_grid_id"),
            _integer(value["weather_grid_x_no"], "weather_grid_x_no"),
            _integer(value["weather_grid_y_no"], "weather_grid_y_no"),
        )
        for value in table.to_pylist()
    )
    _validate_rows(rows)
    if rows != tuple(sorted(rows, key=lambda row: row.weather_grid_id.encode("utf-8"))):
        raise ContractViolation("weather grid output rows가 ID 오름차순이 아닙니다.")
    return rows


def _row_document(row: WeatherGridRow) -> dict[str, Any]:
    """weather grid row를 canonical seed object로 바꾼다."""
    return {
        "weather_grid_id": row.weather_grid_id,
        "weather_grid_x_no": row.weather_grid_x_no,
        "weather_grid_y_no": row.weather_grid_y_no,
    }


def _row_from_document(value: Any) -> WeatherGridRow:
    """canonical seed grid object를 typed row로 바꾼다."""
    document = _exact_object(value, _GRID_KEYS, "weather grid")
    return WeatherGridRow(
        _string(document["weather_grid_id"], "weather_grid_id"),
        _integer(document["weather_grid_x_no"], "weather_grid_x_no"),
        _integer(document["weather_grid_y_no"], "weather_grid_y_no"),
    )


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


def _utc_datetime(value: Any, name: str) -> datetime:
    """timezone-aware datetime을 UTC로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ContractViolation(f"{name}은 timezone-aware datetime이어야 합니다.")
    return value.astimezone(UTC)


def _exact_object(value: Any, keys: frozenset[str], name: str) -> dict[str, Any]:
    """canonical JSON 값이 exact-key object인지 확인한다."""
    if type(value) is not dict or set(value) != keys:
        raise ContractViolation(f"{name} key 집합이 정확하지 않습니다.")
    return value


def _list(value: Any, name: str) -> list[Any]:
    """canonical JSON 값이 exact list인지 확인한다."""
    if type(value) is not list:
        raise ContractViolation(f"{name}은 array여야 합니다.")
    return value


def _string(value: Any, name: str) -> str:
    """값이 nonblank exact string인지 확인한다."""
    if type(value) is not str or not value.strip():
        raise ContractViolation(f"{name}은 nonblank 문자열이어야 합니다.")
    return value


def _integer(value: Any, name: str) -> int:
    """값이 bool이 아닌 exact integer인지 확인한다."""
    if type(value) is not int:
        raise ContractViolation(f"{name}은 integer여야 합니다.")
    return value
