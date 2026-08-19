"""Gold publisher 공통 immutable object와 직렬화 경계를 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
import pytest
from core.gold_publication import (
    Dependency,
    ImmutablePutOutcome,
    InputArtifact,
    Parameter,
    PublicationOutcome,
    sha256_hex,
    verify_publication_evidence,
)
from core.gold_publication.errors import (
    ContractViolation,
    ObjectChecksumMismatchError,
    ObjectCollisionError,
    ObjectMissingError,
)
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from gold.common import (
    OutputObject,
    build_prepared_publication,
    content_addressed_uri,
    materialize_publication,
    parquet_bytes,
    parse_json_mapping,
    parse_yaml_mapping,
    read_parquet_bytes,
    read_source_snapshot_payload,
    source_snapshot_parquet,
    store_input_payload,
)


class MemoryStore:
    """테스트용 immutable object store와 write 순서를 기록한다."""

    def __init__(self) -> None:
        """빈 object mapping과 write log를 만든다."""
        self.objects: dict[str, bytes] = {}
        self.write_log: list[str] = []

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """정확한 URI와 checksum의 bytes를 반환한다."""
        del require_canonical_json
        try:
            payload = self.objects[uri]
        except KeyError as exc:
            raise ObjectMissingError(uri) from exc
        if sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError(uri)
        return payload

    def put_once(
        self,
        uri: str,
        payload: bytes,
        *,
        expected_sha256: str | None = None,
        require_canonical_json: bool = False,
    ) -> ImmutablePutOutcome:
        """같은 bytes 재시도만 허용하고 최초 write 순서를 기록한다."""
        del require_canonical_json
        if expected_sha256 is not None and sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError(uri)
        existing = self.objects.get(uri)
        if existing is not None:
            if existing != payload:
                raise ObjectCollisionError(uri)
            return ImmutablePutOutcome.ALREADY_EXISTS
        self.objects[uri] = payload
        self.write_log.append(uri)
        return ImmutablePutOutcome.CREATED


def test_strict_yaml_and_json_reject_duplicate_keys() -> None:
    """source decoder가 duplicate key와 비표준 JSON 상수를 거부한다."""
    with pytest.raises(ContractViolation, match="중복"):
        parse_yaml_mapping(b"source_id: a\nsource_id: b\n")
    with pytest.raises(ContractViolation, match="중복"):
        parse_json_mapping(b'{"source_id":"a","source_id":"b"}')
    with pytest.raises(ContractViolation, match="비표준"):
        parse_json_mapping(b'{"value":NaN}')


def test_parquet_boundary_round_trips_exact_schema() -> None:
    """공통 Parquet serializer가 schema와 값을 보존한다."""
    schema = pa.schema((pa.field("identifier", pa.string(), nullable=False),))
    table = pa.Table.from_pylist([{"identifier": "A"}], schema=schema)

    restored = read_parquet_bytes(parquet_bytes(table))

    assert restored.schema.equals(schema, check_metadata=False)
    assert restored.to_pylist() == [{"identifier": "A"}]


def test_materialize_writes_manifest_only_after_evidence_validation() -> None:
    """output·fingerprint가 먼저 쓰이고 publication manifest는 verifier가 마지막에 쓴다."""
    store = MemoryStore()
    source_payload = b"seed: exact\n"
    input_artifact = store_input_payload(
        store,
        base_uri="s3://fixture/gold",
        publication_key="weather_grid",
        role="weather_grid_seed",
        payload=source_payload,
        suffix="yaml",
    )
    output_payload = b"weather-grid-output"
    materials = materialize_publication(
        store,
        base_uri="s3://fixture/gold",
        publication_key="weather_grid",
        input_artifacts=(input_artifact,),
        parameters=(
            Parameter("expected_grid_count", "34"),
            Parameter("grid_seed_version", "weather-grid-v1"),
        ),
        outputs=(OutputObject("weather_grid", output_payload, 34),),
    )
    prepared = build_prepared_publication(
        base_uri="s3://fixture/gold",
        publication_key="weather_grid",
        logical_dttm=datetime(2026, 8, 19, tzinfo=UTC),
        publisher_version="publisher-v1",
        revision_no=0,
        target_row_counts={"weather_grid": 34},
        materials=materials,
    )
    assert prepared.manifest_uri not in store.objects

    def validate_staging(_prepared, payloads):
        """실제 seed와 output bytes가 verifier mapping에 있음을 확인한다."""
        assert payloads[input_artifact.uri] == source_payload
        assert payloads[prepared.manifest.artifacts[0].uri] == output_payload
        return {}

    evidence = verify_publication_evidence(prepared, store, validate_staging)

    assert evidence.manifest.publication_key == "weather_grid"
    assert store.write_log[-1] == prepared.manifest_uri
    assert PublicationOutcome.PUBLISHED.value == "published"


def test_conditional_empty_candidate_requires_explicit_opt_in() -> None:
    """조건부 EMPTY는 준비 허용과 lock 안 증명을 별도 경계로 둔다."""
    store = MemoryStore()
    logical = datetime(2026, 8, 20, tzinfo=UTC)
    dependencies = (
        Dependency(
            "a" * 64,
            "b" * 64,
            logical,
            "s3://fixture/station-manifest.json",
            "station",
            0,
        ),
        Dependency(
            "c" * 64,
            "d" * 64,
            logical,
            "s3://fixture/weather-grid-manifest.json",
            "weather_grid",
            0,
        ),
    )
    materials = materialize_publication(
        store,
        base_uri="s3://fixture/gold",
        publication_key="weather_forecast",
        dependencies=dependencies,
        input_artifacts=(
            InputArtifact(
                "e" * 64,
                "short_term_manifest",
                "s3://fixture/short.json",
            ),
            InputArtifact(
                "f" * 64,
                "ultra_short_manifest",
                "s3://fixture/ultra.json",
            ),
        ),
        parameters=(
            Parameter("forecast_hour_count", "13"),
            Parameter("resolver_version", "weather-resolver-v1"),
        ),
        outputs=(),
    )
    arguments = {
        "base_uri": "s3://fixture/gold",
        "publication_key": "weather_forecast",
        "logical_dttm": logical,
        "publisher_version": "weather-publisher-v1",
        "revision_no": 0,
        "target_row_counts": {"weather_forecast": 0},
        "materials": materials,
    }

    with pytest.raises(ContractViolation, match="active_weather_grid_set_is_empty"):
        build_prepared_publication(**arguments)

    prepared = build_prepared_publication(
        **arguments,
        conditional_empty_candidate=True,
    )
    assert prepared.manifest.published_row_cnt == 0


def test_content_addressed_uri_rejects_ambiguous_base() -> None:
    """content-addressed URI helper가 모호한 S3 path를 만들지 않는다."""
    with pytest.raises(ContractViolation, match="모호"):
        content_addressed_uri(
            "s3://fixture/gold//bad",
            publication_key="weather_grid",
            category="outputs",
            name="weather_grid",
            payload=b"x",
            suffix="parquet",
        )


def test_materialize_rejects_output_roles_before_object_write() -> None:
    """registry와 다른 output role을 immutable object에 남기기 전에 거부한다."""
    store = MemoryStore()
    input_payload = b"seed"
    input_artifact = store_input_payload(
        store,
        base_uri="s3://fixture/gold",
        publication_key="weather_grid",
        role="weather_grid_seed",
        payload=input_payload,
        suffix="yaml",
    )
    writes_before = tuple(store.write_log)

    with pytest.raises(ContractViolation, match="output role"):
        materialize_publication(
            store,
            base_uri="s3://fixture/gold",
            publication_key="weather_grid",
            input_artifacts=(input_artifact,),
            parameters=(
                Parameter("expected_grid_count", "34"),
                Parameter("grid_seed_version", "weather-grid-v1"),
            ),
            outputs=(OutputObject("wrong_role", b"output", 34),),
        )

    assert tuple(store.write_log) == writes_before


def test_source_snapshot_boundary_reads_manifest_owned_silver_bytes() -> None:
    """검증 manifest가 고정한 content-addressed Silver만 exact bytes로 연다."""
    store = MemoryStore()
    schema = pa.schema((pa.field("stationId", pa.string(), nullable=False),))
    silver_bytes = parquet_bytes(
        pa.Table.from_pylist([{"stationId": "ST-1"}], schema=schema)
    )
    silver_sha256 = sha256_hex(silver_bytes)
    silver_uri = f"s3://fixture/silver/sha256={silver_sha256}.parquet"
    store.put_once(silver_uri, silver_bytes, expected_sha256=silver_sha256)
    manifest = build_source_snapshot_manifest(
        source_id="bike_station_realtime",
        logical_dttm=datetime(2026, 8, 19, tzinfo=UTC),
        revision_no=7,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="config-v1",
        silver_uri=silver_uri,
        silver_byte_sha256=silver_sha256,
        counts=SourceSnapshotCounts(1, 1, 1, 0, 0),
        planned_parts=("page-0001",),
        completed_parts=("page-0001",),
    )
    manifest_artifact = store_input_payload(
        store,
        base_uri="s3://fixture/gold",
        publication_key="station_stock",
        role="bike_station_realtime_manifest",
        payload=manifest.canonical_bytes,
        suffix="json",
        require_canonical_json=True,
    )

    snapshot = read_source_snapshot_payload(
        store,
        manifest_artifact=manifest_artifact,
        verified_payloads={manifest_artifact.uri: manifest.canonical_bytes},
        expected_source_id="bike_station_realtime",
        expected_logical_dttm=manifest.logical_dttm,
    )

    assert snapshot.silver_bytes == silver_bytes
    assert source_snapshot_parquet(snapshot).to_pylist() == [{"stationId": "ST-1"}]


def test_source_snapshot_boundary_keeps_confirmed_empty_without_silver() -> None:
    """confirmed EMPTY를 임의 빈 Parquet으로 바꾸지 않고 Silver 없음으로 유지한다."""
    store = MemoryStore()
    manifest = build_source_snapshot_manifest(
        source_id="cultural_event",
        logical_dttm=datetime(2026, 8, 19, tzinfo=UTC),
        revision_no=0,
        status=SourceSnapshotStatus.EMPTY,
        config_version="config-v1",
        silver_uri=None,
        silver_byte_sha256=None,
        counts=SourceSnapshotCounts(0, 0, 0, 0, 0),
        planned_parts=("page-0001",),
        completed_parts=("page-0001",),
    )
    manifest_artifact = store_input_payload(
        store,
        base_uri="s3://fixture/gold",
        publication_key="event:cultural_event",
        role="cultural_event_manifest",
        payload=manifest.canonical_bytes,
        suffix="json",
        require_canonical_json=True,
    )

    snapshot = read_source_snapshot_payload(
        store,
        manifest_artifact=manifest_artifact,
        verified_payloads={manifest_artifact.uri: manifest.canonical_bytes},
        expected_source_id="cultural_event",
    )

    assert snapshot.silver_bytes is None
    with pytest.raises(ContractViolation, match="EMPTY"):
        source_snapshot_parquet(snapshot)


def test_source_snapshot_boundary_rehashes_dishonest_store_bytes() -> None:
    """store 구현이 checksum을 무시해도 Silver actual bytes 변조를 거부한다."""
    store = MemoryStore()
    expected_silver = parquet_bytes(pa.table({"stationId": ["ST-1"]}))
    wrong_silver = parquet_bytes(pa.table({"stationId": ["ST-2"]}))
    expected_sha256 = sha256_hex(expected_silver)
    silver_uri = f"s3://fixture/silver/sha256={expected_sha256}.parquet"
    manifest = build_source_snapshot_manifest(
        source_id="bike_station_realtime",
        logical_dttm=datetime(2026, 8, 19, tzinfo=UTC),
        revision_no=0,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="config-v1",
        silver_uri=silver_uri,
        silver_byte_sha256=expected_sha256,
        counts=SourceSnapshotCounts(1, 1, 1, 0, 0),
        planned_parts=("page-00001-01000",),
        completed_parts=("page-00001-01000",),
    )
    artifact = store_input_payload(
        store,
        base_uri="s3://fixture/gold",
        publication_key="station_stock",
        role="bike_station_realtime_manifest",
        payload=manifest.canonical_bytes,
        suffix="json",
        require_canonical_json=True,
    )
    store.objects[silver_uri] = wrong_silver

    class DishonestStore(MemoryStore):
        """expected checksum을 무시하고 저장 bytes를 그대로 반환한다."""

        def read_bytes(
            self,
            uri: str,
            expected_sha256: str,
            *,
            require_canonical_json: bool = False,
        ) -> bytes:
            """checksum 인자를 무시해 공통 경계의 독립 재해시를 시험한다."""
            del expected_sha256, require_canonical_json
            return self.objects[uri]

    dishonest = DishonestStore()
    dishonest.objects = dict(store.objects)

    with pytest.raises(ContractViolation, match="actual bytes checksum"):
        read_source_snapshot_payload(
            dishonest,
            manifest_artifact=artifact,
            verified_payloads={artifact.uri: manifest.canonical_bytes},
            expected_source_id="bike_station_realtime",
        )


def test_source_snapshot_parquet_binds_physical_rows_to_manifest_count() -> None:
    """self-consistent checksum이어도 counts.kept와 다른 Parquet 행 수를 거부한다."""
    store = MemoryStore()
    silver_bytes = parquet_bytes(pa.table({"stationId": ["ST-1"]}))
    silver_sha256 = sha256_hex(silver_bytes)
    silver_uri = f"s3://fixture/silver/sha256={silver_sha256}.parquet"
    store.put_once(silver_uri, silver_bytes, expected_sha256=silver_sha256)
    manifest = build_source_snapshot_manifest(
        source_id="bike_station_realtime",
        logical_dttm=datetime(2026, 8, 19, tzinfo=UTC),
        revision_no=0,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="config-v1",
        silver_uri=silver_uri,
        silver_byte_sha256=silver_sha256,
        counts=SourceSnapshotCounts(2, 2, 2, 0, 0),
        planned_parts=("page-00001-01000",),
        completed_parts=("page-00001-01000",),
    )
    artifact = store_input_payload(
        store,
        base_uri="s3://fixture/gold",
        publication_key="station_stock",
        role="bike_station_realtime_manifest",
        payload=manifest.canonical_bytes,
        suffix="json",
        require_canonical_json=True,
    )
    snapshot = read_source_snapshot_payload(
        store,
        manifest_artifact=artifact,
        verified_payloads={artifact.uri: manifest.canonical_bytes},
        expected_source_id="bike_station_realtime",
    )

    with pytest.raises(ContractViolation, match="physical row count"):
        source_snapshot_parquet(snapshot)
