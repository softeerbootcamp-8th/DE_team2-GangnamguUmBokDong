"""Gold publication state와 immutable manifest 결합을 검증한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from core.gold_publication import (
    Artifact,
    ImmutablePutOutcome,
    InputArtifact,
    Parameter,
    build_artifact_set,
    build_input_fingerprint,
    build_publication_manifest,
    sha256_hex,
)
from core.gold_publication.errors import ContractViolation
from gold.state import PublicationStateRecord, read_state_manifest


class _MemoryStore:
    """state manifest exact read에 필요한 최소 immutable store다."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        """URI별 bytes를 복사한다."""
        self.objects = dict(objects)

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """expected checksum과 같은 bytes만 반환한다."""
        del require_canonical_json
        payload = self.objects[uri]
        if sha256_hex(payload) != expected_sha256:
            raise AssertionError("unexpected checksum")
        return payload

    def put_once(
        self,
        uri: str,
        payload: bytes,
        *,
        expected_sha256: str | None = None,
        require_canonical_json: bool = False,
    ) -> ImmutablePutOutcome:
        """이 read-only fixture에서 write를 거부한다."""
        del uri, payload, expected_sha256, require_canonical_json
        raise AssertionError("put_once must not be called")


def _state_and_manifest() -> tuple[PublicationStateRecord, bytes]:
    """서로 exact하게 결합된 weather_grid state와 manifest bytes를 만든다."""
    artifact_set = build_artifact_set(
        (
            Artifact(
                byte_sha256="1" * 64,
                role="weather_grid",
                row_count=34,
                uri="s3://fixture/weather-grid.parquet",
            ),
        )
    )
    fingerprint = build_input_fingerprint(
        "weather_grid",
        (),
        (
            InputArtifact(
                byte_sha256="2" * 64,
                role="weather_grid_seed",
                uri="s3://fixture/weather-grid-seed.yaml",
            ),
        ),
        (
            Parameter("expected_grid_count", "34"),
            Parameter("grid_seed_version", "weather-grid-v1"),
        ),
    )
    manifest = build_publication_manifest(
        publication_key="weather_grid",
        artifact_set=artifact_set,
        input_fingerprint=fingerprint,
        input_fingerprint_uri="s3://fixture/weather-grid-fingerprint.json",
        logical_dttm=datetime(2026, 8, 20, tzinfo=UTC),
        publisher_version="publisher-v1",
        revision_no=0,
        target_row_counts={"weather_grid": 34},
    )
    uri = f"s3://fixture/manifests/publication-{manifest.sha256}.json"
    state = PublicationStateRecord(
        publication_key="weather_grid",
        logical_dttm=manifest.logical_dttm,
        revision_no=manifest.revision_no,
        manifest_uri=uri,
        artifact_set_sha256=manifest.artifact_set_sha256,
        input_fingerprint_sha256=manifest.input_fingerprint_sha256,
        published_row_cnt=manifest.published_row_cnt,
    )
    return state, manifest.canonical_bytes


def test_state_manifest_reads_actual_bytes_and_matches_all_state_fields() -> None:
    """content-addressed actual manifest가 state 6-tuple·row count와 같아야 한다."""
    state, payload = _state_and_manifest()
    manifest = read_state_manifest(_MemoryStore({state.manifest_uri: payload}), state)
    assert manifest.publication_key == "weather_grid"
    assert state.dependency.manifest_uri == state.manifest_uri


def test_state_manifest_rejects_db_field_drift() -> None:
    """DB state의 content hash가 actual manifest와 다르면 prior input을 열지 않는다."""
    state, payload = _state_and_manifest()
    drifted = replace(state, input_fingerprint_sha256="3" * 64)
    with pytest.raises(ContractViolation, match="actual manifest"):
        read_state_manifest(_MemoryStore({state.manifest_uri: payload}), drifted)


def test_state_record_rejects_non_content_addressed_manifest_uri() -> None:
    """state가 mutable 또는 예측 가능한 manifest URI를 authority로 쓰지 못하게 한다."""
    state, _ = _state_and_manifest()
    with pytest.raises(ContractViolation, match="content-addressed"):
        replace(state, manifest_uri="s3://fixture/latest.json")
