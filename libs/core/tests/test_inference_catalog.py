"""공용 inference revision catalog의 exact record와 S3 경계를 검증한다."""

from datetime import UTC, datetime

import boto3
import pytest
from core.gold_publication import S3ImmutableObjectStore, canonical_json_bytes
from core.inference_catalog import (
    InMemoryInferenceRevisionCatalog,
    InferenceCatalogError,
    InferenceCatalogSnapshot,
    InferenceRevisionConflictError,
    InferenceRevisionRecord,
    S3InferenceRevisionCatalog,
    parse_inference_revision_record,
)
from moto import mock_aws

LOGICAL = datetime(2026, 8, 20, 1, 25, tzinfo=UTC)
SHA = "a" * 64
MANIFEST_URI = f"s3://fixture/authority/inference/manifests/sha256={SHA}.json"


def _record(*, revision_no: int = 0, bucket: str = "fixture") -> InferenceRevisionRecord:
    """고정 identity의 valid revision record를 만든다."""
    return InferenceRevisionRecord(
        logical_dttm=LOGICAL,
        revision_no=revision_no,
        manifest_byte_sha256=SHA,
        manifest_uri=(
            f"s3://{bucket}/authority/inference/manifests/sha256={SHA}.json"
        ),
    )


def test_record_round_trip_and_in_memory_revision_chain() -> None:
    """Record canonical bytes와 logical별 0..n snapshot을 공용 구현으로 왕복한다."""
    catalog = InMemoryInferenceRevisionCatalog()
    first = _record()
    second = _record(revision_no=1)

    catalog.claim(first)
    catalog.claim(second)

    assert parse_inference_revision_record(first.canonical_bytes) == first
    assert catalog.snapshot(LOGICAL) == InferenceCatalogSnapshot(
        records=(first, second),
    )
    assert catalog.latest_revision(LOGICAL) == second
    with pytest.raises(InferenceRevisionConflictError, match="먼저 claim"):
        catalog.claim(second)


@pytest.mark.parametrize("revision_no", [-1, 1_000_000, True])
def test_record_revision_matches_six_digit_key_space(revision_no: object) -> None:
    """Record revision을 canonical six-digit catalog key 범위에 고정한다."""
    with pytest.raises(InferenceCatalogError, match="0..999999"):
        InferenceRevisionRecord(
            logical_dttm=LOGICAL,
            revision_no=revision_no,  # type: ignore[arg-type]
            manifest_byte_sha256=SHA,
            manifest_uri=MANIFEST_URI,
        )


@pytest.mark.parametrize(
    ("uri", "sha"),
    [
        ("https://fixture/authority/inference/manifests/sha256=" + SHA + ".json", SHA),
        (MANIFEST_URI + "?version=1", SHA),
        ("s3://fixture/authority/inference/manifests/latest.json", SHA),
        (
            "s3://fixture/authority/inference/manifests/sha256=" + "b" * 64 + ".json",
            SHA,
        ),
    ],
)
def test_record_rejects_unbound_manifest_uri(uri: str, sha: str) -> None:
    """Record가 exact S3 content-addressed inference manifest 외 URI를 거부한다."""
    with pytest.raises(InferenceCatalogError, match="manifest URI"):
        InferenceRevisionRecord(
            logical_dttm=LOGICAL,
            revision_no=0,
            manifest_byte_sha256=sha,
            manifest_uri=uri,
        )


def test_parser_rejects_non_bytes_and_unknown_keys() -> None:
    """Canonical parser가 bytes subclass와 schema 확장을 묵인하지 않는다."""

    class _Bytes(bytes):
        """Exact-builtin 검증용 bytes subclass다."""

    with pytest.raises(TypeError, match="exact bytes"):
        parse_inference_revision_record(_Bytes(_record().canonical_bytes))
    with pytest.raises(InferenceCatalogError, match="key 집합"):
        parse_inference_revision_record(canonical_json_bytes({"unexpected": True}))


def test_s3_catalog_uses_injected_backend_and_rejects_cross_bucket_record() -> None:
    """LIST·GET·CAS가 같은 주입 client/store/bucket에서만 실행된다."""
    bucket = "inference-catalog-core-fixture"
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=bucket)
        store = S3ImmutableObjectStore(client)
        catalog = S3InferenceRevisionCatalog(
            client,
            store,
            bucket=bucket,
            object_base_uri=f"s3://{bucket}/authority",
        )
        record = _record(bucket=bucket)

        catalog.claim(record)

        assert catalog.snapshot(LOGICAL).records == (record,)
        assert catalog.latest_revision(LOGICAL) == record
        with pytest.raises(InferenceCatalogError, match="object base"):
            catalog.claim(_record(bucket="other-bucket"))
        with pytest.raises(InferenceCatalogError, match="object base"):
            catalog.claim(
                InferenceRevisionRecord(
                    logical_dttm=LOGICAL,
                    revision_no=1,
                    manifest_byte_sha256=SHA,
                    manifest_uri=(
                        f"s3://{bucket}/other-prefix/inference/manifests/"
                        f"sha256={SHA}.json"
                    ),
                )
            )


def test_s3_catalog_rejects_base_bucket_mismatch() -> None:
    """Object base와 explicit catalog bucket이 갈라지면 construction부터 실패한다."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        store = S3ImmutableObjectStore(client)
        with pytest.raises(InferenceCatalogError, match="bucket"):
            S3InferenceRevisionCatalog(
                client,
                store,
                bucket="one-bucket",
                object_base_uri="s3://other-bucket/authority",
            )


def test_s3_snapshot_does_not_read_another_logical_prefix() -> None:
    """한 tick 조회가 과거·미래 logical catalog object를 LIST/GET하지 않는다."""
    bucket = "inference-catalog-bounded-fixture"
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=bucket)
        client.put_object(
            Bucket=bucket,
            Key=(
                "authority/inference/catalog/"
                "logical=20260820T013000000000Z/revision=000000.json"
            ),
            Body=b"not-canonical-json",
        )
        store = S3ImmutableObjectStore(client)
        catalog = S3InferenceRevisionCatalog(
            client,
            store,
            bucket=bucket,
            object_base_uri=f"s3://{bucket}/authority",
        )

        assert catalog.snapshot(LOGICAL) == InferenceCatalogSnapshot(records=())


def test_s3_catalog_requires_exact_content_length_and_closes_body() -> None:
    """Discovery GET의 scalar가 잘못돼도 response body를 닫고 fail closed한다."""

    class _Body:
        """Close 호출을 관측하는 최소 streaming body다."""

        closed = False

        def read(self) -> bytes:
            """고정 invalid payload를 반환한다."""
            return b"{}"

        def close(self) -> None:
            """Close 호출을 기록한다."""
            self.closed = True

    body = _Body()
    key = (
        "authority/inference/catalog/"
        "logical=20260820T012500000000Z/revision=000000.json"
    )

    class _Client:
        """잘못된 string ContentLength를 반환하는 catalog client다."""

        def list_objects_v2(self, **_kwargs):
            """현재 logical key 하나를 반환한다."""
            return {"Contents": [{"Key": key}], "IsTruncated": False}

        def get_object(self, **_kwargs):
            """Exact int가 아닌 ContentLength를 반환한다."""
            return {"Body": body, "ContentLength": "2"}

    client = _Client()
    catalog = S3InferenceRevisionCatalog(
        client,
        S3ImmutableObjectStore(client),  # type: ignore[arg-type]
        bucket="fixture",
        object_base_uri="s3://fixture/authority",
    )

    with pytest.raises(InferenceCatalogError, match="ContentLength"):
        catalog.snapshot(LOGICAL)
    assert body.closed is True
