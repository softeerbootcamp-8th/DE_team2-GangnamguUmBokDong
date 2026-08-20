"""Inference producer와 Gold loader가 공유하는 immutable revision catalog를 제공한다."""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from urllib.parse import urlsplit

from .gold_publication import (
    ImmutableObjectStore,
    ImmutablePutOutcome,
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    sha256_hex,
)

INFERENCE_REVISION_RECORD_SCHEMA_VERSION = "ml-inference-revision-record-v1"
"""Logical time별 immutable revision slot의 schema version이다."""

_REVISION_RECORD_KEYS = frozenset(
    {
        "logical_dttm",
        "manifest_byte_sha256",
        "manifest_uri",
        "revision_no",
        "schema_version",
    }
)
_REVISION_KEY_PATTERN = re.compile(
    r"logical=(?P<logical>\d{8}T\d{6}\d{6}Z)/revision=(?P<revision>\d{6})\.json\Z"
)
_MANIFEST_PATH_PATTERN = re.compile(
    r"(?:^|/)inference/manifests/sha256=(?P<sha>[0-9a-f]{64})\.json\Z"
)


class InferenceCatalogError(RuntimeError):
    """Inference revision catalog의 계약 또는 저장 동작이 실패했다."""


class InferenceRevisionConflictError(InferenceCatalogError):
    """같은 immutable revision slot을 다른 writer가 먼저 예약했다."""


@dataclass(frozen=True, slots=True)
class InferenceRevisionRecord:
    """Revision number 하나가 예약한 exact manifest identity다."""

    logical_dttm: datetime
    revision_no: int
    manifest_byte_sha256: str
    manifest_uri: str
    schema_version: str = INFERENCE_REVISION_RECORD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Record scalar와 canonical-safe logical time을 검증한다."""
        if type(self.logical_dttm) is not datetime or self.logical_dttm.tzinfo is None:
            raise InferenceCatalogError(
                "revision logical_dttm은 timezone-aware datetime이어야 합니다."
            )
        object.__setattr__(self, "logical_dttm", self.logical_dttm.astimezone(UTC))
        if (
            type(self.revision_no) is not int
            or isinstance(self.revision_no, bool)
            or self.revision_no < 0
            or self.revision_no > 999_999
        ):
            raise InferenceCatalogError(
                "revision_no는 0..999999 exact integer여야 합니다."
            )
        if type(self.manifest_byte_sha256) is not str or re.fullmatch(
            r"[0-9a-f]{64}", self.manifest_byte_sha256
        ) is None:
            raise InferenceCatalogError("manifest SHA-256 형식이 잘못됐습니다.")
        manifest_bucket, manifest_key = _split_exact_s3_uri(self.manifest_uri)
        del manifest_bucket
        match = _MANIFEST_PATH_PATTERN.search(manifest_key)
        if match is None or match.group("sha") != self.manifest_byte_sha256:
            raise InferenceCatalogError(
                "manifest URI는 checksum과 결합된 inference manifest 경로여야 합니다."
            )
        if self.schema_version != INFERENCE_REVISION_RECORD_SCHEMA_VERSION:
            raise InferenceCatalogError("revision record schema version이 다릅니다.")

    @property
    def canonical_bytes(self) -> bytes:
        """Immutable revision slot에 쓸 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(
            {
                "logical_dttm": format_utc_dttm(self.logical_dttm),
                "manifest_byte_sha256": self.manifest_byte_sha256,
                "manifest_uri": self.manifest_uri,
                "revision_no": self.revision_no,
                "schema_version": self.schema_version,
            }
        )


@dataclass(frozen=True, slots=True)
class InferenceCatalogSnapshot:
    """한 logical time의 immutable revision chain snapshot이다."""

    records: tuple[InferenceRevisionRecord, ...]
    latest_logical_dttm: datetime | None = None
    """Deprecated compatibility field이며 bounded catalog는 항상 ``None``을 쓴다."""

    def __post_init__(self) -> None:
        """현재 logical record가 0부터 연속이고 optional metadata가 UTC인지 검증한다."""
        if type(self.records) is not tuple or any(
            type(record) is not InferenceRevisionRecord for record in self.records
        ):
            raise InferenceCatalogError("catalog records는 exact tuple이어야 합니다.")
        revisions = tuple(record.revision_no for record in self.records)
        if revisions != tuple(range(len(self.records))):
            raise InferenceCatalogError(
                "catalog revision은 0부터 빈틈없이 증가해야 합니다."
            )
        if self.records and len({record.logical_dttm for record in self.records}) != 1:
            raise InferenceCatalogError(
                "catalog snapshot records의 logical time이 섞였습니다."
            )
        if self.latest_logical_dttm is not None:
            if (
                type(self.latest_logical_dttm) is not datetime
                or self.latest_logical_dttm.tzinfo is None
            ):
                raise InferenceCatalogError(
                    "catalog latest logical time은 timezone-aware여야 합니다."
                )
            object.__setattr__(
                self,
                "latest_logical_dttm",
                self.latest_logical_dttm.astimezone(UTC),
            )


class InferenceRevisionCatalog(Protocol):
    """Mutable pointer 없이 immutable revision slot만 관리하는 경계다."""

    def snapshot(self, logical_dttm: datetime) -> InferenceCatalogSnapshot:
        """요청 logical의 bounded record chain만 읽는다."""
        ...

    def claim(self, record: InferenceRevisionRecord) -> None:
        """해당 logical/revision의 고정 slot이 비어 있을 때만 예약한다."""
        ...

    def latest_revision(self, logical_dttm: datetime) -> InferenceRevisionRecord | None:
        """해당 logical의 가장 큰 immutable revision record를 반환한다."""
        ...


class InferenceCatalogClient(Protocol):
    """S3 inference catalog discovery가 쓰는 최소 client 계약이다."""

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        """Prefix 아래 exact object key page를 반환한다."""
        ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        """Exact bucket·key의 object와 ContentLength를 반환한다."""
        ...


class S3InferenceRevisionCatalog:
    """S3 LIST와 immutable conditional PUT으로 revision slot을 관리한다."""

    def __init__(
        self,
        client: InferenceCatalogClient,
        object_store: ImmutableObjectStore,
        *,
        bucket: str,
        object_base_uri: str,
    ) -> None:
        """같은 S3 client·store·bucket에 catalog prefix를 고정한다."""
        if not callable(getattr(client, "list_objects_v2", None)) or not callable(
            getattr(client, "get_object", None)
        ):
            raise InferenceCatalogError("catalog client 계약이 잘못됐습니다.")
        if type(bucket) is not str or not bucket or "/" in bucket or any(
            character.isspace() for character in bucket
        ):
            raise InferenceCatalogError("catalog bucket이 잘못됐습니다.")
        base_bucket, prefix = split_inference_object_base_uri(object_base_uri)
        if base_bucket != bucket:
            raise InferenceCatalogError(
                "inference object base와 catalog bucket이 다릅니다."
            )
        self._client = client
        self._bucket = bucket
        self._prefix = f"{prefix}/inference/catalog" if prefix else "inference/catalog"
        self._manifest_prefix = (
            f"{prefix}/inference/manifests/" if prefix else "inference/manifests/"
        )
        self._object_store = object_store

    def snapshot(self, logical_dttm: datetime) -> InferenceCatalogSnapshot:
        """요청 logical prefix만 bounded LIST/GET해 연속 record를 반환한다."""
        requested = _utc_dttm(logical_dttm)
        logical_prefix = f"{self._prefix}/logical={_logical_token(requested)}/"
        keys = self._list_keys(logical_prefix)
        all_records: list[InferenceRevisionRecord] = []
        for key in keys:
            relative = key.removeprefix(f"{self._prefix}/")
            match = _REVISION_KEY_PATTERN.fullmatch(relative)
            if match is None:
                raise InferenceCatalogError(
                    f"알 수 없는 inference catalog object입니다: {key}"
                )
            payload = self._raw_read(key)
            uri = f"s3://{self._bucket}/{key}"
            payload = self._object_store.read_bytes(
                uri,
                sha256_hex(payload),
                require_canonical_json=True,
            )
            record = parse_inference_revision_record(payload)
            if _logical_token(record.logical_dttm) != match.group("logical"):
                raise InferenceCatalogError(
                    "catalog key와 record logical time이 다릅니다."
                )
            if record.revision_no != int(match.group("revision")):
                raise InferenceCatalogError("catalog key와 record revision이 다릅니다.")
            manifest_bucket, _manifest_key = _split_exact_s3_uri(record.manifest_uri)
            if (
                manifest_bucket != self._bucket
                or not _manifest_key.startswith(self._manifest_prefix)
            ):
                raise InferenceCatalogError(
                    "catalog record manifest가 catalog object base 밖입니다."
                )
            all_records.append(record)

        records = tuple(
            sorted(
                (record for record in all_records if record.logical_dttm == requested),
                key=lambda record: record.revision_no,
            )
        )
        return InferenceCatalogSnapshot(records=records)

    def claim(self, record: InferenceRevisionRecord) -> None:
        """고정 revision slot을 If-None-Match로 예약하고 existing은 충돌로 처리한다."""
        manifest_bucket, _manifest_key = _split_exact_s3_uri(record.manifest_uri)
        if (
            manifest_bucket != self._bucket
            or not _manifest_key.startswith(self._manifest_prefix)
        ):
            raise InferenceCatalogError(
                "catalog record manifest가 catalog object base 밖입니다."
            )
        uri = self.record_uri(record.logical_dttm, record.revision_no)
        outcome = self._object_store.put_once(
            uri,
            record.canonical_bytes,
            expected_sha256=sha256_hex(record.canonical_bytes),
            require_canonical_json=True,
        )
        if outcome is not ImmutablePutOutcome.CREATED:
            raise InferenceRevisionConflictError(
                f"inference revision을 다른 writer가 먼저 claim했습니다: {uri}"
            )

    def latest_revision(self, logical_dttm: datetime) -> InferenceRevisionRecord | None:
        """S3 catalog에서 logical별 가장 큰 revision record를 반환한다."""
        records = self.snapshot(logical_dttm).records
        return records[-1] if records else None

    def record_uri(self, logical_dttm: datetime, revision_no: int) -> str:
        """Logical time과 zero-padded revision의 fixed catalog slot URI를 만든다."""
        return (
            f"s3://{self._bucket}/{self._prefix}/logical={_logical_token(logical_dttm)}"
            f"/revision={revision_no:06d}.json"
        )

    def _list_keys(self, prefix: str) -> tuple[str, ...]:
        """Continuation token을 따라 prefix의 모든 exact key를 읽는다."""
        keys: list[str] = []
        continuation: str | None = None
        while True:
            request: dict[str, Any] = {"Bucket": self._bucket, "Prefix": prefix}
            if continuation is not None:
                request["ContinuationToken"] = continuation
            response = self._client.list_objects_v2(**request)
            contents = response.get("Contents", ())
            if type(contents) not in {list, tuple}:
                raise InferenceCatalogError(
                    "inference catalog list Contents가 잘못됐습니다."
                )
            for item in contents:
                if type(item) is not dict or type(item.get("Key")) is not str:
                    raise InferenceCatalogError(
                        "inference catalog list item이 잘못됐습니다."
                    )
                key = item["Key"]
                if not key.startswith(prefix):
                    raise InferenceCatalogError(
                        "inference catalog가 prefix 밖 key를 반환했습니다."
                    )
                keys.append(key)
            truncated = response.get("IsTruncated", False)
            if type(truncated) is not bool:
                raise InferenceCatalogError(
                    "inference catalog IsTruncated가 bool이 아닙니다."
                )
            if not truncated:
                break
            continuation = response.get("NextContinuationToken")
            if type(continuation) is not str or not continuation:
                raise InferenceCatalogError(
                    "inference catalog continuation token이 없습니다."
                )
        if len(keys) != len(set(keys)):
            raise InferenceCatalogError(
                "inference catalog list에 중복 object key가 있습니다."
            )
        return tuple(sorted(keys))

    def _raw_read(self, key: str) -> bytes:
        """Discovery hash를 위해 exact catalog object bytes를 완전히 읽는다."""
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body: Any | None = None
        try:
            body = response["Body"]
            content_length = response["ContentLength"]
            payload = body.read()
        except (KeyError, TypeError, ValueError, OSError) as exc:
            raise InferenceCatalogError(
                "inference catalog object를 완전히 읽을 수 없습니다."
            ) from exc
        finally:
            if body is not None:
                try:
                    body.close()
                except (AttributeError, OSError):
                    pass
        if (
            type(payload) is not bytes
            or type(content_length) is not int
            or content_length < 0
            or len(payload) != content_length
        ):
            raise InferenceCatalogError(
                "inference catalog object ContentLength가 actual bytes와 다릅니다."
            )
        return payload


class InMemoryInferenceRevisionCatalog:
    """단위 테스트와 local composition에 쓰는 thread-safe immutable catalog다."""

    def __init__(self) -> None:
        """빈 record map을 만든다."""
        self._records: dict[tuple[datetime, int], InferenceRevisionRecord] = {}
        self._lock = threading.Lock()

    def snapshot(self, logical_dttm: datetime) -> InferenceCatalogSnapshot:
        """Lock 아래 현재 logical revision chain만 일관되게 복사한다."""
        logical = _utc_dttm(logical_dttm)
        with self._lock:
            values = tuple(self._records.values())
        records = tuple(
            sorted(
                (record for record in values if record.logical_dttm == logical),
                key=lambda record: record.revision_no,
            )
        )
        return InferenceCatalogSnapshot(records=records)

    def claim(self, record: InferenceRevisionRecord) -> None:
        """같은 logical/revision slot의 두 번째 writer를 항상 거부한다."""
        key = (record.logical_dttm, record.revision_no)
        with self._lock:
            if key in self._records:
                raise InferenceRevisionConflictError(
                    f"inference revision을 다른 writer가 먼저 claim했습니다: {key}"
                )
            self._records[key] = record

    def latest_revision(self, logical_dttm: datetime) -> InferenceRevisionRecord | None:
        """Lock-consistent snapshot에서 logical별 가장 큰 revision을 반환한다."""
        records = self.snapshot(logical_dttm).records
        return records[-1] if records else None


def parse_inference_revision_record(payload: bytes) -> InferenceRevisionRecord:
    """Canonical revision record bytes를 exact-key typed 값으로 파싱한다."""
    if type(payload) is not bytes:
        raise TypeError("revision record payload는 exact bytes여야 합니다.")
    document = parse_canonical_json(payload)
    if type(document) is not dict or frozenset(document) != _REVISION_RECORD_KEYS:
        raise InferenceCatalogError("revision record key 집합이 잘못됐습니다.")
    try:
        logical = datetime.strptime(
            cast(str, document["logical_dttm"]),
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ).replace(tzinfo=UTC)
        return InferenceRevisionRecord(
            logical_dttm=logical,
            revision_no=cast(int, document["revision_no"]),
            manifest_byte_sha256=cast(str, document["manifest_byte_sha256"]),
            manifest_uri=cast(str, document["manifest_uri"]),
            schema_version=cast(str, document["schema_version"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise InferenceCatalogError("revision record scalar가 잘못됐습니다.") from exc


def split_inference_object_base_uri(uri: str) -> tuple[str, str]:
    """Prefix를 가리키는 query/fragment 없는 S3 base URI를 검증한다."""
    if type(uri) is not str:
        raise TypeError("object_base_uri는 string이어야 합니다.")
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise InferenceCatalogError(
            "object_base_uri는 query/fragment 없는 s3:// URI여야 합니다."
        )
    prefix = parsed.path.lstrip("/").rstrip("/")
    return parsed.netloc, prefix


def _split_exact_s3_uri(uri: object) -> tuple[str, str]:
    """Query·fragment 없는 exact S3 object URI를 bucket·key로 나눈다."""
    if type(uri) is not str:
        raise InferenceCatalogError("manifest URI는 exact string이어야 합니다.")
    parsed = urlsplit(uri)
    key = parsed.path.lstrip("/")
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not key
        or parsed.query
        or parsed.fragment
        or "//" in key
        or any(part in {"", ".", ".."} for part in key.split("/"))
    ):
        raise InferenceCatalogError(
            "manifest URI는 query/fragment 없는 exact s3:// object여야 합니다."
        )
    return parsed.netloc, key


def _logical_token(value: datetime) -> str:
    """UTC logical time을 catalog path의 고정 폭 token으로 만든다."""
    return _utc_dttm(value).strftime("%Y%m%dT%H%M%S%fZ")


def _utc_dttm(value: datetime) -> datetime:
    """Timezone-aware datetime을 UTC로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise InferenceCatalogError(
            "logical_dttm은 timezone-aware datetime이어야 합니다."
        )
    return value.astimezone(UTC)


__all__ = [
    "INFERENCE_REVISION_RECORD_SCHEMA_VERSION",
    "InMemoryInferenceRevisionCatalog",
    "InferenceCatalogError",
    "InferenceCatalogClient",
    "InferenceCatalogSnapshot",
    "InferenceRevisionCatalog",
    "InferenceRevisionConflictError",
    "InferenceRevisionRecord",
    "S3InferenceRevisionCatalog",
    "parse_inference_revision_record",
    "split_inference_object_base_uri",
]
