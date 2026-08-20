"""immutable source snapshot authority manifest를 S3 revision chain에서 탐색한다."""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from core.gold_publication import (
    ContractViolation,
    ImmutableObjectStore,
    format_utc_dttm,
    sha256_hex,
)
from core.source_snapshot import (
    SourceSnapshotManifest,
    parse_source_snapshot_manifest,
)

_SOURCE_ID = re.compile(r"[a-z][a-z0-9_]*\Z")
_REVISION_SUFFIX = re.compile(r"revision=([0-9]{10})\.json\Z")
_AUTHORITY_KEY = re.compile(
    r"source_snapshot_manifest/(?P<source>[a-z][a-z0-9_]*)/"
    r"dt=(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})/"
    r"hh=(?P<hour>[0-9]{2})/"
    r"logical=(?P<logical>[0-9]{8}T[0-9]{12}Z)/"
    r"revision=(?P<revision>[0-9]{10})\.json\Z"
)


class SourceCatalogClient(Protocol):
    """source authority 탐색이 쓰는 최소 S3 client 계약이다."""

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        """prefix의 object key page를 반환한다."""
        ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        """exact bucket·key object를 반환한다."""
        ...


@dataclass(frozen=True, slots=True)
class SourceManifestArtifact:
    """actual immutable manifest bytes와 parsed source authority를 결합한다."""

    manifest: SourceSnapshotManifest
    uri: str
    byte_sha256: str
    payload: bytes

    def __post_init__(self) -> None:
        """manifest·URI·checksum·payload 결합을 검증한다."""
        if type(self.manifest) is not SourceSnapshotManifest:
            raise ContractViolation("source artifact manifest type이 잘못됐습니다.")
        if type(self.uri) is not str or not self.uri:
            raise ContractViolation("source artifact URI가 필요합니다.")
        if type(self.payload) is not bytes:
            raise ContractViolation("source artifact payload는 bytes여야 합니다.")
        if sha256_hex(self.payload) != self.byte_sha256:
            raise ContractViolation(
                "source artifact manifest checksum이 payload와 다릅니다."
            )
        if parse_source_snapshot_manifest(self.payload) != self.manifest:
            raise ContractViolation(
                "source artifact parsed manifest가 actual bytes와 다릅니다."
            )


class S3SourceSnapshotCatalog:
    """S3 authority namespace의 exact manifest revision chain을 검증한다."""

    def __init__(
        self,
        client: SourceCatalogClient,
        object_store: ImmutableObjectStore,
        *,
        bucket: str,
    ) -> None:
        """list/get client·immutable store·bucket을 고정한다."""
        if (
            not bucket
            or any(character.isspace() for character in bucket)
            or "/" in bucket
        ):
            raise ContractViolation("source catalog bucket이 잘못됐습니다.")
        self._client = client
        self._object_store = object_store
        self._bucket = bucket

    def list_source(self, source_id: str) -> tuple[SourceManifestArtifact, ...]:
        """source의 모든 authority manifest를 logical·revision 오름차순으로 읽는다."""
        source = _validated_source_id(source_id)
        prefix = f"source_snapshot_manifest/{source}/"
        keys = self._list_keys(prefix)
        artifacts = tuple(self._read_artifact(source, key) for key in keys)
        ordered = tuple(
            sorted(
                artifacts,
                key=lambda artifact: (
                    artifact.manifest.logical_dttm,
                    artifact.manifest.revision_no,
                ),
            )
        )
        self._validate_revision_chains(source, ordered)
        return ordered

    def latest_at_or_before(
        self,
        source_id: str,
        logical_dttm: datetime,
        *,
        lookback: timedelta,
    ) -> SourceManifestArtifact:
        """bounded hour prefix에서 기준 이전 최신 window와 correction을 반환한다."""
        source = _validated_source_id(source_id)
        cutoff = _utc_dttm(logical_dttm, "logical_dttm")
        lower = _lookback_lower_bound(cutoff, lookback)
        for hour in _hours_descending(cutoff, lower):
            keys = self._list_keys(_hour_prefix(source, hour))
            identities = tuple(_key_identity(key, source) for key in keys)
            eligible = tuple(
                logical
                for logical, _revision in identities
                if lower <= logical <= cutoff
            )
            if eligible:
                selected = max(eligible)
                return self._latest_revision(source, selected, keys)
        raise ContractViolation(
            f"{source} source authority가 bounded lookback 안에 없습니다."
        )

    def exact_window(
        self,
        source_id: str,
        logical_dttm: datetime,
    ) -> SourceManifestArtifact:
        """exact logical window의 최대 correction revision을 반환한다."""
        source = _validated_source_id(source_id)
        logical = _utc_dttm(logical_dttm, "logical_dttm")
        keys = self._list_keys(_logical_prefix(source, logical))
        if not keys:
            raise ContractViolation(
                f"{source} exact source authority window가 없습니다: "
                f"{format_utc_dttm(logical)}"
            )
        return self._latest_revision(source, logical, keys)

    def recent_windows(
        self,
        source_id: str,
        logical_dttm: datetime,
        *,
        limit: int,
        lookback: timedelta,
    ) -> tuple[SourceManifestArtifact, ...]:
        """bounded hour prefix에서 최신 distinct window의 correction만 반환한다."""
        if type(limit) is not int or limit <= 0:
            raise ContractViolation(
                "source recent window limit은 양의 integer여야 합니다."
            )
        source = _validated_source_id(source_id)
        cutoff = _utc_dttm(logical_dttm, "logical_dttm")
        lower = _lookback_lower_bound(cutoff, lookback)
        selected: list[SourceManifestArtifact] = []
        for hour in _hours_descending(cutoff, lower):
            keys = self._list_keys(_hour_prefix(source, hour))
            identities = tuple(_key_identity(key, source) for key in keys)
            logicals = sorted(
                {
                    logical
                    for logical, _revision in identities
                    if lower <= logical <= cutoff
                },
                reverse=True,
            )
            for logical in logicals:
                selected.append(self._latest_revision(source, logical, keys))
                if len(selected) == limit:
                    return tuple(selected)
        if not selected:
            raise ContractViolation(
                f"{source} recent source authority window가 bounded lookback 안에 없습니다."
            )
        first_key = self._first_key(f"source_snapshot_manifest/{source}/")
        if first_key is not None and _key_identity(first_key, source)[0] < lower:
            raise ContractViolation(
                "source recent window lookback 밖에 더 오래된 authority가 있어 "
                "latest window set 완전성을 증명할 수 없습니다."
            )
        return tuple(selected)

    def _first_key(self, prefix: str) -> str | None:
        """prefix의 lexicographic 첫 authority key를 GET 없이 반환한다."""
        response = self._client.list_objects_v2(
            Bucket=self._bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
        contents = response.get("Contents", ())
        if type(contents) not in {list, tuple}:
            raise ContractViolation("source catalog first-key Contents가 잘못됐습니다.")
        if not contents:
            return None
        item = contents[0]
        if type(item) is not dict or type(item.get("Key")) is not str:
            raise ContractViolation("source catalog first-key item이 잘못됐습니다.")
        key = item["Key"]
        if not key.startswith(prefix):
            raise ContractViolation("source catalog first-key가 prefix 밖입니다.")
        return key

    def _latest_revision(
        self,
        source_id: str,
        logical_dttm: datetime,
        keys: tuple[str, ...],
    ) -> SourceManifestArtifact:
        """list된 한 logical window의 0..n correction chain에서 최대 revision을 연다."""
        matching = tuple(
            key for key in keys if _key_identity(key, source_id)[0] == logical_dttm
        )
        if not matching:
            raise ContractViolation("source logical window correction key가 없습니다.")
        identities = tuple(_key_identity(key, source_id) for key in matching)
        revisions = tuple(revision for _logical, revision in identities)
        if revisions != tuple(range(len(revisions))):
            raise ContractViolation(
                "source authority revision chain이 0부터 빈틈없이 증가하지 "
                f"않습니다: {source_id}@{format_utc_dttm(logical_dttm)}={revisions}"
            )
        return self._read_artifact(source_id, matching[-1])

    def _list_keys(self, prefix: str) -> tuple[str, ...]:
        """ContinuationToken을 따라 prefix의 모든 exact object key를 읽는다."""
        keys: list[str] = []
        continuation: str | None = None
        while True:
            request: dict[str, Any] = {"Bucket": self._bucket, "Prefix": prefix}
            if continuation is not None:
                request["ContinuationToken"] = continuation
            response = self._client.list_objects_v2(**request)
            contents = response.get("Contents", ())
            if type(contents) not in {list, tuple}:
                raise ContractViolation(
                    "source catalog list response Contents가 잘못됐습니다."
                )
            for item in contents:
                if type(item) is not dict or type(item.get("Key")) is not str:
                    raise ContractViolation("source catalog list item이 잘못됐습니다.")
                key = item["Key"]
                if not key.startswith(prefix):
                    raise ContractViolation(
                        "source catalog가 prefix 밖 key를 반환했습니다."
                    )
                keys.append(key)
            truncated = response.get("IsTruncated", False)
            if type(truncated) is not bool:
                raise ContractViolation("source catalog IsTruncated가 bool이 아닙니다.")
            if not truncated:
                break
            continuation = response.get("NextContinuationToken")
            if type(continuation) is not str or not continuation:
                raise ContractViolation("source catalog continuation token이 없습니다.")
        if len(keys) != len(set(keys)):
            raise ContractViolation("source catalog list에 중복 object key가 있습니다.")
        return tuple(sorted(keys))

    def _read_artifact(self, source_id: str, key: str) -> SourceManifestArtifact:
        """list된 key를 first-read hash 후 immutable exact read로 재검증한다."""
        revision_match = _REVISION_SUFFIX.search(key)
        if revision_match is None:
            raise ContractViolation(
                f"source authority key revision 형식이 잘못됐습니다: {key}"
            )
        first_payload = self._raw_read(key)
        checksum = sha256_hex(first_payload)
        uri = f"s3://{self._bucket}/{key}"
        payload = self._object_store.read_bytes(
            uri,
            checksum,
            require_canonical_json=True,
        )
        manifest = parse_source_snapshot_manifest(payload)
        if manifest.source_id != source_id:
            raise ContractViolation(
                "source authority key와 manifest source_id가 다릅니다."
            )
        expected_key = _manifest_key(manifest)
        if key != expected_key:
            raise ContractViolation(
                f"source authority key가 manifest identity와 다릅니다: {key}"
            )
        if manifest.revision_no != int(revision_match.group(1)):
            raise ContractViolation(
                "source authority revision path가 manifest와 다릅니다."
            )
        return SourceManifestArtifact(manifest, uri, checksum, payload)

    def _raw_read(self, key: str) -> bytes:
        """discovery hash 계산을 위해 exact key의 전체 bytes를 한 번 읽는다."""
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body: Any | None = None
        try:
            body = response["Body"]
            content_length = int(response["ContentLength"])
            payload = body.read()
        except (KeyError, TypeError, ValueError, OSError) as exc:
            raise ContractViolation(
                "source authority object를 완전히 읽을 수 없습니다."
            ) from exc
        finally:
            if body is not None:
                try:
                    body.close()
                except (AttributeError, OSError):
                    pass
        if (
            type(payload) is not bytes
            or content_length < 0
            or len(payload) != content_length
        ):
            raise ContractViolation(
                "source authority object ContentLength가 actual bytes와 다릅니다."
            )
        return payload

    @staticmethod
    def _validate_revision_chains(
        source_id: str,
        artifacts: tuple[SourceManifestArtifact, ...],
    ) -> None:
        """logical window별 revision이 0부터 빈틈없이 증가하는지 검증한다."""
        by_logical: dict[datetime, list[int]] = {}
        for artifact in artifacts:
            if artifact.manifest.source_id != source_id:
                raise ContractViolation(
                    "source revision chain에 다른 source가 섞였습니다."
                )
            by_logical.setdefault(artifact.manifest.logical_dttm, []).append(
                artifact.manifest.revision_no
            )
        for logical, revisions in by_logical.items():
            if revisions != list(range(len(revisions))):
                raise ContractViolation(
                    "source authority revision chain이 0부터 빈틈없이 증가하지 "
                    f"않습니다: {source_id}@{format_utc_dttm(logical)}={revisions}"
                )


def _manifest_key(manifest: SourceSnapshotManifest) -> str:
    """source manifest identity의 canonical authority S3 key를 반환한다."""
    logical = manifest.logical_dttm.astimezone(UTC)
    return (
        f"source_snapshot_manifest/{manifest.source_id}/"
        f"dt={logical:%Y-%m-%d}/hh={logical:%H}/"
        f"logical={logical:%Y%m%dT%H%M%S}{logical.microsecond:06d}Z/"
        f"revision={manifest.revision_no:010d}.json"
    )


def _hour_prefix(source_id: str, value: datetime) -> str:
    """UTC hour 하나에 한정된 authority list prefix를 반환한다."""
    utc = value.astimezone(UTC)
    return f"source_snapshot_manifest/{source_id}/dt={utc:%Y-%m-%d}/hh={utc:%H}/"


def _logical_prefix(source_id: str, value: datetime) -> str:
    """logical window 하나에 한정된 authority list prefix를 반환한다."""
    utc = value.astimezone(UTC)
    return (
        f"{_hour_prefix(source_id, utc)}"
        f"logical={utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z/"
    )


def _key_identity(key: str, expected_source_id: str) -> tuple[datetime, int]:
    """authority key path를 GET 없이 logical time과 revision으로 검증·파싱한다."""
    if type(key) is not str:
        raise ContractViolation("source authority key는 문자열이어야 합니다.")
    match = _AUTHORITY_KEY.fullmatch(key)
    if match is None or match.group("source") != expected_source_id:
        raise ContractViolation(f"source authority key 형식이 잘못됐습니다: {key}")
    try:
        logical = datetime.strptime(
            match.group("logical"),
            "%Y%m%dT%H%M%S%fZ",
        ).replace(tzinfo=UTC)
    except ValueError as exc:
        raise ContractViolation(
            "source authority key logical time이 잘못됐습니다."
        ) from exc
    revision = int(match.group("revision"))
    expected = (
        f"source_snapshot_manifest/{expected_source_id}/"
        f"dt={logical:%Y-%m-%d}/hh={logical:%H}/"
        f"logical={logical:%Y%m%dT%H%M%S}{logical.microsecond:06d}Z/"
        f"revision={revision:010d}.json"
    )
    if key != expected:
        raise ContractViolation(
            "source authority key path와 logical identity가 다릅니다."
        )
    return logical, revision


def _lookback_lower_bound(cutoff: datetime, lookback: timedelta) -> datetime:
    """명시적 양수 lookback을 UTC lower bound로 변환한다."""
    if type(lookback) is not timedelta or lookback <= timedelta(0):
        raise ContractViolation("source catalog lookback은 양수 timedelta여야 합니다.")
    try:
        return cutoff - lookback
    except OverflowError as exc:
        raise ContractViolation("source catalog lookback 범위가 너무 큽니다.") from exc


def _hours_descending(cutoff: datetime, lower: datetime) -> Iterator[datetime]:
    """cutoff부터 lower가 포함된 UTC hour bucket을 내림차순 생성한다."""
    current = cutoff.replace(minute=0, second=0, microsecond=0)
    final = lower.replace(minute=0, second=0, microsecond=0)
    while current >= final:
        yield current
        current -= timedelta(hours=1)


def _validated_source_id(value: str) -> str:
    """source ID를 authority path에 안전한 canonical 형식으로 검증한다."""
    if type(value) is not str or _SOURCE_ID.fullmatch(value) is None:
        raise ContractViolation(
            "source_id가 canonical lowercase snake case가 아닙니다."
        )
    return value


def _utc_dttm(value: Any, name: str) -> datetime:
    """exact aware datetime을 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise ContractViolation(f"{name}은 datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)
