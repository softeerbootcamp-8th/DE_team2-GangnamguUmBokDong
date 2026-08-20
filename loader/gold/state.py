"""publisher가 짧은 read transaction과 locked callback에서 읽는 Gold state를 제공한다."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.gold_publication import (
    Dependency,
    ImmutableObjectStore,
    PublicationManifest,
    get_publication_spec,
    parse_publication_manifest,
)
from core.gold_publication.canonical import validate_sha256_hex
from core.gold_publication.errors import ContractViolation, PublicationDependencyError
from psycopg import Connection, Cursor
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

_MANIFEST_URI_SHA = re.compile(r"publication-([0-9a-f]{64})\.json\Z")


@dataclass(frozen=True, slots=True)
class PublicationStateRecord:
    """gold_meta.publication_state의 exact dependency·manifest 필드를 표현한다."""

    publication_key: str
    logical_dttm: datetime
    revision_no: int
    manifest_uri: str
    artifact_set_sha256: str
    input_fingerprint_sha256: str
    published_row_cnt: int

    def __post_init__(self) -> None:
        """DB state 필드를 publication contract 타입과 범위로 고정한다."""
        get_publication_spec(self.publication_key)
        if type(self.logical_dttm) is not datetime:
            raise ContractViolation(
                "publication state logical_dttm은 datetime이어야 합니다."
            )
        if self.logical_dttm.tzinfo is None or self.logical_dttm.utcoffset() is None:
            raise ContractViolation(
                "publication state logical_dttm은 timezone-aware여야 합니다."
            )
        object.__setattr__(self, "logical_dttm", self.logical_dttm.astimezone(UTC))
        if type(self.revision_no) is not int or self.revision_no < 0:
            raise ContractViolation("publication state revision_no가 잘못됐습니다.")
        if (
            type(self.manifest_uri) is not str
            or _MANIFEST_URI_SHA.search(self.manifest_uri) is None
        ):
            raise ContractViolation(
                "publication state manifest URI가 content-addressed 형식이 아닙니다."
            )
        validate_sha256_hex(self.artifact_set_sha256)
        validate_sha256_hex(self.input_fingerprint_sha256)
        if type(self.published_row_cnt) is not int or self.published_row_cnt < 0:
            raise ContractViolation(
                "publication state published_row_cnt가 잘못됐습니다."
            )

    @property
    def dependency(self) -> Dependency:
        """state를 fingerprint에 넣을 exact 6-tuple dependency로 반환한다."""
        return Dependency(
            artifact_set_sha256=self.artifact_set_sha256,
            input_fingerprint_sha256=self.input_fingerprint_sha256,
            logical_dttm=self.logical_dttm,
            manifest_uri=self.manifest_uri,
            publication_key=self.publication_key,
            revision_no=self.revision_no,
        )


def load_dependencies(
    connection: Connection[Any],
    publication_keys: Iterable[str],
) -> tuple[Dependency, ...]:
    """DB publication_state에서 exact dependency 6-tuple을 짧게 읽는다."""
    keys = _dependency_keys(publication_keys)
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "dependency loader는 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            """
            SELECT publication_key,
                   logical_dttm,
                   revision_no,
                   manifest_uri,
                   artifact_set_sha256,
                   input_fingerprint_sha256
              FROM gold_meta.publication_state
             WHERE publication_key = ANY(%s::TEXT[])
             ORDER BY publication_key
            """,
            (list(keys),),
        )
        rows = cursor.fetchall()
    dependencies = tuple(
        Dependency(
            artifact_set_sha256=row[4],
            input_fingerprint_sha256=row[5],
            logical_dttm=row[1],
            manifest_uri=row[3],
            publication_key=row[0],
            revision_no=row[2],
        )
        for row in rows
    )
    actual_keys = tuple(item.publication_key for item in dependencies)
    if actual_keys != keys:
        missing = sorted(set(keys) - set(actual_keys))
        raise PublicationDependencyError(
            f"Gold dependency publication state가 없습니다: {missing}"
        )
    return dependencies


def load_publication_state(
    connection: Connection[Any],
    publication_key: str,
) -> PublicationStateRecord | None:
    """publication key 하나의 현재 state를 짧은 transaction으로 읽는다."""
    get_publication_spec(publication_key)
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "state loader는 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        return publication_state_locked(cursor, publication_key)


def publication_state_locked(
    cursor: Cursor[tuple[Any, ...]],
    publication_key: str,
) -> PublicationStateRecord | None:
    """publication lock 안에서 key의 현재 state를 읽는다."""
    get_publication_spec(publication_key)
    cursor.execute(
        """
        SELECT publication_key,
               logical_dttm,
               revision_no,
               manifest_uri,
               artifact_set_sha256,
               input_fingerprint_sha256,
               published_row_cnt
          FROM gold_meta.publication_state
         WHERE publication_key = %s
        """,
        (publication_key,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    logical = row[1]
    if (
        type(logical) is not datetime
        or logical.tzinfo is None
        or logical.utcoffset() is None
    ):
        raise ContractViolation(
            "publication state logical_dttm이 aware datetime이 아닙니다."
        )
    return PublicationStateRecord(
        publication_key=row[0],
        logical_dttm=logical.astimezone(UTC),
        revision_no=row[2],
        manifest_uri=row[3],
        artifact_set_sha256=row[4],
        input_fingerprint_sha256=row[5],
        published_row_cnt=row[6],
    )


def read_state_manifest(
    object_store: ImmutableObjectStore,
    state: PublicationStateRecord,
) -> PublicationManifest:
    """content-addressed state manifest actual bytes를 읽고 state 필드와 결합한다."""
    if type(state) is not PublicationStateRecord:
        raise ContractViolation("state는 PublicationStateRecord이어야 합니다.")
    match = _MANIFEST_URI_SHA.search(state.manifest_uri)
    if match is None:
        raise ContractViolation(
            "publication state manifest URI가 content-addressed 형식이 아닙니다."
        )
    payload = object_store.read_bytes(
        state.manifest_uri,
        match.group(1),
        require_canonical_json=True,
    )
    manifest = parse_publication_manifest(payload)
    if (
        manifest.publication_key != state.publication_key
        or manifest.logical_dttm != state.logical_dttm
        or manifest.revision_no != state.revision_no
        or manifest.artifact_set_sha256 != state.artifact_set_sha256
        or manifest.input_fingerprint_sha256 != state.input_fingerprint_sha256
        or manifest.published_row_cnt != state.published_row_cnt
    ):
        raise ContractViolation("publication state가 actual manifest bytes와 다릅니다.")
    return manifest


def load_active_weather_grid_ids(
    connection: Connection[Any],
) -> tuple[str, ...]:
    """active station이 참조하는 distinct weather grid ID를 짧게 읽는다."""
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "active grid loader는 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        return active_weather_grid_ids_locked(cursor)


def active_weather_grid_ids_locked(
    cursor: Cursor[tuple[Any, ...]],
) -> tuple[str, ...]:
    """topology shared lock을 획득한 cursor에서 active grid ID를 읽는다."""
    cursor.execute(
        """
        SELECT DISTINCT weather_grid_id
          FROM station
         WHERE is_active
         ORDER BY weather_grid_id
        """
    )
    values = tuple(row[0] for row in cursor.fetchall())
    if any(type(value) is not str or not value for value in values):
        raise ContractViolation(
            "DB active weather grid ID가 nonblank 문자열이 아닙니다."
        )
    return values


def _dependency_keys(publication_keys: Iterable[str]) -> tuple[str, ...]:
    """dependency key iterable을 registry 검증 후 UTF-8 unique 순으로 고정한다."""
    values = tuple(publication_keys)
    if not values or any(type(value) is not str for value in values):
        raise ContractViolation("dependency publication key가 하나 이상 필요합니다.")
    for value in values:
        get_publication_spec(value)
    ordered = tuple(sorted(set(values), key=lambda value: value.encode("utf-8")))
    if len(ordered) != len(values):
        raise ContractViolation("dependency publication key가 중복됩니다.")
    return ordered
