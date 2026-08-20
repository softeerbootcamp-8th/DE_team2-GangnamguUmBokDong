"""Gold publication key와 logical time별 correction revision을 배정한다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.gold_publication import format_utc_dttm, get_publication_spec
from core.gold_publication.canonical import validate_sha256_hex
from core.gold_publication.errors import ContractViolation, PublicationTransactionError
from psycopg import Connection
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

_MAX_DATABASE_REVISION = 2_147_483_647


@dataclass(frozen=True, slots=True)
class PublicationCandidate:
    """revision 배정에 필요한 incoming publication content를 표현한다."""

    publication_key: str
    logical_dttm: datetime
    artifact_set_sha256: str
    input_fingerprint_sha256: str
    published_row_cnt: int

    def __post_init__(self) -> None:
        """candidate key·time·hash·row count를 검증하고 UTC로 정규화한다."""
        get_publication_spec(self.publication_key)
        if type(self.logical_dttm) is not datetime:
            raise ContractViolation("candidate logical_dttm은 datetime이어야 합니다.")
        if self.logical_dttm.tzinfo is None or self.logical_dttm.utcoffset() is None:
            raise ContractViolation(
                "candidate logical_dttm은 timezone-aware여야 합니다."
            )
        normalized = self.logical_dttm.astimezone(UTC)
        format_utc_dttm(normalized)
        object.__setattr__(self, "logical_dttm", normalized)
        validate_sha256_hex(self.artifact_set_sha256)
        validate_sha256_hex(self.input_fingerprint_sha256)
        if type(self.published_row_cnt) is not int or self.published_row_cnt < 0:
            raise ContractViolation(
                "candidate published_row_cnt는 0 이상 integer여야 합니다."
            )


@dataclass(frozen=True, slots=True)
class CurrentPublication:
    """revision 비교에 필요한 현재 publication_state 일부를 표현한다."""

    logical_dttm: datetime
    revision_no: int
    artifact_set_sha256: str
    input_fingerprint_sha256: str
    published_row_cnt: int

    def __post_init__(self) -> None:
        """DB에서 읽은 state 값의 타입과 범위를 검증한다."""
        if type(self.logical_dttm) is not datetime:
            raise ContractViolation("current logical_dttm은 datetime이어야 합니다.")
        if self.logical_dttm.tzinfo is None or self.logical_dttm.utcoffset() is None:
            raise ContractViolation("current logical_dttm은 timezone-aware여야 합니다.")
        object.__setattr__(self, "logical_dttm", self.logical_dttm.astimezone(UTC))
        if (
            type(self.revision_no) is not int
            or not 0 <= self.revision_no <= _MAX_DATABASE_REVISION
        ):
            raise ContractViolation(
                "current revision_no가 PostgreSQL INTEGER 범위 밖입니다."
            )
        validate_sha256_hex(self.artifact_set_sha256)
        validate_sha256_hex(self.input_fingerprint_sha256)
        if type(self.published_row_cnt) is not int or self.published_row_cnt < 0:
            raise ContractViolation(
                "current published_row_cnt는 0 이상 integer여야 합니다."
            )


def choose_revision(
    candidate: PublicationCandidate,
    current: CurrentPublication | None,
) -> int:
    """현재 state와 candidate를 비교해 replay 또는 correction revision을 정한다."""
    if type(candidate) is not PublicationCandidate:
        raise ContractViolation("candidate는 PublicationCandidate여야 합니다.")
    if current is not None and type(current) is not CurrentPublication:
        raise ContractViolation("current는 CurrentPublication 또는 None이어야 합니다.")
    if current is None or candidate.logical_dttm != current.logical_dttm:
        return 0

    same_content = (
        candidate.artifact_set_sha256 == current.artifact_set_sha256
        and candidate.input_fingerprint_sha256 == current.input_fingerprint_sha256
        and candidate.published_row_cnt == current.published_row_cnt
    )
    if same_content:
        return current.revision_no
    if current.revision_no == _MAX_DATABASE_REVISION:
        raise ContractViolation(
            "publication correction revision이 INTEGER 한계에 도달했습니다."
        )
    return current.revision_no + 1


def allocate_revision(
    connection: Connection[Any],
    candidate: PublicationCandidate,
) -> int:
    """현재 publication_state를 짧게 읽어 key/logical별 revision을 배정한다.

    state read와 실제 claim 사이 경쟁은 공통 transaction 실행기의 same-version conflict로
    fail-closed 된다. upstream revision은 입력으로 받거나 복사하지 않는다.
    """
    if type(candidate) is not PublicationCandidate:
        raise ContractViolation("candidate는 PublicationCandidate여야 합니다.")
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise PublicationTransactionError(
            "revision allocator는 transaction이 시작되지 않은 연결이 필요합니다."
        )

    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            """
            SELECT logical_dttm,
                   revision_no,
                   artifact_set_sha256,
                   input_fingerprint_sha256,
                   published_row_cnt
              FROM gold_meta.publication_state
             WHERE publication_key = %s
            """,
            (candidate.publication_key,),
        )
        row = cursor.fetchone()

    current = (
        None
        if row is None
        else CurrentPublication(
            logical_dttm=row[0],
            revision_no=row[1],
            artifact_set_sha256=row[2],
            input_fingerprint_sha256=row[3],
            published_row_cnt=row[4],
        )
    )
    return choose_revision(candidate, current)
