"""Gold target 변경과 publication state 전진을 묶는 transaction 실행기를 제공한다."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from psycopg import Connection, Cursor
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .contract import (
    Dependency,
    EmptyPolicy,
    PublicationManifest,
    get_publication_spec,
    validate_publication_manifest,
)
from .errors import (
    ContractViolation,
    PublicationConflictError,
    PublicationDependencyError,
    PublicationEmptyError,
    PublicationTimeError,
    PublicationTransactionError,
)
from .evidence import (
    PreparedPublication,
    VerifiedPublicationEvidence,
    _snapshot_verified_publication_evidence,
)

_MAX_FUTURE_SKEW = timedelta(minutes=5)


class LockScope(StrEnum):
    """publication transaction이 먼저 획득할 교차 dataset lock 범위를 나타낸다."""

    NONE = "none"
    TOPOLOGY_SHARED = "topology_shared"
    TOPOLOGY_EXCLUSIVE = "topology_exclusive"
    TOPOLOGY_SHARED_ROUTE = "topology_shared_route"


class PublicationOutcome(StrEnum):
    """publication 실행 결과를 stale·replay·publish로 구분한다."""

    STALE = "stale"
    EXACT_REPLAY = "exact_replay"
    PUBLISHED = "published"


@dataclass(frozen=True, slots=True)
class PublicationResult:
    """여러 key를 함께 실행한 publication 결과와 정렬된 key를 반환한다."""

    outcome: PublicationOutcome
    publication_keys: tuple[str, ...]


LockedValidator = Callable[
    [Cursor[tuple[Any, ...]], tuple[VerifiedPublicationEvidence, ...]],
    None,
]
ConditionalEmptyValidator = Callable[
    [Cursor[tuple[Any, ...]], VerifiedPublicationEvidence],
    bool,
]
MutationCallback = Callable[
    [Cursor[tuple[Any, ...]], tuple[VerifiedPublicationEvidence, ...]],
    None,
]


@dataclass(frozen=True, slots=True)
class _PublicationState:
    """DB publication_state 한 행을 비교 가능한 값으로 보관한다."""

    publication_key: str
    logical_dttm: datetime
    revision_no: int
    manifest_uri: str
    artifact_set_sha256: str
    input_fingerprint_sha256: str
    published_row_cnt: int


_LOCK_SCOPE_BY_KEY = {
    "weather_grid": LockScope.TOPOLOGY_EXCLUSIVE,
    "dispatch_center": LockScope.TOPOLOGY_EXCLUSIVE,
    "station": LockScope.TOPOLOGY_EXCLUSIVE,
    "station_stock": LockScope.NONE,
    "station_demand_forecast": LockScope.TOPOLOGY_SHARED,
    "weather_forecast": LockScope.TOPOLOGY_SHARED,
    "event:cultural_event": LockScope.NONE,
    "event:performance_event": LockScope.NONE,
    "station_urgency": LockScope.TOPOLOGY_SHARED,
    "rebalance_route": LockScope.TOPOLOGY_SHARED_ROUTE,
}


def required_lock_scope(publication_keys: Iterable[str]) -> LockScope:
    """publication key 집합에 필요한 가장 강한 교차 dataset lock 범위를 반환한다."""
    keys = tuple(publication_keys)
    if not keys:
        raise ContractViolation("publication key가 하나 이상 필요합니다.")

    scopes: set[LockScope] = set()
    for key in keys:
        get_publication_spec(key)
        try:
            scope = _LOCK_SCOPE_BY_KEY[key]
        except KeyError as exc:
            raise ContractViolation(
                f"publication key의 lock scope가 등록되지 않았습니다: {key}"
            ) from exc
        scopes.add(scope)

    if LockScope.TOPOLOGY_EXCLUSIVE in scopes:
        return LockScope.TOPOLOGY_EXCLUSIVE
    if LockScope.TOPOLOGY_SHARED_ROUTE in scopes:
        return LockScope.TOPOLOGY_SHARED_ROUTE
    if LockScope.TOPOLOGY_SHARED in scopes:
        return LockScope.TOPOLOGY_SHARED
    return LockScope.NONE


def execute_publication(
    connection: Connection[Any],
    publications: Iterable[VerifiedPublicationEvidence],
    mutate_targets: MutationCallback,
    *,
    validate_locked: LockedValidator | None = None,
    validate_conditional_empty: ConditionalEmptyValidator | None = None,
) -> PublicationResult:
    """검증된 publication의 target 변경과 state claim을 한 transaction으로 실행한다.

    args:
        connection: transaction이 시작되지 않은 psycopg 연결
        publications: immutable bytes와 business time을 검증한 sealed evidence 목록
        mutate_targets: 같은 cursor로 target projection을 변경하는 callback
        validate_locked: 모든 lock 뒤 dependency 외 table별 불변식을 검증하는 callback
        validate_conditional_empty: 조건부 EMPTY 근거를 lock 안에서 증명하는 callback
    returns:
        stale, exact replay 또는 published 결과와 정렬된 publication key
    raises:
        ContractViolation: manifest·fingerprint 결합이나 key 구성이 잘못됐을 때
        PublicationConflictError: 같은 version 내용 또는 multi-key 판정이 충돌할 때
        PublicationDependencyError: lock 안의 dependency state가 fingerprint와 다를 때
        PublicationEmptyError: 조건부 EMPTY 근거가 없거나 거짓일 때
        PublicationTimeError: logical/business 시각이 DB 현재보다 5분 넘게 미래일 때
        PublicationTransactionError: 이미 열린 transaction 연결을 전달했을 때

    target callback 예외와 psycopg 오류는 숨기지 않는다. transaction context가 claim과
    target 변경을 함께 rollback한 뒤 원래 예외를 호출자에게 전달한다.
    """
    issued_publications = tuple(publications)
    ordered = _prepare_publications(issued_publications)
    if not callable(mutate_targets):
        raise ContractViolation("mutate_targets callback이 필요합니다.")

    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise PublicationTransactionError(
            "publication 실행기는 transaction이 시작되지 않은 연결이 필요합니다."
        )

    prepared = tuple(item.publication for item in ordered)
    publication_keys = tuple(item.manifest.publication_key for item in ordered)
    lock_scope = required_lock_scope(publication_keys)
    lock_keys = _all_lock_keys(prepared)

    with (
        connection.transaction(),
        connection.cursor(row_factory=tuple_row) as cursor,
    ):
        cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        _acquire_cross_dataset_locks(cursor, lock_scope)
        _acquire_publication_locks(cursor, lock_keys)
        states = _load_states(cursor, lock_keys)
        db_now = _database_now(cursor)
        _validate_times(ordered, db_now)

        decisions = tuple(
            _classify(item.manifest, states.get(item.manifest.publication_key))
            for item in ordered
        )
        outcome = _single_outcome(decisions)
        if outcome is not PublicationOutcome.PUBLISHED:
            return PublicationResult(outcome, publication_keys)

        _validate_dependencies(ordered, states)
        if validate_locked is not None:
            validate_locked(cursor, ordered)
        ordered = _prepare_publications(issued_publications)
        _validate_empty_publications(
            cursor,
            ordered,
            validate_conditional_empty,
        )
        ordered = _prepare_publications(issued_publications)
        _claim_publications(cursor, ordered)
        mutate_targets(cursor, ordered)

    return PublicationResult(PublicationOutcome.PUBLISHED, publication_keys)


def _prepare_publications(
    publications: Iterable[VerifiedPublicationEvidence],
) -> tuple[VerifiedPublicationEvidence, ...]:
    """검증 evidence를 key 순으로 정렬하고 중복·필수 조합을 검사한다."""
    issued_values = tuple(publications)
    if not issued_values:
        raise ContractViolation("publication이 하나 이상 필요합니다.")
    snapshots: list[VerifiedPublicationEvidence] = []
    for item in issued_values:
        snapshot = _snapshot_verified_publication_evidence(item)
        if snapshot is None:
            raise ContractViolation(
                "모든 publication은 공통 verifier가 만든 evidence여야 합니다."
            )
        snapshots.append(snapshot)
    values = tuple(snapshots)

    ordered = tuple(
        sorted(values, key=lambda item: item.manifest.publication_key.encode("utf-8"))
    )
    keys = tuple(item.manifest.publication_key for item in ordered)
    if len(set(keys)) != len(keys):
        raise ContractViolation(
            "같은 publication key를 한 transaction에 중복할 수 없습니다."
        )
    if "station_stock" in keys and "station" not in keys:
        raise ContractViolation(
            "station_stock은 같은 realtime release의 station과 함께 게시해야 합니다."
        )
    return ordered


def _all_lock_keys(publications: tuple[PreparedPublication, ...]) -> tuple[str, ...]:
    """소유 key와 dependency key 합집합을 UTF-8 byte 순으로 반환한다."""
    keys = {
        dependency.publication_key
        for item in publications
        for dependency in item.input_fingerprint.dependencies
    }
    keys.update(item.manifest.publication_key for item in publications)
    return tuple(sorted(keys, key=lambda key: key.encode("utf-8")))


def _acquire_cross_dataset_locks(
    cursor: Cursor[tuple[Any, ...]],
    scope: LockScope,
) -> None:
    """topology 뒤 route-operation 순서로 필요한 advisory lock을 획득한다."""
    if scope is LockScope.NONE:
        return
    if scope is LockScope.TOPOLOGY_EXCLUSIVE:
        cursor.execute("SELECT gold_meta.lock_topology_exclusive()")
        return

    cursor.execute("SELECT gold_meta.lock_topology_shared()")
    if scope is LockScope.TOPOLOGY_SHARED_ROUTE:
        cursor.execute("SELECT gold_meta.lock_route_operation()")


def _acquire_publication_locks(
    cursor: Cursor[tuple[Any, ...]],
    publication_keys: tuple[str, ...],
) -> None:
    """claim 함수와 같은 advisory key lock을 문자열 순서로 먼저 획득한다."""
    for publication_key in publication_keys:
        cursor.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('gold-publication:' || %s, 0)"
            ")",
            (publication_key,),
        )


def _load_states(
    cursor: Cursor[tuple[Any, ...]],
    publication_keys: tuple[str, ...],
) -> dict[str, _PublicationState]:
    """잠근 key들의 현재 publication state를 key 순서의 row lock으로 읽는다."""
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
         WHERE publication_key = ANY(%s::TEXT[])
         ORDER BY publication_key
         FOR UPDATE
        """,
        (list(publication_keys),),
    )
    states: dict[str, _PublicationState] = {}
    for row in cursor.fetchall():
        state = _PublicationState(
            publication_key=row[0],
            logical_dttm=_utc_dttm(row[1], "stored logical_dttm"),
            revision_no=row[2],
            manifest_uri=row[3],
            artifact_set_sha256=row[4],
            input_fingerprint_sha256=row[5],
            published_row_cnt=row[6],
        )
        states[state.publication_key] = state
    return states


def _database_now(cursor: Cursor[tuple[Any, ...]]) -> datetime:
    """같은 transaction의 PostgreSQL 현재 시각을 UTC로 반환한다."""
    cursor.execute("SELECT clock_timestamp()")
    row = cursor.fetchone()
    if row is None:
        raise PublicationTransactionError("DB clock_timestamp() 결과가 없습니다.")
    return _utc_dttm(row[0], "DB clock_timestamp")


def _validate_times(
    publications: tuple[VerifiedPublicationEvidence, ...],
    db_now: datetime,
) -> None:
    """모든 logical/business 시각이 DB 현재보다 5분 넘게 미래인지 검사한다."""
    future_limit = db_now + _MAX_FUTURE_SKEW
    for item in publications:
        values = (("logical_dttm", item.manifest.logical_dttm),) + tuple(
            item.business_times.all_values
        )
        for name, value in values:
            normalized = _utc_dttm(value, name)
            if normalized > future_limit:
                raise PublicationTimeError(
                    f"{item.manifest.publication_key} {name}이 DB 현재보다 "
                    "5분 넘게 미래입니다."
                )


def _classify(
    manifest: PublicationManifest,
    current: _PublicationState | None,
) -> PublicationOutcome:
    """현재 state와 incoming manifest를 stale·replay·publish로 분류한다."""
    if current is None:
        return PublicationOutcome.PUBLISHED
    incoming_version = (manifest.logical_dttm, manifest.revision_no)
    current_version = (current.logical_dttm, current.revision_no)
    if incoming_version < current_version:
        return PublicationOutcome.STALE
    if incoming_version > current_version:
        return PublicationOutcome.PUBLISHED

    same_content = (
        manifest.artifact_set_sha256 == current.artifact_set_sha256
        and manifest.input_fingerprint_sha256 == current.input_fingerprint_sha256
        and manifest.published_row_cnt == current.published_row_cnt
    )
    if not same_content:
        raise PublicationConflictError(
            "같은 publication version에 서로 다른 fingerprint 또는 row count가 있습니다: "
            f"key={manifest.publication_key}, revision={manifest.revision_no}"
        )
    return PublicationOutcome.EXACT_REPLAY


def _single_outcome(
    decisions: tuple[PublicationOutcome, ...],
) -> PublicationOutcome:
    """multi-key transaction의 모든 판정이 같지 않으면 hard fail한다."""
    unique = set(decisions)
    if len(unique) != 1:
        raise PublicationConflictError(
            "multi-key publication의 stale/replay/publish 판정이 섞여 있습니다: "
            f"{sorted(unique)}"
        )
    return decisions[0]


def _validate_dependencies(
    publications: tuple[VerifiedPublicationEvidence, ...],
    states: dict[str, _PublicationState],
) -> None:
    """fingerprint dependency를 잠근 DB 또는 같은 incoming state와 대조한다."""
    incoming = {item.manifest.publication_key: item for item in publications}
    for item in publications:
        for dependency in item.input_fingerprint.dependencies:
            incoming_dependency = incoming.get(dependency.publication_key)
            if incoming_dependency is not None:
                actual = _state_from_prepared(incoming_dependency.publication)
            else:
                actual = states.get(dependency.publication_key)
            if actual is None:
                raise PublicationDependencyError(
                    f"dependency publication state가 없습니다: {dependency.publication_key}"
                )
            if not _dependency_matches_state(dependency, actual):
                raise PublicationDependencyError(
                    "dependency tuple이 잠근 publication state와 다릅니다: "
                    f"owner={item.manifest.publication_key}, "
                    f"dependency={dependency.publication_key}"
                )


def _state_from_prepared(item: PreparedPublication) -> _PublicationState:
    """같은 transaction의 incoming publication을 dependency 비교 state로 바꾼다."""
    manifest = item.manifest
    return _PublicationState(
        publication_key=manifest.publication_key,
        logical_dttm=manifest.logical_dttm,
        revision_no=manifest.revision_no,
        manifest_uri=item.manifest_uri,
        artifact_set_sha256=manifest.artifact_set_sha256,
        input_fingerprint_sha256=manifest.input_fingerprint_sha256,
        published_row_cnt=manifest.published_row_cnt,
    )


def _dependency_matches_state(
    dependency: Dependency,
    state: _PublicationState,
) -> bool:
    """dependency 6-tuple이 publication state의 같은 필드와 일치하는지 반환한다."""
    return (
        dependency.publication_key == state.publication_key
        and dependency.logical_dttm == state.logical_dttm
        and dependency.revision_no == state.revision_no
        and dependency.manifest_uri == state.manifest_uri
        and dependency.artifact_set_sha256 == state.artifact_set_sha256
        and dependency.input_fingerprint_sha256 == state.input_fingerprint_sha256
    )


def _validate_empty_publications(
    cursor: Cursor[tuple[Any, ...]],
    publications: tuple[VerifiedPublicationEvidence, ...],
    validator: ConditionalEmptyValidator | None,
) -> None:
    """조건부 EMPTY를 lock 안에서 증명하고 manifest 정책을 다시 검증한다."""
    for item in publications:
        manifest = item.manifest
        spec = get_publication_spec(manifest.publication_key)
        conditional_empty_proven = True
        if (
            manifest.published_row_cnt == 0
            and spec.empty_policy is EmptyPolicy.CONDITIONAL
        ):
            if validator is None:
                raise PublicationEmptyError(
                    f"{manifest.publication_key} 조건부 EMPTY 검증 callback이 없습니다."
                )
            conditional_empty_proven = validator(cursor, item)
            if type(conditional_empty_proven) is not bool:
                raise ContractViolation(
                    "조건부 EMPTY 검증 callback은 bool을 반환해야 합니다."
                )
            if conditional_empty_proven is False:
                raise PublicationEmptyError(
                    f"{manifest.publication_key} 조건부 EMPTY 근거가 충족되지 않았습니다."
                )
        validate_publication_manifest(
            manifest,
            conditional_empty_proven=conditional_empty_proven,
        )


def _claim_publications(
    cursor: Cursor[tuple[Any, ...]],
    publications: tuple[VerifiedPublicationEvidence, ...],
) -> None:
    """정렬된 key를 claim하고 예상 밖 false 결과를 hard fail한다."""
    for item in publications:
        manifest = item.manifest
        cursor.execute(
            """
            SELECT gold_meta.claim_publication(
                %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                manifest.publication_key,
                manifest.logical_dttm,
                manifest.revision_no,
                item.manifest_uri,
                manifest.artifact_set_sha256,
                manifest.input_fingerprint_sha256,
                manifest.published_row_cnt,
            ),
        )
        row = cursor.fetchone()
        if row is None or row[0] is not True:
            raise PublicationConflictError(
                "lock 안의 사전 판정과 claim_publication() 결과가 다릅니다: "
                f"{manifest.publication_key}"
            )


def _utc_dttm(value: datetime, name: str) -> datetime:
    """timezone-aware datetime을 UTC로 바꾸고 naive 값을 거부한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise PublicationTimeError(f"{name}은 timezone-aware datetime이어야 합니다.")
    return value.astimezone(UTC)
