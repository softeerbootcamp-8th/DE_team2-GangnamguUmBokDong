"""Gold publication transaction의 상태 판정·원자성·lock 계약을 검증한다."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import core.gold_publication.transaction as transaction_module
import psycopg
import pytest
from core.gold_publication.canonical import parse_canonical_json, sha256_hex
from core.gold_publication.contract import (
    Artifact,
    Dependency,
    InputArtifact,
    Parameter,
    build_artifact_set,
    build_input_fingerprint,
    build_publication_manifest,
    get_publication_spec,
)
from core.gold_publication.errors import (
    ContractViolation,
    ObjectChecksumMismatchError,
    ObjectCollisionError,
    ObjectMissingError,
    PublicationConflictError,
    PublicationDependencyError,
    PublicationEmptyError,
    PublicationTimeError,
)
from core.gold_publication.evidence import (
    VerifiedPublicationEvidence,
    required_business_time_fields,
    verify_publication_evidence,
)
from core.gold_publication.storage import ImmutablePutOutcome
from core.gold_publication.transaction import (
    LockScope,
    PreparedPublication,
    PublicationOutcome,
    _prepare_publications,
    execute_publication,
    required_lock_scope,
)
from psycopg import Connection, Cursor

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_PUBLIC_TABLES = (
    "rebalance_route_stop",
    "rebalance_route",
    "station_urgency",
    "event",
    "weather_forecast",
    "station_demand_forecast",
    "station_stock",
    "station",
    "dispatch_center",
    "weather_grid",
)


class _MemoryObjectStore:
    """transaction fixture가 실제 bytes를 검증하도록 하는 in-memory immutable store다."""

    def __init__(self, objects: Mapping[str, bytes]) -> None:
        """초기 immutable object를 복사해 보관한다."""
        self._objects = dict(objects)

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """정확한 URI bytes와 checksum 및 canonical JSON을 검증한다."""
        try:
            payload = self._objects[uri]
        except KeyError as exc:
            raise ObjectMissingError(f"immutable object가 없습니다: {uri}") from exc
        actual = sha256_hex(payload)
        if actual != expected_sha256:
            raise ObjectChecksumMismatchError(
                f"immutable object checksum이 다릅니다: expected={expected_sha256}, "
                f"actual={actual}"
            )
        if require_canonical_json:
            parse_canonical_json(payload)
        return payload

    def put_once(
        self,
        uri: str,
        payload: bytes,
        *,
        expected_sha256: str | None = None,
        require_canonical_json: bool = False,
    ) -> ImmutablePutOutcome:
        """URI가 비었을 때만 쓰고 동일 bytes 재시도만 허용한다."""
        if expected_sha256 is not None and sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError("immutable put checksum이 다릅니다.")
        if require_canonical_json:
            parse_canonical_json(payload)
        existing = self._objects.get(uri)
        if existing is None:
            self._objects[uri] = payload
            return ImmutablePutOutcome.CREATED
        if existing == payload:
            return ImmutablePutOutcome.ALREADY_EXISTS
        raise ObjectCollisionError(f"immutable URI collision: {uri}")


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적인 disposable gold151_* DB 연결을 깨끗한 상태로 제공한다."""
    if _DATABASE_URL is None:
        pytest.skip(
            "GOLD_PUBLICATION_TEST_DATABASE_URL이 없어 PostGIS 통합 검증을 건너뜁니다."
        )

    connection = psycopg.connect(_DATABASE_URL)
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        row = cursor.fetchone()
    connection.rollback()
    if row is None or not row[0].startswith("gold151_"):
        connection.close()
        pytest.fail(
            "publication 통합 테스트는 이름이 gold151_로 시작하는 disposable DB만 허용합니다."
        )

    _reset_database(connection)
    try:
        yield connection
    finally:
        _reset_database(connection)
        connection.close()


def test_required_lock_scope_follows_topology_route_order() -> None:
    """key 조합에 필요한 가장 강한 교차 dataset lock을 선택한다."""
    assert required_lock_scope(["event:cultural_event"]) is LockScope.NONE
    assert required_lock_scope(["station_demand_forecast"]) is LockScope.TOPOLOGY_SHARED
    assert required_lock_scope(["rebalance_route"]) is LockScope.TOPOLOGY_SHARED_ROUTE
    assert (
        required_lock_scope(["rebalance_route", "station"])
        is LockScope.TOPOLOGY_EXCLUSIVE
    )


def test_required_lock_scope_reports_missing_registry_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """등록된 publication key의 lock scope 누락을 계약 오류로 보고한다."""
    monkeypatch.delitem(transaction_module._LOCK_SCOPE_BY_KEY, "station")

    with pytest.raises(ContractViolation, match="lock scope가 등록되지 않았습니다"):
        required_lock_scope(["station"])


def test_executor_rejects_unverified_prepared_before_database_access() -> None:
    """sealed evidence가 아닌 raw publication은 DB 연결을 보기 전에 거부한다."""
    evidence = _prepared_event(
        datetime.now(UTC) - timedelta(minutes=1),
        revision_no=0,
        content="unverified",
    )

    with pytest.raises(ContractViolation, match="공통 verifier"):
        execute_publication(  # type: ignore[arg-type]
            object(),
            [evidence.publication],  # type: ignore[list-item]
            _noop_mutation,
        )


def test_station_stock_requires_station_in_same_transaction() -> None:
    """authoritative realtime stock만 단독 claim하는 경로를 DB 접근 전에 막는다."""
    stock = _prepared(
        "station_stock",
        datetime.now(UTC) - timedelta(minutes=1),
        revision_no=0,
        content="stock-only",
        target_row_counts={"station_stock": 1},
    )

    with pytest.raises(ContractViolation, match="station과 함께"):
        execute_publication(  # type: ignore[arg-type]
            object(),
            [stock],
            _noop_mutation,
        )


def test_executor_snapshot_does_not_reuse_stateful_token_mapping() -> None:
    """token 검증 뒤 값을 바꾸는 mapping이어도 registry snapshot만 쓴다."""

    class StatefulBusinessTimes(dict[str, tuple[datetime, ...]]):
        """첫 조회 뒤 business time을 숨기는 상태성 mapping이다."""

        calls = 0

        def __getitem__(self, key: str) -> tuple[datetime, ...]:
            """첫 검증에만 원본 시각을 반환한다."""
            self.calls += 1
            if self.calls == 1:
                return super().__getitem__(key)
            return ()

    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    evidence = _prepared_event(
        logical_dttm,
        revision_no=0,
        content="stateful-business-time",
    )
    stateful = StatefulBusinessTimes({"last_seen_dttm": (logical_dttm,)})
    object.__setattr__(evidence.business_times, "values_by_field", stateful)

    snapshot = _prepare_publications((evidence,))[0]

    assert stateful.calls == 1
    assert snapshot.business_times.all_values == (("last_seen_dttm", logical_dttm),)


def test_publish_replay_stale_conflict_and_correction(
    gold_connection: Connection[Any],
) -> None:
    """claim 의미를 publish·exact replay·stale·conflict·correction으로 구분한다."""
    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    initial = _prepared_event(logical_dttm, revision_no=0, content="initial")
    mutation_calls: list[str] = []

    def record_mutation(
        _cursor: Cursor[tuple[Any, ...]],
        _publications: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """실제 publish에서만 mutation callback 호출을 기록한다."""
        mutation_calls.append("called")

    first = execute_publication(gold_connection, [initial], record_mutation)
    assert first.outcome is PublicationOutcome.PUBLISHED
    assert mutation_calls == ["called"]

    replay = execute_publication(gold_connection, [initial], _fail_if_mutated)
    assert replay.outcome is PublicationOutcome.EXACT_REPLAY

    stale = _prepared_event(
        logical_dttm - timedelta(minutes=1),
        revision_no=99,
        content="stale",
    )
    stale_result = execute_publication(gold_connection, [stale], _fail_if_mutated)
    assert stale_result.outcome is PublicationOutcome.STALE

    conflict = _prepared_event(logical_dttm, revision_no=0, content="conflict")
    with pytest.raises(PublicationConflictError, match="같은 publication version"):
        execute_publication(gold_connection, [conflict], _fail_if_mutated)

    correction = _prepared_event(logical_dttm, revision_no=1, content="correction")
    correction_result = execute_publication(
        gold_connection, [correction], record_mutation
    )
    assert correction_result.outcome is PublicationOutcome.PUBLISHED
    assert mutation_calls == ["called", "called"]

    state = _state_row(gold_connection, "event:cultural_event")
    assert state is not None
    assert state[0] == logical_dttm
    assert state[1] == 1
    assert state[2] == correction.manifest.artifact_set_sha256


def test_target_error_rolls_back_target_and_publication_state(
    gold_connection: Connection[Any],
) -> None:
    """target mutation 오류가 선행 claim과 target INSERT를 함께 rollback한다."""
    publication = _prepared(
        "weather_grid",
        datetime.now(UTC) - timedelta(minutes=1),
        revision_no=0,
        content="atomicity",
        target_row_counts={"weather_grid": 1},
    )

    def insert_then_fail(
        cursor: Cursor[tuple[Any, ...]],
        _publications: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """target 행을 넣은 뒤 의도한 오류를 발생시킨다."""
        cursor.execute(
            """
            INSERT INTO weather_grid (
                weather_grid_id,
                weather_grid_x_no,
                weather_grid_y_no
            )
            VALUES ('61_126', 61, 126)
            """
        )
        raise RuntimeError("injected target failure")

    with pytest.raises(RuntimeError, match="injected target failure"):
        execute_publication(gold_connection, [publication], insert_then_fail)

    assert _scalar(gold_connection, "SELECT count(*) FROM weather_grid") == 0
    assert (
        _scalar(
            gold_connection,
            "SELECT count(*) FROM gold_meta.publication_state "
            "WHERE publication_key = 'weather_grid'",
        )
        == 0
    )


def test_future_logical_and_business_times_fail_before_claim(
    gold_connection: Connection[Any],
) -> None:
    """DB 현재보다 5분 넘게 미래인 logical/business 시각을 fail-closed한다."""
    future = datetime.now(UTC) + timedelta(minutes=10)
    future_logical = _prepared_event(future, revision_no=0, content="future-logical")
    with pytest.raises(PublicationTimeError, match="logical_dttm"):
        execute_publication(gold_connection, [future_logical], _fail_if_mutated)

    normal_logical = datetime.now(UTC) - timedelta(minutes=1)
    future_business = _prepared(
        "event:cultural_event",
        normal_logical,
        revision_no=0,
        content="future-business",
        target_row_counts={"event": 2},
        business_time_values=(normal_logical, future),
    )
    with pytest.raises(PublicationTimeError, match="last_seen_dttm"):
        execute_publication(gold_connection, [future_business], _fail_if_mutated)

    assert (
        _scalar(gold_connection, "SELECT count(*) FROM gold_meta.publication_state")
        == 0
    )


def test_dependency_tuple_is_rechecked_under_lock(
    gold_connection: Connection[Any],
) -> None:
    """staging fingerprint의 dependency 6-tuple을 현재 state와 다시 대조한다."""
    dependency = _seed_state(gold_connection, "station")
    publication = _prepared(
        "station_demand_forecast",
        datetime.now(UTC) - timedelta(minutes=1),
        revision_no=0,
        content="dependency-match",
        target_row_counts={"station_demand_forecast": 1},
        dependencies=(dependency,),
    )
    result = execute_publication(gold_connection, [publication], _noop_mutation)
    assert result.outcome is PublicationOutcome.PUBLISHED

    _reset_database(gold_connection)
    current = _seed_state(gold_connection, "station")
    mismatched = Dependency(
        artifact_set_sha256=sha256_hex(b"mismatched-artifact"),
        input_fingerprint_sha256=current.input_fingerprint_sha256,
        logical_dttm=current.logical_dttm,
        manifest_uri=current.manifest_uri,
        publication_key=current.publication_key,
        revision_no=current.revision_no,
    )
    invalid = _prepared(
        "station_demand_forecast",
        datetime.now(UTC) - timedelta(minutes=1),
        revision_no=0,
        content="dependency-mismatch",
        target_row_counts={"station_demand_forecast": 1},
        dependencies=(mismatched,),
    )
    with pytest.raises(PublicationDependencyError, match="dependency tuple"):
        execute_publication(gold_connection, [invalid], _fail_if_mutated)


def test_conditional_empty_requires_locked_proof(
    gold_connection: Connection[Any],
) -> None:
    """conditional EMPTY는 lock 안 callback이 true를 반환할 때만 게시한다."""
    dependency = _seed_state(gold_connection, "station")
    publication = _prepared(
        "station_demand_forecast",
        datetime.now(UTC) - timedelta(minutes=1),
        revision_no=0,
        content="conditional-empty",
        target_row_counts={"station_demand_forecast": 0},
        dependencies=(dependency,),
        conditional_empty_proven=True,
    )

    with pytest.raises(PublicationEmptyError, match="callback"):
        execute_publication(gold_connection, [publication], _fail_if_mutated)

    def reject_empty(
        _cursor: Cursor[tuple[Any, ...]],
        _publication: VerifiedPublicationEvidence,
    ) -> bool:
        """조건부 EMPTY 근거가 충족되지 않았음을 반환한다."""
        return False

    with pytest.raises(PublicationEmptyError, match="충족되지"):
        execute_publication(
            gold_connection,
            [publication],
            _fail_if_mutated,
            validate_conditional_empty=reject_empty,
        )

    def approve_empty(
        _cursor: Cursor[tuple[Any, ...]],
        _publication: VerifiedPublicationEvidence,
    ) -> bool:
        """테스트에서 잠근 기대 집합이 비었음을 증명한다."""
        return True

    result = execute_publication(
        gold_connection,
        [publication],
        _noop_mutation,
        validate_conditional_empty=approve_empty,
    )
    assert result.outcome is PublicationOutcome.PUBLISHED


def test_multi_key_mixed_outcome_fails_without_partial_state(
    gold_connection: Connection[Any],
) -> None:
    """multi-key 중 일부만 publish되는 혼합 판정을 hard fail한다."""
    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    existing = _prepared_event(logical_dttm, revision_no=0, content="existing")
    execute_publication(gold_connection, [existing], _noop_mutation)

    new_key = _prepared(
        "event:performance_event",
        logical_dttm,
        revision_no=0,
        content="new-key",
        target_row_counts={"event": 1},
    )
    with pytest.raises(PublicationConflictError, match="판정이 섞여"):
        execute_publication(gold_connection, [existing, new_key], _fail_if_mutated)

    assert _state_row(gold_connection, "event:performance_event") is None


def test_opt_in_mixed_replay_validates_all_and_mutates_published_subset(
    gold_connection: Connection[Any],
) -> None:
    """Opt-in 혼합 실행은 전체를 검증하고 replay target 뒤 신규 key만 변경한다."""
    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    replayed = _prepared_event(logical_dttm, revision_no=0, content="replayed")
    execute_publication(gold_connection, [replayed], _noop_mutation)
    published = _prepared(
        "event:performance_event",
        logical_dttm,
        revision_no=0,
        content="published",
        target_row_counts={"event": 1},
    )
    locked_keys: list[tuple[str, ...]] = []
    replayed_keys: list[tuple[str, ...]] = []
    mutated_keys: list[tuple[str, ...]] = []

    def validate_all_locked(
        _cursor: Cursor[tuple[Any, ...]],
        publications: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Locked validator가 replay와 publish evidence를 모두 받았음을 기록한다."""
        locked_keys.append(
            tuple(item.manifest.publication_key for item in publications)
        )
        replay_snapshot = next(
            item
            for item in publications
            if item.manifest.publication_key == "event:cultural_event"
        )
        object.__setattr__(replay_snapshot.publication, "manifest", published.manifest)

    def validate_replayed_targets(
        _cursor: Cursor[tuple[Any, ...]],
        publications: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Replay target validator가 exact replay subset만 받았음을 기록한다."""
        replayed_keys.append(
            tuple(item.manifest.publication_key for item in publications)
        )

    def mutate_published_targets(
        _cursor: Cursor[tuple[Any, ...]],
        publications: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Mutation callback이 PUBLISHED subset만 받았음을 기록한다."""
        mutated_keys.append(
            tuple(item.manifest.publication_key for item in publications)
        )

    result = execute_publication(
        gold_connection,
        [published, replayed],
        mutate_published_targets,
        validate_locked=validate_all_locked,
        allow_mixed_replay=True,
        validate_replay_targets_locked=validate_replayed_targets,
    )

    assert result.outcome is PublicationOutcome.PUBLISHED
    assert result.publication_keys == (
        "event:cultural_event",
        "event:performance_event",
    )
    assert locked_keys == [result.publication_keys]
    assert replayed_keys == [("event:cultural_event",)]
    assert mutated_keys == [("event:performance_event",)]
    assert _state_row(gold_connection, "event:performance_event") is not None


def test_opt_in_mixed_replay_target_failure_rolls_back_new_claim(
    gold_connection: Connection[Any],
) -> None:
    """Replay target drift가 있으면 혼합 transaction의 신규 claim도 남기지 않는다."""
    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    replayed = _prepared_event(logical_dttm, revision_no=0, content="replayed-drift")
    execute_publication(gold_connection, [replayed], _noop_mutation)
    published = _prepared(
        "event:performance_event",
        logical_dttm,
        revision_no=0,
        content="published-rollback",
        target_row_counts={"event": 1},
    )

    def reject_replayed_target(
        _cursor: Cursor[tuple[Any, ...]],
        _publications: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Sealed replay target drift를 모사한다."""
        raise ContractViolation("replayed target drift")

    with pytest.raises(ContractViolation, match="replayed target drift"):
        execute_publication(
            gold_connection,
            [replayed, published],
            _fail_if_mutated,
            allow_mixed_replay=True,
            validate_replay_targets_locked=reject_replayed_target,
        )

    assert _state_row(gold_connection, "event:performance_event") is None


def test_opt_in_mixed_replay_rechecks_replayed_dependencies(
    gold_connection: Connection[Any],
) -> None:
    """Replay evidence의 dependency도 전체 lock 아래 current state와 다시 대조한다."""
    station_dependency = _seed_state(gold_connection, "station")
    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    replayed = _prepared(
        "station_demand_forecast",
        logical_dttm,
        revision_no=0,
        content="dependent-replay",
        target_row_counts={"station_demand_forecast": 1},
        dependencies=(station_dependency,),
    )
    execute_publication(gold_connection, [replayed], _noop_mutation)
    with gold_connection.transaction(), gold_connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE gold_meta.publication_state
               SET revision_no = 1,
                   manifest_uri = %s,
                   artifact_set_sha256 = %s,
                   input_fingerprint_sha256 = %s
             WHERE publication_key = 'station'
            """,
            (
                "s3://fixture/dependency/station-corrected.json",
                sha256_hex(b"station-corrected-artifact"),
                sha256_hex(b"station-corrected-input"),
            ),
        )
    published = _prepared_event(
        logical_dttm,
        revision_no=0,
        content="new-alongside-dependent-replay",
    )

    with pytest.raises(PublicationDependencyError, match="dependency tuple"):
        execute_publication(
            gold_connection,
            [replayed, published],
            _fail_if_mutated,
            allow_mixed_replay=True,
            validate_replay_targets_locked=_fail_if_mutated,
        )

    assert _state_row(gold_connection, "event:cultural_event") is None


def test_opt_in_keeps_all_replay_and_stale_mixed_legacy_semantics(
    gold_connection: Connection[Any],
) -> None:
    """All-replay는 no-op이고 stale가 섞인 실행은 opt-in에서도 충돌한다."""
    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    current = _prepared_event(logical_dttm, revision_no=0, content="legacy-replay")
    execute_publication(gold_connection, [current], _noop_mutation)
    locked_validations: list[tuple[str, ...]] = []
    replay_validations: list[tuple[str, ...]] = []

    def validate_all_locked(
        _cursor: Cursor[tuple[Any, ...]],
        publications: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Opt-in all-replay도 일반 locked validator를 수행했음을 기록한다."""
        locked_validations.append(
            tuple(item.manifest.publication_key for item in publications)
        )

    def validate_all_replay_targets(
        _cursor: Cursor[tuple[Any, ...]],
        publications: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Opt-in all-replay도 lock 안 target 검증을 수행했음을 기록한다."""
        replay_validations.append(
            tuple(item.manifest.publication_key for item in publications)
        )

    replay = execute_publication(
        gold_connection,
        [current],
        _fail_if_mutated,
        validate_locked=validate_all_locked,
        allow_mixed_replay=True,
        validate_replay_targets_locked=validate_all_replay_targets,
    )
    assert replay.outcome is PublicationOutcome.EXACT_REPLAY
    assert locked_validations == [("event:cultural_event",)]
    assert replay_validations == [("event:cultural_event",)]

    stale = _prepared_event(
        logical_dttm - timedelta(minutes=1),
        revision_no=0,
        content="legacy-stale",
    )
    new_key = _prepared(
        "event:performance_event",
        logical_dttm,
        revision_no=0,
        content="new-with-stale",
        target_row_counts={"event": 1},
    )
    with pytest.raises(PublicationConflictError, match="판정이 섞여"):
        execute_publication(
            gold_connection,
            [stale, new_key],
            _fail_if_mutated,
            allow_mixed_replay=True,
            validate_replay_targets_locked=_fail_if_mutated,
        )


def test_same_key_two_sessions_serialize_to_publish_then_replay(
    gold_connection: Connection[Any],
) -> None:
    """같은 key의 두 세션은 직렬화되어 publish 뒤 exact replay가 된다."""
    del gold_connection
    publication = _prepared_event(
        datetime.now(UTC) - timedelta(minutes=1),
        revision_no=0,
        content="same-key",
    )
    first_inside_mutation = threading.Event()
    release_first = threading.Event()
    second_finished = threading.Event()

    def first_worker() -> PublicationOutcome:
        """첫 세션이 claim 뒤 transaction을 잠시 유지한다."""
        assert _DATABASE_URL is not None
        with psycopg.connect(_DATABASE_URL) as connection:

            def hold_mutation(
                _cursor: Cursor[tuple[Any, ...]],
                _publications: tuple[VerifiedPublicationEvidence, ...],
            ) -> None:
                """두 번째 세션의 lock 대기를 관측할 때까지 commit을 지연한다."""
                first_inside_mutation.set()
                assert release_first.wait(timeout=5)

            return execute_publication(connection, [publication], hold_mutation).outcome

    def second_worker() -> PublicationOutcome:
        """두 번째 세션이 같은 key 결과를 재시도한다."""
        assert _DATABASE_URL is not None
        assert first_inside_mutation.wait(timeout=5)
        with psycopg.connect(_DATABASE_URL) as connection:
            outcome = execute_publication(
                connection, [publication], _fail_if_mutated
            ).outcome
        second_finished.set()
        return outcome

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_worker)
        second_future = executor.submit(second_worker)
        assert first_inside_mutation.wait(timeout=5)
        assert not second_finished.wait(timeout=0.2)
        release_first.set()
        assert first_future.result(timeout=5) is PublicationOutcome.PUBLISHED
        assert second_future.result(timeout=5) is PublicationOutcome.EXACT_REPLAY


def test_different_keys_two_sessions_enter_mutation_concurrently(
    gold_connection: Connection[Any],
) -> None:
    """교차 lock이 없는 서로 다른 event key는 동시에 mutation 단계에 진입한다."""
    del gold_connection
    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    publications = (
        _prepared_event(logical_dttm, revision_no=0, content="cultural"),
        _prepared(
            "event:performance_event",
            logical_dttm,
            revision_no=0,
            content="performance",
            target_row_counts={"event": 1},
        ),
    )
    mutation_barrier = threading.Barrier(2)

    def worker(publication: VerifiedPublicationEvidence) -> PublicationOutcome:
        """서로 다른 key의 mutation callback에서 barrier를 만난다."""
        assert _DATABASE_URL is not None
        with psycopg.connect(_DATABASE_URL) as connection:

            def meet_other_session(
                _cursor: Cursor[tuple[Any, ...]],
                _publications: tuple[VerifiedPublicationEvidence, ...],
            ) -> None:
                """두 세션이 동시에 lock 이후 단계에 들어왔는지 검증한다."""
                mutation_barrier.wait(timeout=5)

            return execute_publication(
                connection, [publication], meet_other_session
            ).outcome

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(worker, publications))
    assert outcomes == (PublicationOutcome.PUBLISHED, PublicationOutcome.PUBLISHED)


def test_reversed_multi_key_inputs_do_not_deadlock(
    gold_connection: Connection[Any],
) -> None:
    """두 세션의 입력 순서가 반대여도 key 정렬 lock으로 deadlock을 피한다."""
    del gold_connection
    logical_dttm = datetime.now(UTC) - timedelta(minutes=1)
    first = _prepared_event(logical_dttm, revision_no=0, content="multi-cultural")
    second = _prepared(
        "event:performance_event",
        logical_dttm,
        revision_no=0,
        content="multi-performance",
        target_row_counts={"event": 1},
    )
    first_inside_mutation = threading.Event()
    release_first = threading.Event()

    def worker(
        publications: tuple[VerifiedPublicationEvidence, ...],
        hold: bool,
    ) -> PublicationOutcome:
        """정방향 또는 역방향 multi-key publication을 실행한다."""
        assert _DATABASE_URL is not None
        with psycopg.connect(_DATABASE_URL) as connection:

            def maybe_hold(
                _cursor: Cursor[tuple[Any, ...]],
                _publications: tuple[VerifiedPublicationEvidence, ...],
            ) -> None:
                """첫 세션만 lock을 유지해 두 번째 세션의 정렬 대기를 만든다."""
                if hold:
                    first_inside_mutation.set()
                    assert release_first.wait(timeout=5)

            return execute_publication(connection, publications, maybe_hold).outcome

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(worker, (second, first), True)
        assert first_inside_mutation.wait(timeout=5)
        second_future = executor.submit(worker, (first, second), False)
        release_first.set()
        assert first_future.result(timeout=5) is PublicationOutcome.PUBLISHED
        assert second_future.result(timeout=5) is PublicationOutcome.EXACT_REPLAY


def _prepared_event(
    logical_dttm: datetime,
    revision_no: int,
    content: str,
    *,
    business_time_values: tuple[datetime, ...] | None = None,
) -> VerifiedPublicationEvidence:
    """cultural event publication fixture를 만든다."""
    return _prepared(
        "event:cultural_event",
        logical_dttm,
        revision_no,
        content,
        target_row_counts={"event": 1},
        business_time_values=business_time_values,
    )


def _prepared(
    publication_key: str,
    logical_dttm: datetime,
    revision_no: int,
    content: str,
    *,
    target_row_counts: dict[str, int],
    dependencies: tuple[Dependency, ...] = (),
    business_time_values: tuple[datetime, ...] | None = None,
    conditional_empty_proven: bool = False,
) -> VerifiedPublicationEvidence:
    """registry를 이용해 transaction 통합 테스트용 publication을 만든다."""
    spec = get_publication_spec(publication_key)
    dependency_by_key = {
        dependency.publication_key: dependency for dependency in dependencies
    }
    input_artifacts = []
    object_payloads: dict[str, bytes] = {}
    manifest_role_dependencies = {
        "demand_publication_manifest": "station_demand_forecast",
        "stock_publication_manifest": "station_stock",
        "urgency_publication_manifest": "station_urgency",
    }
    for cardinality in spec.input_roles:
        if cardinality.minimum == 0:
            continue
        dependency_key = manifest_role_dependencies.get(cardinality.role)
        if dependency_key is not None:
            uri = dependency_by_key[dependency_key].manifest_uri
        else:
            uri = f"s3://fixture/input/{content}/{cardinality.role}.json"
        payload = f"{content}:{cardinality.role}".encode()
        object_payloads[uri] = payload
        input_artifacts.append(
            InputArtifact(
                byte_sha256=sha256_hex(payload),
                role=cardinality.role,
                uri=uri,
            )
        )
    input_by_role = {artifact.role: artifact for artifact in input_artifacts}
    parameter_values = {
        name: (
            sha256_hex(f"{content}:expected-station-ids".encode())
            if name == "expected_sta_id_sha256"
            else input_by_role["route_coverage"].byte_sha256
            if name == "route_coverage_sha256"
            else f"{content}:{name}"
        )
        for name in spec.parameter_names
    }
    parameters = tuple(
        Parameter(name=name, value=parameter_values[name])
        for name in spec.parameter_names
    )
    fingerprint = build_input_fingerprint(
        publication_key,
        dependencies,
        input_artifacts,
        parameters,
    )

    artifacts = []
    if any(target_row_counts.values()):
        for role, target in spec.output_targets:
            output_payload = f"{content}:{role}:output".encode()
            output_uri = f"s3://fixture/output/{content}/{role}.parquet"
            object_payloads[output_uri] = output_payload
            artifacts.append(
                Artifact(
                    byte_sha256=sha256_hex(output_payload),
                    role=role,
                    row_count=target_row_counts[target],
                    uri=output_uri,
                )
            )
    artifact_set = build_artifact_set(artifacts)
    manifest = build_publication_manifest(
        publication_key=publication_key,
        artifact_set=artifact_set,
        input_fingerprint=fingerprint,
        input_fingerprint_uri=f"s3://fixture/input/{content}/fingerprint.json",
        logical_dttm=logical_dttm,
        publisher_version="gold-publisher-v1",
        revision_no=revision_no,
        target_row_counts=target_row_counts,
        conditional_empty_proven=conditional_empty_proven,
    )
    prepared = PreparedPublication(
        manifest=manifest,
        manifest_uri=f"s3://fixture/manifest/{content}.json",
        input_fingerprint=fingerprint,
    )
    object_payloads[manifest.input_fingerprint_uri] = fingerprint.canonical_bytes
    row_values = (
        business_time_values
        if business_time_values is not None
        else (logical_dttm,) * manifest.published_row_cnt
    )
    business_times = {
        field_name: row_values
        for field_name in required_business_time_fields(publication_key)
    }
    return verify_publication_evidence(
        prepared,
        _MemoryObjectStore(object_payloads),
        lambda _publication, _payloads: business_times,
    )


def _seed_state(connection: Connection[Any], publication_key: str) -> Dependency:
    """disposable DB에 dependency 비교용 publication state를 만든다."""
    logical_dttm = datetime.now(UTC) - timedelta(minutes=2)
    artifact_hash = sha256_hex(f"{publication_key}:artifact".encode())
    input_hash = sha256_hex(f"{publication_key}:input".encode())
    manifest_uri = f"s3://fixture/dependency/{publication_key}.json"
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            "SELECT gold_meta.claim_publication(%s, %s, %s, %s, %s, %s, %s)",
            (
                publication_key,
                logical_dttm,
                0,
                manifest_uri,
                artifact_hash,
                input_hash,
                1,
            ),
        )
        assert cursor.fetchone() == (True,)
    return Dependency(
        artifact_set_sha256=artifact_hash,
        input_fingerprint_sha256=input_hash,
        logical_dttm=logical_dttm,
        manifest_uri=manifest_uri,
        publication_key=publication_key,
        revision_no=0,
    )


def _state_row(
    connection: Connection[Any],
    publication_key: str,
) -> tuple[Any, ...] | None:
    """state의 version과 artifact hash를 읽고 연결을 idle로 되돌린다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT logical_dttm, revision_no, artifact_set_sha256
              FROM gold_meta.publication_state
             WHERE publication_key = %s
            """,
            (publication_key,),
        )
        row = cursor.fetchone()
    connection.rollback()
    return row


def _scalar(connection: Connection[Any], query: str) -> Any:
    """단일 값을 읽고 연결을 idle transaction 상태로 되돌린다."""
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0]


def _reset_database(connection: Connection[Any]) -> None:
    """명시적인 disposable DB의 Gold target과 state만 비운다."""
    connection.rollback()
    table_list = ", ".join(_PUBLIC_TABLES)
    with connection.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {table_list} CASCADE")
        cursor.execute("TRUNCATE TABLE gold_meta.publication_state")
    connection.commit()


def _noop_mutation(
    _cursor: Cursor[tuple[Any, ...]],
    _publications: tuple[VerifiedPublicationEvidence, ...],
) -> None:
    """state claim만 검증하는 테스트에서 target을 변경하지 않는다."""


def _fail_if_mutated(
    _cursor: Cursor[tuple[Any, ...]],
    _publications: tuple[VerifiedPublicationEvidence, ...],
) -> None:
    """no-op 경로에서 mutation callback이 호출되면 테스트를 실패시킨다."""
    pytest.fail("stale/replay/conflict 경로가 target mutation을 호출했습니다.")
