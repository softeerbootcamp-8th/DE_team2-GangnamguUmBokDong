"""Immutable model snapshot과 pair serving release의 원자성 계약을 검증한다."""

from __future__ import annotations

import io
import json
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from core.gold_publication import (
    ImmutablePutOutcome,
    canonical_json_bytes,
    parse_canonical_json,
    sha256_hex,
)
from core.gold_publication.errors import ContractViolation, ObjectChecksumMismatchError
from core.model_snapshot import ModelKind

from ml_core.serving_release import (
    CrossContractServingReleaseError,
    ExplicitImmutablePayload,
    ImmutableArtifactRef,
    PointerRead,
    S3ServingReleasePointerStore,
    ServingReleaseManifest,
    ServingReleasePointerConflictError,
    ServingReleasePreflightError,
    build_effective_contract_ref,
    build_effective_serving_contract,
    build_serving_release_manifest,
    effective_contract_version,
    load_current_serving_release,
    parse_effective_serving_contract,
    parse_serving_release_manifest,
    parse_serving_release_pointer,
    preflight_serving_release,
    publish_effective_contract,
    publish_model_snapshot,
    publish_serving_release,
    publish_station_profile,
    validate_station_profile_payload,
)


@dataclass
class _MemoryObjectStore:
    """Immutable object protocol과 호출 순서를 기록하는 in-memory store다."""

    objects: dict[str, bytes] = field(default_factory=dict)
    events: list[tuple[str, str]] = field(default_factory=list)

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """Exact URI bytes와 SHA, 선택적 canonical JSON을 검증한다."""
        self.events.append(("read", uri))
        payload = self.objects[uri]
        if sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError(f"checksum mismatch: {uri}")
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
        """동일 bytes retry만 허용하며 immutable PUT 호출을 기록한다."""
        self.events.append(("put", uri))
        if expected_sha256 is not None and sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError(f"incoming checksum mismatch: {uri}")
        if require_canonical_json:
            parse_canonical_json(payload)
        existing = self.objects.get(uri)
        if existing is not None:
            if existing != payload:
                raise AssertionError(f"immutable collision: {uri}")
            return ImmutablePutOutcome.ALREADY_EXISTS
        self.objects[uri] = payload
        return ImmutablePutOutcome.CREATED


@dataclass
class _MemoryPointerStore:
    """Version token CAS와 장애 주입을 제공하는 in-memory pointer store다."""

    events: list[tuple[str, str]]
    payload: bytes | None = None
    token: str | None = None
    fail_next_cas: bool = False

    def read(self, key: str) -> PointerRead:
        """현재 pointer payload/token snapshot을 반환한다."""
        self.events.append(("pointer-read", key))
        return PointerRead(payload=self.payload, version_token=self.token)

    def compare_and_swap(
        self,
        key: str,
        expected_version_token: str | None,
        payload: bytes,
    ) -> None:
        """Token이 그대로일 때만 pointer를 교체한다."""
        self.events.append(("pointer-cas", key))
        if self.fail_next_cas:
            self.fail_next_cas = False
            raise ServingReleasePointerConflictError("injected conflict")
        if expected_version_token != self.token:
            raise ServingReleasePointerConflictError("stale token")
        self.payload = payload
        next_generation = 0 if self.token is None else int(self.token) + 1
        self.token = str(next_generation)


def _profile(*, grid_tick: int = 20, train_months: int = 12, leaves: int = 63) -> dict:
    """Serving 7-key와 모델별 training-only 값을 가진 full profile을 만든다."""
    return {
        "GRID_TICK_MINUTES": grid_tick,
        "HORIZON_COUNT": 12,
        "LGB_PARAMS_COMMON": {"num_leaves": leaves, "learning_rate": 0.05},
        "ROLLING_EMBARGO_MINUTES": 40,
        "ROLLING_TICK_MINUTES": grid_tick,
        "ROLLING_WINDOW_MINUTES": 60,
        "TARGET_HORIZON_MINUTES": 60,
        "TRAIN_ANCHOR_TICK_MINUTES": grid_tick,
        "TRAIN_LOOKBACK_MONTHS": train_months,
    }


def _json_bytes(value: object) -> bytes:
    """Legacy training write_json과 같은 일반 UTF-8 JSON bytes를 만든다."""
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def _parquet_bytes(rows: dict[str, list]) -> bytes:
    """테스트용 단일-object Parquet bytes를 만든다."""
    buffer = io.BytesIO()
    pq.write_table(pa.table(rows), buffer)
    return buffer.getvalue()


def _station_profile_rows(
    *,
    station_nos: tuple[int, ...] = (1, 2),
    grid_tick: int = 20,
    mean_offset: float = 0.0,
) -> dict[str, list]:
    """Exact 9-column station profile row fixture를 만든다."""
    minutes = tuple(range(0, 1440, grid_tick))
    keys = tuple(
        (station_no, minute) for station_no in station_nos for minute in minutes
    )
    return {
        "station_no": [station_no for station_no, _minute in keys],
        "minute": [minute for _station_no, minute in keys],
        "dow": [0] * len(keys),
        "month": [1] * len(keys),
        "rental_mean": [1.0 + mean_offset] * len(keys),
        "rental_std": [0.1] * len(keys),
        "return_mean": [2.0 + mean_offset] * len(keys),
        "return_std": [0.2] * len(keys),
        "n_samples": [4] * len(keys),
    }


def _station_profile_payload(
    *,
    station_nos: tuple[int, ...] = (1, 2),
    grid_tick: int = 20,
    mean_offset: float = 0.0,
) -> bytes:
    """Exact global minute grid를 가진 station profile Parquet bytes를 만든다."""
    return _parquet_bytes(
        _station_profile_rows(
            station_nos=station_nos,
            grid_tick=grid_tick,
            mean_offset=mean_offset,
        )
    )


def _station_source(rows: dict[str, list]) -> ExplicitImmutablePayload:
    """Content-addressed station master source payload를 만든다."""
    payload = _parquet_bytes(rows)
    digest = sha256_hex(payload)
    return ExplicitImmutablePayload(
        payload=payload,
        byte_sha256=digest,
        uri=f"s3://test-bucket/build-input/station-master/sha256={digest}.parquet",
    )


def _model_payloads(
    model_name: str, profile: dict, categories: list[int]
) -> dict[str, bytes]:
    """Crosswalk을 제외한 inference-required legacy archive payload를 만든다."""
    return {
        "booster_poisson": f"{model_name}-poisson".encode(),
        "booster_q10": f"{model_name}-q10".encode(),
        "booster_q50": f"{model_name}-q50".encode(),
        "booster_q90": f"{model_name}-q90".encode(),
        "conformal_correction": _json_bytes({"correction": 1.0}),
        "effective_profile": _json_bytes(profile),
        "metrics": _json_bytes({"poisson_deviance_test": 1.0}),
        "station_categories": _json_bytes(categories),
    }


def _release_fixture(
    *,
    grid_tick: int = 20,
) -> tuple[
    _MemoryObjectStore,
    _MemoryPointerStore,
    ServingReleaseManifest,
    ExplicitImmutablePayload,
]:
    """두 model snapshot과 release-owned artifact를 실제 publication helper로 만든다."""
    events: list[tuple[str, str]] = []
    store = _MemoryObjectStore(events=events)
    pointer_store = _MemoryPointerStore(events=events)
    source = _station_source({"station_id": ["ST-1", "ST-2"], "station_no": [1, 2]})
    rental_profile = _profile(grid_tick=grid_tick, train_months=12, leaves=63)
    return_profile = _profile(grid_tick=grid_tick, train_months=6, leaves=31)
    rental = publish_model_snapshot(
        model_kind=ModelKind.RENTAL,
        artifact_payloads=_model_payloads("rental", rental_profile, [2, 1]),
        station_source=source,
        object_store=store,
        bucket="test-bucket",
    )
    returned = publish_model_snapshot(
        model_kind=ModelKind.RETURN,
        artifact_payloads=_model_payloads("return", return_profile, [1, 2]),
        station_source=source,
        object_store=store,
        bucket="test-bucket",
    )
    contract_ref = publish_effective_contract(
        _json_bytes(rental_profile),
        object_store=store,
        bucket="test-bucket",
    )
    station_profile = publish_station_profile(
        _station_profile_payload(grid_tick=grid_tick),
        object_store=store,
        bucket="test-bucket",
    )
    manifest = build_serving_release_manifest(
        rental_model_manifest=rental.manifest_ref,
        return_model_manifest=returned.manifest_ref,
        station_profile=station_profile,
        effective_contract=contract_ref,
    )
    return store, pointer_store, manifest, source


def test_effective_contract_ignores_training_only_profile_differences() -> None:
    """Training/LGB 차이는 pair serving contract version을 바꾸지 않는다."""
    rental = build_effective_serving_contract(_profile(train_months=12, leaves=63))
    returned = build_effective_serving_contract(_profile(train_months=6, leaves=31))

    assert rental == returned
    assert effective_contract_version(rental) == effective_contract_version(returned)


@pytest.mark.parametrize(
    "overrides",
    (
        {
            "GRID_TICK_MINUTES": 7,
            "ROLLING_TICK_MINUTES": 7,
            "TRAIN_ANCHOR_TICK_MINUTES": 7,
        },
        {"ROLLING_TICK_MINUTES": 10},
        {"TARGET_HORIZON_MINUTES": 50},
        {"TRAIN_ANCHOR_TICK_MINUTES": 30},
        {"HORIZON_COUNT": 6},
    ),
)
def test_effective_contract_rejects_invalid_grid_anchor_or_gold_horizon(
    overrides: dict[str, int],
) -> None:
    """양수이기만 한 grid/anchor와 12가 아닌 Gold horizon은 계약이 아니다."""
    profile = _profile()
    profile.update(overrides)
    contract_document = {
        key: profile[key]
        for key in (
            "ROLLING_TICK_MINUTES",
            "ROLLING_WINDOW_MINUTES",
            "ROLLING_EMBARGO_MINUTES",
            "TARGET_HORIZON_MINUTES",
            "GRID_TICK_MINUTES",
            "TRAIN_ANCHOR_TICK_MINUTES",
            "HORIZON_COUNT",
        )
    }

    with pytest.raises(ContractViolation):
        build_effective_serving_contract(profile)
    with pytest.raises(ContractViolation):
        parse_effective_serving_contract(canonical_json_bytes(contract_document))


def test_publish_station_profile_rejects_non_exact_schema_before_put() -> None:
    """n_samples를 포함한 exact 9-column schema가 아니면 immutable write를 하지 않는다."""
    rows = _station_profile_rows()
    del rows["n_samples"]
    store = _MemoryObjectStore()

    with pytest.raises(ServingReleasePreflightError, match="exact 9-column"):
        publish_station_profile(
            _parquet_bytes(rows),
            object_store=store,
            bucket="test-bucket",
        )

    assert store.objects == {}


@pytest.mark.parametrize(
    ("column", "value", "match"),
    (
        ("station_no", 1.5, "integer column"),
        ("dow", None, "non-null"),
        ("rental_mean", float("nan"), "finite"),
        ("return_std", -0.1, "nonnegative"),
        ("n_samples", 0, "1..2147483647"),
    ),
)
def test_publish_station_profile_rejects_invalid_scalar_semantics(
    column: str,
    value: object,
    match: str,
) -> None:
    """Profile key/stat/sample scalar 위반은 content-addressed write 전에 실패한다."""
    rows = _station_profile_rows()
    rows[column][0] = value
    store = _MemoryObjectStore()

    with pytest.raises(ServingReleasePreflightError, match=match):
        publish_station_profile(
            _parquet_bytes(rows),
            object_store=store,
            bucket="test-bucket",
        )

    assert store.objects == {}


def test_publish_station_profile_rejects_duplicate_key_or_incomplete_global_grid() -> (
    None
):
    """Logical-key 중복과 한쪽 방향의 minute-grid 구멍을 모두 거부한다."""
    duplicate_rows = _station_profile_rows()
    for values in duplicate_rows.values():
        values.append(values[0])
    with pytest.raises(ServingReleasePreflightError, match="logical key"):
        validate_station_profile_payload(_parquet_bytes(duplicate_rows))

    incomplete_rows = _station_profile_rows()
    keep = [minute != 1420 for minute in incomplete_rows["minute"]]
    incomplete_rows = {
        name: [value for value, include in zip(values, keep, strict=True) if include]
        for name, values in incomplete_rows.items()
    }
    with pytest.raises(ServingReleasePreflightError, match="global minute set"):
        validate_station_profile_payload(_parquet_bytes(incomplete_rows))


def test_model_snapshot_materializes_crosswalk_and_support_from_explicit_source() -> (
    None
):
    """Category 순서와 무관하게 explicit crosswalk로 Gold support ID set을 만든다."""
    store = _MemoryObjectStore()
    source = _station_source({"station_id": ["ST-10", "ST-2"], "station_no": [10, 2]})

    published = publish_model_snapshot(
        model_kind=ModelKind.RENTAL,
        artifact_payloads=_model_payloads("rental", _profile(), [10, 2]),
        station_source=source,
        object_store=store,
        bucket="test-bucket",
    )

    assert published.support_sta_ids.ids == ("ST-10", "ST-2")
    assert [entry.station_no for entry in published.station_crosswalk.entries] == [
        2,
        10,
    ]
    assert published.manifest_ref.uri in store.objects
    assert source.uri in store.objects


def test_model_snapshot_rejects_unmapped_category_before_manifest_publication() -> None:
    """Crosswalk에 없는 model category는 support를 추정하지 않고 fail-closed한다."""
    store = _MemoryObjectStore()
    source = _station_source({"station_id": ["ST-1"], "station_no": [1]})

    with pytest.raises(ContractViolation, match="mapping"):
        publish_model_snapshot(
            model_kind=ModelKind.RETURN,
            artifact_payloads=_model_payloads("return", _profile(), [1, 2]),
            station_source=source,
            object_store=store,
            bucket="test-bucket",
        )

    assert not any("/manifests/" in uri for uri in store.objects)


def test_explicit_station_source_rejects_mutable_uri() -> None:
    """Actual SHA가 filename에 없는 current station master URI는 provenance가 아니다."""
    payload = _parquet_bytes({"station_id": ["ST-1"], "station_no": [1]})

    with pytest.raises(ContractViolation, match="content-addressed"):
        ExplicitImmutablePayload(
            payload=payload,
            byte_sha256=sha256_hex(payload),
            uri="s3://test-bucket/processed_v2/station_master.parquet",
        )


def test_release_manifest_round_trips_exact_canonical_bytes() -> None:
    """Release manifest는 versionless identity SHA version과 exact keys를 보존한다."""
    _, _, manifest, _ = _release_fixture()

    parsed = parse_serving_release_manifest(manifest.canonical_bytes)

    assert parsed == manifest
    assert parsed.release_version.startswith("sha256:")


def test_preflight_reads_all_transitive_model_and_release_references() -> None:
    """Model manifest뿐 아니라 9 artifacts, support, profile binding까지 검증한다."""
    store, _, manifest, _ = _release_fixture()
    store.events.clear()

    result = preflight_serving_release(manifest, store)

    assert result.rental_model.model_kind is ModelKind.RENTAL
    assert result.return_model.model_kind is ModelKind.RETURN
    assert result.station_profile.row_count == 2 * (1440 // 20)
    assert result.station_profile.station_nos == (1, 2)
    assert result.station_profile.grid_tick_minutes == 20
    assert result.station_profile_payload == result.station_profile.payload
    read_uris = {uri for operation, uri in store.events if operation == "read"}
    expected_uris = {
        artifact.uri
        for model in (result.rental_model, result.return_model)
        for artifact in model.artifacts
    }
    expected_uris.update(
        {
            result.rental_model.support_sta_ids.uri,
            result.return_model.support_sta_ids.uri,
            manifest.rental_model_manifest.uri,
            manifest.return_model_manifest.uri,
            manifest.station_profile.uri,
            manifest.effective_contract.uri,
        }
    )
    assert expected_uris <= read_uris


@pytest.mark.parametrize("profile_grid", (10, 60))
def test_preflight_rejects_station_profile_grid_in_either_direction(
    profile_grid: int,
) -> None:
    """Release grid보다 촘촘하거나 성긴 profile global minute set을 모두 거부한다."""
    store, _, manifest, _ = _release_fixture(grid_tick=20)
    station_profile = publish_station_profile(
        _station_profile_payload(grid_tick=profile_grid),
        object_store=store,
        bucket="test-bucket",
    )
    mismatched = build_serving_release_manifest(
        rental_model_manifest=manifest.rental_model_manifest,
        return_model_manifest=manifest.return_model_manifest,
        station_profile=station_profile,
        effective_contract=manifest.effective_contract,
    )

    with pytest.raises(ServingReleasePreflightError, match="release model grid"):
        preflight_serving_release(mismatched, store)


def test_preflight_rejects_profile_station_outside_shared_crosswalk() -> None:
    """Profile station_no는 두 model이 공유하는 exact crosswalk 밖으로 나갈 수 없다."""
    store, _, manifest, _ = _release_fixture()
    station_profile = publish_station_profile(
        _station_profile_payload(station_nos=(1, 2, 3)),
        object_store=store,
        bucket="test-bucket",
    )
    invalid = build_serving_release_manifest(
        rental_model_manifest=manifest.rental_model_manifest,
        return_model_manifest=manifest.return_model_manifest,
        station_profile=station_profile,
        effective_contract=manifest.effective_contract,
    )

    with pytest.raises(ServingReleasePreflightError, match="shared model crosswalk"):
        preflight_serving_release(invalid, store)


def test_preflight_requires_both_model_category_station_numbers_in_profile() -> None:
    """Rental/return category 합집합 중 profile에서 빠진 station은 활성화하지 않는다."""
    store, _, manifest, _ = _release_fixture()
    station_profile = publish_station_profile(
        _station_profile_payload(station_nos=(1,)),
        object_store=store,
        bucket="test-bucket",
    )
    incomplete = build_serving_release_manifest(
        rental_model_manifest=manifest.rental_model_manifest,
        return_model_manifest=manifest.return_model_manifest,
        station_profile=station_profile,
        effective_contract=manifest.effective_contract,
    )

    with pytest.raises(ServingReleasePreflightError, match="모두 포함"):
        preflight_serving_release(incomplete, store)


def test_publish_writes_release_manifest_then_pointer_as_last_mutation() -> None:
    """모든 readback 뒤 release pointer CAS가 유일한 마지막 write다."""
    store, pointer_store, manifest, source = _release_fixture()
    store.events.clear()

    pointer = publish_serving_release(
        manifest,
        station_source=source,
        object_store=store,
        pointer_store=pointer_store,
        release_manifest_uri=(
            "s3://test-bucket/models/serving-release/manifests/"
            f"sha256={manifest.sha256}.json"
        ),
    )

    assert pointer.generation == 0
    assert store.events[-1][0] == "pointer-cas"
    assert parse_serving_release_pointer(pointer_store.payload) == pointer


def test_publish_requires_explicit_feature_build_station_source() -> None:
    """Mutable current master를 다시 읽는 fallback 없이 station source 누락을 거부한다."""
    store, pointer_store, manifest, _ = _release_fixture()

    with pytest.raises(
        ServingReleasePreflightError, match="explicit immutable station"
    ):
        publish_serving_release(
            manifest,
            object_store=store,
            pointer_store=pointer_store,
        )

    assert pointer_store.payload is None


def test_publish_rejects_station_source_different_from_model_crosswalk() -> None:
    """Feature build source와 model crosswalk가 다르면 release pointer를 쓰지 않는다."""
    store, pointer_store, manifest, _ = _release_fixture()
    other_source = _station_source(
        {"station_id": ["ST-1", "ST-999"], "station_no": [1, 2]}
    )

    with pytest.raises(ServingReleasePreflightError, match="station source"):
        publish_serving_release(
            manifest,
            station_source=other_source,
            object_store=store,
            pointer_store=pointer_store,
        )

    assert pointer_store.payload is None


def test_same_release_retry_reuses_pointer_generation() -> None:
    """Exact same release replay는 pointer를 다시 쓰거나 generation을 올리지 않는다."""
    store, pointer_store, manifest, source = _release_fixture()
    uri = (
        "s3://test-bucket/models/serving-release/manifests/"
        f"sha256={manifest.sha256}.json"
    )
    first = publish_serving_release(
        manifest,
        station_source=source,
        object_store=store,
        pointer_store=pointer_store,
        release_manifest_uri=uri,
    )
    pointer_cas_count = sum(operation == "pointer-cas" for operation, _ in store.events)

    second = publish_serving_release(
        manifest,
        station_source=source,
        object_store=store,
        pointer_store=pointer_store,
        release_manifest_uri=uri,
    )

    assert second == first
    assert (
        sum(operation == "pointer-cas" for operation, _ in store.events)
        == pointer_cas_count
    )


def test_runtime_loader_pins_pointer_once_and_returns_typed_transitive_snapshot() -> (
    None
):
    """Inference loader는 pointer를 한 번만 읽고 release/model 전체를 exact 검증한다."""
    store, pointer_store, manifest, source = _release_fixture()
    uri = (
        "s3://test-bucket/models/serving-release/manifests/"
        f"sha256={manifest.sha256}.json"
    )
    pointer = publish_serving_release(
        manifest,
        station_source=source,
        object_store=store,
        pointer_store=pointer_store,
        release_manifest_uri=uri,
    )
    store.events.clear()

    pinned = load_current_serving_release(
        object_store=store,
        pointer_store=pointer_store,
    )

    assert pinned.pointer == pointer
    assert pinned.pointer_payload == pointer.canonical_bytes
    assert pinned.manifest == manifest
    assert pinned.manifest_payload == manifest.canonical_bytes
    assert pinned.preflight.rental_model.model_kind is ModelKind.RENTAL
    assert pinned.preflight.return_model.model_kind is ModelKind.RETURN
    assert (
        pinned.preflight.effective_contract_payload
        == store.objects[manifest.effective_contract.uri]
    )
    assert (
        pinned.preflight.station_profile_payload
        == store.objects[manifest.station_profile.uri]
    )
    rental_poisson = pinned.preflight.rental_snapshot.artifact_payload(
        "booster_poisson"
    )
    assert (
        rental_poisson
        == store.objects[
            next(
                artifact.uri
                for artifact in pinned.preflight.rental_model.artifacts
                if artifact.role == "booster_poisson"
            )
        ]
    )
    assert (
        pinned.preflight.rental_snapshot.support_sta_ids_payload
        == store.objects[pinned.preflight.rental_model.support_sta_ids.uri]
    )
    assert sum(operation == "pointer-read" for operation, _ in store.events) == 1


def test_pointer_cas_conflict_keeps_previous_release_active() -> None:
    """Pointer CAS 장애는 immutable orphan만 남기고 기존 serving pointer를 보존한다."""
    store, pointer_store, manifest, source = _release_fixture()
    uri = (
        "s3://test-bucket/models/serving-release/manifests/"
        f"sha256={manifest.sha256}.json"
    )
    old_pointer = publish_serving_release(
        manifest,
        station_source=source,
        object_store=store,
        pointer_store=pointer_store,
        release_manifest_uri=uri,
    )
    old_payload = pointer_store.payload
    changed_profile = publish_station_profile(
        _station_profile_payload(grid_tick=20, mean_offset=1.0),
        object_store=store,
        bucket="test-bucket",
    )
    changed = build_serving_release_manifest(
        rental_model_manifest=manifest.rental_model_manifest,
        return_model_manifest=manifest.return_model_manifest,
        station_profile=changed_profile,
        effective_contract=manifest.effective_contract,
    )
    pointer_store.fail_next_cas = True

    with pytest.raises(ServingReleasePointerConflictError):
        publish_serving_release(
            changed,
            station_source=source,
            object_store=store,
            pointer_store=pointer_store,
            release_manifest_uri=(
                "s3://test-bucket/models/serving-release/manifests/"
                f"sha256={changed.sha256}.json"
            ),
        )

    assert pointer_store.payload == old_payload
    assert parse_serving_release_pointer(pointer_store.payload) == old_pointer


def test_same_contract_release_update_increments_generation_by_one() -> None:
    """같은 contract의 새 model/profile release는 pointer generation을 정확히 1 올린다."""
    store, pointer_store, manifest, source = _release_fixture()
    first = publish_serving_release(
        manifest,
        station_source=source,
        object_store=store,
        pointer_store=pointer_store,
        release_manifest_uri=(
            "s3://test-bucket/models/serving-release/manifests/"
            f"sha256={manifest.sha256}.json"
        ),
    )
    changed_profile = publish_station_profile(
        _station_profile_payload(grid_tick=20, mean_offset=1.0),
        object_store=store,
        bucket="test-bucket",
    )
    changed = build_serving_release_manifest(
        rental_model_manifest=manifest.rental_model_manifest,
        return_model_manifest=manifest.return_model_manifest,
        station_profile=changed_profile,
        effective_contract=manifest.effective_contract,
    )

    second = publish_serving_release(
        changed,
        station_source=source,
        object_store=store,
        pointer_store=pointer_store,
        release_manifest_uri=(
            "s3://test-bucket/models/serving-release/manifests/"
            f"sha256={changed.sha256}.json"
        ),
    )

    assert first.generation == 0
    assert second.generation == 1


def test_cross_contract_update_requires_explicit_maintenance_gate() -> None:
    """Active release와 다른 contract는 일반 pair publication에서도 자동 전환하지 않는다."""
    store, pointer_store, manifest, source = _release_fixture(grid_tick=20)
    publish_serving_release(
        manifest,
        station_source=source,
        object_store=store,
        pointer_store=pointer_store,
        release_manifest_uri=(
            "s3://test-bucket/models/serving-release/manifests/"
            f"sha256={manifest.sha256}.json"
        ),
    )
    new_store, _, changed, changed_source = _release_fixture(grid_tick=10)
    store.objects.update(new_store.objects)

    with pytest.raises(CrossContractServingReleaseError, match="maintenance"):
        publish_serving_release(
            changed,
            station_source=changed_source,
            object_store=store,
            pointer_store=pointer_store,
            release_manifest_uri=(
                "s3://test-bucket/models/serving-release/manifests/"
                f"sha256={changed.sha256}.json"
            ),
        )


def test_preflight_rejects_changed_referenced_bytes() -> None:
    """Manifest 작성 후 artifact bytes가 바뀌면 pointer에 도달하기 전에 실패한다."""
    store, _, manifest, _ = _release_fixture()
    target_uri = manifest.station_profile.uri
    store.objects[target_uri] = b"corrupted"

    with pytest.raises(ObjectChecksumMismatchError):
        preflight_serving_release(manifest, store)


def test_effective_contract_ref_binds_version_to_exact_bytes_sha() -> None:
    """Effective contract version은 semantic 별칭이 아니라 exact bytes SHA다."""
    payload = build_effective_serving_contract(_profile())
    digest = sha256_hex(payload)
    reference = build_effective_contract_ref(
        payload,
        f"s3://test-bucket/contracts/sha256={digest}.json",
    )

    assert reference.version == f"sha256:{digest}"
    assert isinstance(
        ImmutableArtifactRef(reference.byte_sha256, reference.uri),
        ImmutableArtifactRef,
    )


def test_s3_pointer_store_uses_etag_compare_and_swap() -> None:
    """Concrete S3 adapter는 stale ETag writer를 조건부 PUT에서 거부한다."""
    store = S3ServingReleasePointerStore(bucket="test-bucket")
    key = "models/serving-release/current.json"

    missing = store.read(key)
    store.compare_and_swap(key, missing.version_token, b"first")
    first = store.read(key)
    store.compare_and_swap(key, first.version_token, b"second")

    assert store.read(key).payload == b"second"
    with pytest.raises(ServingReleasePointerConflictError):
        store.compare_and_swap(key, first.version_token, b"stale")
