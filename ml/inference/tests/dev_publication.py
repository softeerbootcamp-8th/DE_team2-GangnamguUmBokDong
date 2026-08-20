"""Pinned inference authority producer의 manifest-last/revision 계약을 검증한다."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from core import s3 as s3_io
from core.gold_publication import (
    Dependency,
    ImmutablePutOutcome,
    ObjectChecksumMismatchError,
    ObjectCollisionError,
    ObjectMissingError,
    build_id_set,
    canonical_json_bytes,
    parse_canonical_json,
    sha256_hex,
)
from core.inference_snapshot import parse_inference_output_parquet
from core.model_snapshot import (
    MODEL_ARTIFACT_ROLES,
    IdSetArtifactRef,
    ModelArtifact,
    ModelKind,
    build_model_snapshot_manifest,
)
from ml_core.serving_contract import SERVING_FEATURE_PROFILE_KEYS

from inference import config
from inference import predict_single as ps
from inference import publication as pub

LOGICAL = datetime(2026, 8, 20, 14, 55, tzinfo=UTC)  # KST 23:55, rollover 검증
OBJECT_BASE_URI = "s3://fixture/gold-authority"
_ARTIFACT_EXTENSIONS = {
    "booster_poisson": "txt",
    "booster_q10": "txt",
    "booster_q50": "txt",
    "booster_q90": "txt",
    "conformal_correction": "json",
    "effective_profile": "json",
    "metrics": "json",
    "station_categories": "json",
    "station_crosswalk": "json",
}


class MemoryObjectStore:
    """ImmutableObjectStore의 exact retry/collision 동작을 메모리로 재현한다."""

    def __init__(self) -> None:
        """빈 URI→bytes map을 만든다."""
        self.objects: dict[str, bytes] = {}

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """Missing/checksum/canonical 검증 뒤 exact bytes를 반환한다."""
        if uri not in self.objects:
            raise ObjectMissingError(f"missing: {uri}")
        payload = self.objects[uri]
        if sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError(f"checksum: {uri}")
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
        """빈 URI만 생성하고 같은 bytes retry만 허용한다."""
        if expected_sha256 is not None and sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError(f"incoming checksum: {uri}")
        if require_canonical_json:
            parse_canonical_json(payload)
        if uri in self.objects:
            if self.objects[uri] == payload:
                return ImmutablePutOutcome.ALREADY_EXISTS
            raise ObjectCollisionError(f"collision: {uri}")
        self.objects[uri] = payload
        return ImmutablePutOutcome.CREATED


class RejectingObjectStore(MemoryObjectStore):
    """지정 namespace의 put/readback 실패를 주입하는 object-store fixture다."""

    def __init__(
        self, *, reject_put: str | None = None, reject_read: str | None = None
    ) -> None:
        """실패시킬 URI substring을 고정한다."""
        super().__init__()
        self.reject_put = reject_put
        self.reject_read = reject_read

    def put_once(self, uri, payload, **kwargs):
        """지정 namespace write를 collision으로 실패시킨다."""
        if self.reject_put is not None and self.reject_put in uri:
            raise ObjectCollisionError(f"injected collision: {uri}")
        return super().put_once(uri, payload, **kwargs)

    def read_bytes(self, uri, expected_sha256, **kwargs):
        """지정 namespace readback을 access failure처럼 실패시킨다."""
        if self.reject_read is not None and self.reject_read in uri:
            raise ObjectChecksumMismatchError(f"injected readback: {uri}")
        return super().read_bytes(uri, expected_sha256, **kwargs)


@dataclass
class _Snapshot:
    """Producer가 사용하는 verified snapshot 표면만 제공하는 fixture다."""

    manifest: object
    support_sta_ids: object
    artifacts: tuple[object, ...]
    payload_by_role: dict[str, bytes]

    def artifact_payload(self, role: str) -> bytes:
        """Role 하나의 exact fixture payload를 반환한다."""
        return self.payload_by_role[role]


def _uri(namespace: str, payload: bytes, extension: str) -> str:
    """Fixture content-addressed URI를 만든다."""
    return f"s3://fixture/{namespace}/sha256={sha256_hex(payload)}.{extension}"


def _runtime_contract() -> dict[str, int]:
    """현재 inference process의 exact 7-key serving contract를 반환한다."""
    return {key: getattr(config, key) for key in SERVING_FEATURE_PROFILE_KEYS}


def _station_profile_payload(station_nos: tuple[int, ...]) -> bytes:
    """Exact 9-column/dtype의 작은 valid station profile Parquet을 만든다."""
    count = len(station_nos)
    table = pa.Table.from_arrays(
        (
            pa.array(station_nos, type=pa.int16()),
            pa.array([0] * count, type=pa.int16()),
            pa.array([0] * count, type=pa.int8()),
            pa.array([1] * count, type=pa.int8()),
            pa.array([1.0] * count, type=pa.float32()),
            pa.array([0.0] * count, type=pa.float32()),
            pa.array([1.0] * count, type=pa.float32()),
            pa.array([0.0] * count, type=pa.float32()),
            pa.array([1] * count, type=pa.int32()),
        ),
        names=pub._PROFILE_COLUMNS,
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def _parquet_bytes(table: pa.Table) -> bytes:
    """Arrow table을 fixture Parquet bytes로 직렬화한다."""
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def _model_snapshot(
    model_kind: ModelKind,
    station_nos: tuple[int, ...],
    sta_ids: tuple[str, ...],
    contract_version: str,
) -> _Snapshot:
    """Core model manifest와 producer가 쓰는 retained payload fixture를 만든다."""
    categories = canonical_json_bytes(list(station_nos))
    payload_by_role = {
        role: (
            categories
            if role == "station_categories"
            else canonical_json_bytes({"fixture": f"{model_kind.value}-{role}"})
            if _ARTIFACT_EXTENSIONS[role] == "json"
            else f"{model_kind.value}-{role}".encode()
        )
        for role in MODEL_ARTIFACT_ROLES
    }
    artifacts = tuple(
        ModelArtifact(
            byte_sha256=sha256_hex(payload_by_role[role]),
            role=role,
            uri=_uri(
                f"models/{model_kind.value}/{role}",
                payload_by_role[role],
                _ARTIFACT_EXTENSIONS[role],
            ),
        )
        for role in MODEL_ARTIFACT_ROLES
    )
    support = build_id_set(sta_ids)
    support_ref = IdSetArtifactRef(
        byte_sha256=support.sha256,
        id_count=len(support.ids),
        schema_version=support.schema_version,
        uri=_uri(f"models/{model_kind.value}/support", support.canonical_bytes, "json"),
    )
    manifest = build_model_snapshot_manifest(
        model_kind=model_kind,
        effective_contract_version=contract_version,
        artifacts=artifacts,
        support_sta_ids=support_ref,
    )
    verified = tuple(
        SimpleNamespace(reference=artifact, payload=payload_by_role[artifact.role])
        for artifact in manifest.artifacts
    )
    return _Snapshot(
        manifest=manifest,
        support_sta_ids=support,
        artifacts=verified,
        payload_by_role=payload_by_role,
    )


def _pinned_release(
    *,
    rental_station_nos: tuple[int, ...] = (1,),
    rental_sta_ids: tuple[str, ...] = ("ST-1",),
    return_station_nos: tuple[int, ...] = (1,),
    return_sta_ids: tuple[str, ...] = ("ST-1",),
    contract: dict[str, int] | None = None,
    profile_station_nos: tuple[int, ...] | None = None,
) -> object:
    """Pointer 한 번으로 반환되는 retained release fixture를 만든다."""
    contract_payload = canonical_json_bytes(contract or _runtime_contract())
    contract_version = f"sha256:{sha256_hex(contract_payload)}"
    rental = _model_snapshot(
        ModelKind.RENTAL,
        rental_station_nos,
        rental_sta_ids,
        contract_version,
    )
    returned = _model_snapshot(
        ModelKind.RETURN,
        return_station_nos,
        return_sta_ids,
        contract_version,
    )
    rental_manifest_uri = _uri(
        "models/rental/manifest", rental.manifest.canonical_bytes, "json"
    )
    return_manifest_uri = _uri(
        "models/return/manifest", returned.manifest.canonical_bytes, "json"
    )
    release_sha = "a" * 64
    release_uri = f"s3://fixture/releases/sha256={release_sha}.json"
    station_nos = profile_station_nos or tuple(
        sorted(set(rental_station_nos).union(return_station_nos))
    )
    return SimpleNamespace(
        pointer=SimpleNamespace(
            release_manifest_byte_sha256=release_sha,
            release_manifest_uri=release_uri,
        ),
        manifest=SimpleNamespace(
            effective_contract=SimpleNamespace(version=contract_version),
            release_version=f"sha256:{'b' * 64}",
            rental_model_manifest=SimpleNamespace(uri=rental_manifest_uri),
            return_model_manifest=SimpleNamespace(uri=return_manifest_uri),
        ),
        preflight=SimpleNamespace(
            rental_snapshot=rental,
            return_snapshot=returned,
            effective_contract_payload=contract_payload,
            station_profile_payload=_station_profile_payload(station_nos),
        ),
    )


def _dependency(logical: datetime = LOGICAL) -> Dependency:
    """Station publication dependency fixture를 만든다."""
    return Dependency(
        artifact_set_sha256="1" * 64,
        input_fingerprint_sha256="2" * 64,
        logical_dttm=logical,
        manifest_uri="s3://fixture/station-publication.json",
        publication_key="station",
        revision_no=0,
    )


def _predictor_state() -> tuple[dict[str, object], object]:
    """Captured source bytes와 prediction 값을 바꿀 수 있는 complete predictor를 만든다."""
    state: dict[str, object] = {"payload": b"silver-a", "prediction": 1.25}

    def _predictor(**kwargs):
        s3_io._record_object_read("silver/source.parquet", state["payload"])
        base = datetime.fromisoformat(
            f"{kwargs['date']}T{kwargs['hour']:02d}:{kwargs['minute']:02d}:00"
        )
        rows = []
        for station_id in kwargs["station_ids"]:
            for horizon in range(1, 13):
                target = base + timedelta(hours=horizon - 1)
                rows.append(
                    {
                        "station_id": station_id,
                        "date": target.strftime("%Y-%m-%d"),
                        "hour": target.hour,
                        "minute": target.minute,
                        "horizon": horizon,
                        "rental": {"pred_mean": state["prediction"]},
                        "return": {"pred_mean": state["prediction"]},
                    }
                )
        count = len(kwargs["station_ids"]) * 12
        return {
            "results": rows,
            "failed": [],
            "expected_count": count,
            "actual_count": count,
        }

    return state, _predictor


@pytest.fixture
def _pinned_scoring_stubs(monkeypatch):
    """LightGBM text parsing만 대체하고 producer의 context 배선은 유지한다."""

    @contextmanager
    def _context(_models):
        yield

    monkeypatch.setattr(pub, "build_pinned_scoring_model", lambda _payloads: object())
    monkeypatch.setattr(pub, "use_pinned_scoring_models", _context)


def _install_release_loader(monkeypatch, pinned: object) -> list[dict[str, object]]:
    """Pinned release를 반환하며 호출 횟수/인자를 기록하는 loader를 설치한다."""
    calls: list[dict[str, object]] = []

    def _load(**kwargs):
        calls.append(kwargs)
        return pinned

    monkeypatch.setattr(pub, "load_current_serving_release", _load)
    return calls


def test_success_exact_replay_and_correction_are_revisioned_manifest_last(
    monkeypatch,
    _pinned_scoring_stubs,
):
    """동일 bytes는 rev/URI 재사용, 변경된 같은 logical은 rev+1로 공개한다."""
    pinned = _pinned_release()
    calls = _install_release_loader(monkeypatch, pinned)
    state, predictor = _predictor_state()
    store = MemoryObjectStore()
    catalog = pub.InMemoryInferenceRevisionCatalog()
    kwargs = {
        "logical_dttm": LOGICAL,
        "station_dependency": _dependency(),
        "expected_sta_ids": build_id_set(["ST-1"]),
        "object_base_uri": OBJECT_BASE_URI,
        "object_store": store,
        "revision_catalog": catalog,
        "predictor": predictor,
    }

    first = pub.run_and_publish_inference(**kwargs)
    replay = pub.run_and_publish_inference(**kwargs)
    state.update(payload=b"silver-b", prediction=2.5)
    corrected = pub.run_and_publish_inference(**kwargs)

    assert len(calls) == 3  # run마다 pointer를 정확히 한 번 pin한다.
    assert first.status is pub.InferenceRunStatus.PUBLISHED
    assert replay.status is pub.InferenceRunStatus.REPLAYED
    assert replay.manifest_uri == first.manifest_uri
    assert replay.manifest.revision_no == first.manifest.revision_no == 0
    assert corrected.status is pub.InferenceRunStatus.PUBLISHED
    assert corrected.manifest.revision_no == 1
    assert corrected.manifest_uri != first.manifest_uri
    assert corrected.manifest.output.row_count == 12
    assert corrected.manifest.inputs[0].role == (
        f"s3_input_{sha256_hex(b'silver/source.parquet')}"
    )
    output_payload = store.read_bytes(
        corrected.manifest.output.uri,
        corrected.manifest.output.byte_sha256,
    )
    output = parse_inference_output_parquet(
        output_payload,
        logical_dttm=LOGICAL,
        expected_sta_ids=build_id_set(["ST-1"]),
    ).to_pandas()
    rollover = output.loc[output["horizon"] == 2].iloc[0]
    assert str(rollover["date"]) == "2026-08-21"
    assert (rollover["hour"], rollover["minute"]) == (0, 55)
    records = catalog.snapshot(LOGICAL).records
    assert tuple(record.revision_no for record in records) == (0, 1)
    assert sum("/inference/manifests/" in uri for uri in store.objects) == 2


def test_partial_failure_writes_no_output_manifest_or_catalog_record(
    monkeypatch,
    _pinned_scoring_stubs,
):
    """Station 하나라도 failed면 authority object와 revision claim을 만들지 않는다."""
    _install_release_loader(monkeypatch, _pinned_release())
    store = MemoryObjectStore()
    catalog = pub.InMemoryInferenceRevisionCatalog()

    def _partial(**_kwargs):
        s3_io._record_object_read("silver/source.parquet", b"partial")
        return {
            "results": [],
            "failed": [{"station_id": "ST-1", "error": "boom"}],
            "expected_count": 12,
            "actual_count": 0,
        }

    with pytest.raises(pub.InferencePublicationError, match="partial/failed"):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set(["ST-1"]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=store,
            revision_catalog=catalog,
            predictor=_partial,
        )

    assert not any("/outputs/" in uri or "/manifests/" in uri for uri in store.objects)
    assert catalog.snapshot(LOGICAL).records == ()


def test_input_drift_fails_before_authority_manifest(
    monkeypatch, _pinned_scoring_stubs
):
    """같은 mutable source key의 두 번째 GET bytes가 달라지면 run을 중단한다."""
    _install_release_loader(monkeypatch, _pinned_release())
    store = MemoryObjectStore()
    catalog = pub.InMemoryInferenceRevisionCatalog()

    def _drift(**_kwargs):
        s3_io._record_object_read("silver/drift.parquet", b"before")
        s3_io._record_object_read("silver/drift.parquet", b"after")

    with pytest.raises(s3_io.S3InputDriftError, match="run 중 변경"):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set(["ST-1"]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=store,
            revision_catalog=catalog,
            predictor=_drift,
        )

    assert not any("/manifests/" in uri for uri in store.objects)
    assert catalog.snapshot(LOGICAL).records == ()


def test_disjoint_model_support_publishes_true_empty_without_predictor(
    monkeypatch,
    _pinned_scoring_stubs,
):
    """두 nonempty model support 교집합이 비면 output 없는 EMPTY authority를 만든다."""
    pinned = _pinned_release(
        rental_station_nos=(1,),
        rental_sta_ids=("ST-1",),
        return_station_nos=(2,),
        return_sta_ids=("ST-2",),
    )
    _install_release_loader(monkeypatch, pinned)
    store = MemoryObjectStore()
    catalog = pub.InMemoryInferenceRevisionCatalog()

    def _must_not_run(**_kwargs):
        raise AssertionError("EMPTY support에서는 predictor를 호출하면 안 됩니다.")

    result = pub.run_and_publish_inference(
        logical_dttm=LOGICAL,
        station_dependency=_dependency(),
        expected_sta_ids=build_id_set([]),
        object_base_uri=OBJECT_BASE_URI,
        object_store=store,
        revision_catalog=catalog,
        predictor=_must_not_run,
    )

    assert result.manifest.status.value == "empty"
    assert result.manifest.output is None
    assert result.manifest.counts.expected_row_count == 0
    assert result.manifest.inputs == ()


def test_support_mismatch_fails_before_predictor(monkeypatch, _pinned_scoring_stubs):
    """Caller expected IDs가 pinned support 교집합과 다르면 fail-closed한다."""
    _install_release_loader(monkeypatch, _pinned_release())
    with pytest.raises(pub.InferencePublicationError, match="support 교집합"):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set(["ST-X"]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=MemoryObjectStore(),
            revision_catalog=pub.InMemoryInferenceRevisionCatalog(),
        )


def test_runtime_contract_mismatch_fails_before_profile_or_scoring(
    monkeypatch,
    _pinned_scoring_stubs,
):
    """Cross-contract release를 old runtime으로 해석해 success를 쓰지 않는다."""
    contract = _runtime_contract()
    contract["GRID_TICK_MINUTES"] = 10
    contract["ROLLING_TICK_MINUTES"] = 10
    _install_release_loader(monkeypatch, _pinned_release(contract=contract))
    store = MemoryObjectStore()

    with pytest.raises(pub.InferencePublicationError, match="runtime config"):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set(["ST-1"]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=store,
            revision_catalog=pub.InMemoryInferenceRevisionCatalog(),
        )
    assert not any("/manifests/" in uri for uri in store.objects)


def test_station_profile_must_cover_both_model_category_sets(
    monkeypatch,
    _pinned_scoring_stubs,
):
    """SHA만 맞는 partial profile이 model support station_no를 누락하면 거부한다."""
    pinned = _pinned_release(
        rental_station_nos=(1,),
        rental_sta_ids=("ST-1",),
        return_station_nos=(2,),
        return_sta_ids=("ST-2",),
        profile_station_nos=(1,),
    )
    _install_release_loader(monkeypatch, pinned)

    with pytest.raises(pub.InferencePublicationError, match="station_no를 누락"):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set([]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=MemoryObjectStore(),
            revision_catalog=pub.InMemoryInferenceRevisionCatalog(),
        )


def test_station_profile_semantics_reject_empty_duplicate_and_nonfinite_values():
    """Release preflight의 SHA 검증 뒤 producer가 profile 의미도 fail-closed한다."""
    valid = pq.read_table(pa.BufferReader(_station_profile_payload((1,))))
    duplicate = pa.concat_tables([valid, valid])
    nonfinite = valid.set_column(
        valid.schema.get_field_index("rental_mean"),
        "rental_mean",
        pa.array([float("nan")], type=pa.float32()),
    )
    empty = valid.slice(0, 0)

    pub._validate_station_profile_payload(
        _parquet_bytes(valid),
        required_station_nos={1},
    )
    with pytest.raises(pub.InferencePublicationError, match="0행"):
        pub._validate_station_profile_payload(
            _parquet_bytes(empty),
            required_station_nos={1},
        )
    with pytest.raises(pub.InferencePublicationError, match="logical key가 중복"):
        pub._validate_station_profile_payload(
            _parquet_bytes(duplicate),
            required_station_nos={1},
        )
    with pytest.raises(pub.InferencePublicationError, match="finite nonnegative"):
        pub._validate_station_profile_payload(
            _parquet_bytes(nonfinite),
            required_station_nos={1},
        )


def test_stale_logical_fails_before_serving_pointer_read(monkeypatch):
    """Global latest보다 과거 logical은 model pointer도 읽지 않고 거부한다."""
    catalog = pub.InMemoryInferenceRevisionCatalog()
    later = LOGICAL + timedelta(minutes=5)
    catalog.claim(
        pub.InferenceRevisionRecord(
            logical_dttm=later,
            revision_no=0,
            manifest_byte_sha256="c" * 64,
            manifest_uri="s3://fixture/inference/sha256=" + "c" * 64 + ".json",
        )
    )
    monkeypatch.setattr(
        pub,
        "load_current_serving_release",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("pointer read")),
    )

    with pytest.raises(pub.InferenceStaleError):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set(["ST-1"]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=MemoryObjectStore(),
            revision_catalog=catalog,
        )


def test_catalog_change_during_compute_is_concurrent_writer_conflict(
    monkeypatch,
    _pinned_scoring_stubs,
):
    """계산 시작 snapshot 이후 catalog가 바뀌면 manifest write 전에 중단한다."""
    _install_release_loader(monkeypatch, _pinned_release())
    state, predictor = _predictor_state()
    del state
    store = MemoryObjectStore()

    class _ChangingCatalog(pub.InMemoryInferenceRevisionCatalog):
        def __init__(self):
            super().__init__()
            self.read_count = 0

        def snapshot(self, logical_dttm):
            self.read_count += 1
            snapshot = super().snapshot(logical_dttm)
            if self.read_count == 2:
                return pub.InferenceCatalogSnapshot(
                    records=snapshot.records,
                    latest_logical_dttm=logical_dttm + timedelta(minutes=5),
                )
            return snapshot

    catalog = _ChangingCatalog()
    with pytest.raises(pub.InferenceRevisionConflictError, match="catalog가 변경"):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set(["ST-1"]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=store,
            revision_catalog=catalog,
            predictor=predictor,
        )
    assert not any("/manifests/" in uri for uri in store.objects)


@pytest.mark.parametrize("failure", ["collision", "readback"])
def test_output_collision_or_readback_failure_never_claims_manifest(
    monkeypatch,
    _pinned_scoring_stubs,
    failure,
):
    """Output put/readback이 완전하지 않으면 catalog와 authority manifest를 건드리지 않는다."""
    _install_release_loader(monkeypatch, _pinned_release())
    _state, predictor = _predictor_state()
    store = RejectingObjectStore(
        reject_put="/inference/outputs/" if failure == "collision" else None,
        reject_read="/inference/outputs/" if failure == "readback" else None,
    )
    catalog = pub.InMemoryInferenceRevisionCatalog()

    with pytest.raises((ObjectCollisionError, ObjectChecksumMismatchError)):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set(["ST-1"]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=store,
            revision_catalog=catalog,
            predictor=predictor,
        )

    assert catalog.snapshot(LOGICAL).records == ()
    assert not any("/manifests/" in uri for uri in store.objects)


def test_manifest_collision_leaves_no_authority_bytes_and_retryable_reservation(
    monkeypatch,
    _pinned_scoring_stubs,
):
    """Catalog claim 뒤 manifest put collision은 기존 bytes를 덮지 않고 slot만 남긴다."""
    _install_release_loader(monkeypatch, _pinned_release())
    _state, predictor = _predictor_state()
    store = RejectingObjectStore(reject_put="/inference/manifests/")
    catalog = pub.InMemoryInferenceRevisionCatalog()

    with pytest.raises(ObjectCollisionError):
        pub.run_and_publish_inference(
            logical_dttm=LOGICAL,
            station_dependency=_dependency(),
            expected_sta_ids=build_id_set(["ST-1"]),
            object_base_uri=OBJECT_BASE_URI,
            object_store=store,
            revision_catalog=catalog,
            predictor=predictor,
        )

    assert len(catalog.snapshot(LOGICAL).records) == 1
    assert not any("/manifests/" in uri for uri in store.objects)


def test_authority_context_uses_pinned_profile_instead_of_legacy_global_cache():
    """이전 offline 호출이 채운 mutable profile cache를 authority run이 재사용하지 않는다."""
    ps._station_profile_station_index = {999: 0}
    ps._station_profile_values = pa.array([999.0]).to_numpy()

    with ps.authority_inference_run(_station_profile_payload((1,))):
        station_index, values = ps._get_station_profile()

    assert station_index == {1: 0}
    assert values.shape[0] == 1
    assert ps._station_profile_values is None
