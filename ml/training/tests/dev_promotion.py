"""모델 승격 정책과 포인터 전환을 검증한다.

개별 challenger 판정뿐 아니라 rental/return pair serving release의 원자적 전환과
fail-closed 조건을 함께 확인한다.
"""

import io
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from core import s3 as s3_io
from core.gold_publication import sha256_hex
from core.model_snapshot import (
    ModelKind,
    StationCrosswalkEntry,
    build_station_crosswalk,
)
from ml_core import common_config, scoring
from ml_core.paths import (
    model_json_key,
    model_key,
    read_champion_prefix,
    serving_release_pointer_key,
    write_champion_pointer,
)
from ml_core.serving_contract import ServingProfileContractError
from ml_core.serving_release import (
    CrossContractServingReleaseError,
    EffectiveContractRef,
    ExplicitImmutablePayload,
    ImmutableArtifactRef,
    ModelManifestRef,
    build_effective_serving_contract,
    build_serving_release_manifest,
    load_current_serving_release,
)

from training.promotion import (
    ChampionAlreadyExistsError,
    PairServingReleaseRequiredError,
    bootstrap_challenger,
    prepare_and_promote_serving_release_pair,
    promote_challenger,
    promote_serving_release_pair,
    should_promote,
)

_CHAMPION = {"poisson_deviance_test": 1.0, "p10_p90_coverage_calibrated_test": 0.83}


@pytest.fixture(autouse=True)
def _clear_champion_prefix_cache():
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()
    scoring.validate_champion_serving_contract.cache_clear()
    yield
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()
    scoring.validate_champion_serving_contract.cache_clear()


def _write_profile(model_name: str, archive_prefix: str, profile: dict | None = None) -> dict:
    """승격 대상 아카이브에 effective profile을 저장하고 그 값을 반환한다."""
    payload = profile or common_config.effective_profile()
    s3_io.write_json(model_json_key(model_name, "profile", archive_prefix), payload)
    return payload


def _release_manifest(profile: dict):
    """Promotion gate 테스트용 valid pair release manifest를 만든다."""
    contract_payload = build_effective_serving_contract(profile)
    contract_sha = sha256_hex(contract_payload)
    contract_version = f"sha256:{contract_sha}"

    def _model_ref(model_kind: ModelKind, digest: str) -> ModelManifestRef:
        """지정 kind의 content-addressed model manifest ref를 만든다."""
        return ModelManifestRef(
            byte_sha256=digest,
            effective_contract_version=contract_version,
            model_kind=model_kind,
            model_version=f"sha256:{digest}",
            uri=(
                f"s3://test-bucket/models/{model_kind.value}/"
                f"sha256={digest}.json"
            ),
        )

    profile_sha = "c" * 64
    return build_serving_release_manifest(
        rental_model_manifest=_model_ref(ModelKind.RENTAL, "a" * 64),
        return_model_manifest=_model_ref(ModelKind.RETURN, "b" * 64),
        station_profile=ImmutableArtifactRef(
            byte_sha256=profile_sha,
            uri=f"s3://test-bucket/profiles/sha256={profile_sha}.parquet",
        ),
        effective_contract=EffectiveContractRef(
            byte_sha256=contract_sha,
            uri=f"s3://test-bucket/contracts/sha256={contract_sha}.json",
            version=contract_version,
        ),
    )


def _station_source() -> ExplicitImmutablePayload:
    """Pair promotion gate에 전달할 canonical immutable crosswalk source를 만든다."""
    crosswalk = build_station_crosswalk(
        [StationCrosswalkEntry(station_no=1, sta_id="ST-1")]
    )
    return ExplicitImmutablePayload(
        payload=crosswalk.canonical_bytes,
        byte_sha256=crosswalk.sha256,
        uri=(
            "s3://test-bucket/build-input/station-crosswalk/"
            f"sha256={crosswalk.sha256}.json"
        ),
    )


def _parquet_bytes(rows: dict[str, list]) -> bytes:
    """Production orchestration 테스트용 single-object Parquet bytes를 만든다."""
    buffer = io.BytesIO()
    pq.write_table(pa.table(rows), buffer)
    return buffer.getvalue()


def _station_profile_rows(
    station_nos: tuple[int, ...] = (1, 2),
    *,
    grid_tick_minutes: int = 20,
) -> dict[str, list]:
    """전역 tick과 model station coverage를 가진 최소 valid profile 행을 만든다."""
    minutes = list(range(0, 1440, grid_tick_minutes))
    repeated_station_nos = [
        station_nos[index % len(station_nos)] for index in range(len(minutes))
    ]
    row_count = len(minutes)
    return {
        "station_no": repeated_station_nos,
        "minute": minutes,
        "dow": [0] * row_count,
        "month": [1] * row_count,
        "rental_mean": [1.0] * row_count,
        "rental_std": [0.0] * row_count,
        "return_mean": [1.0] * row_count,
        "return_std": [0.0] * row_count,
        "n_samples": [1] * row_count,
    }


def _write_model_archive(
    model_name: str,
    archive_prefix: str,
    *,
    profile: dict,
    categories: list[int],
) -> tuple[str, ...]:
    """기존 training archive의 serving artifact 8개를 moto S3에 쓴다."""
    keys: list[str] = []
    for suffix in ("poisson", "q10", "q50", "q90"):
        key = model_key(model_name, suffix, archive_prefix)
        s3_io.put_object_bytes(key, f"{model_name}-{suffix}".encode())
        keys.append(key)
    json_payloads = {
        "conformal_correction": {"correction": 1.0, "target_coverage": 0.8},
        "metrics": {"poisson_deviance_test": 1.0},
        "profile": profile,
        "station_categories": categories,
    }
    for kind, payload in json_payloads.items():
        key = model_json_key(model_name, kind, archive_prefix)
        s3_io.write_json(key, payload)
        keys.append(key)
    return tuple(keys)


def test_should_promote_when_no_champion_exists_yet():
    promote, reasons = should_promote({"poisson_deviance_test": 999.0, "p10_p90_coverage_calibrated_test": 0.0}, None)
    assert promote is True
    assert any("챔피언이 아직 없음" in r for r in reasons)


def test_should_promote_when_challenger_improves_deviance_and_coverage_in_range():
    challenger = {"poisson_deviance_test": 0.9, "p10_p90_coverage_calibrated_test": 0.83}
    promote, reasons = should_promote(challenger, _CHAMPION)
    assert promote is True
    assert len(reasons) == 2


def test_should_reject_when_challenger_deviance_is_worse():
    challenger = {"poisson_deviance_test": 1.1, "p10_p90_coverage_calibrated_test": 0.83}
    promote, reasons = should_promote(challenger, _CHAMPION)
    assert promote is False
    assert any("미달" in r and "deviance" in r for r in reasons)


def test_should_reject_when_coverage_out_of_range():
    lo = common_config.CONFORMAL_TARGET_COVERAGE - common_config.COVERAGE_DRIFT_THRESHOLD
    challenger = {"poisson_deviance_test": 0.9, "p10_p90_coverage_calibrated_test": lo - 0.01}
    promote, reasons = should_promote(challenger, _CHAMPION)
    assert promote is False
    assert any("미달" in r and "coverage" in r for r in reasons)


def test_should_promote_when_deviance_exactly_ties_champion():
    challenger = {"poisson_deviance_test": _CHAMPION["poisson_deviance_test"], "p10_p90_coverage_calibrated_test": 0.83}
    promote, _ = should_promote(challenger, _CHAMPION)
    assert promote is True


def test_promote_challenger_points_champion_at_archive_prefix():
    """더 이상 파일을 복사하지 않는다 — 포인터가 archive_prefix를 가리키는지만 확인한다."""
    archive_prefix = "models/archive/dt=2026-08-17/default"
    _write_profile("rental", archive_prefix)

    promote_challenger("rental", archive_prefix)

    assert read_champion_prefix("rental") == archive_prefix


def test_promote_challenger_does_not_touch_other_model_names_pointer():
    """rental을 승격해도 return의 챔피언 포인터는 그대로여야 한다."""
    return_prefix = "models/archive/dt=2026-08-01/default"
    _write_profile("return", return_prefix)
    promote_challenger("return", return_prefix)

    rental_prefix = "models/archive/dt=2026-08-17/default"
    _write_profile("rental", rental_prefix)
    promote_challenger("rental", rental_prefix)

    assert read_champion_prefix("return") == return_prefix


def test_promote_challenger_invalidates_scoring_caches_so_repromotion_is_consistent():
    """**사용자 질문 재현**: "학습해봤더니 구려서 같은 프로세스 안에서 계속
    재학습→재승격"을 반복하면, 재승격 직후 다음 채점부터는 read_champion_prefix()/
    load_conformal_correction()이 전부 새 archive 하나로 일관되게 나와야 한다.
    write_champion_pointer() 혼자 read_champion_prefix만 비우면 scoring.py 쪽
    캐시가 옛 archive에 머물러 오히려 더 나쁜 불일치가 생긴다(dev_champion_pointer.py/
    dev_scoring.py의 회귀 테스트가 그 실패 모드를 고정해둠) — promote_challenger()가
    셋을 한꺼번에 비워서 막는다."""
    old_prefix = "models/archive/dt=2026-08-17/default"
    _write_profile("rental", old_prefix)
    promote_challenger("rental", old_prefix)
    s3_io.write_json(f"{old_prefix}/rental_conformal_correction.json", {"correction": 1.5, "target_coverage": 0.8})
    assert scoring.load_conformal_correction("rental") == 1.5  # 캐시를 채워둔다
    assert scoring.validate_champion_serving_contract("rental")
    assert scoring.validate_champion_serving_contract.cache_info().currsize == 1

    new_prefix = "models/archive/dt=2026-08-18/default"
    _write_profile("rental", new_prefix)
    s3_io.write_json(f"{new_prefix}/rental_conformal_correction.json", {"correction": 9.9, "target_coverage": 0.8})
    promote_challenger("rental", new_prefix)

    assert read_champion_prefix("rental") == new_prefix
    assert scoring.validate_champion_serving_contract.cache_info().currsize == 0
    assert scoring.load_conformal_correction("rental") == 9.9


def test_promote_challenger_does_not_copy_any_archive_files():
    """archive는 immutable하게 그대로 둔다 — 챔피언 자리(MODELS_PREFIX 루트)에
    파일이 새로 생기지 않아야 한다."""
    archive_prefix = "models/archive/dt=2026-08-17/default"
    _write_profile("rental", archive_prefix)
    s3_io.put_object_bytes(f"{archive_prefix}/rental_poisson.txt", b"rental-booster-bytes")

    promote_challenger("rental", archive_prefix)

    assert s3_io.get_object_bytes("models/rental_poisson.txt") is None
    # archive 원본은 그대로 남아있어야 한다.
    assert s3_io.get_object_bytes(f"{archive_prefix}/rental_poisson.txt") == b"rental-booster-bytes"


def test_promote_challenger_bootstraps_when_other_champion_does_not_exist():
    """최초 배포에서는 반대 모델 포인터가 아직 없어도 첫 모델을 승격할 수 있다."""
    archive_prefix = "models/archive/dt=2026-08-17/bootstrap"
    _write_profile("rental", archive_prefix)

    promote_challenger("rental", archive_prefix)

    assert read_champion_prefix("rental") == archive_prefix
    with pytest.raises(FileNotFoundError):
        read_champion_prefix("return")


def test_bootstrap_challenger_refuses_to_overwrite_existing_same_model_pointer():
    """최초 승격 전용 경로는 이미 생긴 같은 모델 챔피언을 절대 교체하지 않는다."""
    old_prefix = "models/archive/dt=2026-08-17/initial"
    _write_profile("rental", old_prefix)
    promote_challenger("rental", old_prefix)

    new_prefix = "models/archive/dt=2026-08-18/accidental-rerun"
    _write_profile("rental", new_prefix)
    with pytest.raises(ChampionAlreadyExistsError, match="이미 존재"):
        bootstrap_challenger("rental", new_prefix)

    read_champion_prefix.cache_clear()
    assert read_champion_prefix("rental") == old_prefix


def test_bootstrap_challenger_still_runs_serving_profile_guard():
    """챔피언이 없어도 현재 서빙과 다른 profile을 최초 포인터로 만들 수 없다."""
    archive_prefix = "models/archive/dt=2026-08-17/incompatible-bootstrap"
    profile = common_config.effective_profile()
    profile["GRID_TICK_MINUTES"] += 5
    _write_profile("return", archive_prefix, profile)

    with pytest.raises(ServingProfileContractError, match="GRID_TICK_MINUTES"):
        bootstrap_challenger("return", archive_prefix)

    with pytest.raises(FileNotFoundError):
        read_champion_prefix("return")


def test_promote_challenger_rejects_profile_incompatible_with_active_serving():
    """현재 실시간 feature 계약과 다른 챌린저는 포인터를 쓰기 전에 거부한다."""
    archive_prefix = "models/archive/dt=2026-08-17/incompatible"
    profile = common_config.effective_profile()
    profile["ROLLING_EMBARGO_MINUTES"] += 5
    _write_profile("rental", archive_prefix, profile)

    with pytest.raises(PairServingReleaseRequiredError, match="pair.*ROLLING_EMBARGO_MINUTES"):
        promote_challenger("rental", archive_prefix)

    read_champion_prefix.cache_clear()
    with pytest.raises(FileNotFoundError):
        read_champion_prefix("rental")


def test_promote_challenger_rejects_contract_different_from_other_champion():
    """대여와 반납 챔피언이 서로 다른 feature 의미를 갖는 상태를 만들지 않는다."""
    return_prefix = "models/archive/dt=2026-08-01/legacy-mismatch"
    return_profile = common_config.effective_profile()
    return_profile["TARGET_HORIZON_MINUTES"] += 30
    _write_profile("return", return_prefix, return_profile)
    # 이미 잘못 승격된 레거시 상태를 재현하려고 저수준 포인터 함수를 직접 쓴다.
    write_champion_pointer("return", return_prefix)

    rental_prefix = "models/archive/dt=2026-08-17/current"
    _write_profile("rental", rental_prefix)

    with pytest.raises(PairServingReleaseRequiredError, match="pair.*TARGET_HORIZON_MINUTES"):
        promote_challenger("rental", rental_prefix)

    read_champion_prefix.cache_clear()
    with pytest.raises(FileNotFoundError):
        read_champion_prefix("rental")


def test_promote_challenger_allows_training_and_lgb_only_differences():
    """같은 serving contract라면 모델별 튜닝과 학습 기간은 달라도 승격한다."""
    return_prefix = "models/archive/dt=2026-08-01/return-tuned"
    return_profile = common_config.effective_profile()
    return_profile["TRAIN_LOOKBACK_MONTHS"] = 6
    return_profile["LGB_PARAMS_COMMON"] = {**return_profile["LGB_PARAMS_COMMON"], "num_leaves": 31}
    _write_profile("return", return_prefix, return_profile)
    promote_challenger("return", return_prefix)

    rental_prefix = "models/archive/dt=2026-08-17/rental-tuned"
    rental_profile = common_config.effective_profile()
    rental_profile["TRAIN_LOOKBACK_MONTHS"] = 18
    rental_profile["LGB_PARAMS_COMMON"] = {**rental_profile["LGB_PARAMS_COMMON"], "num_leaves": 127}
    _write_profile("rental", rental_prefix, rental_profile)

    promote_challenger("rental", rental_prefix)

    assert read_champion_prefix("rental") == rental_prefix
    assert read_champion_prefix("return") == return_prefix


def test_promote_challenger_rejects_missing_effective_profile():
    """profile 아티팩트가 없는 챌린저는 계약을 확인할 수 없어 승격하지 않는다."""
    archive_prefix = "models/archive/dt=2026-08-17/no-profile"

    with pytest.raises(ServingProfileContractError, match="effective profile이 없습니다"):
        promote_challenger("rental", archive_prefix)

    with pytest.raises(FileNotFoundError):
        read_champion_prefix("rental")


def test_promote_challenger_rejects_other_champion_without_profile():
    """반대 모델의 계약을 증명할 수 없으면 새 모델도 섞어 승격하지 않는다."""
    return_prefix = "models/archive/dt=2026-08-01/legacy-no-profile"
    write_champion_pointer("return", return_prefix)

    rental_prefix = "models/archive/dt=2026-08-17/current"
    _write_profile("rental", rental_prefix)

    with pytest.raises(ServingProfileContractError, match="effective profile이 없습니다"):
        promote_challenger("rental", rental_prefix)

    read_champion_prefix.cache_clear()
    with pytest.raises(FileNotFoundError):
        read_champion_prefix("rental")


def test_pair_promotion_rejects_legacy_cross_contract_without_maintenance_gate():
    """Release pointer가 없어도 기존 champion과 다른 contract는 자동 migration하지 않는다."""
    legacy_prefix = "models/archive/dt=2026-08-01/legacy"
    _write_profile("rental", legacy_prefix)
    write_champion_pointer("rental", legacy_prefix)

    changed_profile = common_config.effective_profile()
    changed_profile["GRID_TICK_MINUTES"] = 10
    changed_profile["ROLLING_TICK_MINUTES"] = 10
    changed_profile["TRAIN_ANCHOR_TICK_MINUTES"] = 10

    with pytest.raises(CrossContractServingReleaseError, match="maintenance"):
        promote_serving_release_pair(
            _release_manifest(changed_profile),
            station_source=_station_source(),
        )


def test_pair_promotion_preserves_same_contract_legacy_compatibility(monkeypatch):
    """기존 champion과 같은 serving contract pair는 기존 환경에서도 승격 경로에 진입한다."""
    legacy_prefix = "models/archive/dt=2026-08-01/same-contract"
    _write_profile("return", legacy_prefix)
    write_champion_pointer("return", legacy_prefix)
    manifest = _release_manifest(common_config.effective_profile())
    sentinel = object()

    def _publish(candidate, **kwargs):
        """Pair wrapper가 검증 뒤 publication 경계를 호출했는지 기록한다."""
        assert candidate is manifest
        assert kwargs["allow_contract_change"] is False
        return sentinel

    monkeypatch.setattr("training.promotion.publish_serving_release", _publish)

    assert (
        promote_serving_release_pair(manifest, station_source=_station_source())
        is sentinel
    )


def test_prepare_and_promote_pair_from_existing_archives_reads_sources_once(
    monkeypatch,
):
    """기존 archive와 exact build source에서 production pair pointer까지 만든다."""
    rental_prefix = "models/archive/dt=2026-08-20/rental"
    return_prefix = "models/archive/dt=2026-08-20/return"
    rental_profile = common_config.effective_profile()
    return_profile = common_config.effective_profile()
    return_profile["TRAIN_LOOKBACK_MONTHS"] = 6
    return_profile["LGB_PARAMS_COMMON"] = {
        **return_profile["LGB_PARAMS_COMMON"],
        "num_leaves": 31,
    }
    source_keys = set(
        _write_model_archive(
            "rental",
            rental_prefix,
            profile=rental_profile,
            categories=[2, 1],
        )
    )
    source_keys.update(
        _write_model_archive(
            "return",
            return_prefix,
            profile=return_profile,
            categories=[1, 2],
        )
    )
    station_profile_key = "processed/features/release/station_profile.parquet"
    station_master_key = "processed/features/release/station_master.parquet"
    s3_io.put_object_bytes(
        station_profile_key,
        _parquet_bytes(_station_profile_rows()),
    )
    s3_io.put_object_bytes(
        station_master_key,
        _parquet_bytes(
            {
                "station_id": ["ST-1", "ST-2"],
                "station_no": [1, 2],
            }
        ),
    )
    source_keys.update({station_profile_key, station_master_key})

    read_counts: Counter[str] = Counter()
    original_get_object_bytes = s3_io.get_object_bytes

    def _counted_get_object_bytes(
        key: str,
        timeout_seconds: float | None = None,
    ) -> bytes | None:
        """Source exact-key read 횟수를 기록하고 기존 S3 helper를 호출한다."""
        read_counts[key] += 1
        return original_get_object_bytes(key, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(s3_io, "get_object_bytes", _counted_get_object_bytes)

    pointer = prepare_and_promote_serving_release_pair(
        rental_archive_prefix=rental_prefix,
        return_archive_prefix=return_prefix,
        station_profile_source_key=station_profile_key,
        station_master_source_key=station_master_key,
    )

    assert pointer.generation == 0
    assert {key: read_counts[key] for key in source_keys} == {
        key: 1 for key in source_keys
    }
    pinned = load_current_serving_release()
    assert pinned.pointer == pointer
    assert pinned.preflight.rental_model.model_kind is ModelKind.RENTAL
    assert pinned.preflight.return_model.model_kind is ModelKind.RETURN
    rental_crosswalk = next(
        artifact
        for artifact in pinned.preflight.rental_model.artifacts
        if artifact.role == "station_crosswalk"
    )
    return_crosswalk = next(
        artifact
        for artifact in pinned.preflight.return_model.artifacts
        if artifact.role == "station_crosswalk"
    )
    assert rental_crosswalk.byte_sha256 == return_crosswalk.byte_sha256


def test_prepare_pair_rejects_spark_station_master_prefix_before_pointer():
    """여러 part가 있는 Spark prefix를 exact station source로 추정하지 않는다."""
    station_profile_key = "processed/features/release/station_profile.parquet"
    station_master_prefix = "processed_v2/station_master.parquet"
    s3_io.put_object_bytes(
        station_profile_key,
        _parquet_bytes(_station_profile_rows(station_nos=(1,))),
    )
    s3_io.put_object_bytes(
        f"{station_master_prefix}/part-00000.snappy.parquet",
        _parquet_bytes({"station_id": ["ST-1"], "station_no": [1]}),
    )

    with pytest.raises(FileNotFoundError, match="exact single S3 object"):
        prepare_and_promote_serving_release_pair(
            rental_archive_prefix="models/archive/dt=2026-08-20/rental",
            return_archive_prefix="models/archive/dt=2026-08-20/return",
            station_profile_source_key=station_profile_key,
            station_master_source_key=station_master_prefix,
        )

    assert s3_io.get_object_bytes(serving_release_pointer_key()) is None
