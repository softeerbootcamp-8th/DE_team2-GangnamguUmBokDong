"""Gold urgency projection의 완전성·artifact 분리 계약을 검증한다."""

import importlib.util
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

import boto3
import pyarrow as pa
import pytest
from core.forecast import enrich_forecast_points
from core.gold_publication import (
    ContractViolation,
    InputArtifact,
    S3ImmutableObjectStore,
    sha256_hex,
)
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from gold.common import parquet_bytes
from gold.demand import DemandForecastRecord
from gold.rebalance_policy import (
    PICKUP_SAFETY_STRATEGY_QUANTILE_ADVERSE,
    risk_band_policy,
)
from gold.source_catalog import S3SourceSnapshotCatalog
from gold.station_stock import StationStockRecord
from gold.urgency import (
    URGENCY_PUBLISHER_VERSION,
    ActiveStation,
    StationUrgencyRecord,
    StockHistoryPoint,
    UrgencyCalculationInputs,
    UrgencyProjection,
    UrgencyRecord,
    _bike_qty_risk_band_v5,
    _bike_qty_v1,
    _history_window_from_manifest,
    _pickup_model_lower_stock_path,
    _recent_stock_projection_v3,
    _serving_release_manifest_refs,
    _stock_history_input_artifacts,
    _urgency_score_v1,
    _validate_history_catalog,
    build_urgency_projection,
    compute_urgency_projection,
    urgency_records_from_parquet,
    urgency_records_to_parquet,
)

BASE = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)
BUCKET = "test-bucket"


def test_urgency_publisher_version_matches_v5_capacity_reserve_contract() -> None:
    """정원보존 publisher의 최종 v5 식별자를 고정한다."""
    assert URGENCY_PUBLISHER_VERSION == "gold-urgency-publisher-v5-capacity-reserve"


def _stock_history(*quantities: int, spacing_minutes: int = 5) -> list[dict[str, Any]]:
    """현재를 마지막 점으로 하는 oldest-first 재고 이력을 만든다."""
    return [
        {
            "observed_at": BASE
            - timedelta(minutes=spacing_minutes * (len(quantities) - index - 1)),
            "parking_bike_tot_cnt": quantity,
        }
        for index, quantity in enumerate(quantities)
    ]


def test_serving_release_refs_require_exact_three_key_mapping() -> None:
    """Urgency가 finalize의 station·demand·stock exact URI/SHA 외 입력을 거부한다."""
    valid = {
        "station": ("s3://fixture/station.json", "a" * 64),
        "station_demand_forecast": ("s3://fixture/demand.json", "b" * 64),
        "station_stock": ("s3://fixture/stock.json", "c" * 64),
    }
    assert _serving_release_manifest_refs(valid) == valid
    with pytest.raises(ContractViolation, match="key 집합"):
        _serving_release_manifest_refs({"station": valid["station"]})
    with pytest.raises(ContractViolation, match="SHA"):
        _serving_release_manifest_refs(
            {**valid, "station_stock": ("s3://fixture/stock.json", "bad")}
        )


def _record(
    station_id: str,
    *,
    base: datetime = BASE,
    score: float = 42.5,
    critical: int = 25,
    need: str = "supply_needed",
    bike_qty: int = 3,
) -> UrgencyRecord:
    """테스트용 유효 urgency 계산 행을 반환한다."""
    return UrgencyRecord(station_id, base, score, critical, need, bike_qty)


def test_projection_uses_exact_authoritative_intersection_and_sorts() -> None:
    """active·current stock·demand support 교집합을 한 번씩 정렬한다."""
    projection = build_urgency_projection(
        (_record("ST-2"), _record("ST-1")),
        base_dttm=BASE,
        active_station_ids=("ST-3", "ST-2", "ST-1"),
        current_stock_station_ids=("ST-1", "ST-2", "ST-4"),
        demand_support_station_ids=("ST-2", "ST-1", "ST-5"),
    )
    assert projection.expected_sta_ids == ("ST-1", "ST-2")
    assert tuple(record.sta_id for record in projection.records) == (
        "ST-1",
        "ST-2",
    )
    assert all(record.base_dttm.tzinfo is UTC for record in projection.records)


def test_target_records_omit_route_only_bike_quantity() -> None:
    """RDS target 행에는 route producer 전용 bike_qty를 중복 저장하지 않는다."""
    projection = build_urgency_projection(
        (_record("ST-1", bike_qty=7),),
        base_dttm=BASE,
        active_station_ids=("ST-1",),
        current_stock_station_ids=("ST-1",),
        demand_support_station_ids=("ST-1",),
    )
    [target] = projection.target_records
    assert target.sta_id == "ST-1"
    assert not hasattr(target, "bike_qty")
    assert projection.records[0].bike_qty == 7


def test_public_target_record_revalidates_ddl_values() -> None:
    """publisher가 target record를 직접 만들더라도 DDL 밖 값을 허용하지 않는다."""
    with pytest.raises(ContractViolation, match="0..100"):
        StationUrgencyRecord(
            sta_id="ST-1",
            base_dttm=BASE,
            urgency_score=101.0,
            critical_remaining_min=5,
            rebalance_need_type_cd="normal",
        )


@pytest.mark.parametrize(
    ("records", "active", "stock", "demand", "message"),
    [
        ((), ("ST-1",), ("ST-1",), ("ST-1",), "missing"),
        ((_record("ST-2"),), ("ST-1",), ("ST-1",), ("ST-1",), "extra"),
        (
            (_record("ST-1"), _record("ST-1")),
            ("ST-1",),
            ("ST-1",),
            ("ST-1",),
            "중복",
        ),
    ],
)
def test_projection_rejects_missing_extra_and_duplicate_results(
    records: tuple[UrgencyRecord, ...],
    active: tuple[str, ...],
    stock: tuple[str, ...],
    demand: tuple[str, ...],
    message: str,
) -> None:
    """기대 ID 누락·extra·중복에서 부분 projection을 게시하지 않는다."""
    with pytest.raises(ContractViolation, match=message):
        build_urgency_projection(
            records,
            base_dttm=BASE,
            active_station_ids=active,
            current_stock_station_ids=stock,
            demand_support_station_ids=demand,
        )


def test_projection_accepts_empty_only_when_expected_intersection_is_empty() -> None:
    """조건부 EMPTY는 authoritative 기대 교집합이 빈 때만 만든다."""
    projection = build_urgency_projection(
        (),
        base_dttm=BASE,
        active_station_ids=("ST-1",),
        current_stock_station_ids=(),
        demand_support_station_ids=("ST-1",),
    )
    assert projection.records == ()
    assert projection.expected_sta_ids == ()


def test_projection_normalizes_equivalent_timezone_to_utc() -> None:
    """같은 instant의 timezone 표기는 output에서 UTC로 고정한다."""
    kst = timezone(timedelta(hours=9))
    local = BASE.astimezone(kst)
    projection = build_urgency_projection(
        (_record("ST-1", base=local),),
        base_dttm=local,
        active_station_ids=("ST-1",),
        current_stock_station_ids=("ST-1",),
        demand_support_station_ids=("ST-1",),
    )
    assert projection.base_dttm == BASE
    assert projection.records[0].base_dttm == BASE


def test_projection_rejects_different_anchor() -> None:
    """다른 stock tick의 urgency 행을 같은 publication에 섞지 않는다."""
    with pytest.raises(ContractViolation, match="anchor"):
        build_urgency_projection(
            (_record("ST-1", base=BASE - timedelta(minutes=5)),),
            base_dttm=BASE,
            active_station_ids=("ST-1",),
            current_stock_station_ids=("ST-1",),
            demand_support_station_ids=("ST-1",),
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"station_id": "BAD"}, "ST-숫자"),
        ({"score": float("nan")}, "finite"),
        ({"score": 100.1}, "0..100"),
        ({"critical": -1}, "비음수"),
        ({"critical": 2_147_483_648}, "INTEGER"),
        ({"need": "urgent"}, "allowlist"),
        ({"bike_qty": -1}, "비음수"),
        ({"bike_qty": 2_147_483_648}, "INTEGER"),
    ],
)
def test_record_rejects_values_outside_target_and_artifact_contract(
    overrides: dict[str, object],
    message: str,
) -> None:
    """DDL 범위 밖 값과 route에 쓸 수 없는 수량을 immutable output 전에 거부한다."""
    arguments: dict[str, object] = {
        "station_id": "ST-1",
        "score": 42.5,
        "critical": 25,
        "need": "supply_needed",
        "bike_qty": 3,
    }
    arguments.update(overrides)
    with pytest.raises(ContractViolation, match=message):
        _record(**arguments)  # type: ignore[arg-type]


def test_expected_sets_reject_duplicates_before_intersection() -> None:
    """입력 집합 중복을 set 변환으로 숨기지 않는다."""
    with pytest.raises(ContractViolation, match="중복"):
        build_urgency_projection(
            (_record("ST-1"),),
            base_dttm=BASE,
            active_station_ids=("ST-1", "ST-1"),
            current_stock_station_ids=("ST-1",),
            demand_support_station_ids=("ST-1",),
        )


def test_projection_object_rejects_noncanonical_expected_id_order() -> None:
    """직접 만든 projection도 expected ID canonical order를 우회하지 못한다."""
    with pytest.raises(ContractViolation, match="UTF-8"):
        UrgencyProjection(
            records=(_record("ST-1"), _record("ST-2")),
            base_dttm=BASE,
            expected_sta_ids=("ST-2", "ST-1"),
        )


def test_urgency_parquet_round_trip_is_deterministic_and_preserves_bike_qty() -> None:
    """fixed schema urgency artifact를 재실행해도 같은 bytes와 값을 만든다."""
    records = (
        _record("ST-1", need="normal", bike_qty=0),
        _record("ST-2", need="retrieval_needed", bike_qty=4),
    )
    first = urgency_records_to_parquet(
        records,
        expected_sta_ids=("ST-1", "ST-2"),
    )
    second = urgency_records_to_parquet(
        records,
        expected_sta_ids=("ST-1", "ST-2"),
    )
    assert first == second
    assert (
        urgency_records_from_parquet(
            first,
            expected_base_dttm=BASE,
            expected_sta_ids=("ST-1", "ST-2"),
        )
        == records
    )


def test_urgency_parquet_rejects_wrong_schema() -> None:
    """이름이 비슷해도 bike_qty가 빠진 artifact는 route 입력으로 받지 않는다."""
    payload = parquet_bytes(
        pa.table(
            {
                "sta_id": ["ST-1"],
                "base_dttm": [BASE],
                "urgency_score": [42.5],
                "critical_remaining_min": [25],
                "rebalance_need_type_cd": ["supply_needed"],
            }
        )
    )
    with pytest.raises(ContractViolation, match="schema"):
        urgency_records_from_parquet(
            payload,
            expected_base_dttm=BASE,
            expected_sta_ids=("ST-1",),
        )


def test_urgency_parquet_requires_canonical_record_order() -> None:
    """artifact row order를 sta_id UTF-8 순으로 고정한다."""
    with pytest.raises(ContractViolation, match="exact"):
        urgency_records_to_parquet(
            (_record("ST-2"), _record("ST-1")),
            expected_sta_ids=("ST-1", "ST-2"),
        )


def test_urgency_parquet_rejects_whole_station_omission() -> None:
    """artifact payload에서 기대 집합을 역추론해 station 누락을 숨기지 않는다."""
    with pytest.raises(ContractViolation, match="exact"):
        urgency_records_to_parquet(
            (_record("ST-1"),),
            expected_sta_ids=("ST-1", "ST-2"),
        )


def test_empty_urgency_uses_no_artifact_instead_of_empty_parquet() -> None:
    """조건부 EMPTY publication은 artifacts=[] 계약을 우회하지 않는다."""
    with pytest.raises(ContractViolation, match=r"artifacts=\[\]"):
        urgency_records_to_parquet((), expected_sta_ids=())


def test_compute_projection_allows_new_station_absent_from_old_complete_windows() -> (
    None
):
    """Complete history window 자체는 필수지만 신설 station의 과거 row 부재는 허용한다."""
    inputs = _calculation_inputs(history_station_ids=())

    projection = compute_urgency_projection(inputs)

    assert projection.expected_sta_ids == ("ST-1",)
    assert projection.records == (
        UrgencyRecord(
            sta_id="ST-1",
            base_dttm=BASE,
            urgency_score=53.5,
            critical_remaining_min=0,
            rebalance_need_type_cd="supply_needed",
            bike_qty=7,
        ),
    )


def test_calculation_inputs_fix_history_roles_to_oldest_first_offsets() -> None:
    """History role 하나라도 t-25..-5 순서를 바꾸면 계산 전에 거부한다."""
    inputs = _calculation_inputs(history_station_ids=("ST-1",))
    swapped = (
        inputs.history_windows[1],
        inputs.history_windows[0],
        *inputs.history_windows[2:],
    )

    with pytest.raises(ContractViolation, match="시각 순서"):
        UrgencyCalculationInputs(
            active_stations=inputs.active_stations,
            history_offsets_minutes=inputs.history_offsets_minutes,
            history_windows=swapped,
            current_stock=inputs.current_stock,
            demand=inputs.demand,
            base_dttm=inputs.base_dttm,
        )


def test_scoring_and_bike_quantity_are_equivalent_to_rebalance_v1() -> None:
    """Production publisher 계산이 기존 rebalance urgency 함수와 exact하게 같다."""
    legacy = _load_legacy_urgency_module()
    history = [
        {"observed_at": BASE - timedelta(minutes=10), "parking_bike_tot_cnt": 6},
        {"observed_at": BASE, "parking_bike_tot_cnt": 4},
    ]
    raw_points = [
        {"predicted_rent_cnt": 3, "predicted_return_cnt": 1},
        {"predicted_rent_cnt": 0, "predicted_return_cnt": 8},
    ]
    points = enrich_forecast_points(4, 20, raw_points)

    expected = legacy.urgency_score(4, 20, history, points, BASE)
    actual = _urgency_score_v1(4, 20, history, points, BASE)

    assert actual == expected
    assert _bike_qty_v1(4, 20, actual[2], points) == legacy.bike_qty(
        4,
        20,
        expected[2],
        points,
    )


def test_risk_band_pickup_preserves_lower_stock_across_protection_horizon() -> None:
    """평균 재고가 유지돼도 확률 하방에서 정원을 뺀 만큼만 수거한다."""
    points = enrich_forecast_points(
        40,
        10,
        [
            {"predicted_rent_cnt": 15, "predicted_return_cnt": 15},
            {"predicted_rent_cnt": 15, "predicted_return_cnt": 15},
        ],
    )
    policy = risk_band_policy(
        protection_horizon_hours=2,
        minimum_stock_ratio=0.2,
        uncertainty_z=1.645,
    )
    assert _bike_qty_v1(40, 10, "retrieval_needed", points) == 30
    assert (
        _bike_qty_risk_band_v5(
            40,
            10,
            "retrieval_needed",
            points,
            _stock_history(40, 40, 40),
            BASE,
            policy,
        )
        == 17
    )


def test_pickup_safety_strategies_share_stock_path_interface() -> None:
    """정상 synthetic 입력에서 두 strategy가 같은 stock-path 계약을 제공한다."""
    points = [
        {
            "predicted_rent_cnt": 2,
            "predicted_return_cnt": 1,
            "rental_pred_p90": 4.0,
            "return_pred_p10": 1.0,
        },
        {
            "predicted_rent_cnt": 2,
            "predicted_return_cnt": 1,
            "rental_pred_p90": 3.0,
            "return_pred_p10": 2.0,
        },
    ]
    poisson = risk_band_policy(
        protection_horizon_hours=2,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )
    quantile = risk_band_policy(
        protection_horizon_hours=2,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
        pickup_safety_strategy=PICKUP_SAFETY_STRATEGY_QUANTILE_ADVERSE,
    )

    assert _pickup_model_lower_stock_path(20, points, poisson) == (20.0, 19.0, 18.0)
    assert _pickup_model_lower_stock_path(20, points, quantile) == (
        20.0,
        17.0,
        16.0,
    )


def test_risk_band_dropoff_raises_lower_stock_to_minimum() -> None:
    """배치량은 보호 구간의 최저 하방 재고를 최소 안전재고까지 올린다."""
    points = enrich_forecast_points(
        0,
        10,
        [{"predicted_rent_cnt": 5, "predicted_return_cnt": 0}],
    )
    policy = risk_band_policy(
        protection_horizon_hours=1,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )
    assert (
        _bike_qty_risk_band_v5(
            0,
            10,
            "supply_needed",
            points,
            [],
            BASE,
            policy,
        )
        == 7
    )


def test_risk_band_pickup_uses_full_capacity_surplus() -> None:
    """미래에도 정원을 지키면 임의 비율 없이 현재 정원 초과분을 회수한다."""
    points = enrich_forecast_points(
        40,
        10,
        [{"predicted_rent_cnt": 0, "predicted_return_cnt": 0}],
    )
    policy = risk_band_policy(
        protection_horizon_hours=1,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )
    assert (
        _bike_qty_risk_band_v5(
            40,
            10,
            "retrieval_needed",
            points,
            _stock_history(40, 40, 40),
            BASE,
            policy,
        )
        == 30
    )


def test_risk_band_pickup_uses_only_current_capacity_surplus() -> None:
    """미래 반납 초과량은 현재 실제 정원 초과분보다 많은 회수를 만들지 않는다."""
    points = enrich_forecast_points(
        12,
        10,
        [{"predicted_rent_cnt": 0, "predicted_return_cnt": 50}],
    )
    policy = risk_band_policy(
        protection_horizon_hours=1,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )

    assert (
        _bike_qty_risk_band_v5(
            12,
            10,
            "retrieval_needed",
            points,
            _stock_history(12, 12, 12),
            BASE,
            policy,
        )
        == 2
    )


def test_risk_band_pickup_fails_closed_without_three_points_including_current() -> None:
    """현재 포함 고유 재고 관측이 세 점 미만이면 pickup을 만들지 않는다."""
    points = enrich_forecast_points(
        20,
        10,
        [{"predicted_rent_cnt": 0, "predicted_return_cnt": 0}],
    )
    policy = risk_band_policy(
        protection_horizon_hours=1,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )

    assert (
        _bike_qty_risk_band_v5(
            20,
            10,
            "retrieval_needed",
            points,
            _stock_history(20, 20),
            BASE,
            policy,
        )
        == 0
    )
    assert (
        _bike_qty_risk_band_v5(
            20,
            10,
            "retrieval_needed",
            points,
            _stock_history(20, 20, 19),
            BASE,
            policy,
        )
        == 0
    )


@pytest.mark.parametrize(
    ("history", "expected"),
    (
        ((18, 19, 20), 10),
        ((20, 20, 20), 10),
        ((22, 21, 20), 0),
    ),
)
def test_risk_band_pickup_applies_recent_stock_direction(
    history: tuple[int, ...],
    expected: int,
) -> None:
    """한 방향만 평탄할 때 기존 recent lower-bound 수량을 보존한다."""
    points = enrich_forecast_points(
        20,
        10,
        [{"predicted_rent_cnt": 0, "predicted_return_cnt": 0}],
    )
    policy = risk_band_policy(
        protection_horizon_hours=1,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )

    assert (
        _bike_qty_risk_band_v5(
            20,
            10,
            "retrieval_needed",
            points,
            _stock_history(*history),
            BASE,
            policy,
        )
        == expected
    )


def test_risk_band_pickup_reduces_quantity_for_both_depletion_signals() -> None:
    """최근 실측과 모델이 감소하면 미래 정원을 지키도록 회수량을 줄인다."""
    points = enrich_forecast_points(
        40,
        10,
        [{"predicted_rent_cnt": 1, "predicted_return_cnt": 0}],
    )
    policy = risk_band_policy(
        protection_horizon_hours=1,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )

    assert (
        _bike_qty_risk_band_v5(
            40,
            10,
            "retrieval_needed",
            points,
            _stock_history(41, 40, 40),
            BASE,
            policy,
        )
        == 21
    )


def test_risk_band_pickup_uses_recent_lower_bound_when_model_rises() -> None:
    """모델이 상승해도 최근 실측 하방을 넘지 않게 회수한다."""
    points = enrich_forecast_points(
        40,
        10,
        [{"predicted_rent_cnt": 0, "predicted_return_cnt": 1}],
    )
    policy = risk_band_policy(
        protection_horizon_hours=1,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )

    assert (
        _bike_qty_risk_band_v5(
            40,
            10,
            "retrieval_needed",
            points,
            _stock_history(41, 40, 40),
            BASE,
            policy,
        )
        == 21
    )


@pytest.mark.parametrize("history", ((40, 40, 40), (39, 40, 40)))
def test_risk_band_pickup_preserves_capacity_despite_model_decline(
    history: tuple[int, ...],
) -> None:
    """모델이 감소하면 보수적 미래에도 정원을 남기는 만큼만 회수한다."""
    points = enrich_forecast_points(
        40,
        10,
        [{"predicted_rent_cnt": 1, "predicted_return_cnt": 0}],
    )
    policy = risk_band_policy(
        protection_horizon_hours=1,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )

    assert (
        _bike_qty_risk_band_v5(
            40,
            10,
            "retrieval_needed",
            points,
            _stock_history(*history),
            BASE,
            policy,
        )
        == 29
    )


def test_risk_band_pickup_bounds_gaehwa_2965_by_recent_projection() -> None:
    """개화 ST-2965 반례도 최근 하방에서 정원을 지키는 수량만 허용한다."""
    points = enrich_forecast_points(
        53,
        11,
        [
            {"predicted_rent_cnt": 3, "predicted_return_cnt": 3},
            {"predicted_rent_cnt": 3, "predicted_return_cnt": 3},
        ],
    )
    policy = risk_band_policy(
        protection_horizon_hours=2,
        minimum_stock_ratio=0.2,
        uncertainty_z=1.645,
    )

    assert (
        _bike_qty_risk_band_v5(
            53,
            11,
            "retrieval_needed",
            points,
            _stock_history(55, 53, 53, 53, 53, 53),
            BASE,
            policy,
        )
        == 33
    )


def test_recent_stock_projection_includes_response_lag() -> None:
    """하락 추세 reserve는 보호 한 시간 뒤 추가 출동 지연 30분까지 지킨다."""
    history = _stock_history(22, 21, 20)

    horizon_only = _recent_stock_projection_v3(
        20,
        history,
        BASE,
        protection_minutes=60,
    )
    with_response_lag = _recent_stock_projection_v3(
        20,
        history,
        BASE,
        protection_minutes=90,
    )

    assert horizon_only == pytest.approx((-0.2, 8.0))
    assert with_response_lag == pytest.approx((-0.2, 2.0))


def test_history_null_parking_is_station_observation_absence() -> None:
    """Complete window의 nullable parking 한 건은 batch 실패가 아닌 해당 point 부재다."""
    store = S3ImmutableObjectStore(boto3.client("s3", region_name="us-east-1"))
    silver = parquet_bytes(
        pa.table(
            {
                "stationId": pa.array(["ST-1", "ST-2"], type=pa.string()),
                "parkingBikeTotCnt": pa.array([None, 4], type=pa.int64()),
            }
        )
    )
    silver_sha = sha256_hex(silver)
    silver_uri = f"s3://{BUCKET}/history/sha256={silver_sha}.parquet"
    store.put_once(silver_uri, silver, expected_sha256=silver_sha)
    manifest = _source_manifest(
        BASE - timedelta(minutes=25), 0, silver_uri, silver_sha, 2
    )
    manifest_uri = f"s3://{BUCKET}/history/manifest-{manifest.sha256}.json"
    store.put_once(
        manifest_uri,
        manifest.canonical_bytes,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    artifact = InputArtifact(
        byte_sha256=manifest.sha256,
        role="stock_history_manifest_m25",
        uri=manifest_uri,
    )

    points = _history_window_from_manifest(
        store,
        artifact,
        {manifest_uri: manifest.canonical_bytes},
        expected_logical_dttm=BASE - timedelta(minutes=25),
    )

    assert points == (StockHistoryPoint("ST-2", BASE - timedelta(minutes=25), 4),)


def test_history_ref_must_be_exact_window_latest_correction() -> None:
    """Caller가 같은 logical의 old revision URI를 주면 catalog latest와 달라 거부한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    references = []
    for offset in (-25, -20, -15, -10, -5):
        logical = BASE + timedelta(minutes=offset)
        revision_zero = _source_manifest(
            logical,
            0,
            f"s3://{BUCKET}/silver/sha256={'1' * 64}.parquet",
            "1" * 64,
            1,
        )
        uri = _authority_uri(revision_zero)
        client.put_object(
            Bucket=BUCKET,
            Key=uri.removeprefix(f"s3://{BUCKET}/"),
            Body=revision_zero.canonical_bytes,
        )
        references.append((offset, uri, revision_zero.sha256))
        if offset == -25:
            revision_one = _source_manifest(
                logical,
                1,
                f"s3://{BUCKET}/silver/sha256={'2' * 64}.parquet",
                "2" * 64,
                1,
            )
            corrected_uri = _authority_uri(revision_one)
            client.put_object(
                Bucket=BUCKET,
                Key=corrected_uri.removeprefix(f"s3://{BUCKET}/"),
                Body=revision_one.canonical_bytes,
            )
    catalog = S3SourceSnapshotCatalog(client, store, bucket=BUCKET)
    inputs = _stock_history_input_artifacts(tuple(references))

    with pytest.raises(ContractViolation, match="latest correction"):
        _validate_history_catalog(catalog, inputs, BASE)


def test_missing_history_window_is_tolerated_when_it_truly_does_not_exist() -> None:
    """수집되지 않은 tick은 건너뛰고 게시할 수 있다 — 소급 수집이 불가능하기 때문이다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    references = []
    for offset in (-25, -20, -15, -10, -5):
        if offset == -15:
            continue  # tick 하나가 아예 실행되지 않은 상황을 재현한다.
        logical = BASE + timedelta(minutes=offset)
        manifest = _source_manifest(
            logical,
            0,
            f"s3://{BUCKET}/silver/sha256={'1' * 64}.parquet",
            "1" * 64,
            1,
        )
        uri = _authority_uri(manifest)
        client.put_object(
            Bucket=BUCKET,
            Key=uri.removeprefix(f"s3://{BUCKET}/"),
            Body=manifest.canonical_bytes,
        )
        references.append((offset, uri, manifest.sha256))
    catalog = S3SourceSnapshotCatalog(client, store, bucket=BUCKET)

    inputs = _stock_history_input_artifacts(tuple(references))
    _validate_history_catalog(catalog, inputs, BASE)

    assert tuple(offset for offset, _ in inputs) == (-25, -20, -10, -5)
    assert [artifact.role for _, artifact in inputs] == [
        "stock_history_manifest_m25",
        "stock_history_manifest_m20",
        "stock_history_manifest_m10",
        "stock_history_manifest_m05",
    ]


def test_omitting_an_existing_history_window_is_rejected() -> None:
    """실제로 존재하는 window를 빠뜨리면 같은 anchor에서 결과가 갈리므로 거부한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    references = []
    for offset in (-25, -20, -15, -10, -5):
        logical = BASE + timedelta(minutes=offset)
        manifest = _source_manifest(
            logical,
            0,
            f"s3://{BUCKET}/silver/sha256={'1' * 64}.parquet",
            "1" * 64,
            1,
        )
        uri = _authority_uri(manifest)
        client.put_object(
            Bucket=BUCKET,
            Key=uri.removeprefix(f"s3://{BUCKET}/"),
            Body=manifest.canonical_bytes,
        )
        if offset == -15:
            continue  # S3에는 있지만 caller가 ref에서 뺀 상황.
        references.append((offset, uri, manifest.sha256))
    catalog = S3SourceSnapshotCatalog(client, store, bucket=BUCKET)
    inputs = _stock_history_input_artifacts(tuple(references))

    with pytest.raises(ContractViolation, match="빠뜨렸습니다"):
        _validate_history_catalog(catalog, inputs, BASE)


def test_history_below_minimum_window_count_is_rejected() -> None:
    """단발성 결측이 아니라 수집 장애 수준이면 조용히 저품질 게시하지 않는다."""
    with pytest.raises(ContractViolation, match="하한 미만"):
        _stock_history_input_artifacts(
            ((-5, f"s3://{BUCKET}/authority/a.json", "1" * 64),)
        )


def test_history_refs_must_stay_oldest_first_without_duplicates() -> None:
    """offset 순서가 뒤바뀌거나 중복되면 입력 단계에서 거부한다."""
    reference = f"s3://{BUCKET}/authority/a.json"
    with pytest.raises(ContractViolation, match="oldest-first"):
        _stock_history_input_artifacts(
            ((-5, reference, "1" * 64), (-25, reference, "1" * 64))
        )
    with pytest.raises(ContractViolation, match="oldest-first"):
        _stock_history_input_artifacts(
            ((-25, reference, "1" * 64), (-25, reference, "1" * 64))
        )


def test_history_offset_outside_scoring_config_is_rejected() -> None:
    """scoring config에 없는 offset은 받지 않는다."""
    with pytest.raises(ContractViolation, match="scoring config"):
        _stock_history_input_artifacts(
            (
                (-30, f"s3://{BUCKET}/authority/a.json", "1" * 64),
                (-5, f"s3://{BUCKET}/authority/b.json", "1" * 64),
            )
        )


def test_projection_is_computable_from_partial_history() -> None:
    """window 2개만 있어도 추세가 성립해 게시 가능한 projection이 나온다."""
    inputs = _calculation_inputs(
        history_station_ids=("ST-1",), history_offsets=(-10, -5)
    )

    projection = compute_urgency_projection(inputs)

    assert projection.expected_sta_ids == ("ST-1",)
    assert len(projection.records) == 1


def _calculation_inputs(
    *,
    history_station_ids: tuple[str, ...],
    history_offsets: tuple[int, ...] = (-25, -20, -15, -10, -5),
) -> UrgencyCalculationInputs:
    """Pure production scoring 테스트용 6-window 입력을 만든다."""
    active = (ActiveStation("ST-1", 20, 127.0, 37.5, "center"),)
    history = tuple(
        tuple(
            StockHistoryPoint(station_id, BASE + timedelta(minutes=offset), 2)
            for station_id in history_station_ids
        )
        for offset in history_offsets
    )
    current = (StationStockRecord("ST-1", BASE, 1),)
    demand = tuple(
        DemandForecastRecord(
            base_dttm=BASE,
            sta_id="ST-1",
            predicted_dttm=BASE + timedelta(hours=horizon),
            predicted_rent_cnt=3,
            predicted_rtn_cnt=1,
        )
        for horizon in range(1, 13)
    )
    return UrgencyCalculationInputs(
        active, history_offsets, history, current, demand, BASE
    )


def _load_legacy_urgency_module() -> ModuleType:
    """Sibling rebalance의 production scoring module을 동등성 검증용으로 연다."""
    rebalance_dir = Path(__file__).resolve().parents[3] / "rebalance"
    spec = importlib.util.spec_from_file_location(
        "legacy_rebalance_urgency",
        rebalance_dir / "urgency.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(rebalance_dir))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(rebalance_dir))
    return module


def _source_manifest(
    logical_dttm: datetime,
    revision_no: int,
    silver_uri: str,
    silver_sha256: str,
    count: int,
) -> Any:
    """Realtime complete source authority fixture를 만든다."""
    return build_source_snapshot_manifest(
        source_id="bike_station_realtime",
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="urgency-test-v1",
        silver_uri=silver_uri,
        silver_byte_sha256=silver_sha256,
        counts=SourceSnapshotCounts(count, count, count, 0, 0),
        planned_parts=("page-1",),
        completed_parts=("page-1",),
    )


def _authority_uri(manifest: Any) -> str:
    """Source catalog canonical authority URI를 만든다."""
    logical = manifest.logical_dttm.astimezone(UTC)
    return (
        f"s3://{BUCKET}/source_snapshot_manifest/{manifest.source_id}/"
        f"dt={logical:%Y-%m-%d}/hh={logical:%H}/"
        f"logical={logical:%Y%m%dT%H%M%S}{logical.microsecond:06d}Z/"
        f"revision={manifest.revision_no:010d}.json"
    )
