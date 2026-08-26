"""Gold demand 순수 projection의 완결성·시간·수량 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pyarrow as pa
import pytest
from core.gold_publication import ContractViolation, build_id_set
from core.inference_snapshot import (
    LEGACY_INFERENCE_OUTPUT_COLUMN_NAMES,
    LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    canonicalize_inference_output_table,
    serialize_inference_output_parquet,
)
from gold.common import parquet_bytes, read_parquet_bytes
from gold.demand import (
    HORIZON_COUNT,
    POSTGRES_INTEGER_MAX,
    DemandPredictionRecord,
    DemandProjection,
    build_demand_projection,
    demand_predictions_from_inference_parquet,
    demand_records_from_parquet,
    demand_records_to_parquet,
)

BASE = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)


def _prediction(
    station_id: str,
    horizon: int,
    *,
    base: datetime = BASE,
    rental: float = 2.5,
    returned: float = 3.5,
    target: datetime | None = None,
) -> DemandPredictionRecord:
    """테스트용 typed inference row를 반환한다."""
    return DemandPredictionRecord(
        base_dttm=base,
        station_id=station_id,
        horizon=horizon,
        target_dttm=target or base + timedelta(hours=horizon - 1),
        rental_pred_mean=rental,
        rental_pred_p10=0.5,
        rental_pred_p50=1.5,
        rental_pred_p90=2.5,
        return_pred_mean=returned,
        return_pred_p10=1.0,
        return_pred_p50=2.0,
        return_pred_p90=3.0,
    )


def _complete_predictions(
    station_ids: tuple[str, ...] = ("ST-2", "ST-10"),
    *,
    base: datetime = BASE,
) -> tuple[DemandPredictionRecord, ...]:
    """station별 horizon 1..12가 완전한 inference tuple을 반환한다."""
    return tuple(
        _prediction(station_id, horizon, base=base)
        for station_id in reversed(station_ids)
        for horizon in reversed(range(1, HORIZON_COUNT + 1))
    )


def _projection(
    predictions: tuple[DemandPredictionRecord, ...] | None = None,
    *,
    active: tuple[str, ...] = ("ST-10", "ST-2", "ST-99"),
    rental: tuple[str, ...] = ("ST-2", "ST-10", "ST-88"),
    returned: tuple[str, ...] = ("ST-10", "ST-2", "ST-77"),
    base: datetime = BASE,
) -> DemandProjection:
    """기본 support 교집합에 대한 projection을 만든다."""
    return build_demand_projection(
        _complete_predictions(base=base) if predictions is None else predictions,
        base_dttm=base,
        active_station_ids=active,
        rental_model_station_ids=rental,
        return_model_station_ids=returned,
    )


def test_projection_accepts_plan_expected_subset_of_active_model_support() -> None:
    """Prepare에서 격리한 station은 demand 완결성 기대 집합에서도 제외한다."""
    projection = build_demand_projection(
        _complete_predictions(("ST-2",)),
        base_dttm=BASE,
        active_station_ids=("ST-10", "ST-2"),
        rental_model_station_ids=("ST-10", "ST-2"),
        return_model_station_ids=("ST-10", "ST-2"),
        expected_station_ids=("ST-2",),
    )

    assert projection.expected_sta_ids == ("ST-2",)
    assert len(projection.records) == HORIZON_COUNT


def _inference_authority_payload(
    station_ids: tuple[str, ...] = ("ST-1",),
) -> bytes:
    """Core contract로 canonical 13-column inference authority bytes를 만든다."""
    local_base = BASE + timedelta(hours=9)
    rows = []
    for station_id in station_ids:
        for horizon in range(1, HORIZON_COUNT + 1):
            target = local_base + timedelta(hours=horizon - 1)
            rows.append(
                {
                    "station_id": station_id,
                    "date": target.date().isoformat(),
                    "hour": target.hour,
                    "minute": target.minute,
                    "horizon": horizon,
                    "rental_pred_mean": float(horizon) + 0.5,
                    "rental_pred_p10": -0.5,
                    "rental_pred_p50": float(horizon) + 0.25,
                    "rental_pred_p90": float(horizon) + 1.0,
                    "return_pred_mean": float(horizon) + 1.5,
                    "return_pred_p10": -1.5,
                    "return_pred_p50": float(horizon) + 1.25,
                    "return_pred_p90": float(horizon) + 2.0,
                }
            )
    table = canonicalize_inference_output_table(
        pd.DataFrame(rows),
        logical_dttm=BASE,
        expected_sta_ids=build_id_set(station_ids),
    )
    return serialize_inference_output_parquet(table)


def test_inference_authority_adapter_uses_core_exact_schema_and_utc_anchor() -> None:
    """Core authority rows를 target start 시각과 float64를 보존해 typed row로 읽는다."""
    records = demand_predictions_from_inference_parquet(
        _inference_authority_payload(),
        expected_base_dttm=BASE,
        expected_sta_ids=("ST-1",),
    )

    assert len(records) == HORIZON_COUNT
    assert records[0] == DemandPredictionRecord(
        base_dttm=BASE,
        station_id="ST-1",
        horizon=1,
        target_dttm=BASE,
        rental_pred_mean=1.5,
        rental_pred_p10=-0.5,
        rental_pred_p50=1.25,
        rental_pred_p90=2.0,
        return_pred_mean=2.5,
        return_pred_p10=-1.5,
        return_pred_p50=2.25,
        return_pred_p90=3.0,
    )
    assert records[-1].target_dttm == BASE + timedelta(hours=11)


def test_v1_inference_adapter_preserves_mean_and_marks_quantiles_missing() -> None:
    """v1 7-column authority는 mean-only로 읽고 quantile 부재를 보존한다."""
    current_payload = _inference_authority_payload()
    current_table = read_parquet_bytes(current_payload)
    legacy_payload = parquet_bytes(
        current_table.select(LEGACY_INFERENCE_OUTPUT_COLUMN_NAMES)
    )

    records = demand_predictions_from_inference_parquet(
        legacy_payload,
        expected_base_dttm=BASE,
        expected_sta_ids=("ST-1",),
        schema_version=LEGACY_INFERENCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    )

    assert records[0].rental_pred_mean == 1.5
    assert records[0].return_pred_mean == 2.5
    assert records[0].rental_pred_p90 is None
    assert records[0].return_pred_p10 is None


def test_inference_authority_adapter_binds_expected_station_id_set() -> None:
    """Manifest expected ID와 output station 집합이 다르면 consumer도 fail closed한다."""
    with pytest.raises(ContractViolation, match="station 집합"):
        demand_predictions_from_inference_parquet(
            _inference_authority_payload(),
            expected_base_dttm=BASE,
            expected_sta_ids=("ST-2",),
        )


def test_inference_authority_adapter_rejects_non_target_station_id() -> None:
    """Core의 generic ID 계약을 통과해도 Gold ST-숫자 key가 아니면 거부한다."""
    with pytest.raises(ContractViolation, match="ST-숫자"):
        demand_predictions_from_inference_parquet(
            _inference_authority_payload(("legacy-1",)),
            expected_base_dttm=BASE,
            expected_sta_ids=("legacy-1",),
        )


def test_projection_uses_exact_active_rental_return_intersection() -> None:
    """active와 두 모델 support의 교집합만 12개씩 게시한다."""
    projection = _projection()

    assert projection.expected_sta_ids == ("ST-10", "ST-2")
    assert len(projection.records) == 2 * HORIZON_COUNT
    assert {record.sta_id for record in projection.records} == {"ST-10", "ST-2"}


def test_source_interval_start_becomes_gold_interval_end() -> None:
    """source h target과 Gold predicted 시각의 한 시간 차이를 고정한다."""
    projection = _projection()
    station = [record for record in projection.records if record.sta_id == "ST-10"]

    assert station[0].predicted_dttm == BASE + timedelta(hours=1)
    assert station[-1].predicted_dttm == BASE + timedelta(hours=12)


def test_prediction_rejects_wrong_source_target_for_horizon() -> None:
    """source target이 base+(h-1)시간이 아니면 변환 전에 거부한다."""
    with pytest.raises(ContractViolation, match=r"base\+\(horizon-1\)"):
        _prediction("ST-2", 2, target=BASE + timedelta(hours=2))


def test_prediction_wraps_datetime_overflow_as_contract_failure() -> None:
    """극단 base의 horizon 산술도 raw OverflowError 대신 fail-closed한다."""
    with pytest.raises(ContractViolation, match="datetime 범위"):
        DemandPredictionRecord(
            base_dttm=datetime.max.replace(tzinfo=UTC),
            station_id="ST-1",
            horizon=2,
            target_dttm=datetime.max.replace(tzinfo=UTC),
            rental_pred_mean=1.0,
            rental_pred_p10=0.0,
            rental_pred_p50=1.0,
            rental_pred_p90=2.0,
            return_pred_mean=1.0,
            return_pred_p10=0.0,
            return_pred_p50=1.0,
            return_pred_p90=2.0,
        )


def test_projection_rejects_mixed_base_dttm() -> None:
    """서로 다른 inference batch base를 한 projection에 섞지 않는다."""
    rows = list(_complete_predictions())
    rows[-1] = _prediction("ST-2", 1, base=BASE + timedelta(minutes=5))

    with pytest.raises(ContractViolation, match="base_dttm"):
        _projection(tuple(rows))


def test_projection_rejects_duplicate_station_horizon() -> None:
    """중복 station·horizon이 다른 누락을 가려도 실패한다."""
    rows = list(_complete_predictions())
    rows[-1] = rows[-2]

    with pytest.raises(ContractViolation, match="중복 station·horizon"):
        _projection(tuple(rows))


def test_projection_rejects_extra_inactive_or_unsupported_station() -> None:
    """active·두 model support 교집합 밖 prediction은 extra로 거부한다."""
    rows = _complete_predictions() + tuple(
        _prediction("ST-99", horizon) for horizon in range(1, HORIZON_COUNT + 1)
    )

    with pytest.raises(ContractViolation, match="extra=12"):
        _projection(rows)


def test_projection_rejects_missing_horizon() -> None:
    """기대 station에서 horizon 하나라도 빠지면 partial 전체를 거부한다."""
    with pytest.raises(ContractViolation, match="missing=1"):
        _projection(_complete_predictions()[:-1])


def test_empty_projection_requires_proven_empty_intersection() -> None:
    """세 집합 교집합이 0인 경우에만 빈 prediction을 완전 projection으로 받는다."""
    projection = _projection(
        (),
        active=("ST-1",),
        rental=("ST-1",),
        returned=("ST-2",),
    )
    assert projection.records == ()
    assert projection.expected_sta_ids == ()

    with pytest.raises(ContractViolation, match="missing=12"):
        _projection(
            (),
            active=("ST-1",),
            rental=("ST-1",),
            returned=("ST-1",),
        )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), -0.1])
@pytest.mark.parametrize("field", ["rental", "returned"])
def test_prediction_rejects_nonfinite_and_negative_means(
    value: float,
    field: str,
) -> None:
    """rent와 return mean은 finite·비음수 float64만 허용한다."""
    kwargs = {field: value}
    with pytest.raises(ContractViolation, match="finite·비음수 float64"):
        _prediction("ST-1", 1, **kwargs)


@pytest.mark.parametrize("value", [0, True, "1.0", None])
def test_prediction_requires_python_float64(value: object) -> None:
    """integer·bool·문자열 coercion으로 inference schema drift를 숨기지 않는다."""
    with pytest.raises(ContractViolation, match="float64"):
        _prediction("ST-1", 1, rental=value)  # type: ignore[arg-type]


def test_rounding_is_python_ties_to_even_for_both_counts() -> None:
    """정확한 .5 tie를 PostgreSQL numeric이 아닌 Python even으로 반올림한다."""
    predictions = tuple(
        _prediction(
            "ST-1",
            horizon,
            rental=0.5 if horizon == 1 else 2.5,
            returned=1.5 if horizon == 1 else 3.5,
        )
        for horizon in range(1, HORIZON_COUNT + 1)
    )
    projection = _projection(
        predictions,
        active=("ST-1",),
        rental=("ST-1",),
        returned=("ST-1",),
    )

    assert projection.records[0].predicted_rent_cnt == 0
    assert projection.records[0].predicted_rtn_cnt == 2
    assert projection.records[1].predicted_rent_cnt == 2
    assert projection.records[1].predicted_rtn_cnt == 4


@pytest.mark.parametrize("field", ["rental", "returned"])
def test_rounding_rejects_postgres_integer_overflow(field: str) -> None:
    """ties-to-even 결과가 PostgreSQL INTEGER max를 넘으면 fail-closed한다."""
    rows = list(_complete_predictions(("ST-2",)))
    kwargs = {
        "rental": 1.0,
        "returned": 1.0,
        field: float(POSTGRES_INTEGER_MAX) + 0.5,
    }
    rows[0] = _prediction("ST-2", 12, **kwargs)

    with pytest.raises(ContractViolation, match="PostgreSQL INTEGER"):
        _projection(
            tuple(rows),
            active=("ST-2",),
            rental=("ST-2",),
            returned=("ST-2",),
        )


def test_projection_sorts_deterministically_by_station_and_predicted_time() -> None:
    """입력 순서와 무관하게 (sta_id,predicted_dttm) canonical 순서를 만든다."""
    rows = _complete_predictions()
    first = _projection(rows)
    second = _projection(tuple(reversed(rows)))

    assert first.records == second.records
    assert tuple(
        (record.sta_id, record.predicted_dttm) for record in first.records
    ) == tuple(
        sorted(
            ((record.sta_id, record.predicted_dttm) for record in first.records),
            key=lambda key: (key[0].encode("utf-8"), key[1]),
        )
    )
    assert demand_records_to_parquet(
        first.records,
        expected_sta_ids=first.expected_sta_ids,
    ) == demand_records_to_parquet(
        second.records,
        expected_sta_ids=second.expected_sta_ids,
    )


def test_fixed_schema_parquet_round_trip_is_exact() -> None:
    """output Parquet이 DDL business column schema와 canonical 행을 보존한다."""
    records = _projection().records
    payload = demand_records_to_parquet(
        records,
        expected_sta_ids=_projection().expected_sta_ids,
    )
    table = read_parquet_bytes(payload)

    assert table.schema == pa.schema(
        (
            pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("sta_id", pa.string(), nullable=False),
            pa.field("predicted_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("predicted_rent_cnt", pa.int32(), nullable=False),
            pa.field("predicted_rtn_cnt", pa.int32(), nullable=False),
        )
    )
    assert (
        demand_records_from_parquet(
            payload,
            expected_base_dttm=BASE,
            expected_sta_ids=_projection().expected_sta_ids,
        )
        == records
    )


def test_parquet_reader_rejects_nullable_or_wide_count_schema() -> None:
    """nullable·int64 drift가 값 호환이어도 fixed output으로 받지 않는다."""
    wrong_schema = pa.schema(
        (
            pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("sta_id", pa.string(), nullable=False),
            pa.field("predicted_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("predicted_rent_cnt", pa.int64(), nullable=True),
            pa.field("predicted_rtn_cnt", pa.int32(), nullable=False),
        )
    )
    row = _projection().records[0]
    payload = parquet_bytes(
        pa.Table.from_pylist(
            [
                {
                    "base_dttm": row.base_dttm,
                    "sta_id": row.sta_id,
                    "predicted_dttm": row.predicted_dttm,
                    "predicted_rent_cnt": row.predicted_rent_cnt,
                    "predicted_rtn_cnt": row.predicted_rtn_cnt,
                }
            ],
            schema=wrong_schema,
        )
    )

    with pytest.raises(ContractViolation, match="schema"):
        demand_records_from_parquet(
            payload,
            expected_base_dttm=BASE,
            expected_sta_ids=("ST-10", "ST-2"),
        )


def test_parquet_reader_rejects_incomplete_observed_station_snapshot() -> None:
    """schema가 맞아도 station별 12시간 미만인 output payload는 거부한다."""
    projection = _projection()
    complete = demand_records_to_parquet(
        projection.records,
        expected_sta_ids=projection.expected_sta_ids,
    )
    table = read_parquet_bytes(complete).slice(0, HORIZON_COUNT - 1)

    with pytest.raises(ContractViolation, match="완전하지 않습니다"):
        demand_records_from_parquet(
            parquet_bytes(table),
            expected_base_dttm=BASE,
            expected_sta_ids=projection.expected_sta_ids,
        )


def test_parquet_boundary_rejects_whole_station_omission() -> None:
    """payload 자체에서 station 집합을 역추론해 전체 누락을 숨기지 않는다."""
    projection = _projection()
    subset = tuple(record for record in projection.records if record.sta_id != "ST-2")

    with pytest.raises(ContractViolation, match="완전하지 않습니다"):
        demand_records_to_parquet(
            subset,
            expected_sta_ids=projection.expected_sta_ids,
        )


def test_empty_demand_uses_no_artifact_instead_of_empty_parquet() -> None:
    """조건부 EMPTY publication은 artifacts=[] 계약을 우회하지 않는다."""
    with pytest.raises(ContractViolation, match=r"artifacts=\[\]"):
        demand_records_to_parquet((), expected_sta_ids=())


@pytest.mark.parametrize("station_id", ["1", "ST-A", "ST-1 ", " ST-1", "ST-"])
def test_station_id_must_match_target_schema_pattern(station_id: str) -> None:
    """source와 support 모두 target의 canonical ST-숫자 ID만 허용한다."""
    with pytest.raises(ContractViolation, match="ST-숫자"):
        _prediction(station_id, 1)


def test_projection_object_rejects_noncanonical_record_order() -> None:
    """직접 만든 projection도 artifact row 순서 drift를 허용하지 않는다."""
    valid = _projection()
    with pytest.raises(ContractViolation, match="순이어야"):
        DemandProjection(
            valid.base_dttm,
            valid.expected_sta_ids,
            tuple(reversed(valid.records)),
        )
