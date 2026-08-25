"""여러 held-out 결과 집계가 계약 불일치와 날짜별 악화를 숨기지 않는지 검증한다."""

import copy
import json
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from core.scoring_config import URGENCY_SCORING_CONFIG_VERSION
from evaluation import aggregate_policy_results as aggregate_module
from evaluation.aggregate_policy_results import aggregate_results, main
from evaluation.backtest_contract import BACKTEST_CONTRACT_VERSION, PRIMARY_METRIC
from evaluation.production_policy_contract import (
    PRODUCTION_TARGET_DATES,
    production_evidence_scope,
    PRODUCTION_POLICY_NAME,
    production_policy_configuration,
)
from gold.rebalance_route import ROUTE_ALGORITHM_VERSION

SEOUL = ZoneInfo("Asia/Seoul")


def _policy(
    name: str,
    requests: int,
    fulfilled: int,
    empty: float,
    *,
    window_start: str,
    window_end: str,
    configuration: dict | None = None,
) -> dict:
    """집계 테스트용 최소 정책 결과를 만든다."""
    unfulfilled = requests - fulfilled
    return {
        "policy": name,
        "policy_configuration": (
            production_policy_configuration()
            if configuration is None and name == PRODUCTION_POLICY_NAME
            else configuration
            or {
                "version": "test-policy-v1",
                "max_stops_per_route": 5,
            }
        ),
        "window_start": window_start,
        "window_end": window_end,
        "observed_requests": requests,
        "fulfilled_requests": fulfilled,
        "unfulfilled_requests": unfulfilled,
        "observed_demand_fulfillment_rate": fulfilled / requests,
        "empty_station_minutes": empty,
        "moved_bikes": 1 if name != "no_rebalance" else 0,
        "dispatched_routes": 1 if name != "no_rebalance" else 0,
        "vehicle_busy_minutes": 10.0 if name != "no_rebalance" else 0.0,
        "planned_bikes": 1 if name != "no_rebalance" else 0,
        "movement_budget_used": 1 if name != "no_rebalance" else 0,
        "unfulfilled_request_log": [
            {
                "bike_id": f"B-{index}",
                "rented_at": window_start,
                "station_no": index + 1,
            }
            for index in range(unfulfilled)
        ],
    }


def _source_file(name: str, digest: str) -> dict:
    """집계 테스트용 원천 파일 fingerprint를 만든다."""
    return {"path": f"/fixtures/{name}", "size_bytes": 1, "sha256": digest}


def _document(target_date: str, baseline_fulfilled: int, model_fulfilled: int) -> dict:
    """계약이 같은 날짜별 60·120·180분 결과를 만든다."""
    durations = (60, 120, 180)
    scope = production_evidence_scope()
    station_surface = scope["station_surface_sha256_by_date"].get(
        target_date,
        next(iter(scope["station_surface_sha256_by_date"].values())),
    )
    station_count = scope["station_count_by_date"].get(
        target_date,
        next(iter(scope["station_count_by_date"].values())),
    )
    start = datetime.fromisoformat(target_date).replace(
        hour=scope["start_hour"],
        tzinfo=SEOUL,
    )
    return {
        "target_date": target_date,
        "center_id": scope["center_id"],
        "start_hour": scope["start_hour"],
        "model_bundle_sha256": scope["model_bundle_sha256"],
        "source_provenance": {
            "rental_csv": _source_file("rental.csv", "b" * 64),
            "stock_csv": _source_file("stock.csv", "c" * 64),
            "weather_csv": _source_file(
                "weather.csv",
                scope["weather_csv_sha256"],
            ),
            "population_csvs": [_source_file("population.csv", "e" * 64)],
            "station_master_content_sha256": station_surface,
            "station_crosswalk_count": 10,
            "station_crosswalk_sha256": "1" * 64,
            "population_excluded_station_count": 0,
            "population_excluded_grid_ids": [],
            "backtest_contract_version": BACKTEST_CONTRACT_VERSION,
            "route_algorithm_version": ROUTE_ALGORITHM_VERSION,
            "urgency_scoring_config_version": URGENCY_SCORING_CONFIG_VERSION,
        },
        "evidence_gate": {
            "point_in_time_feature_inputs": True,
            "operation_contract_passed": True,
            "legacy_endpoint_reconciliation_passed": True,
            "heldout_day_of_month": True,
        },
        "contracts": [
            {
                "primary_metric": PRIMARY_METRIC,
                "contract": {
                    "target_date": target_date,
                    "start_hour": scope["start_hour"],
                    "evaluation_minutes": minutes,
                    **scope["operation_contract"],
                }
            }
            for minutes in durations
        ],
        "durations": [
            {
                "evaluation_minutes": minutes,
                "station_count": station_count,
                "legacy_movement": {
                    "balanced_movement_budget": 1,
                    "added_bikes": 1,
                    "removed_bikes": 1,
                },
                "legacy_timing": [
                    {
                        "empty_station_minutes": 90.0,
                        "negative_station_minutes": 0.0,
                        "endpoint_max_absolute_error": 0,
                    }
                ],
                "no_rebalance": _policy(
                    "no_rebalance",
                    100,
                    baseline_fulfilled,
                    100.0,
                    window_start=start.isoformat(),
                    window_end=(start + timedelta(minutes=minutes)).isoformat(),
                ),
                "model_policies": [
                    _policy(
                        PRODUCTION_POLICY_NAME,
                        100,
                        model_fulfilled,
                        80.0,
                        window_start=start.isoformat(),
                        window_end=(
                            start + timedelta(minutes=minutes)
                        ).isoformat(),
                    )
                ],
            }
            for minutes in durations
        ],
    }


def _production_documents(
    baseline_fulfilled: int = 99,
    model_fulfilled: int = 100,
) -> tuple[dict, ...]:
    """Production evidence scope 전체 날짜의 합성 결과를 만든다."""
    return tuple(
        _document(target.isoformat(), baseline_fulfilled, model_fulfilled)
        for target in PRODUCTION_TARGET_DATES
    )


@pytest.fixture(autouse=True)
def _bind_synthetic_input_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """합성 원천의 날짜별 digest만 gate 기대값에 결속해 공격 검증을 격리한다."""
    documents = _production_documents()
    expected = production_evidence_scope()
    expected["input_provenance_sha256_by_date"] = {
        document["target_date"]: aggregate_module._input_provenance_sha256(
            aggregate_module._validate_source_provenance(
                document,
                document["target_date"],
            )
        )
        for document in documents
    }
    monkeypatch.setattr(
        aggregate_module,
        "production_evidence_scope",
        lambda: copy.deepcopy(expected),
    )


def test_production_evidence_scope_pins_monthly_surfaces() -> None:
    """Production 날짜별 provenance·station surface·개수를 exact 값으로 고정한다."""
    scope = production_evidence_scope()

    assert scope["input_provenance_sha256_by_date"] == {
        "2025-03-17": "d32e89245e11728cecef3333644492fae32637c6af8522cd9c8ca1ec433d6ba4",
        "2025-04-17": "5249d04d9a5ac616709494111765aa4c486fa73f05c8f64988cb16006c99d4ab",
        "2025-05-17": "dff232ccd391edfe4490eed990e98144666dcac657592aeb2bcf2d1a14839ae0",
        "2025-06-17": "19b2d0fd4a763063f5fa086bdc3d28a0bd905bcc1dcf2cef38b26646a37730f6",
        "2025-07-17": "d9f5a37351628ffb9fa723211cb3cbcec02b18eea04ce97cdadf57f620b29868",
        "2025-08-17": "92732889294d82ae3862e27a417acd9a6012fdfb5eb67062359ec8dfdb7f4a23",
        "2025-09-17": "8ec6e100e7fb3e8248dde909f3195837bb613a9cdeefde9e71310898ca541343",
        "2025-10-17": "51e781d367a26844dc6008b5e593eacde18e2ffc06f553cb96979c8a6b502c74",
        "2025-11-17": "74864cccee867ca3cad9c87f91af4711a9b076ad4b072db3d1af13377fa87463",
        "2025-12-17": "a52cc0ab62fab047892d1c8e48537c08793d1290eb6fb04f64712f3c82f8cbf5",
    }
    assert scope["station_surface_sha256_by_date"] == {
        "2025-03-17": "17c2c5b98728d932bb6460db39c6ca2189629d773a3d86c7eb5bc918282a74e4",
        "2025-04-17": "17c2c5b98728d932bb6460db39c6ca2189629d773a3d86c7eb5bc918282a74e4",
        "2025-05-17": "394df2124f4395f7bc772ac82895fb3ecc6f4e81865fa0a8f659f29d32485f19",
        "2025-06-17": "fb34af1b0ea10c67030fced70af9db5016649548562596591be8b8873213e843",
        "2025-07-17": "fb34af1b0ea10c67030fced70af9db5016649548562596591be8b8873213e843",
        "2025-08-17": "30e75ad4bb0d334609c78ca303a88a058cc3b8446e9271adf45ae7d8a9ae6ef2",
        "2025-09-17": "8f5b8ef0cb6ffb8f85f48ab035c56f8ed2715506ccc7f237bbc07378790410a4",
        "2025-10-17": "8f5b8ef0cb6ffb8f85f48ab035c56f8ed2715506ccc7f237bbc07378790410a4",
        "2025-11-17": "88a6edbf7222a241af6eb324fffffa72f91ab554a28bfc962f62096a97f0118b",
        "2025-12-17": "3d8a49f5cb8ec13199047f4007e21b51bd70698c0d761d05461861e193057932",
    }
    assert scope["station_count_by_date"] == {
        "2025-03-17": 262,
        "2025-04-17": 262,
        "2025-05-17": 263,
        "2025-06-17": 264,
        "2025-07-17": 264,
        "2025-08-17": 265,
        "2025-09-17": 266,
        "2025-10-17": 266,
        "2025-11-17": 267,
        "2025-12-17": 267,
    }


def test_input_provenance_digest_excludes_versions_but_binds_inputs() -> None:
    """날짜 digest는 코드 버전을 제외하고 실제 입력 audit 변경에는 반응한다."""
    document = _document("2025-03-17", 99, 100)
    audit = aggregate_module._validate_source_provenance(document, "2025-03-17")
    original = aggregate_module._input_provenance_sha256(audit)
    version_changed = dict(audit)
    version_changed["backtest_contract_version"] = "another-version"
    version_changed["route_algorithm_version"] = "another-route"
    version_changed["urgency_scoring_config_version"] = "another-scoring"
    input_changed = dict(audit)
    input_changed["rental_csv_sha256"] = "9" * 64

    assert aggregate_module._input_provenance_sha256(version_changed) == original
    assert aggregate_module._input_provenance_sha256(input_changed) != original


def test_aggregate_reports_better_and_worse_dates_separately() -> None:
    """micro 평균만으로 날짜 하나의 서비스 악화를 가리지 않는다."""
    result = aggregate_results(
        (_document("2025-05-17", 98, 99), _document("2025-06-17", 100, 99))
    )
    row = next(
        row
        for row in result["rows"]
        if row["policy"] == PRODUCTION_POLICY_NAME
        and row["evaluation_minutes"] == 120
    )
    assert row["dates_fulfillment_better"] == 1
    assert row["dates_fulfillment_worse"] == 1
    assert row["empty_station_minutes_change_vs_no_rebalance_pct"] == -20.0


def test_aggregate_rejects_resource_contract_mismatch() -> None:
    """날짜 사이 fleet가 다르면 같은 실험으로 집계하지 않는다."""
    first = _document("2025-05-17", 100, 100)
    second = _document("2025-06-17", 100, 100)
    second["contracts"][0]["contract"]["fleet_size"] = 2
    with pytest.raises(ValueError, match="operation contract|운영 계약"):
        aggregate_results((first, second))


@pytest.mark.parametrize("value", (None, "empty_station_minutes", ""))
def test_aggregate_rejects_wrong_primary_metric(value: object) -> None:
    """모든 결과가 같은 값이어도 등록 primary metric이 아니면 거부한다."""
    document = _document("2025-05-17", 99, 100)
    for contract in document["contracts"]:
        contract["primary_metric"] = value

    with pytest.raises(ValueError, match="primary_metric"):
        aggregate_results((document,))


def test_aggregate_reports_primary_metric_in_markdown() -> None:
    """집계 JSON과 Markdown이 release 판단의 primary metric을 명시한다."""
    result = aggregate_results((_document("2025-05-17", 99, 100),))

    assert result["primary_metric"] == PRIMARY_METRIC
    assert (
        f"- Primary metric: `{PRIMARY_METRIC}`"
        in aggregate_module.aggregate_markdown(result)
    )


def test_acceptance_gate_rejects_mutated_aggregate_primary_metric() -> None:
    """집계 후 primary metric metadata가 바뀌어도 release gate가 닫힌다."""
    result = aggregate_results(_production_documents())
    result["primary_metric"] = "empty_station_minutes"

    gate = aggregate_module.evaluate_acceptance_gate(result)

    assert gate["primary_metric_matches"] is False
    assert gate["passed"] is False
    assert gate["passing_policies"] == []


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("backtest_contract_version", "point-in-time-policy-backtest-v2"),
        ("route_algorithm_version", "route-v2"),
        ("urgency_scoring_config_version", "urgency-scoring-v1"),
    ),
)
def test_aggregate_rejects_stale_production_versions(field: str, value: str) -> None:
    """과거 코드 버전 결과는 같은 suite에 하나만 있어도 fail-closed한다."""
    document = _document("2025-05-17", 99, 100)
    document["source_provenance"][field] = value
    with pytest.raises(ValueError, match=field):
        aggregate_results((document,))


def test_aggregate_rejects_source_surface_mismatch() -> None:
    """날짜 사이 공통 weather·station surface가 바뀌면 집계하지 않는다."""
    first = _document("2025-05-17", 99, 100)
    second = _document("2025-06-17", 99, 100)
    second["source_provenance"]["weather_csv"]["sha256"] = "2" * 64
    with pytest.raises(ValueError, match="source provenance"):
        aggregate_results((first, second))


def test_aggregate_accepts_and_exposes_expected_monthly_station_surfaces() -> None:
    """날짜별 정상 대여소 surface 변화는 보존한 채 같은 suite로 집계한다."""
    first = _document("2025-05-17", 99, 100)
    second = _document("2025-06-17", 99, 100)

    result = aggregate_results((first, second))

    assert result["station_surface_sha256_by_date"] == {
        "2025-05-17": first["source_provenance"][
            "station_master_content_sha256"
        ],
        "2025-06-17": second["source_provenance"][
            "station_master_content_sha256"
        ],
    }


def test_aggregate_rejects_policy_configuration_mismatch() -> None:
    """같은 policy label이 다른 설정을 가리키는 집계를 거부한다."""
    first = _document("2025-05-17", 99, 100)
    second = _document("2025-06-17", 99, 100)
    second["durations"][1]["model_policies"][0]["policy_configuration"][
        "max_stops_per_route"
    ] = 8
    with pytest.raises(ValueError, match="policy_configuration"):
        aggregate_results((first, second))


def test_aggregate_rejects_incomplete_policy_duration_matrix() -> None:
    """한 날짜·구간에서 정책 하나가 빠지면 평균으로 숨기지 않는다."""
    document = _document("2025-05-17", 99, 100)
    document["durations"][1]["model_policies"] = []
    with pytest.raises(ValueError, match="비어 있습니다|불완전"):
        aggregate_results((document,))


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("blank_bike_id", "bike_id"),
        ("missing_key", "key가 exact"),
        ("extra_key", "key가 exact"),
        ("invalid_timestamp", "ISO-8601"),
        ("naive_timestamp", "timezone"),
        ("invalid_station", "station_no"),
    ),
)
def test_aggregate_rejects_invalid_unfulfilled_event_schema(
    mutation: str,
    message: str,
) -> None:
    """미충족 이벤트는 exact 3-key·aware 시각·양수 대여소를 요구한다."""
    document = _document("2025-05-17", 99, 100)
    event = document["durations"][0]["no_rebalance"][
        "unfulfilled_request_log"
    ][0]
    if mutation == "blank_bike_id":
        event["bike_id"] = "   "
    elif mutation == "missing_key":
        event.pop("bike_id")
    elif mutation == "extra_key":
        event["station_id"] = "ST-1"
    elif mutation == "invalid_timestamp":
        event["rented_at"] = "not-a-timestamp"
    elif mutation == "naive_timestamp":
        event["rented_at"] = "2025-01-01T00:00:00"
    else:
        event["station_no"] = True

    with pytest.raises(ValueError, match=message):
        aggregate_results((document,))


@pytest.mark.parametrize("field", ("window_start", "window_end"))
def test_aggregate_rejects_nonexact_policy_window(field: str) -> None:
    """동일 instant 표현이어도 정책 창은 계약의 KST 문자열과 exact해야 한다."""
    document = _document("2025-05-17", 99, 100)
    policy = document["durations"][0]["model_policies"][0]
    policy[field] = datetime.fromisoformat(policy[field]).astimezone(UTC).isoformat()

    with pytest.raises(ValueError, match=field):
        aggregate_results((document,))


@pytest.mark.parametrize("boundary", ("before", "end"))
def test_aggregate_rejects_event_outside_policy_window(boundary: str) -> None:
    """미충족 이벤트 시각은 정책별 반개구간 [start,end) 안에 있어야 한다."""
    document = _document("2025-05-17", 99, 100)
    baseline = document["durations"][0]["no_rebalance"]
    start = datetime.fromisoformat(baseline["window_start"])
    end = datetime.fromisoformat(baseline["window_end"])
    event = baseline["unfulfilled_request_log"][0]
    event["rented_at"] = (
        start - timedelta(microseconds=1) if boundary == "before" else end
    ).isoformat()

    with pytest.raises(ValueError, match=r"\[window_start, window_end\)"):
        aggregate_results((document,))


def test_aggregate_normalizes_equivalent_event_offsets_before_set_diff() -> None:
    """같은 실패 instant의 다른 offset 표기가 신규 실패로 오인되지 않는다."""
    document = _document("2025-05-17", 98, 99)
    candidate = document["durations"][0]["model_policies"][0]
    candidate_event = candidate["unfulfilled_request_log"][0]
    candidate_event["rented_at"] = datetime.fromisoformat(
        candidate_event["rented_at"]
    ).astimezone(UTC).isoformat()

    result = aggregate_results((document,))
    row = next(
        value
        for value in result["rows"]
        if value["policy"] == PRODUCTION_POLICY_NAME
        and value["evaluation_minutes"] == 60
    )
    comparison = row["per_date_comparison"][0]

    assert comparison["new_unfulfilled_request_count"] == 0
    assert comparison["new_unfulfilled_request_keys"] == []
    assert comparison["resolved_unfulfilled_request_count"] == 1


def test_aggregate_rejects_unfulfilled_log_count_mismatch() -> None:
    """미충족 수와 event log 길이가 다르면 집계 전에 거부한다."""
    document = _document("2025-05-17", 99, 100)
    document["durations"][0]["no_rebalance"]["unfulfilled_request_log"] = []

    with pytest.raises(ValueError, match="로그|log 길이"):
        aggregate_results((document,))


def test_aggregate_rejects_duplicate_unfulfilled_event_key() -> None:
    """같은 미충족 event key를 수량 맞추기에 중복 사용하는 것을 거부한다."""
    document = _document("2025-05-17", 98, 100)
    log = document["durations"][0]["no_rebalance"]["unfulfilled_request_log"]
    log[1] = copy.deepcopy(log[0])

    with pytest.raises(ValueError, match="중복 event key"):
        aggregate_results((document,))


@pytest.mark.parametrize("value", (0, -1, 1.5, True, None))
def test_aggregate_rejects_invalid_station_count(value: object) -> None:
    """Duration station_count는 bool이 아닌 양의 정수여야 한다."""
    document = _document("2025-05-17", 99, 100)
    document["durations"][0]["station_count"] = value

    with pytest.raises(ValueError, match="station_count"):
        aggregate_results((document,))


def test_aggregate_rejects_station_count_mismatch_between_horizons() -> None:
    """같은 날짜의 모든 horizon은 동일한 station surface 개수를 사용해야 한다."""
    document = _document("2025-05-17", 99, 100)
    document["durations"][1]["station_count"] += 1

    with pytest.raises(ValueError, match="구간별 station_count"):
        aggregate_results((document,))


def test_acceptance_gate_requires_non_worsening_and_strict_improvements() -> None:
    """모든 날짜·구간 비악화와 180분·품절 aggregate strict 개선을 통과한다."""
    result = aggregate_results(_production_documents())
    gate = result["acceptance_gate"]
    assert gate["passed"] is True
    assert result["schema_version"] == "point-in-time-policy-suite-v3"
    assert result["primary_metric"] == PRIMARY_METRIC
    assert gate["version"] == "production-policy-acceptance-gate-v3"
    assert gate["primary_metric_matches"] is True
    assert result["candidate_gate"]["version"] == "policy-candidate-gate-v2"
    assert gate["passing_policies"] == [PRODUCTION_POLICY_NAME]
    assert gate["production_evidence_scope_matches"] is True
    assert result["center_id"] == production_evidence_scope()["center_id"]
    assert result["start_hour"] == production_evidence_scope()["start_hour"]
    assert result["station_surface_sha256_by_date"] == production_evidence_scope()[
        "station_surface_sha256_by_date"
    ]
    assert result["station_count_by_date"] == production_evidence_scope()[
        "station_count_by_date"
    ]
    assert set(result["input_provenance_sha256_by_date"]) == {
        target.isoformat() for target in PRODUCTION_TARGET_DATES
    }
    assert all(gate["policies"][0]["aggregate_empty_by_horizon"].values())


def test_acceptance_gate_rejects_one_day_subset() -> None:
    """지표가 좋아도 production 10일 중 일부만 있으면 release하지 않는다."""
    result = aggregate_results((_production_documents()[0],))

    assert result["candidate_gate"]["passed"] is True
    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["evidence_scope_checks"][
        "exact_result_count"
    ] is False


def test_acceptance_gate_rejects_different_date_surface() -> None:
    """10개 결과여도 사전 고정한 3~12월 17일 집합이 아니면 release하지 않는다."""
    documents = list(_production_documents())
    replacement = _document("2025-01-17", 99, 100)
    documents[0] = replacement

    result = aggregate_results(tuple(documents))

    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["evidence_scope_checks"][
        "exact_target_dates"
    ] is False


def test_aggregate_rejects_mixed_centers() -> None:
    """서로 다른 센터 결과는 하나의 집계 문서로 만들지 않는다."""
    documents = list(_production_documents())
    documents[0]["center_id"] = "another-center"

    with pytest.raises(ValueError, match="center_id"):
        aggregate_results(tuple(documents))


def test_aggregate_rejects_top_level_start_contract_mismatch() -> None:
    """Top-level 시작 시각과 내부 평가 계약이 다르면 fail-closed한다."""
    documents = list(_production_documents())
    documents[0]["start_hour"] = 22

    with pytest.raises(ValueError, match="top-level start_hour"):
        aggregate_results(tuple(documents))


@pytest.mark.parametrize("surface", ("center_id", "start_hour"))
def test_acceptance_gate_rejects_consistent_wrong_location_or_start(
    surface: str,
) -> None:
    """일관된 값이어도 production 센터·시작 시각과 다르면 release하지 않는다."""
    documents = list(_production_documents())
    for document in documents:
        if surface == "center_id":
            document["center_id"] = "another-center"
        else:
            document["start_hour"] = 22
            for audit in document["contracts"]:
                audit["contract"]["start_hour"] = 22
            start = datetime.fromisoformat(document["target_date"]).replace(
                hour=22,
                tzinfo=SEOUL,
            )
            for duration in document["durations"]:
                end = start + timedelta(
                    minutes=duration["evaluation_minutes"]
                )
                for policy in (
                    duration["no_rebalance"],
                    *duration["model_policies"],
                ):
                    policy["window_start"] = start.isoformat()
                    policy["window_end"] = end.isoformat()
                    for event in policy["unfulfilled_request_log"]:
                        event["rented_at"] = start.isoformat()

    result = aggregate_results(tuple(documents))

    assert result["candidate_gate"]["passed"] is True
    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["evidence_scope_checks"][
        f"exact_{surface}"
    ] is False


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("fleet_size", 1),
        ("speed_kmh", 7.5),
        ("service_minutes_per_stop", 0.0),
        ("approval_delay_minutes", 99),
    ),
)
def test_acceptance_gate_rejects_different_operation_scope(
    field: str,
    value: object,
) -> None:
    """모든 날짜가 같아도 production 자원 계약과 다르면 release하지 않는다."""
    documents = list(_production_documents())
    for document in documents:
        for audit in document["contracts"]:
            audit["contract"][field] = value

    result = aggregate_results(tuple(documents))

    assert result["candidate_gate"]["passed"] is True
    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["evidence_scope_checks"][
        "exact_operation_contract"
    ] is False


def test_acceptance_gate_rejects_consistent_wrong_station_count() -> None:
    """모든 horizon이 일관돼도 날짜별 production station_count와 다르면 거부한다."""
    documents = list(_production_documents())
    for document in documents:
        for duration in document["durations"]:
            duration["station_count"] = 999

    result = aggregate_results(tuple(documents))

    assert result["candidate_gate"]["passed"] is True
    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["evidence_scope_checks"][
        "exact_station_count_by_date"
    ] is False


@pytest.mark.parametrize(
    ("surface", "value"),
    (
        ("model_bundle_sha256", "9" * 64),
        ("station_master_content_sha256", "8" * 64),
        ("weather_csv", "7" * 64),
        ("rental_csv", "6" * 64),
        ("stock_csv", "5" * 64),
        ("population_csvs", "4" * 64),
        ("station_crosswalk_sha256", "3" * 64),
        ("station_crosswalk_count", 9999),
    ),
)
def test_acceptance_gate_rejects_different_fingerprinted_scope(
    surface: str,
    value: object,
) -> None:
    """날짜별 원천·대여소·crosswalk fingerprint 변경을 release하지 않는다."""
    documents = list(_production_documents())
    for document in documents:
        if surface == "model_bundle_sha256":
            document[surface] = value
        elif surface in {
            "station_master_content_sha256",
            "station_crosswalk_sha256",
            "station_crosswalk_count",
        }:
            document["source_provenance"][surface] = value
        elif surface == "population_csvs":
            document["source_provenance"][surface][0]["sha256"] = value
        else:
            document["source_provenance"][surface]["sha256"] = value

    result = aggregate_results(tuple(documents))

    assert result["candidate_gate"]["passed"] is True
    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["production_evidence_scope_matches"] is False


def test_acceptance_gate_rejects_arbitrary_candidate_configuration() -> None:
    """지표가 좋아도 production 이름·exact 설정이 아닌 후보는 release가 아니다."""
    document = _document("2025-05-17", 99, 100)
    for duration in document["durations"]:
        baseline = duration["no_rebalance"]
        duration["model_policies"] = [
            _policy(
                "experimental_policy",
                100,
                100,
                80.0,
                window_start=baseline["window_start"],
                window_end=baseline["window_end"],
            )
        ]

    result = aggregate_results((document,))

    assert result["candidate_gate"]["passed"] is True
    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["production_policy_present"] is False


def test_acceptance_gate_cannot_be_masked_by_passing_experiment() -> None:
    """Production 악화를 다른 실험 후보의 개선으로 가려 release할 수 없다."""
    document = _document("2025-05-17", 99, 98)
    for duration in document["durations"]:
        baseline = duration["no_rebalance"]
        duration["model_policies"].append(
            _policy(
                "experimental_policy",
                100,
                100,
                80.0,
                window_start=baseline["window_start"],
                window_end=baseline["window_end"],
            )
        )

    result = aggregate_results((document,))

    assert result["candidate_gate"]["passing_policies"] == ["experimental_policy"]
    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["passing_policies"] == []


def test_acceptance_gate_requires_exact_production_configuration() -> None:
    """Production label이어도 작업 상한을 포함한 설정이 다르면 release하지 않는다."""
    documents = list(_production_documents())
    for document in documents:
        for duration in document["durations"]:
            duration["model_policies"][0]["policy_configuration"][
                "max_stops_per_route"
            ] = 8

    result = aggregate_results(tuple(documents))

    assert result["candidate_gate"]["passed"] is True
    assert result["acceptance_gate"]["passed"] is False
    assert (
        result["acceptance_gate"]["production_policy_configuration_matches"]
        is False
    )


def test_acceptance_gate_requires_all_production_horizons() -> None:
    """180분 단독 개선은 후보 진단일 뿐 production release가 아니다."""
    documents = list(_production_documents())
    for document in documents:
        document["contracts"] = document["contracts"][2:]
        document["durations"] = document["durations"][2:]

    result = aggregate_results(tuple(documents))

    assert result["evaluation_minutes"] == [180]
    assert result["candidate_gate"]["passed"] is True
    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"]["required_horizons_present"] is False


def test_acceptance_gate_fails_on_single_date_fulfillment_regression() -> None:
    """합산 수치와 무관하게 날짜 하나의 관측 대여 충족 악화를 거부한다."""
    documents = list(_production_documents())
    for duration in documents[0]["durations"]:
        baseline = duration["no_rebalance"]
        baseline["fulfilled_requests"] = 100
        baseline["unfulfilled_requests"] = 0
        baseline["observed_demand_fulfillment_rate"] = 1.0
        baseline["unfulfilled_request_log"] = []
        policy = duration["model_policies"][0]
        policy["fulfilled_requests"] = 99
        policy["unfulfilled_requests"] = 1
        policy["observed_demand_fulfillment_rate"] = 0.99
        policy["unfulfilled_request_log"] = [
            {
                "bike_id": "B-new",
                "rented_at": policy["window_start"],
                "station_no": 999,
            }
        ]
    result = aggregate_results(tuple(documents))
    gate = next(
        row
        for row in result["acceptance_gate"]["policies"]
        if row["policy"] == PRODUCTION_POLICY_NAME
    )

    assert gate["passed"] is False
    assert gate["all_date_horizon_unfulfilled_non_worsening"] is False


def test_acceptance_gate_rejects_failure_shifting_at_equal_better_total() -> None:
    """총 미충족이 줄어도 baseline에 없던 시민 실패를 새로 만들면 거부한다."""
    documents = list(_production_documents(98, 99))
    candidate = documents[0]["durations"][0]["model_policies"][0]
    candidate["unfulfilled_request_log"] = [
        {
            "bike_id": "B-newly-failed",
            "rented_at": "2025-03-17T06:30:00+09:00",
            "station_no": 999,
        }
    ]

    result = aggregate_results(tuple(documents))
    gate = next(
        row
        for row in result["acceptance_gate"]["policies"]
        if row["policy"] == PRODUCTION_POLICY_NAME
    )
    comparison = next(
        row
        for row in next(
            row
            for row in result["rows"]
            if row["policy"] == PRODUCTION_POLICY_NAME
            and row["evaluation_minutes"] == 60
        )["per_date_comparison"]
        if row["date"] == documents[0]["target_date"]
    )

    assert gate["all_date_horizon_unfulfilled_non_worsening"] is True
    assert gate["aggregate_180_unfulfilled_strict_improvement"] is True
    assert gate["all_date_horizon_new_unfulfilled_request_set_empty"] is False
    assert gate["passed"] is False
    assert comparison["new_unfulfilled_request_count"] == 1
    assert comparison["new_unfulfilled_request_keys"] == [
        {
            **candidate["unfulfilled_request_log"][0],
            "rented_at": "2025-03-16T21:30:00+00:00",
        }
    ]


def test_acceptance_gate_fails_on_single_date_empty_regression() -> None:
    """aggregate가 개선돼도 날짜 하나의 품절 시간 악화를 채택 gate가 거부한다."""
    documents = list(_production_documents())
    documents[0]["durations"][0]["model_policies"][0][
        "empty_station_minutes"
    ] = 101.0
    result = aggregate_results(tuple(documents))
    gate = result["acceptance_gate"]["policies"][0]
    assert gate["passed"] is False
    assert gate["all_date_horizon_empty_non_worsening"] is False


def test_acceptance_gate_requires_strict_180_unmet_improvement() -> None:
    """모든 구간이 동일한 미충족이면 180분 strict 개선 기준을 통과하지 못한다."""
    result = aggregate_results(_production_documents(100, 100))
    gate = result["acceptance_gate"]["policies"][0]
    assert gate["passed"] is False
    assert gate["aggregate_180_unfulfilled_strict_improvement"] is False


def test_acceptance_gate_requires_strict_aggregate_empty_by_horizon() -> None:
    """날짜별 비악화만으로는 부족하고 각 horizon의 품절 합계가 strict 개선돼야 한다."""
    documents = list(_production_documents())
    for document in documents:
        document["durations"][1]["model_policies"][0][
            "empty_station_minutes"
        ] = 100.0
    result = aggregate_results(tuple(documents))
    gate = result["acceptance_gate"]["policies"][0]
    assert gate["passed"] is False
    assert gate["aggregate_empty_by_horizon"]["120"] is False


def test_cli_returns_nonzero_when_no_policy_passes(tmp_path) -> None:
    """채택 기준을 만족하는 정책이 없으면 집계 CLI가 nonzero로 끝난다."""
    document = _document("2025-05-17", 100, 99)
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(document), encoding="utf-8")
    exit_code = main(
        (
            "--inputs",
            str(input_path),
            "--output-json",
            str(tmp_path / "aggregate.json"),
            "--output-markdown",
            str(tmp_path / "aggregate.md"),
        )
    )
    assert exit_code == 1


def test_cli_returns_zero_only_for_exact_production_release(tmp_path) -> None:
    """Standalone 집계 CLI는 exact production 전체 gate 통과 때만 0을 반환한다."""
    input_paths = []
    for index, document in enumerate(_production_documents()):
        input_path = tmp_path / f"input-{index}.json"
        input_path.write_text(json.dumps(document), encoding="utf-8")
        input_paths.append(input_path)

    exit_code = main(
        (
            "--inputs",
            *(str(path) for path in input_paths),
            "--output-json",
            str(tmp_path / "aggregate.json"),
            "--output-markdown",
            str(tmp_path / "aggregate.md"),
        )
    )

    assert exit_code == 0
