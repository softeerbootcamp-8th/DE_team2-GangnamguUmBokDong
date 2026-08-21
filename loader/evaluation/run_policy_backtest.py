"""실제 2025 원천과 고정 모델로 point-in-time 재배치 백테스트를 실행한다."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from gold.demand import DemandForecastRecord
from gold.rebalance_route import DispatchCenterTopology

from .backtest_contract import (
    EVIDENCE_GRADE,
    EvaluationContract,
    validate_sensitivity_contracts,
)
from .historical_inputs import (
    HORIZON_COUNT,
    HistoricalStation,
    ModelBundle,
    PredictionAudit,
    build_population_nowcast,
    latest_published_weather,
    load_model_bundle,
    load_station_master_from_s3,
    predict_point_in_time,
    read_weather_history,
)
from .legacy_baseline import (
    LegacyMovementEstimate,
    LegacyTimingMetrics,
    infer_legacy_movements,
    replay_legacy_timing,
)
from .policy_simulator import SimulationMetrics, simulate_no_rebalance, simulate_policy
from .rebalance_backtest import (
    BikeLineageReadResult,
    RentalTrip,
    StockObservation,
    load_centers,
    read_bike_relocation_intervals,
    read_rental_trips,
    read_station_crosswalk,
    read_stock_observations,
)

SEOUL = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True, slots=True)
class EvidenceGate:
    """결과에서 허용할 수 없는 주장을 기계 판독 가능하게 표현한다."""

    point_in_time_feature_inputs: bool
    operation_contract_passed: bool
    legacy_endpoint_reconciliation_passed: bool
    heldout_day_of_month: bool
    model_existed_at_historical_anchor: bool
    latent_failed_demand_observed: bool
    operator_route_logs_observed: bool
    bike_lineage_observed: bool
    bike_lineage_hybrid_replay_passed: bool
    same_bike_movement_budget_cap_enforced: bool
    common_residual_movement_cap_enforced: bool
    same_vehicle_budget_proven: bool
    causal_superiority_vs_legacy_allowed: bool
    publication_grade_system_claim_allowed: bool
    reason: str


@dataclass(frozen=True, slots=True)
class DurationResult:
    """한 평가 시간의 기존 운영·무재배치·모델 정책 비교를 표현한다."""

    evaluation_minutes: int
    station_count: int
    legacy_movement: LegacyMovementEstimate
    legacy_timing: tuple[LegacyTimingMetrics, ...]
    no_rebalance: SimulationMetrics
    model_policies: tuple[SimulationMetrics, ...]


@dataclass(frozen=True, slots=True)
class PolicyBacktestResult:
    """사전 계약·근거 gate·민감도 결과를 한 문서로 묶는다."""

    evidence_grade: str
    target_date: str
    center_id: str
    center_name: str
    start_hour: int
    model_bundle_root: str
    model_bundle_sha256: str
    source_trip_count: int
    bike_lineage: BikeLineageAudit
    source_provenance: SourceProvenance
    evidence_gate: EvidenceGate
    contracts: tuple[Mapping[str, object], ...]
    durations: tuple[DurationResult, ...]


@dataclass(frozen=True, slots=True)
class SourceFile:
    """백테스트가 읽은 로컬 파일의 경로·크기·내용 해시를 표현한다."""

    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class BikeLineageAudit:
    """월 전체 자전거 연속 이력의 완전성과 평가 구간 후보 수를 기록한다."""

    source_trip_count: int
    bike_count: int
    consecutive_pair_count: int
    changed_station_pair_count: int
    overlapping_pair_count: int
    evaluation_window_candidate_count: int


@dataclass(frozen=True, slots=True)
class SourceProvenance:
    """결과 재현에 필요한 원천·station surface·코드 계약 fingerprint를 표현한다."""

    rental_csv: SourceFile
    stock_csv: SourceFile
    weather_csv: SourceFile
    population_csvs: tuple[SourceFile, ...]
    station_master_content_sha256: str
    station_crosswalk_count: int
    station_crosswalk_sha256: str
    population_excluded_station_count: int
    population_excluded_grid_ids: tuple[str, ...]
    backtest_contract_version: str
    route_algorithm_version: str


def run_policy_backtest(
    *,
    target_date: date,
    center_id: str,
    start_hour: int,
    evaluation_minutes: tuple[int, ...],
    fleet_size: int,
    max_stops_variants: tuple[int, ...],
    rental_csv: Path,
    stock_csv: Path,
    weather_csv: Path,
    population_dir: Path,
    model_bundle_root: Path,
    center_seed: Path,
    endpoint_url: str,
    bucket: str,
    access_key: str,
    secret_key: str,
) -> PolicyBacktestResult:
    """실제 입력 로드부터 동일 예산 정책 민감도까지 전체 평가를 실행한다."""
    contracts = tuple(
        EvaluationContract(
            target_date=target_date,
            start_hour=start_hour,
            evaluation_minutes=minutes,
            fleet_size=fleet_size,
        )
        for minutes in evaluation_minutes
    )
    if evaluation_minutes == (60, 120, 180):
        validate_sensitivity_contracts(contracts)
    centers = load_centers(center_seed)
    matches = [row for row in centers if row[0].dispatch_center_id == center_id]
    if len(matches) != 1:
        raise ValueError(f"활성 dispatch center를 하나 찾을 수 없습니다: {center_id}")
    center, center_name = matches[0]
    model = load_model_bundle(model_bundle_root)
    station_master = load_station_master_from_s3(
        endpoint_url=endpoint_url,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
    )
    selected = _select_center_stations(station_master, center, centers, model)
    station_crosswalk = read_station_crosswalk(rental_csv)
    trips = read_rental_trips(
        rental_csv,
        target_date,
        station_crosswalk=station_crosswalk,
    )
    max_end = _window_start(target_date, start_hour) + timedelta(
        minutes=max(evaluation_minutes)
    )
    lineage = read_bike_relocation_intervals(
        rental_csv,
        window_start=_window_start(target_date, start_hour),
        window_end=max_end,
        station_crosswalk=station_crosswalk,
    )
    observations = read_stock_observations(
        stock_csv,
        target_date,
        station_crosswalk=station_crosswalk,
    )
    selected = _require_hourly_observation_coverage(
        selected,
        observations,
        window_start=_window_start(target_date, start_hour),
        window_end=max_end,
    )
    if not selected:
        raise ValueError(
            "선택 권역에서 model·station·재고 공통 평가 대여소가 없습니다."
        )
    population_requirements = _population_required_hours(
        window_start=_window_start(target_date, start_hour),
        window_end=max_end,
        tick_minutes=contracts[0].tick_minutes,
    )
    population_dates = tuple(population_requirements)
    candidate_grid_ids = frozenset(station.grid_id for station in selected.values())
    population = build_population_nowcast(
        population_dir=population_dir,
        target_dates=population_dates,
        grid_ids=candidate_grid_ids,
        required_hours_by_date=population_requirements,
        require_complete=False,
    )
    complete_grid_ids = population.complete_grid_ids(
        candidate_grid_ids,
        population_requirements,
    )
    population_excluded_grid_ids = tuple(sorted(candidate_grid_ids - complete_grid_ids))
    station_count_before_population = len(selected)
    selected = {
        station_no: station
        for station_no, station in selected.items()
        if station.grid_id in complete_grid_ids
    }
    if not selected:
        raise ValueError("point-in-time 생활인구가 완전한 평가 대여소가 없습니다.")
    population_excluded_station_count = station_count_before_population - len(selected)
    initial_stock = _stock_at(
        observations,
        frozenset(selected),
        _window_start(target_date, start_hour),
    )
    weather = read_weather_history(weather_csv)
    population_source_paths = tuple(
        sorted(
            {
                population_dir / f"250_LOCAL_RESD_{source_date:%Y%m%d}.csv"
                for target in population_dates
                for source_date in population.source_dates(target)
            }
        )
    )
    provenance = SourceProvenance(
        rental_csv=_source_file(rental_csv),
        stock_csv=_source_file(stock_csv),
        weather_csv=_source_file(weather_csv),
        population_csvs=tuple(_source_file(path) for path in population_source_paths),
        station_master_content_sha256=_station_master_sha256(tuple(selected.values())),
        station_crosswalk_count=len(station_crosswalk),
        station_crosswalk_sha256=_station_crosswalk_sha256(station_crosswalk),
        population_excluded_station_count=population_excluded_station_count,
        population_excluded_grid_ids=population_excluded_grid_ids,
        backtest_contract_version="point-in-time-policy-backtest-v3-bike-lineage",
        route_algorithm_version="route-v2",
    )

    def provider(
        anchor: datetime,
        stock: Mapping[int, int],
        successful_trips: Sequence[RentalTrip],
    ) -> tuple[tuple[DemandForecastRecord, ...], PredictionAudit]:
        """한 tick의 게시 가능 날씨와 나우캐스트 인구로 모델을 실행한다."""
        contract = contracts[0]
        cutoff = anchor - timedelta(minutes=contract.weather_publication_lag_minutes)
        observed_weather = latest_published_weather(
            weather,
            anchor=anchor,
            publication_lag_minutes=contract.weather_publication_lag_minutes,
        )
        return predict_point_in_time(
            anchor=anchor,
            stations=tuple(selected.values()),
            stock=stock,
            successful_trips=successful_trips,
            weather=observed_weather,
            weather_cutoff=cutoff,
            population=population,
            model=model,
        )

    duration_results = []
    for contract in contracts:
        window_start = _window_start(target_date, start_hour)
        window_end = window_start + timedelta(minutes=contract.evaluation_minutes)
        station_nos = frozenset(selected)
        legacy = infer_legacy_movements(
            observations=observations,
            trips=trips,
            station_nos=station_nos,
            window_start=window_start,
            window_end=window_end,
            relocations=lineage.intervals,
        )
        legacy_timing = tuple(
            replay_legacy_timing(
                timing=timing,
                estimate=legacy,
                observations=observations,
                trips=trips,
                initial_stock=initial_stock,
                station_nos=station_nos,
                window_start=window_start,
                window_end=window_end,
                use_lineage_assignment=use_lineage_assignment,
            )
            for use_lineage_assignment in (False, True)
            for timing in contract.operator_timing_scenarios
        )
        no_rebalance = simulate_no_rebalance(
            contract=contract,
            center=center,
            stations=tuple(selected.values()),
            initial_stock=initial_stock,
            trips=trips,
        )
        model_policies = tuple(
            simulate_policy(
                policy=f"model_route_v2_max_stops_{max_stops}",
                contract=contract,
                center=center,
                stations=tuple(selected.values()),
                initial_stock=initial_stock,
                trips=trips,
                forecast_provider=provider,
                max_stops_per_route=max_stops,
                movement_budget=legacy.balanced_movement_budget,
            )
            for max_stops in max_stops_variants
        )
        duration_results.append(
            DurationResult(
                evaluation_minutes=contract.evaluation_minutes,
                station_count=len(selected),
                legacy_movement=legacy,
                legacy_timing=legacy_timing,
                no_rebalance=no_rebalance,
                model_policies=model_policies,
            )
        )
    point_in_time_passed = _audit_prediction_inputs(
        tuple(duration_results), target_date
    )
    operation_contract_passed = _audit_operation_contract(
        tuple(duration_results),
        contracts,
    )
    legacy_reconciliation_passed = all(
        row.endpoint_max_absolute_error == 0
        for duration in duration_results
        for row in duration.legacy_timing
    )
    evidence_gate = EvidenceGate(
        point_in_time_feature_inputs=point_in_time_passed,
        operation_contract_passed=operation_contract_passed,
        legacy_endpoint_reconciliation_passed=legacy_reconciliation_passed,
        heldout_day_of_month=target_date.day == 17,
        model_existed_at_historical_anchor=False,
        latent_failed_demand_observed=False,
        operator_route_logs_observed=False,
        bike_lineage_observed=(
            lineage.source_trip_count > 0
            and lineage.consecutive_pair_count > 0
            and lineage.changed_station_pair_count > 0
        ),
        bike_lineage_hybrid_replay_passed=legacy_reconciliation_passed,
        same_bike_movement_budget_cap_enforced=False,
        common_residual_movement_cap_enforced=all(
            policy.movement_budget is not None
            and policy.movement_budget_used <= policy.movement_budget
            for duration in duration_results
            for policy in duration.model_policies
        ),
        same_vehicle_budget_proven=False,
        causal_superiority_vs_legacy_allowed=False,
        publication_grade_system_claim_allowed=False,
        reason=(
            "입력은 point-in-time이고 17일은 학습 제외 test split이지만, 이 모델은 "
            "2025년 전체 자료로 사후 학습됐다. 또한 실패 수요와 기존 운영자의 route·truck "
            "로그가 없어 실제 운영 대비 인과적 우월성은 증명할 수 없다."
        ),
    )
    return PolicyBacktestResult(
        evidence_grade=EVIDENCE_GRADE,
        target_date=target_date.isoformat(),
        center_id=center_id,
        center_name=center_name,
        start_hour=start_hour,
        model_bundle_root=str(model_bundle_root),
        model_bundle_sha256=model.bundle_sha256,
        source_trip_count=len(trips),
        bike_lineage=_lineage_audit(lineage),
        source_provenance=provenance,
        evidence_gate=evidence_gate,
        contracts=tuple(contract.audit_document() for contract in contracts),
        durations=tuple(duration_results),
    )


def _lineage_audit(lineage: BikeLineageReadResult) -> BikeLineageAudit:
    """원시 이동구간을 제외한 월 자전거 연속성 감사 지표를 만든다."""
    return BikeLineageAudit(
        source_trip_count=lineage.source_trip_count,
        bike_count=lineage.bike_count,
        consecutive_pair_count=lineage.consecutive_pair_count,
        changed_station_pair_count=lineage.changed_station_pair_count,
        overlapping_pair_count=lineage.overlapping_pair_count,
        evaluation_window_candidate_count=len(lineage.intervals),
    )


def _select_center_stations(
    stations: Sequence[HistoricalStation],
    center: DispatchCenterTopology,
    centers: Sequence[tuple[DispatchCenterTopology, str]],
    model: ModelBundle,
) -> dict[int, HistoricalStation]:
    """최근접 센터·두 모델 category 교집합으로 평가 대여소를 선택한다."""
    rental = set(model.rental.station_dtype.categories)
    returned = set(model.returned.station_dtype.categories)
    selected = {}
    for station in stations:
        nearest = min(
            (row[0] for row in centers),
            key=lambda item: (
                _distance_sq(station, item),
                item.dispatch_center_id,
            ),
        )
        if (
            nearest.dispatch_center_id == center.dispatch_center_id
            and station.station_no in rental
            and station.station_no in returned
        ):
            selected[station.station_no] = station
    return selected


def _require_hourly_observation_coverage(
    stations: Mapping[int, HistoricalStation],
    observations: Sequence[StockObservation],
    *,
    window_start: datetime,
    window_end: datetime,
) -> dict[int, HistoricalStation]:
    """민감도 최대 구간의 모든 정시 재고가 있는 대여소만 남긴다."""
    observed = {(row.observed_at, row.station_no) for row in observations}
    checkpoints = []
    moment = window_start
    while moment <= window_end:
        checkpoints.append(moment)
        moment += timedelta(hours=1)
    return {
        station_no: station
        for station_no, station in stations.items()
        if all((checkpoint, station_no) in observed for checkpoint in checkpoints)
    }


def _stock_at(
    observations: Sequence[StockObservation],
    station_nos: frozenset[int],
    moment: datetime,
) -> dict[int, int]:
    """평가 시작 정시의 실측 재고를 exact station 집합으로 반환한다."""
    result = {
        row.station_no: row.quantity
        for row in observations
        if row.observed_at == moment and row.station_no in station_nos
    }
    if set(result) != set(station_nos):
        raise ValueError("평가 시작 실측 재고가 exact station 집합과 다릅니다.")
    return result


def _audit_prediction_inputs(
    durations: tuple[DurationResult, ...],
    target_date: date,
) -> bool:
    """모든 정책 tick의 외생변수가 anchor 이후 정보를 쓰지 않았는지 검증한다."""
    audits = [
        tick.prediction
        for duration in durations
        for policy in duration.model_policies
        for tick in policy.tick_audits
    ]
    if not audits:
        return False
    for audit in audits:
        anchor = datetime.fromisoformat(audit.anchor)
        cutoff = datetime.fromisoformat(audit.weather_cutoff)
        weather = datetime.fromisoformat(audit.weather_observed_at)
        if weather > cutoff.replace(tzinfo=None):
            return False
        if any(
            date.fromisoformat(day) >= target_date
            for day in audit.population_candidate_dates
        ):
            return False
        if datetime.fromisoformat(audit.rental_visibility_cutoff) != anchor:
            return False
    return True


def _audit_operation_contract(
    durations: tuple[DurationResult, ...],
    contracts: tuple[EvaluationContract, ...],
) -> bool:
    """5분 tick·예산·cutoff·작업 실행량이 사전 계약을 지켰는지 검증한다."""
    contract_by_minutes = {
        contract.evaluation_minutes: contract for contract in contracts
    }
    for duration in durations:
        contract = contract_by_minutes[duration.evaluation_minutes]
        expected_ticks = contract.evaluation_minutes // contract.tick_minutes
        for policy in duration.model_policies:
            if policy.decision_ticks != expected_ticks:
                return False
            if (
                policy.movement_budget is None
                or policy.movement_budget_used > policy.movement_budget
                or policy.trucks_still_busy_at_cutoff != 0
            ):
                return False
            end = datetime.fromisoformat(policy.window_end)
            for job in policy.job_audits:
                if datetime.fromisoformat(job.return_at) > end:
                    return False
                if job.moved_bikes > job.planned_bikes:
                    return False
                if any(
                    stop.actual_quantity is not None
                    and stop.actual_quantity > stop.planned_quantity
                    for stop in job.stops
                ):
                    return False
    return True


def _window_start(target_date: date, start_hour: int) -> datetime:
    """대상일 KST 정시 평가 시작점을 반환한다."""
    return datetime.combine(target_date, datetime.min.time(), tzinfo=SEOUL) + timedelta(
        hours=start_hour
    )


def _population_required_hours(
    *,
    window_start: datetime,
    window_end: datetime,
    tick_minutes: int,
) -> dict[date, frozenset[int]]:
    """모든 정책 tick의 12시간 모델 target이 실제 조회할 날짜·시간을 계산한다."""
    required: dict[date, set[int]] = {}
    anchor = window_start
    while anchor < window_end:
        for horizon in range(HORIZON_COUNT):
            target = anchor + timedelta(hours=horizon)
            required.setdefault(target.date(), set()).add(target.hour)
        anchor += timedelta(minutes=tick_minutes)
    return {
        target_date: frozenset(hours) for target_date, hours in sorted(required.items())
    }


def _distance_sq(
    station: HistoricalStation,
    center: DispatchCenterTopology,
) -> float:
    """최근접 센터 선택용 위경도 제곱거리를 반환한다."""
    return (station.latitude - center.latitude) ** 2 + (
        station.longitude - center.longitude
    ) ** 2


def _source_file(path: Path) -> SourceFile:
    """큰 원천 파일을 chunk 단위로 읽어 SHA-256 provenance를 만든다."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return SourceFile(
        path=str(path),
        size_bytes=path.stat().st_size,
        sha256=digest.hexdigest(),
    )


def _station_master_sha256(stations: tuple[HistoricalStation, ...]) -> str:
    """실제 평가 surface의 station master 필드를 canonical 내용 해시로 고정한다."""
    rows = [
        asdict(station)
        for station in sorted(stations, key=lambda row: row.station_id.encode("utf-8"))
    ]
    payload = json.dumps(
        rows,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _station_crosswalk_sha256(station_crosswalk: Mapping[int, int]) -> str:
    """공공 번호와 내부 ST suffix의 canonical 대응표 내용 해시를 계산한다."""
    rows = [
        {"public_station_no": public_no, "internal_station_no": internal_no}
        for public_no, internal_no in sorted(station_crosswalk.items())
    ]
    payload = json.dumps(
        rows,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def result_markdown(result: PolicyBacktestResult) -> str:
    """백테스트 결과를 과장 주장 방지 문구와 함께 검토용 표로 만든다."""
    lines = [
        f"# {result.target_date} {result.center_name} 정책 백테스트",
        "",
        f"- 근거 등급: `{result.evidence_grade}`",
        f"- 모델 SHA-256: `{result.model_bundle_sha256}`",
        (
            "- 자전거 ID 연속 쌍: "
            f"{result.bike_lineage.consecutive_pair_count:,}개, "
            f"위치 변경 {result.bike_lineage.changed_station_pair_count:,}개, "
            f"평가 창 후보 {result.bike_lineage.evaluation_window_candidate_count:,}개"
        ),
        f"- 발표용 시스템 개선 주장 허용: **{result.evidence_gate.publication_grade_system_claim_allowed}**",
        f"- 제한 이유: {result.evidence_gate.reason}",
        "",
        "| 평가 | 정책 | 관측 수요 충족률 | 미충족 | 품절 대여소-분 | 이동 대수 | 경로 | 차량 분 |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for duration in result.durations:
        base = duration.no_rebalance
        lines.append(
            f"| {duration.evaluation_minutes}분 | 재배치 없음 | "
            f"{base.observed_demand_fulfillment_rate:.4f} | {base.unfulfilled_requests} | "
            f"{base.empty_station_minutes:.1f} | 0 | 0 | 0.0 |"
        )
        for policy in duration.model_policies:
            lines.append(
                f"| {duration.evaluation_minutes}분 | {policy.policy} | "
                f"{policy.observed_demand_fulfillment_rate:.4f} | "
                f"{policy.unfulfilled_requests} | {policy.empty_station_minutes:.1f} | "
                f"{policy.moved_bikes} | {policy.dispatched_routes} | "
                f"{policy.vehicle_busy_minutes:.1f} |"
            )
        legacy_values = [row.empty_station_minutes for row in duration.legacy_timing]
        evidence = duration.legacy_movement.relocation_evidence
        lines.append(
            f"| {duration.evaluation_minutes}분 | 추정 기존 운영 범위 | 해당 없음 | 해당 없음 | "
            f"{min(legacy_values):.1f}~{max(legacy_values):.1f} | "
            f"잔차 상한 {duration.legacy_movement.balanced_movement_budget} "
            f"(ID 양립 {evidence.residual_compatible_internal_intervals}, "
            f"잔차 설명 {evidence.residual_explained_pct}%) | "
            "경로 미관측 | 미관측 |"
        )
    lines.extend(
        (
            "",
            (
                "> 기존 운영 범위는 잔차만 구간 초·중·말에 적용한 세 경우와, 자전거 ID "
                "이동 가능구간 중 잔차 방향과 양립하는 후보를 짧은 구간부터 한 번씩 "
                "할당한 세 경우를 모두 포함한다. ID 할당은 가능한 재구성이지 확정시각이 아니다. "
                "ID는 이동 출발지·도착지와 가능 시간구간만 알려주며 운영 route·truck과 "
                "정확한 이동시각, 실패 수요는 알려주지 않는다."
            ),
            "",
        )
    )
    return "\n".join(lines)


def write_result(result: PolicyBacktestResult, output_dir: Path) -> tuple[Path, Path]:
    """전체 tick 감사 JSON과 사람이 검토할 Markdown 결과를 저장한다."""
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{result.target_date}-{result.center_id}-{result.start_hour:02d}h-policy"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(
        json.dumps(
            asdict(result),
            ensure_ascii=False,
            indent=2,
            default=_json_default,
        )
        + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(result_markdown(result), encoding="utf-8")
    return json_path, markdown_path


def _json_default(value: object) -> str:
    """JSON 표준형이 아닌 date·datetime만 ISO 문자열로 직렬화한다."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"JSON으로 직렬화할 수 없는 타입입니다: {type(value).__name__}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """point-in-time 정책 백테스트 CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(description="실제 모델 5분 재배치 정책 백테스트")
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--center", required=True)
    parser.add_argument("--start-hour", type=int, default=6)
    parser.add_argument(
        "--evaluation-minutes", nargs="+", type=int, default=[60, 120, 180]
    )
    parser.add_argument("--fleet-size", type=int, default=3)
    parser.add_argument("--max-stops", nargs="+", type=int, default=[5, 8])
    parser.add_argument("--rental-csv", required=True, type=Path)
    parser.add_argument("--stock-csv", required=True, type=Path)
    parser.add_argument(
        "--weather-csv",
        type=Path,
        default=Path("../data/issue163-full-year/bootstrap/weather_realtime_2025.csv"),
    )
    parser.add_argument(
        "--population-dir",
        type=Path,
        default=Path("../data/issue163-full-year/population"),
    )
    parser.add_argument(
        "--model-bundle",
        type=Path,
        default=Path("../models/aws-temporary-model-2025-d20-h12-r20"),
    )
    parser.add_argument(
        "--center-seed",
        type=Path,
        default=Path("../docs/gold/dispatch-center-seed.yaml"),
    )
    parser.add_argument(
        "--s3-endpoint",
        default=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
    )
    parser.add_argument("--s3-bucket", default="issue163-full-year")
    parser.add_argument(
        "--access-key", default=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    )
    parser.add_argument(
        "--secret-key", default=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("../data/backtest-results")
    )
    args = parser.parse_args(argv)
    if not 0 <= args.start_hour <= 23:
        parser.error("--start-hour는 0..23이어야 합니다.")
    if args.date.day != 17:
        parser.error("현재 고정 모델의 held-out test split인 매월 17일만 평가합니다.")
    if tuple(args.evaluation_minutes) != (60, 120, 180):
        parser.error(
            "기본 근거 실행은 --evaluation-minutes 60 120 180을 모두 사용해야 합니다."
        )
    if args.fleet_size <= 0:
        parser.error("--fleet-size는 양수여야 합니다.")
    if any(not 2 <= value <= 32767 for value in args.max_stops):
        parser.error("--max-stops는 각각 2..32767이어야 합니다.")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """실제 정책 백테스트를 실행하고 결과 위치를 출력한다."""
    args = parse_args(argv)
    result = run_policy_backtest(
        target_date=args.date,
        center_id=args.center,
        start_hour=args.start_hour,
        evaluation_minutes=tuple(args.evaluation_minutes),
        fleet_size=args.fleet_size,
        max_stops_variants=tuple(dict.fromkeys(args.max_stops)),
        rental_csv=args.rental_csv,
        stock_csv=args.stock_csv,
        weather_csv=args.weather_csv,
        population_dir=args.population_dir,
        model_bundle_root=args.model_bundle,
        center_seed=args.center_seed,
        endpoint_url=args.s3_endpoint,
        bucket=args.s3_bucket,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    json_path, markdown_path = write_result(result, args.output_dir)
    print(result_markdown(result))
    print(f"JSON: {json_path}")
    print(f"Markdown: {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
