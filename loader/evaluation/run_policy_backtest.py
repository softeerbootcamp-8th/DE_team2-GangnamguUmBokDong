"""실제 2025 원천과 고정 모델로 point-in-time 재배치 백테스트를 실행한다."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from core.scoring_config import URGENCY_SCORING_CONFIG_VERSION
from gold.demand import DemandForecastRecord
from gold.rebalance_policy import (
    DEFAULT_REBALANCE_POLICY,
    RebalancePolicyConfig,
)
from gold.rebalance_route import (
    ROUTE_ALGORITHM_VERSION,
    DispatchCenterTopology,
)

from .backtest_contract import (
    BACKTEST_CONTRACT_VERSION,
    EVIDENCE_GRADE,
    EvaluationContract,
    validate_sensitivity_contracts,
)
from .evaluation_profiles import PRODUCTION_POLICY_NAME
from .historical_inputs import (
    HORIZON_COUNT,
    HistoricalStation,
    ModelBundle,
    PredictionAudit,
    build_population_nowcast,
    latest_published_weather,
    load_model_bundle,
    load_station_dispatch_center_lineage,
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
    RentalTrip,
    StockObservation,
    load_centers,
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
    same_bike_movement_budget_cap_enforced: bool
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
class PolicyVariant:
    """한 백테스트에서 비교할 정책 이름과 설정을 묶는다."""

    name: str
    policy_config: RebalancePolicyConfig

    def __post_init__(self) -> None:
        """정책 이름과 설정 타입을 검증한다."""
        if type(self.name) is not str or not self.name.strip():
            raise ValueError("policy variant name은 nonblank여야 합니다.")
        if type(self.policy_config) is not RebalancePolicyConfig:
            raise ValueError("policy variant config 타입이 잘못됐습니다.")


def default_policy_variants() -> tuple[PolicyVariant, ...]:
    """직접 실행의 기본 production 정책 하나를 반환한다."""
    return (
        PolicyVariant(
            name=PRODUCTION_POLICY_NAME,
            policy_config=DEFAULT_REBALANCE_POLICY,
        ),
    )


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
    urgency_scoring_config_version: str


def run_policy_backtest(
    *,
    target_date: date,
    center_id: str,
    start_hour: int,
    evaluation_minutes: tuple[int, ...],
    fleet_size: int,
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
    database_url: str,
    policy_variants: tuple[PolicyVariant, ...] | None = None,
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
    variants = default_policy_variants() if policy_variants is None else policy_variants
    if (
        type(variants) is not tuple
        or not variants
        or any(type(variant) is not PolicyVariant for variant in variants)
        or len({variant.name for variant in variants}) != len(variants)
    ):
        raise ValueError("policy variants는 이름이 고유한 nonempty tuple이어야 합니다.")
    centers = load_centers(center_seed)
    matches = [row for row in centers if row[0].dispatch_center_id == center_id]
    if len(matches) != 1:
        raise ValueError(f"활성 dispatch center를 하나 찾을 수 없습니다: {center_id}")
    center, center_name = matches[0]
    model = load_model_bundle(model_bundle_root)
    dispatch_center_by_station_id = load_station_dispatch_center_lineage(database_url)
    station_master = load_station_master_from_s3(
        endpoint_url=endpoint_url,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        dispatch_center_by_station_id=dispatch_center_by_station_id,
    )
    selected = _select_center_stations(station_master, center_id, model)
    station_crosswalk = read_station_crosswalk(rental_csv)
    trips = read_rental_trips(
        rental_csv,
        target_date,
        station_crosswalk=station_crosswalk,
    )
    observations = read_stock_observations(
        stock_csv,
        target_date,
        station_crosswalk=station_crosswalk,
    )
    max_end = _window_start(target_date, start_hour) + timedelta(
        minutes=max(evaluation_minutes)
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
        backtest_contract_version=BACKTEST_CONTRACT_VERSION,
        route_algorithm_version=ROUTE_ALGORITHM_VERSION,
        urgency_scoring_config_version=URGENCY_SCORING_CONFIG_VERSION,
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
            )
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
                policy=variant.name,
                contract=contract,
                center=center,
                stations=tuple(selected.values()),
                initial_stock=initial_stock,
                trips=trips,
                forecast_provider=provider,
                movement_budget=legacy.balanced_movement_budget,
                policy_config=variant.policy_config,
            )
            for variant in variants
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
        same_bike_movement_budget_cap_enforced=all(
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
        source_provenance=provenance,
        evidence_gate=evidence_gate,
        contracts=tuple(contract.audit_document() for contract in contracts),
        durations=tuple(duration_results),
    )


def _select_center_stations(
    stations: Sequence[HistoricalStation],
    center_id: str,
    model: ModelBundle,
) -> dict[int, HistoricalStation]:
    """운영 Gold center lineage와 두 모델 category 교집합을 선택한다."""
    rental = set(model.rental.station_dtype.categories)
    returned = set(model.returned.station_dtype.categories)
    selected = {}
    for station in stations:
        if (
            station.dispatch_center_id == center_id
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
