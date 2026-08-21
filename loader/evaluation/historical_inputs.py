"""과거 시점에 관측 가능한 입력만으로 고정 모델의 12시간 예측을 만든다."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
from gold.demand import DemandForecastRecord
from ml_core.day_index import day_index
from ml_core.holidays_kr import korean_holidays
from ml_core.model_contract import (
    RENTAL_FEATURE_COLUMN_DTYPES,
    RETURN_FEATURE_COLUMN_DTYPES,
)
from ml_core.scoring import (
    PinnedScoringModel,
    build_pinned_scoring_model,
    predict,
    use_pinned_scoring_models,
)

from .rebalance_backtest import RentalTrip

HORIZON_COUNT = 12
EXPOSURE_STOCKOUT_VALUE = 0.05
POPULATION_WEIGHTS = (0.4, 0.3, 0.2, 0.1)


@dataclass(frozen=True, slots=True)
class HistoricalStation:
    """고정 station master에서 모델과 경로가 함께 사용하는 대여소 필드를 표현한다."""

    station_id: str
    station_no: int
    station_name: str
    capacity: int
    latitude: float
    longitude: float
    grid_id: str


@dataclass(frozen=True, slots=True)
class WeatherObservation:
    """게시 지연을 적용하기 전 원천 관측시각과 모델용 날씨를 표현한다."""

    observed_at: datetime
    temperature_c: float
    precipitation_mm: float


@dataclass(frozen=True, slots=True)
class ModelBundle:
    """로컬 release에서 byte 단위로 고정한 대여·반납 scorer를 표현한다."""

    rental: PinnedScoringModel
    returned: PinnedScoringModel
    bundle_sha256: str
    root: str


@dataclass(frozen=True, slots=True)
class PredictionAudit:
    """한 5분 tick 예측이 사용한 정보시점과 입력 출처를 기록한다."""

    anchor: str
    weather_observed_at: str
    weather_cutoff: str
    population_candidate_dates: tuple[str, ...]
    rental_lag_start: str
    rental_lag_end: str
    rental_visibility_cutoff: str
    return_lag_start: str
    return_lag_end: str
    model_bundle_sha256: str
    station_count: int


@dataclass(frozen=True, slots=True)
class DemandForecastQuantiles:
    """한 대여소·horizon의 대여·반납 q10·q50·q90 예측을 표현한다."""

    base_dttm: datetime
    sta_id: str
    predicted_dttm: datetime
    rental_p10: float
    rental_p50: float
    rental_p90: float
    return_p10: float
    return_p50: float
    return_p90: float

    def __post_init__(self) -> None:
        """시각·식별자·비음수 quantile 순서를 검증한다."""
        if self.base_dttm.tzinfo is None or self.predicted_dttm.tzinfo is None:
            raise ValueError("quantile forecast 시각은 timezone-aware여야 합니다.")
        if not self.sta_id:
            raise ValueError("quantile forecast sta_id는 nonblank여야 합니다.")
        for values, name in (
            (
                (self.rental_p10, self.rental_p50, self.rental_p90),
                "rental",
            ),
            (
                (self.return_p10, self.return_p50, self.return_p90),
                "return",
            ),
        ):
            if any(not math.isfinite(value) or value < 0 for value in values):
                raise ValueError(f"{name} quantile은 finite nonnegative여야 합니다.")
            if values != tuple(sorted(values)):
                raise ValueError(f"{name} quantile은 q10 <= q50 <= q90이어야 합니다.")


@dataclass(frozen=True, slots=True)
class PointInTimeForecast:
    """Gold 평균 예측과 정책 실험용 quantile, 입력 감사를 함께 보관한다."""

    demand: tuple[DemandForecastRecord, ...]
    quantiles: tuple[DemandForecastQuantiles, ...]
    audit: PredictionAudit

    def __post_init__(self) -> None:
        """평균과 quantile의 대여소·예측시각 표면이 exact한지 검증한다."""
        demand_keys = tuple((row.sta_id, row.predicted_dttm) for row in self.demand)
        quantile_keys = tuple(
            (row.sta_id, row.predicted_dttm) for row in self.quantiles
        )
        if demand_keys != quantile_keys:
            raise ValueError("평균과 quantile forecast 표면이 다릅니다.")


class PopulationNowcast:
    """대상일보다 앞선 자료만으로 만든 격자·시간별 생활인구 추정치를 제공한다."""

    def __init__(
        self,
        values: Mapping[tuple[date, int, str], float],
        source_dates: Mapping[date, tuple[date, ...]],
    ) -> None:
        """완전한 값과 날짜별 원천일을 불변 사전으로 보관한다."""
        self._values = dict(values)
        self._source_dates = dict(source_dates)

    def value(self, target: datetime, grid_id: str) -> float:
        """대상 시각·격자의 point-in-time 나우캐스트 값을 반환한다."""
        key = (target.date(), target.hour, grid_id)
        try:
            return self._values[key]
        except KeyError as exc:
            raise ValueError(f"생활인구 nowcast가 없습니다: {key}") from exc

    def source_dates(self, target_date: date) -> tuple[date, ...]:
        """대상일 추정에 실제 사용된 과거 원천 날짜를 반환한다."""
        return self._source_dates[target_date]

    def complete_grid_ids(
        self,
        grid_ids: frozenset[str],
        required_hours_by_date: Mapping[date, frozenset[int]],
    ) -> frozenset[str]:
        """요구된 모든 날짜·시간에 point-in-time 값이 있는 격자만 반환한다."""
        return frozenset(
            grid_id
            for grid_id in grid_ids
            if all(
                (target_date, hour, grid_id) in self._values
                for target_date, hours in required_hours_by_date.items()
                for hour in hours
            )
        )


def load_station_master_from_s3(
    *,
    endpoint_url: str,
    bucket: str,
    access_key: str,
    secret_key: str,
    prefix: str = "processed_v2/station_master.parquet/",
) -> tuple[HistoricalStation, ...]:
    """모델 학습에 사용한 고정 station master Parquet을 객체 저장소에서 읽는다."""
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = sorted(
        item["Key"]
        for item in response.get("Contents", ())
        if item["Key"].endswith(".parquet") and item.get("Size", 0) > 0
    )
    if not keys:
        raise FileNotFoundError(
            f"station master Parquet이 없습니다: s3://{bucket}/{prefix}"
        )
    frames = []
    for key in keys:
        payload = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        frames.append(pq.read_table(io.BytesIO(payload)).to_pandas())
    frame = pd.concat(frames, ignore_index=True)
    required = {
        "station_id",
        "station_no",
        "station_name",
        "capacity",
        "lat",
        "lon",
        "grid_id",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"station master 필수 컬럼이 없습니다: {sorted(missing)}")
    if frame["station_id"].duplicated().any() or frame["station_no"].duplicated().any():
        raise ValueError("station master 식별자가 중복됩니다.")
    records = []
    for row in frame.to_dict("records"):
        if any(pd.isna(row[name]) for name in required):
            continue
        records.append(
            HistoricalStation(
                station_id=str(row["station_id"]),
                station_no=int(row["station_no"]),
                station_name=str(row["station_name"]),
                capacity=int(row["capacity"]),
                latitude=float(row["lat"]),
                longitude=float(row["lon"]),
                grid_id=str(row["grid_id"]).strip(),
            )
        )
    return tuple(sorted(records, key=lambda row: row.station_id.encode("utf-8")))


def read_weather_history(path: Path) -> tuple[WeatherObservation, ...]:
    """기상 관측 CSV에서 유효한 서울 시간별 기온·강수량을 읽는다."""
    observations = []
    with path.open("r", encoding="cp949", newline="") as stream:
        for row in csv.DictReader(stream):
            try:
                observed_at = datetime.strptime(  # noqa: DTZ007
                    row["일시"], "%Y-%m-%d %H:%M"
                )
                temperature = float(row["기온(°C)"])
                raw_precipitation = row.get("강수량(mm)", "").strip()
                precipitation = float(raw_precipitation) if raw_precipitation else 0.0
            except (KeyError, TypeError, ValueError):
                continue
            observations.append(
                WeatherObservation(observed_at, temperature, precipitation)
            )
    if not observations:
        raise ValueError("유효한 날씨 관측이 없습니다.")
    return tuple(sorted(observations, key=lambda row: row.observed_at))


def latest_published_weather(
    observations: Sequence[WeatherObservation],
    *,
    anchor: datetime,
    publication_lag_minutes: int,
) -> WeatherObservation:
    """anchor에서 게시됐다고 보수적으로 판단할 수 있는 최신 날씨를 선택한다."""
    cutoff = anchor.replace(tzinfo=None) - timedelta(minutes=publication_lag_minutes)
    candidates = [row for row in observations if row.observed_at <= cutoff]
    if not candidates:
        raise ValueError(f"날씨 publication cutoff 이전 관측이 없습니다: {cutoff}")
    return candidates[-1]


def build_population_nowcast(
    *,
    population_dir: Path,
    target_dates: tuple[date, ...],
    grid_ids: frozenset[str],
    required_hours_by_date: Mapping[date, frozenset[int]] | None = None,
    require_complete: bool = True,
) -> PopulationNowcast:
    """운영 nowcaster의 1~4주 가중평균을 미래 자료 없이 재구성한다.

    최근 네 후보가 모두 결측인 셀만 5~8주 전의 가장 가까운 값으로 대체한다.
    그 이전에도 값이 없으면 미래나 대상일 실측으로 채우지 않고 fail-closed한다.
    ``required_hours_by_date``를 주면 실제 추론이 조회할 시간 셀만 검증한다.
    """
    if type(require_complete) is not bool:
        raise ValueError("생활인구 require_complete는 bool이어야 합니다.")
    target_set = set(target_dates)
    if required_hours_by_date is not None:
        if set(required_hours_by_date) != target_set:
            raise ValueError("생활인구 required hour 날짜가 target_dates와 다릅니다.")
        if any(
            not hours
            or any(type(hour) is not int or not 0 <= hour <= 23 for hour in hours)
            for hours in required_hours_by_date.values()
        ):
            raise ValueError(
                "생활인구 required hour는 날짜별 0..23의 nonempty 집합이어야 합니다."
            )
    values: dict[tuple[date, int, str], float] = {}
    source_dates_by_target: dict[date, tuple[date, ...]] = {}
    cache: dict[date, dict[tuple[int, str], float]] = {}
    for target in target_dates:
        candidates = _population_candidate_dates(target)
        used_dates = set(candidates)
        candidate_values = [
            _read_population_day(
                population_dir,
                day,
                grid_ids,
                cache,
                required=False,
            )
            for day in candidates
        ]
        extended_dates = tuple(target - timedelta(weeks=week) for week in range(5, 9))
        extended_values = [
            _read_population_day(population_dir, day, grid_ids, cache, required=False)
            for day in extended_dates
        ]
        missing = []
        required_hours = (
            range(24)
            if required_hours_by_date is None
            else sorted(required_hours_by_date[target])
        )
        for hour in required_hours:
            for grid_id in grid_ids:
                key = (hour, grid_id)
                weighted = [
                    (frame.get(key), weight)
                    for frame, weight in zip(
                        candidate_values, POPULATION_WEIGHTS, strict=True
                    )
                    if frame.get(key) is not None
                ]
                if weighted:
                    numerator = sum(value * weight for value, weight in weighted)
                    denominator = sum(weight for _, weight in weighted)
                    values[(target, hour, grid_id)] = numerator / denominator
                    continue
                fallback = None
                for extended_date, frame in zip(
                    extended_dates,
                    extended_values,
                    strict=True,
                ):
                    if key in frame:
                        fallback = frame[key]
                        used_dates.add(extended_date)
                        break
                if fallback is None:
                    missing.append(key)
                else:
                    values[(target, hour, grid_id)] = fallback
        if missing and require_complete:
            raise ValueError(
                f"과거 자료만으로 생활인구를 만들 수 없는 셀이 있습니다: "
                f"date={target}, missing={missing[:10]}, count={len(missing)}"
            )
        source_dates_by_target[target] = tuple(sorted(used_dates))
    return PopulationNowcast(values, source_dates_by_target)


def load_model_bundle(root: Path) -> ModelBundle:
    """고정 로컬 모델 12개 artifact를 검증하고 메모리 scorer로 로드한다."""
    model_dir = root / "models"
    digest = hashlib.sha256()
    pinned: dict[str, PinnedScoringModel] = {}
    for model_name in ("rental", "return"):
        role_paths = {
            "booster_poisson": model_dir / f"{model_name}_poisson.txt",
            "booster_q10": model_dir / f"{model_name}_q10.txt",
            "booster_q50": model_dir / f"{model_name}_q50.txt",
            "booster_q90": model_dir / f"{model_name}_q90.txt",
            "conformal_correction": model_dir
            / f"{model_name}_conformal_correction.json",
            "station_categories": model_dir / f"{model_name}_station_categories.json",
        }
        payloads = {}
        for role, path in sorted(role_paths.items()):
            payload = path.read_bytes()
            digest.update(f"{model_name}:{role}\0".encode())
            digest.update(payload)
            payloads[role] = (
                json.dumps(
                    json.loads(payload),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                if role == "station_categories"
                else payload
            )
        pinned[model_name] = build_pinned_scoring_model(payloads)
    return ModelBundle(
        rental=pinned["rental"],
        returned=pinned["return"],
        bundle_sha256=digest.hexdigest(),
        root=str(root),
    )


def predict_point_in_time(
    *,
    anchor: datetime,
    stations: Sequence[HistoricalStation],
    stock: Mapping[int, int],
    successful_trips: Sequence[RentalTrip],
    weather: WeatherObservation,
    weather_cutoff: datetime,
    population: PopulationNowcast,
    model: ModelBundle,
) -> PointInTimeForecast:
    """동일 anchor lag와 point-in-time 외생변수로 실제 고정 모델을 채점한다."""
    if anchor.tzinfo is None:
        raise ValueError("anchor는 timezone-aware여야 합니다.")
    if weather.observed_at > weather_cutoff.replace(tzinfo=None):
        raise ValueError("선택한 날씨가 publication cutoff 뒤입니다.")
    rental_categories = set(model.rental.station_dtype.categories)
    return_categories = set(model.returned.station_dtype.categories)
    supported = [
        station
        for station in stations
        if station.station_no in stock
        and station.station_no in rental_categories
        and station.station_no in return_categories
    ]
    if not supported:
        raise ValueError("두 모델과 현재 재고가 공통 지원하는 대여소가 없습니다.")
    rental_lag, return_lag = _lag_counts(successful_trips, anchor)
    rows = []
    for horizon in range(1, HORIZON_COUNT + 1):
        target = anchor + timedelta(hours=horizon - 1)
        target_text = target.date().isoformat()
        dow = target.weekday()
        holidays = korean_holidays(target.year)
        for station in supported:
            rows.append(
                {
                    "station_id": station.station_id,
                    "station_no": station.station_no,
                    "capacity": station.capacity,
                    "lat": station.latitude,
                    "lon": station.longitude,
                    "temp": weather.temperature_c,
                    "precip": weather.precipitation_mm,
                    "pop_total": population.value(target, station.grid_id),
                    "minute": target.hour * 60 + target.minute,
                    "dow": dow,
                    "is_holiday": int(target_text in holidays or dow >= 5),
                    "day": day_index(target.date()),
                    "horizon": horizon,
                    "rental_lag_1h": float(rental_lag.get(station.station_no, 0)),
                    "return_lag_1h": float(return_lag.get(station.station_no, 0)),
                    "rental_exposure": (
                        EXPOSURE_STOCKOUT_VALUE
                        if stock[station.station_no] <= 0
                        else 1.0
                    ),
                    "date": target_text,
                    "hour": target.hour,
                }
            )
    frame = pd.DataFrame(rows)
    rental_frame = frame.astype(RENTAL_FEATURE_COLUMN_DTYPES)
    return_frame = frame.astype(RETURN_FEATURE_COLUMN_DTYPES)
    with use_pinned_scoring_models({"rental": model.rental, "return": model.returned}):
        rental_prediction = predict(
            rental_frame,
            "rental",
            exposure_col="rental_exposure",
        )
        return_prediction = predict(return_frame, "return")
    base_utc = anchor.astimezone(UTC)
    records = []
    quantiles = []
    for row, rental_result, return_result in zip(
        rows,
        rental_prediction.to_dict("records"),
        return_prediction.to_dict("records"),
        strict=True,
    ):
        horizon = int(row["horizon"])
        predicted_dttm = base_utc + timedelta(hours=horizon)
        records.append(
            DemandForecastRecord(
                base_dttm=base_utc,
                sta_id=str(row["station_id"]),
                predicted_dttm=predicted_dttm,
                predicted_rent_cnt=round(float(rental_result["pred_mean"])),
                predicted_rtn_cnt=round(float(return_result["pred_mean"])),
            )
        )
        quantiles.append(
            DemandForecastQuantiles(
                base_dttm=base_utc,
                sta_id=str(row["station_id"]),
                predicted_dttm=predicted_dttm,
                rental_p10=float(rental_result["pred_p10"]),
                rental_p50=float(rental_result["pred_p50"]),
                rental_p90=float(rental_result["pred_p90"]),
                return_p10=float(return_result["pred_p10"]),
                return_p50=float(return_result["pred_p50"]),
                return_p90=float(return_result["pred_p90"]),
            )
        )
    combined = sorted(
        zip(records, quantiles, strict=True),
        key=lambda pair: (pair[0].sta_id.encode("utf-8"), pair[0].predicted_dttm),
    )
    records = [pair[0] for pair in combined]
    quantiles = [pair[1] for pair in combined]
    population_dates = tuple(
        sorted(
            {
                day.isoformat()
                for target_date in {
                    anchor.date(),
                    (anchor + timedelta(hours=11)).date(),
                }
                for day in population.source_dates(target_date)
            }
        )
    )
    audit = PredictionAudit(
        anchor=anchor.isoformat(),
        weather_observed_at=weather.observed_at.isoformat(),
        weather_cutoff=weather_cutoff.isoformat(),
        population_candidate_dates=population_dates,
        rental_lag_start=(anchor - timedelta(minutes=100)).isoformat(),
        rental_lag_end=(anchor - timedelta(minutes=40)).isoformat(),
        rental_visibility_cutoff=anchor.isoformat(),
        return_lag_start=(anchor - timedelta(minutes=60)).isoformat(),
        return_lag_end=anchor.isoformat(),
        model_bundle_sha256=model.bundle_sha256,
        station_count=len(supported),
    )
    return PointInTimeForecast(tuple(records), tuple(quantiles), audit)


def _lag_counts(
    successful_trips: Sequence[RentalTrip],
    anchor: datetime,
) -> tuple[dict[int, int], dict[int, int]]:
    """운영과 같은 embargo·가시성 계약으로 대여·반납 lag를 센다."""
    rental_start = anchor - timedelta(minutes=100)
    rental_end = anchor - timedelta(minutes=40)
    return_start = anchor - timedelta(minutes=60)
    rental: dict[int, int] = {}
    returned: dict[int, int] = {}
    for trip in successful_trips:
        if rental_start <= trip.rented_at < rental_end and trip.returned_at <= anchor:
            rental[trip.rent_station_no] = rental.get(trip.rent_station_no, 0) + 1
        if return_start <= trip.returned_at < anchor:
            returned[trip.return_station_no] = (
                returned.get(trip.return_station_no, 0) + 1
            )
    return rental, returned


def _population_candidate_dates(target: date) -> tuple[date, date, date, date]:
    """운영 nowcaster와 같은 평일/특수일 규칙으로 네 과거 후보일을 선택한다."""
    holidays = korean_holidays([target.year - 1, target.year])

    def special(day: date) -> bool:
        """일요일 또는 대한민국 공휴일인지 반환한다."""
        return day.weekday() == 6 or day.isoformat() in holidays

    if not special(target):
        return tuple(target - timedelta(weeks=week) for week in range(1, 5))  # type: ignore[return-value]
    result = []
    cursor = target - timedelta(days=1)
    while len(result) < 4 and (target - cursor).days <= 60:
        if special(cursor):
            result.append(cursor)
        cursor -= timedelta(days=1)
    if len(result) != 4:
        raise ValueError(f"특수일 인구 후보 네 날짜를 찾지 못했습니다: {target}")
    return tuple(result)  # type: ignore[return-value]


def _read_population_day(
    population_dir: Path,
    target: date,
    grid_ids: frozenset[str],
    cache: dict[date, dict[tuple[int, str], float]],
    *,
    required: bool = True,
) -> dict[tuple[int, str], float]:
    """하루 원천 CSV에서 필요한 격자·시간의 인구만 읽고 캐시한다."""
    if target in cache:
        return cache[target]
    path = population_dir / f"250_LOCAL_RESD_{target:%Y%m%d}.csv"
    if not path.exists():
        if required:
            raise FileNotFoundError(f"과거 생활인구 원천이 없습니다: {path}")
        cache[target] = {}
        return cache[target]
    frame = pd.read_csv(
        path,
        encoding="euc-kr",
        usecols=["시간", "250M격자", "생활인구합계"],
        dtype=str,
        na_values=["*"],
    )
    frame["250M격자"] = frame["250M격자"].str.strip()
    frame = frame[frame["250M격자"].isin(grid_ids)].copy()
    frame["시간"] = pd.to_numeric(frame["시간"], errors="coerce")
    frame["생활인구합계"] = pd.to_numeric(frame["생활인구합계"], errors="coerce")
    frame = frame.dropna(subset=["시간", "250M격자", "생활인구합계"])
    result = {
        (int(row["시간"]), str(row["250M격자"])): float(row["생활인구합계"])
        for row in frame.to_dict("records")
        if math.isfinite(float(row["생활인구합계"]))
    }
    cache[target] = result
    return result
