"""빈 로컬 환경에서 realtime E2E가 요구하는 최소 fixture를 게시한다."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import boto3
import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from core.db import get_connection
from core.gold_publication import (
    ContractViolation,
    S3ImmutableObjectStore,
    canonical_json_bytes,
    sha256_hex,
)
from core.model_snapshot import ModelKind
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from core.source_snapshot_io import (
    SourceSnapshotNotFoundError,
    read_exact_source_snapshot,
)
from core.weather_grid import latlon_to_grid
from gold.dispatch_center import load_dispatch_center_seed, publish_dispatch_center
from gold.source_policy import validate_source_snapshot_policy
from gold.state import load_dependencies
from gold.weather_grid import load_weather_grid_seed, publish_weather_grid
from ml_core import common_config
from ml_core.model_contract import RENTAL_FEATURE_COLUMNS, RETURN_FEATURE_COLUMNS
from ml_core.serving_release import (
    ExplicitImmutablePayload,
    S3ServingReleasePointerStore,
    build_serving_release_manifest,
    load_current_serving_release,
    publish_effective_contract,
    publish_model_snapshot,
    publish_release_artifact,
    publish_serving_release,
    publish_station_profile,
)

_ROOT = Path(__file__).resolve().parent.parent
_CELL_ID = "다사53815262"
_H_DNG_CD = "1168064000"
_AGE_COLUMNS = (
    "M00",
    "M10",
    "M15",
    "M20",
    "M25",
    "M30",
    "M35",
    "M40",
    "M45",
    "M50",
    "M55",
    "M60",
    "M65",
    "M70",
    "F00",
    "F10",
    "F15",
    "F20",
    "F25",
    "F30",
    "F35",
    "F40",
    "F45",
    "F50",
    "F55",
    "F60",
    "F65",
    "F70",
)
_HISTORY_OFFSETS_MINUTES = (-25, -20, -15, -10, -5)
_LOCAL_HOSTS = frozenset({"127.0.0.1", "host.docker.internal", "localhost", "minio", "postgres"})
_KST = ZoneInfo("Asia/Seoul")
_STATION_MASTER_COLUMNS = (
    "station_id",
    "station_no",
    "station_name",
    "capacity",
    "lat",
    "lon",
    "grid_id",
    "weather_nx",
    "weather_ny",
)


def _required_env(name: str) -> str:
    """필수 환경변수의 공백 없는 값을 반환한다."""
    value = os.environ.get(name)
    if value is None or not value or value != value.strip():
        raise ValueError(f"필수 환경변수가 없습니다: {name}")
    return value


def _require_local_fixture_environment() -> None:
    """명시적 opt-in과 로컬 DB·S3 endpoint를 모두 확인한다."""
    if os.environ.get("LOCAL_E2E_ALLOW_FIXTURE") != "1":
        raise ValueError("LOCAL_E2E_ALLOW_FIXTURE=1 명시적 opt-in이 필요합니다.")
    endpoints = {
        "DATABASE_URL": _required_env("DATABASE_URL"),
        "S3_ENDPOINT_URL": _required_env("S3_ENDPOINT_URL"),
    }
    for name, value in endpoints.items():
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "postgres", "postgresql"}:
            raise ValueError(f"{name}이 로컬 허용 scheme이 아닙니다.")
        if parsed.hostname not in _LOCAL_HOSTS:
            raise ValueError(f"{name}이 로컬 허용 host가 아닙니다: {parsed.hostname}")


def _s3_client() -> Any:
    """로컬 MinIO를 포함한 표준 S3 client를 만든다."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
    )


def _parse_logical_dttm(raw: str) -> datetime:
    """Offset이 포함된 ISO 8601 logical time을 5분 경계로 검증한다."""
    try:
        logical = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("--logical-dttm은 ISO 8601 시각이어야 합니다.") from exc
    if logical.tzinfo is None or logical.utcoffset() is None:
        raise ValueError("--logical-dttm에는 timezone offset이 필요합니다.")
    if logical.second or logical.microsecond or logical.minute % 5:
        raise ValueError("--logical-dttm은 초가 0인 5분 경계여야 합니다.")
    return logical


def _stations_from_realtime(table: pa.Table) -> tuple[dict[str, object], ...]:
    """Exact realtime snapshot을 station master·모델 공통 fixture로 변환한다."""
    required = {
        "stationId",
        "stationName",
        "rackTotCnt",
        "stationLatitude",
        "stationLongitude",
    }
    if missing := required - set(table.column_names):
        raise ValueError(f"realtime station fixture 필수 컬럼이 없습니다: {sorted(missing)}")
    by_station_id: dict[str, dict[str, object]] = {}
    for raw in table.to_pylist():
        try:
            station_id = str(raw["stationId"]).strip()
            station_name = str(raw["stationName"]).strip()
            latitude = float(raw["stationLatitude"])
            longitude = float(raw["stationLongitude"])
            capacity = int(raw["rackTotCnt"])
        except (KeyError, TypeError, ValueError):
            continue
        if (
            not station_id.startswith("ST-")
            or not station_name
            or not math.isfinite(latitude)
            or not math.isfinite(longitude)
            or not 36.5 <= latitude <= 38.5
            or not 125.5 <= longitude <= 128.5
            or capacity <= 0
        ):
            continue
        by_station_id[station_id] = {
            "capacity": capacity,
            "lat": latitude,
            "lon": longitude,
            "station_address": f"로컬 E2E fixture {station_name}",
            "station_id": station_id,
            "station_name": station_name,
        }
    if not by_station_id:
        raise ValueError("realtime snapshot에 유효한 station fixture 행이 없습니다.")
    return tuple(
        {**by_station_id[station_id], "station_no": station_no}
        for station_no, station_id in enumerate(sorted(by_station_id), start=1)
    )


def _load_stations(logical: datetime) -> tuple[dict[str, object], ...]:
    """미리 수집한 exact realtime source에서 공통 station fixture를 읽는다."""
    snapshot = read_exact_source_snapshot("bike_station_realtime", logical)
    if snapshot.table is None:
        raise ValueError("station fixture용 bike_station_realtime snapshot이 EMPTY입니다.")
    return _stations_from_realtime(snapshot.table)


def _parquet_bytes(table: pa.Table) -> bytes:
    """PyArrow table을 단일 Parquet object bytes로 직렬화한다."""
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def _json_bytes(value: object) -> bytes:
    """Float도 허용하는 모델 학습 산출물 JSON bytes를 만든다."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")


def _put_object(client: Any, bucket: str, key: str, payload: bytes) -> None:
    """로컬 fixture의 교체 가능한 파생 object를 S3에 기록한다."""
    client.put_object(Bucket=bucket, Key=key, Body=payload)


def _nowcast_table() -> pa.Table:
    """모든 시간대를 포함하는 최소 생활인구 baseline을 만든다."""
    rows = []
    for hour in range(24):
        row: dict[str, object] = {
            "H_DNG_CD": _H_DNG_CD,
            "CELL_ID": _CELL_ID,
            "TT": hour,
            "SPOP": 1_000.0,
            "is_estimated": True,
            "estimation_method": "local_e2e_fixture",
        }
        row.update({column: 1_000.0 / len(_AGE_COLUMNS) for column in _AGE_COLUMNS})
        rows.append(row)
    return pa.Table.from_pylist(rows)


def _publish_nowcasts(
    client: Any,
    bucket: str,
    logical: datetime,
) -> tuple[str, ...]:
    """현재와 다음 날짜의 nowcast baseline을 로컬 Silver에 쓴다."""
    payload = _parquet_bytes(_nowcast_table())
    keys = []
    for target_date in (logical.date(), logical.date() + timedelta(days=1)):
        key = (
            "silver/living_population_grid/"
            f"dt={target_date.isoformat()}/hh=00/nowcast.parquet"
        )
        _put_object(client, bucket, key, payload)
        keys.append(key)
    return tuple(keys)


def _enriched_station_master_table(
    stations: tuple[dict[str, object], ...],
) -> pa.Table:
    """추론기가 읽을 수 있는 보강 station master fixture를 만든다."""
    rows = []
    for station in stations:
        weather_nx, weather_ny = latlon_to_grid(
            float(station["lat"]),
            float(station["lon"]),
        )
        rows.append(
            {
                **station,
                "grid_id": _CELL_ID,
                "weather_nx": weather_nx,
                "weather_ny": weather_ny,
            }
        )
    schema = pa.schema(
        (
            pa.field("station_id", pa.string(), nullable=False),
            pa.field("station_no", pa.int16(), nullable=False),
            pa.field("station_name", pa.string(), nullable=False),
            pa.field("capacity", pa.int64(), nullable=False),
            pa.field("lat", pa.float64(), nullable=False),
            pa.field("lon", pa.float64(), nullable=False),
            pa.field("grid_id", pa.string(), nullable=False),
            pa.field("weather_nx", pa.int64(), nullable=False),
            pa.field("weather_ny", pa.int64(), nullable=False),
        )
    )
    return pa.Table.from_pylist(rows, schema=schema)


def _publish_enriched_station_master(
    client: Any,
    bucket: str,
    logical: datetime,
    stations: tuple[dict[str, object], ...],
) -> str:
    """로컬 정적 자산에서 보강 station master Silver를 게시한다."""
    key = (
        "silver/station_master_enriched/"
        f"dt={logical:%Y-%m-%d}/hh={logical:%H}/{logical:%H%M}.parquet"
    )
    _put_object(
        client,
        bucket,
        key,
        _parquet_bytes(_enriched_station_master_table(stations)),
    )
    return key


def _realtime_table(stations: tuple[dict[str, object], ...]) -> pa.Table:
    """과거 재고 시계열에 사용할 deterministic realtime Silver를 만든다."""
    return pa.Table.from_pylist(
        [
            {
                "stationId": station["station_id"],
                "stationName": station["station_name"],
                "rackTotCnt": station["capacity"],
                "parkingBikeTotCnt": max(0, int(station["capacity"]) // 2),
                "shared": 0,
                "stationLatitude": station["lat"],
                "stationLongitude": station["lon"],
            }
            for station in stations
        ]
    )


def _master_table(stations: tuple[dict[str, object], ...]) -> pa.Table:
    """Station Gold publisher가 소비하는 master source fixture를 만든다."""
    return pa.Table.from_pylist(
        [
            {
                "RNTLS_ID": station["station_id"],
                "ADDR1": station["station_address"],
                "ADDR2": "",
                "LAT": station["lat"],
                "LOT": station["lon"],
            }
            for station in stations
        ]
    )


def _floor_logical(logical: datetime, tick_minutes: int) -> datetime:
    """Timezone을 보존하며 logical time을 지정 분 경계로 내린다."""
    midnight = logical.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_minutes = logical.hour * 60 + logical.minute
    return midnight + timedelta(minutes=(elapsed_minutes // tick_minutes) * tick_minutes)


def _weather_forecast_table(
    logical: datetime,
    base: datetime,
    *,
    source_id: str,
) -> pa.Table:
    """34개 grid와 향후 13시간을 덮는 결정적 KMA 예보 fixture를 만든다."""
    grid_seed = load_weather_grid_seed(
        _ROOT,
        seed_version="local-e2e-weather-grid-v1",
        effective_dttm=logical.astimezone(UTC) - timedelta(days=1),
    )
    first = logical.astimezone(_KST).replace(minute=0, second=0, microsecond=0)
    first += timedelta(hours=1)
    base_kst = base.astimezone(_KST)
    rows = []
    for grid in grid_seed.rows:
        for offset in range(13):
            target = first + timedelta(hours=offset)
            common = {
                "nx": grid.weather_grid_x_no,
                "ny": grid.weather_grid_y_no,
                "baseDate": f"{base_kst:%Y%m%d}",
                "baseTime": f"{base_kst:%H%M}",
                "fcstDate": f"{target:%Y%m%d}",
                "fcstTime": f"{target:%H%M}",
                "POP": 0.0,
                "PTY": 0,
                "REH": 50.0,
                "SKY": 1,
                "WSD": 1.0,
            }
            if source_id == "weather_short_term_forecast":
                common.update({"PCP": "강수없음", "TMP": 20.0})
            elif source_id == "weather_ultra_short_forecast":
                common.update({"RN1": "강수없음", "T1H": 20.0})
            else:
                raise ValueError(f"지원하지 않는 forecast source입니다: {source_id}")
            rows.append(common)
    return pa.Table.from_pylist(rows)


def _weather_live_table(logical: datetime) -> pa.Table:
    """Inference horizon 1의 관측 fallback에 쓸 KMA 실황 fixture를 만든다."""
    grid_seed = load_weather_grid_seed(
        _ROOT,
        seed_version="local-e2e-weather-grid-v1",
        effective_dttm=logical.astimezone(UTC) - timedelta(days=1),
    )
    base = logical.astimezone(_KST)
    return pa.Table.from_pylist(
        [
            {
                "nx": grid.weather_grid_x_no,
                "ny": grid.weather_grid_y_no,
                "baseDate": f"{base:%Y%m%d}",
                "baseTime": f"{base:%H%M}",
                "T1H": 20.0,
                "REH": 50.0,
                "WSD": 1.0,
                "RN1": 0.0,
                "PTY": 0,
            }
            for grid in grid_seed.rows
        ]
    )


def _kma_parts(logical: datetime) -> tuple[str, ...]:
    """Checked-in weather seed에서 collector의 exact 34-grid part를 만든다."""
    seed = load_weather_grid_seed(
        _ROOT,
        seed_version="local-e2e-weather-grid-v1",
        effective_dttm=logical.astimezone(UTC) - timedelta(days=1),
    )
    return tuple(
        sorted(
            f"grid-{row.weather_grid_x_no:03d}x{row.weather_grid_y_no:03d}"
            for row in seed.rows
        )
    )


def _source_manifest_key(source_id: str, logical: datetime, revision: int) -> str:
    """Collector source authority와 같은 UTC manifest key를 만든다."""
    utc = logical.astimezone(UTC)
    return (
        f"source_snapshot_manifest/{source_id}/dt={utc:%Y-%m-%d}/hh={utc:%H}/"
        f"logical={utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z/"
        f"revision={revision:010d}.json"
    )


def _existing_snapshot_is_usable(
    source_id: str,
    snapshot: Any,
    planned_parts: tuple[str, ...],
) -> bool:
    """기존 authority가 배포 policy 또는 fixture plan을 만족하는지 판정한다."""
    if snapshot.table is None:
        return False
    try:
        validate_source_snapshot_policy(snapshot.manifest)
    except ContractViolation:
        return snapshot.manifest.planned_parts == planned_parts
    return True


def _publish_source_snapshot(
    object_store: S3ImmutableObjectStore,
    bucket: str,
    *,
    source_id: str,
    logical: datetime,
    table: pa.Table,
    planned_parts: tuple[str, ...],
) -> str:
    """단일 source fixture를 공식 immutable authority manifest로 게시한다."""
    try:
        existing = read_exact_source_snapshot(source_id, logical)
    except SourceSnapshotNotFoundError:
        existing = None
    revision = 0
    if existing is not None:
        if _existing_snapshot_is_usable(source_id, existing, planned_parts):
            return _source_manifest_key(
                source_id,
                logical,
                existing.manifest.revision_no,
            )
        revision = existing.manifest.revision_no + 1

    payload = _parquet_bytes(table)
    digest = sha256_hex(payload)
    silver_uri = (
        f"s3://{bucket}/local_e2e/source_snapshot_silver/{source_id}/"
        f"sha256={digest}.parquet"
    )
    object_store.put_once(silver_uri, payload, expected_sha256=digest)
    config_payload = (_ROOT / f"collector/sources/{source_id}.yaml").read_bytes()
    count = table.num_rows
    manifest = build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical,
        revision_no=revision,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=f"sha256:{sha256_hex(config_payload)}",
        silver_uri=silver_uri,
        silver_byte_sha256=digest,
        counts=SourceSnapshotCounts(count, count, count, 0, 0),
        planned_parts=planned_parts,
        completed_parts=planned_parts,
    )
    key = _source_manifest_key(source_id, logical, revision)
    object_store.put_once(
        f"s3://{bucket}/{key}",
        manifest.canonical_bytes,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    return key


def _publish_prerequisite_source_snapshots(
    object_store: S3ImmutableObjectStore,
    bucket: str,
    logical: datetime,
    stations: tuple[dict[str, object], ...],
) -> dict[str, str]:
    """운영 DAG가 수집하지 않는 station·weather authority를 준비한다."""
    kma_parts = _kma_parts(logical)
    short_logical = _floor_logical(logical, 180)
    ultra_logical = _floor_logical(logical, 30)
    return {
        "bike_station_master": _publish_source_snapshot(
            object_store,
            bucket,
            source_id="bike_station_master",
            logical=logical,
            table=_master_table(stations),
            planned_parts=(
                "page-00001-01000",
                "page-01001-02000",
                "page-02001-03000",
            ),
        ),
        "weather_short_term_forecast": _publish_source_snapshot(
            object_store,
            bucket,
            source_id="weather_short_term_forecast",
            logical=short_logical,
            table=_weather_forecast_table(
                logical,
                short_logical,
                source_id="weather_short_term_forecast",
            ),
            planned_parts=kma_parts,
        ),
        "weather_ultra_short_forecast": _publish_source_snapshot(
            object_store,
            bucket,
            source_id="weather_ultra_short_forecast",
            logical=ultra_logical,
            table=_weather_forecast_table(
                logical,
                ultra_logical,
                source_id="weather_ultra_short_forecast",
            ),
            planned_parts=kma_parts,
        ),
        "weather_ultra_short_live": _publish_source_snapshot(
            object_store,
            bucket,
            source_id="weather_ultra_short_live",
            logical=logical,
            table=_weather_live_table(logical),
            planned_parts=kma_parts,
        ),
    }


def _publish_history_snapshots(
    object_store: S3ImmutableObjectStore,
    bucket: str,
    logical: datetime,
    stations: tuple[dict[str, object], ...],
) -> tuple[str, ...]:
    """Urgency가 요구하는 직전 5개 complete realtime authority를 만든다."""
    parts = (
        "page-00001-01000",
        "page-01001-02000",
        "page-02001-03000",
        "page-03001-04000",
    )
    keys = []
    for offset in _HISTORY_OFFSETS_MINUTES:
        window = logical + timedelta(minutes=offset)
        keys.append(
            _publish_source_snapshot(
                object_store,
                bucket,
                source_id="bike_station_realtime",
                logical=window,
                table=_realtime_table(stations),
                planned_parts=parts,
            )
        )
    return tuple(keys)


def _training_frame(feature_columns: list[str], station_nos: tuple[int, ...]) -> pd.DataFrame:
    """모든 station category를 담은 1-round LightGBM 학습 입력을 만든다."""
    count = len(station_nos)
    values: dict[str, object] = {}
    for column in feature_columns:
        if column == "station_no":
            values[column] = pd.Categorical(station_nos, categories=station_nos)
        elif column in {"lat", "lon", "temp", "precip", "pop_total"} or column.endswith(
            "_lag_1h"
        ):
            values[column] = np.ones(count, dtype=np.float32)
        else:
            values[column] = np.ones(count, dtype=np.int16)
    return pd.DataFrame(values, columns=feature_columns)


def _booster_payload(feature_columns: list[str], station_nos: tuple[int, ...]) -> bytes:
    """실제 추론기로 읽을 수 있는 최소 LightGBM model text를 만든다."""
    frame = _training_frame(feature_columns, station_nos)
    dataset = lgb.Dataset(
        frame,
        label=np.ones(len(frame), dtype=np.float32),
        categorical_feature=["station_no"],
        free_raw_data=False,
    )
    booster = lgb.train(
        {
            "deterministic": True,
            "feature_pre_filter": False,
            "force_col_wise": True,
            "min_data_in_leaf": 1,
            "num_threads": 1,
            "objective": "poisson",
            "seed": 173,
            "verbosity": -1,
        },
        dataset,
        num_boost_round=1,
    )
    return booster.model_to_string().encode("utf-8")


def _station_source_payload(stations: tuple[dict[str, object], ...]) -> bytes:
    """Model crosswalk의 explicit immutable station source를 만든다."""
    return _parquet_bytes(
        pa.table(
            {
                "station_id": [station["station_id"] for station in stations],
                "station_no": pa.array(
                    [station["station_no"] for station in stations],
                    type=pa.int16(),
                ),
            }
        )
    )


def _station_profile_payload(station_nos: tuple[int, ...]) -> bytes:
    """모델 category와 global minute grid를 모두 포함한 작은 profile을 만든다."""
    grid_tick = common_config.GRID_TICK_MINUTES
    minutes = tuple(range(0, 1440, grid_tick))
    rows = len(station_nos)
    table = pa.Table.from_arrays(
        (
            pa.array(station_nos, type=pa.int16()),
            pa.array([minutes[index % len(minutes)] for index in range(rows)], type=pa.int16()),
            pa.array([0] * rows, type=pa.int8()),
            pa.array([1] * rows, type=pa.int8()),
            pa.array([1.0] * rows, type=pa.float32()),
            pa.array([0.0] * rows, type=pa.float32()),
            pa.array([1.0] * rows, type=pa.float32()),
            pa.array([0.0] * rows, type=pa.float32()),
            pa.array([1] * rows, type=pa.int32()),
        ),
        names=(
            "station_no",
            "minute",
            "dow",
            "month",
            "rental_mean",
            "rental_std",
            "return_mean",
            "return_std",
            "n_samples",
        ),
    )
    return _parquet_bytes(table)


def _model_payloads(booster: bytes, station_nos: tuple[int, ...]) -> dict[str, bytes]:
    """Crosswalk을 제외한 local E2E model snapshot 아티팩트를 만든다."""
    profile = _json_bytes(common_config.effective_profile())
    return {
        "booster_poisson": booster,
        "booster_q10": booster,
        "booster_q50": booster,
        "booster_q90": booster,
        "conformal_correction": _json_bytes(
            {
                "correction": 0.0,
                "target_coverage": common_config.CONFORMAL_TARGET_COVERAGE,
            }
        ),
        "effective_profile": profile,
        "metrics": _json_bytes({"fixture": "local-e2e", "poisson_deviance_test": 0.0}),
        "station_categories": canonical_json_bytes(list(station_nos)),
    }


def _publish_serving_release(
    client: Any,
    object_store: S3ImmutableObjectStore,
    bucket: str,
    stations: tuple[dict[str, object], ...],
) -> str:
    """실제 model/release 계약을 통과하는 local E2E 포인터를 게시한다."""
    station_nos = tuple(int(station["station_no"]) for station in stations)
    source_payload = _station_source_payload(stations)
    source_ref = publish_release_artifact(
        source_payload,
        role="station_master_source",
        extension="parquet",
        object_store=object_store,
        bucket=bucket,
    )
    station_source = ExplicitImmutablePayload(
        payload=source_payload,
        byte_sha256=source_ref.byte_sha256,
        uri=source_ref.uri,
    )
    rental_payloads = _model_payloads(
        _booster_payload(RENTAL_FEATURE_COLUMNS, station_nos),
        station_nos,
    )
    return_payloads = _model_payloads(
        _booster_payload(RETURN_FEATURE_COLUMNS, station_nos),
        station_nos,
    )
    rental = publish_model_snapshot(
        model_kind=ModelKind.RENTAL,
        artifact_payloads=rental_payloads,
        station_source=station_source,
        object_store=object_store,
        bucket=bucket,
    )
    returned = publish_model_snapshot(
        model_kind=ModelKind.RETURN,
        artifact_payloads=return_payloads,
        station_source=station_source,
        object_store=object_store,
        bucket=bucket,
    )
    station_profile = publish_station_profile(
        _station_profile_payload(station_nos),
        object_store=object_store,
        bucket=bucket,
    )
    contract = publish_effective_contract(
        rental_payloads["effective_profile"],
        object_store=object_store,
        bucket=bucket,
    )
    release = build_serving_release_manifest(
        rental_model_manifest=rental.manifest_ref,
        return_model_manifest=returned.manifest_ref,
        station_profile=station_profile,
        effective_contract=contract,
    )
    pointer = publish_serving_release(
        release,
        station_source=station_source,
        object_store=object_store,
        pointer_store=S3ServingReleasePointerStore(client, bucket),
        allow_contract_change=True,
    )
    return pointer.release_manifest_uri


def _publish_gold_dependencies(
    object_store: S3ImmutableObjectStore,
    logical: datetime,
) -> tuple[str, str]:
    """Serving plan이 요구하는 dispatch center와 weather grid를 게시한다."""
    object_base_uri = f"s3://{_required_env('S3_BUCKET')}/gold_publication"
    with get_connection() as connection:
        dispatch = publish_dispatch_center(
            connection,
            object_store,
            seed=load_dispatch_center_seed(_ROOT),
            object_base_uri=object_base_uri,
        )
        weather = publish_weather_grid(
            connection,
            object_store,
            seed=load_weather_grid_seed(
                _ROOT,
                seed_version="local-e2e-weather-grid-v1",
                effective_dttm=logical.astimezone(UTC) - timedelta(days=1),
            ),
            object_base_uri=object_base_uri,
        )
    return dispatch.result.outcome.value, weather.result.outcome.value


def seed(logical: datetime) -> dict[str, object]:
    """지정 logical time의 local E2E fixture 전체를 멱등 게시한다."""
    _require_local_fixture_environment()
    bucket = _required_env("S3_BUCKET")
    client = _s3_client()
    object_store = S3ImmutableObjectStore(client)
    stations = _load_stations(logical + timedelta(minutes=-5))
    nowcasts = _publish_nowcasts(client, bucket, logical)
    enriched = _publish_enriched_station_master(client, bucket, logical, stations)
    prerequisite_sources = _publish_prerequisite_source_snapshots(
        object_store,
        bucket,
        logical,
        stations,
    )
    history = _publish_history_snapshots(
        object_store,
        bucket,
        logical,
        stations,
    )
    release_uri = _publish_serving_release(client, object_store, bucket, stations)
    gold_outcomes = _publish_gold_dependencies(object_store, logical)
    check(logical)
    return {
        "enriched_station_master": enriched,
        "gold_outcomes": gold_outcomes,
        "history_window_count": len(history),
        "logical_dttm": logical.isoformat(),
        "nowcasts": nowcasts,
        "prerequisite_source_count": len(prerequisite_sources),
        "serving_release_uri": release_uri,
        "station_count": len(stations),
    }


def check(logical: datetime) -> dict[str, object]:
    """Local E2E fixture의 필수 object·pointer·DB dependency를 빠르게 검증한다."""
    _require_local_fixture_environment()
    bucket = _required_env("S3_BUCKET")
    client = _s3_client()
    required_keys = [
        (
            "silver/living_population_grid/"
            f"dt={target_date.isoformat()}/hh=00/nowcast.parquet"
        )
        for target_date in (logical.date(), logical.date() + timedelta(days=1))
    ]
    required_keys.append(
        "silver/station_master_enriched/"
        f"dt={logical:%Y-%m-%d}/hh={logical:%H}/{logical:%H%M}.parquet"
    )
    for key in required_keys:
        client.head_object(Bucket=bucket, Key=key)
    for offset in _HISTORY_OFFSETS_MINUTES:
        snapshot = read_exact_source_snapshot(
            "bike_station_realtime",
            logical + timedelta(minutes=offset),
        )
        if snapshot.table is None:
            raise ValueError(f"local E2E history snapshot이 EMPTY입니다: {offset}")
    prerequisite_windows = {
        "bike_station_master": logical,
        "weather_short_term_forecast": _floor_logical(logical, 180),
        "weather_ultra_short_forecast": _floor_logical(logical, 30),
        "weather_ultra_short_live": logical,
    }
    for source_id, window in prerequisite_windows.items():
        snapshot = read_exact_source_snapshot(source_id, window)
        if snapshot.table is None:
            raise ValueError(f"local E2E prerequisite snapshot이 EMPTY입니다: {source_id}")
    object_store = S3ImmutableObjectStore(client)
    pinned = load_current_serving_release(
        object_store=object_store,
        pointer_store=S3ServingReleasePointerStore(client, bucket),
    )
    with get_connection() as connection:
        dependencies = load_dependencies(connection, ("dispatch_center", "weather_grid"))
    return {
        "dependency_count": len(dependencies),
        "history_window_count": len(_HISTORY_OFFSETS_MINUTES),
        "prerequisite_source_count": len(prerequisite_windows),
        "release_version": pinned.manifest.release_version,
        "required_object_count": len(required_keys),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Local E2E seed/check CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(description="로컬 realtime E2E fixture를 준비한다.")
    parser.add_argument("command", choices=("seed", "check"))
    parser.add_argument("--logical-dttm", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI 결과를 JSON으로 출력하고 실패를 nonzero로 변환한다."""
    args = parse_args(argv)
    try:
        logical = _parse_logical_dttm(args.logical_dttm)
        result = seed(logical) if args.command == "seed" else check(logical)
    except Exception as exc:  # noqa: BLE001 - CLI 경계에서 원인을 보존해 실패한다.
        print(f"local E2E {args.command} failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
