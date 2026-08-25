"""immutable source authority를 Gold publication으로 전환하는 CLI 진입점이다."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from core.db import get_connection
from core.gold_publication import PublicationOutcome, S3ImmutableObjectStore
from core.gold_publication.errors import ContractViolation
from core.source_snapshot_io import read_partial_source_snapshot
from psycopg import Connection

from gold.dispatch_center import (
    load_dispatch_center_seed,
    publish_dispatch_center,
)
from gold.event import publish_cultural_event, publish_performance_event
from gold.source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from gold.state import load_publication_state, read_state_manifest
from gold.weather_grid import load_weather_grid_seed, publish_weather_grid

_ACTIVE_PUBLICATIONS = (
    "event:cultural_event",
    "event:performance_event",
    "seed:dispatch_center",
    "seed:weather_grid",
)
_RETIRED_PUBLICATIONS = (
    "station-master-correction",
    "station-release",
    "weather-forecast",
)
_PUBLICATIONS = (*_ACTIVE_PUBLICATIONS, *_RETIRED_PUBLICATIONS)
_ROOT = Path(__file__).resolve().parent.parent


def run(
    publication: str,
    window_start: datetime,
) -> str:
    """선택한 원천 publication을 actual S3 authority와 PostGIS에 게시한다."""
    if publication in _RETIRED_PUBLICATIONS:
        raise ContractViolation(
            f"{publication} standalone authority는 retired되었습니다. "
            "serving_cli.py prepare/finalize coordinated chain을 사용하세요."
        )
    if publication not in _PUBLICATIONS:
        raise ContractViolation(f"지원하지 않는 Gold publication입니다: {publication}")
    logical = _utc_dttm(window_start)
    bucket = _required_env("S3_BUCKET")
    if "/" in bucket or any(character.isspace() for character in bucket):
        raise ContractViolation("S3_BUCKET 형식이 잘못됐습니다.")
    client = _s3_client()
    object_store = S3ImmutableObjectStore(client)
    source_catalog = S3SourceSnapshotCatalog(
        client,
        object_store,
        bucket=bucket,
    )
    object_base_uri = f"s3://{bucket}/gold_publication"
    with get_connection() as connection:
        if publication == "seed:dispatch_center":
            seed = load_dispatch_center_seed(_ROOT)
            if seed.effective_dttm != logical:
                raise ContractViolation(
                    "dispatch center --window-start가 SSOT seed effective_dttm과 다릅니다."
                )
            result = publish_dispatch_center(
                connection,
                object_store,
                seed=seed,
                object_base_uri=object_base_uri,
            )
        elif publication == "seed:weather_grid":
            seed = load_weather_grid_seed(
                _ROOT,
                seed_version=_required_env("GOLD_WEATHER_GRID_SEED_VERSION"),
                effective_dttm=logical,
            )
            result = publish_weather_grid(
                connection,
                object_store,
                seed=seed,
                object_base_uri=object_base_uri,
            )
        elif publication == "event:cultural_event":
            source_artifact = _event_source_or_stale(
                connection,
                object_store,
                source_catalog,
                source_id="cultural_event",
                logical=logical,
            )
            if source_artifact is None:
                return PublicationOutcome.STALE.value
            result = publish_cultural_event(
                connection,
                object_store,
                source_artifact=source_artifact,
                source_catalog=source_catalog,
                object_base_uri=object_base_uri,
            )
        else:
            source_artifact = _event_source_or_stale(
                connection,
                object_store,
                source_catalog,
                source_id="performance_event",
                logical=logical,
            )
            if source_artifact is None:
                return PublicationOutcome.STALE.value
            result = publish_performance_event(
                connection,
                object_store,
                source_artifact=source_artifact,
                source_catalog=source_catalog,
                stadium_asset_path=_ROOT / "loader/assets/stadium_coords.json",
                object_base_uri=object_base_uri,
            )
    return result.result.outcome.value


def _event_source_or_stale(
    connection: Connection[Any],
    object_store: S3ImmutableObjectStore,
    source_catalog: S3SourceSnapshotCatalog,
    *,
    source_id: str,
    logical: datetime,
) -> SourceManifestArtifact | None:
    """행사 exact authority를 반환하거나 검증된 PARTIAL에서 기존 Gold를 유지한다.

    Authority prefix가 정확히 빈 경우에만 completed PARTIAL fallback을 검사한다.
    기존 publication state와 그 actual immutable manifest까지 유효해야 stale no-op을
    허용하며, state logical time이나 Gold target은 갱신하지 않는다.
    """
    source_artifact = source_catalog.exact_window_or_none(source_id, logical)
    if source_artifact is not None:
        return source_artifact

    read_partial_source_snapshot(source_id, logical)
    publication_key = f"event:{source_id}"
    state = load_publication_state(connection, publication_key)
    if state is None:
        raise ContractViolation(
            f"{publication_key} PARTIAL 이전에 유지할 Gold publication이 없습니다."
        )
    read_state_manifest(object_store, state)
    return None


def _required_env(name: str) -> str:
    """필수 환경변수의 nonblank exact 값을 반환한다."""
    try:
        value = os.environ[name]
    except KeyError as exc:
        raise ContractViolation(f"필수 환경변수가 없습니다: {name}") from exc
    if type(value) is not str or not value or value != value.strip():
        raise ContractViolation(f"필수 환경변수가 nonblank exact 값이 아닙니다: {name}")
    return value


def _s3_client() -> Any:
    """표준 AWS credential chain과 선택적 S3 endpoint로 client를 만든다."""
    endpoint_url = os.environ.get("S3_ENDPOINT_URL") or None
    return boto3.client("s3", endpoint_url=endpoint_url)


def _utc_dttm(value: datetime) -> datetime:
    """CLI logical time을 timezone-aware UTC instant로 검증한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ContractViolation("--window-start는 timezone offset이 필요합니다.")
    return value.astimezone(UTC)


def _parse_window_start(raw: str) -> datetime:
    """ISO 8601 window-start를 offset 필수 aware datetime으로 파싱한다."""
    if type(raw) is not str:
        raise ContractViolation("--window-start는 문자열이어야 합니다.")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ContractViolation("--window-start가 ISO 8601 시각이 아닙니다.") from exc
    return _utc_dttm(parsed)


def main() -> int:
    """CLI 인자를 파싱해 성공은 0, 실패는 원인과 함께 1을 반환한다."""
    parser = argparse.ArgumentParser(description="Gold source publication을 실행한다.")
    parser.add_argument("--publication", required=True, choices=_PUBLICATIONS)
    parser.add_argument("--window-start", required=True)
    args = parser.parse_args()
    try:
        outcome = run(args.publication, _parse_window_start(args.window_start))
    except Exception as exc:  # noqa: BLE001 - CLI 경계에서 nonzero로 변환한다.
        print(f"Gold publisher failed: {exc}", file=sys.stderr)
        return 1
    print(f"Gold publisher outcome: {outcome}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
