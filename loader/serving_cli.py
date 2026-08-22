"""Serving plan부터 urgency·route까지 production Gold chain을 실행하는 CLI다."""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from contextlib import redirect_stdout
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import boto3
import pyarrow.parquet as pq
from core.db import get_connection
from core.gold_publication import (
    ContractViolation,
    PublicationOutcome,
    S3ImmutableObjectStore,
    parse_publication_manifest,
)
from core.inference_catalog import S3InferenceRevisionCatalog
from core.source_snapshot import SourceSnapshotStatus
from gold.rebalance_route import publish_rebalance_route
from gold.serving_plan import (
    SourceLookbacks,
    prepare_serving_plan,
    publish_serving_plan,
)
from gold.source_catalog import S3SourceSnapshotCatalog
from gold.urgency import (
    BIKE_STATION_REALTIME_SOURCE_ID,
    URGENCY_STOCK_HISTORY_OFFSETS_MINUTES,
    publish_station_urgency,
)
from ml_core import silver_schema
from ml_core.serving_release import (
    S3ServingReleasePointerStore,
    load_current_serving_release_for_plan,
)

_OBJECT_PREFIX = "gold_publication"
_SHORT_TERM_LOOKBACK = timedelta(hours=24)
_ULTRA_SHORT_LOOKBACK = timedelta(hours=6)
_FINAL_KEYS = frozenset(
    {"station", "station_demand_forecast", "station_stock", "weather_forecast"}
)
_SHORT_TERM_SOURCE_ID = "weather_short_term_forecast"
_ULTRA_SHORT_SOURCE_ID = "weather_ultra_short_forecast"


def weather_sources_ready(logical_dttm: datetime) -> bool:
    """해당 schedule 경계에 새로 발행될 날씨 authority가 준비됐는지 반환한다.

    10분 경계가 아닌 realtime tick에는 새 초단기예보가 예정되지 않아 즉시 준비된
    것으로 본다. 3시간 경계에는 초단기·단기예보 두 authority를 모두 요구한다.
    """
    logical = _utc_dttm(logical_dttm)
    kst = logical.astimezone(ZoneInfo("Asia/Seoul"))
    if kst.minute % 10 != 0:
        return True
    _bucket, _client, _object_store, source_catalog = _runtime()
    required = [_ULTRA_SHORT_SOURCE_ID]
    if kst.minute == 0 and kst.hour % 3 == 0:
        required.append(_SHORT_TERM_SOURCE_ID)
    try:
        artifacts = tuple(
            source_catalog.exact_window(source_id, logical) for source_id in required
        )
    except ContractViolation:
        return False
    return all(
        artifact.manifest.status
        in {SourceSnapshotStatus.SUCCEEDED, SourceSnapshotStatus.EMPTY}
        for artifact in artifacts
    )


def prepare(
    logical_dttm: datetime,
    *,
    relocation_approval_uri: str | None = None,
    relocation_approval_sha256: str | None = None,
) -> dict[str, dict[str, str]]:
    """Current release support와 최신 source snapshot으로 immutable plan을 준비한다."""
    logical = _utc_dttm(logical_dttm)
    bucket, client, object_store, source_catalog = _runtime()
    object_base_uri = f"s3://{bucket}/{_OBJECT_PREFIX}"
    pinned = load_current_serving_release_for_plan(
        object_store=object_store,
        pointer_store=S3ServingReleasePointerStore(client, bucket),
    )
    lookbacks = SourceLookbacks(
        master=_lookback_from_env("GOLD_STATION_MASTER_LOOKBACK_HOURS"),
        realtime=_lookback_from_env("GOLD_STATION_REALTIME_LOOKBACK_HOURS"),
        short_term=_SHORT_TERM_LOOKBACK,
        ultra_short=_ULTRA_SHORT_LOOKBACK,
    )
    relocation_payload = _read_optional_ref(
        object_store,
        relocation_approval_uri,
        relocation_approval_sha256,
        "relocation approval",
    )
    eligible_ids, excluded = _load_inference_eligible_station_ids(client, bucket)
    if excluded:
        preview = ", ".join(
            f"{station_id}({reason})" for station_id, reason in excluded[:20]
        )
        print(
            "Serving plan input-quality exclusions: "
            f"count={len(excluded)} preview=[{preview}]",
            file=sys.stderr,
        )
    with get_connection() as connection:
        artifact = prepare_serving_plan(
            connection,
            object_store,
            master_artifact=source_catalog.latest_at_or_before(
                "bike_station_master", logical, lookback=lookbacks.master
            ),
            realtime_candidate=source_catalog.exact_window(
                "bike_station_realtime", logical
            ),
            short_term_artifact=source_catalog.latest_at_or_before(
                "weather_short_term_forecast",
                logical,
                lookback=lookbacks.short_term,
            ),
            ultra_short_artifact=source_catalog.latest_at_or_before(
                "weather_ultra_short_forecast",
                logical,
                lookback=lookbacks.ultra_short,
            ),
            rental_support_sta_ids=(
                pinned.rental_model.support_sta_ids
            ),
            return_support_sta_ids=(
                pinned.return_model.support_sta_ids
            ),
            inference_eligible_sta_ids=eligible_ids,
            source_catalog=source_catalog,
            object_base_uri=object_base_uri,
            source_lookbacks=lookbacks,
            relocation_approval_payload=relocation_payload,
        )
    return {"plan": _ref(artifact.uri, artifact.byte_sha256)}


def _stock_history_refs(
    source_catalog: Any, anchor: datetime
) -> tuple[tuple[tuple[int, str, str], ...], tuple[int, ...]]:
    """가용한 stock history window만 골라 offset·URI·SHA로 만든다.

    지나간 5분 tick의 실시간 스냅샷은 소급 수집이 불가능하다. tick 하나가 빠졌을
    때 25분 내내 urgency를 실패시키는 대신, 없는 window는 건너뛰고 무엇이 빠졌는지
    보고한다. 빠뜨린 window가 실제로 부재한지는 publish_station_urgency가
    다시 검증하므로, 여기서 조용히 누락시켜도 통과되지는 않는다.
    """
    refs: list[tuple[int, str, str]] = []
    missing: list[int] = []
    for offset in URGENCY_STOCK_HISTORY_OFFSETS_MINUTES:
        try:
            artifact = source_catalog.exact_window(
                BIKE_STATION_REALTIME_SOURCE_ID,
                anchor + timedelta(minutes=offset),
            )
        except ContractViolation:
            missing.append(offset)
            continue
        refs.append((offset, artifact.uri, artifact.byte_sha256))
    return tuple(refs), tuple(missing)


def _load_inference_eligible_station_ids(
    client: Any, bucket: str
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
    """최신 enriched master에서 추론 가능 ID와 제외 사유를 계산한다."""
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": silver_schema.STATION_MASTER_ENRICHED_PREFIX,
        }
        if token is not None:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        keys.extend(
            item["Key"]
            for item in page.get("Contents", ())
            if item["Key"].endswith(".parquet")
        )
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
        if type(token) is not str or not token:
            raise ContractViolation("enriched station master LIST token이 없습니다.")
    if not keys:
        raise ContractViolation("최신 station_master_enriched parquet이 없습니다.")
    key = max(keys)
    payload = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    raw = pq.read_table(io.BytesIO(payload)).to_pandas()
    master = raw.rename(columns=silver_schema.STATION_COLUMN_MAP)
    if "station_id" not in master:
        raise ContractViolation("station_master_enriched에 station_id가 없습니다.")
    if (
        master["station_id"].isna().any()
        or not master["station_id"].map(lambda value: type(value) is str and bool(value)).all()
        or master["station_id"].duplicated().any()
    ):
        raise ContractViolation(
            "station_master_enriched의 station_id가 결측 또는 중복입니다."
        )
    eligible: list[str] = []
    excluded: list[tuple[str, str]] = []
    indexed = master.set_index("station_id")
    for station_id in sorted(indexed.index.astype(str), key=lambda value: value.encode("utf-8")):
        try:
            silver_schema.validate_inference_station_row(
                station_id, indexed.loc[station_id]
            )
        except (TypeError, ValueError) as exc:
            excluded.append((station_id, str(exc)))
        else:
            eligible.append(station_id)
    return tuple(eligible), tuple(excluded)


def finalize(
    *,
    plan_uri: str,
    plan_sha256: str,
    inference_uri: str,
    inference_sha256: str,
) -> dict[str, dict[str, str]]:
    """Plan과 same-logical latest inference를 네 Gold key로 원자 게시한다."""
    bucket, client, object_store, source_catalog = _runtime()
    for name, uri in (("plan", plan_uri), ("inference", inference_uri)):
        _require_uri_bucket(uri, bucket, name)
    inference_catalog = S3InferenceRevisionCatalog(
        client,
        object_store,
        bucket=bucket,
        object_base_uri=f"s3://{bucket}/{_OBJECT_PREFIX}",
    )
    with get_connection() as connection:
        execution = publish_serving_plan(
            connection,
            object_store,
            plan_uri=plan_uri,
            plan_sha256=plan_sha256,
            inference_manifest_uri=inference_uri,
            inference_manifest_sha256=inference_sha256,
            inference_catalog=inference_catalog,
            source_catalog=source_catalog,
        )
    if execution.result.outcome is PublicationOutcome.STALE:
        raise ContractViolation(
            "serving finalize가 STALE이므로 후속 chain을 중단합니다."
        )
    result = {
        item.manifest.publication_key: _ref(
            item.manifest_uri,
            item.manifest.sha256,
        )
        for item in execution.evidence
    }
    if frozenset(result) != _FINAL_KEYS:
        raise ContractViolation("serving finalize evidence key 집합이 잘못됐습니다.")
    return result


def urgency(
    *,
    station_uri: str,
    station_sha256: str,
    demand_uri: str,
    demand_sha256: str,
    stock_uri: str,
    stock_sha256: str,
) -> dict[str, dict[str, str]]:
    """Finalize exact release refs와 과거 5개 source window로 urgency를 게시한다."""
    bucket, _client, object_store, source_catalog = _runtime()
    exact_refs = {
        "station": (station_uri, station_sha256),
        "station_demand_forecast": (demand_uri, demand_sha256),
        "station_stock": (stock_uri, stock_sha256),
    }
    manifests = {
        key: _read_publication_manifest(object_store, bucket, key, uri, sha256)
        for key, (uri, sha256) in exact_refs.items()
    }
    anchor = manifests["station_stock"].logical_dttm
    if any(manifest.logical_dttm != anchor for manifest in manifests.values()):
        raise ContractViolation("finalize release ref의 logical_dttm이 섞였습니다.")
    history_refs, missing_offsets = _stock_history_refs(source_catalog, anchor)
    if missing_offsets:
        print(
            "Urgency stock history 결측 window: "
            f"offsets={list(missing_offsets)} "
            f"used={len(history_refs)}/"
            f"{len(URGENCY_STOCK_HISTORY_OFFSETS_MINUTES)}",
            file=sys.stderr,
        )
    with get_connection() as connection:
        execution = publish_station_urgency(
            connection,
            object_store,
            source_catalog=source_catalog,
            stock_history_manifest_refs=history_refs,
            serving_release_manifest_refs=exact_refs,
            object_base_uri=f"s3://{bucket}/{_OBJECT_PREFIX}",
        )
    if execution.result.outcome is PublicationOutcome.STALE:
        raise ContractViolation("urgency publication이 STALE이므로 route를 중단합니다.")
    item = _single_evidence(execution.evidence, "station_urgency")
    return {"station_urgency": _ref(item.manifest_uri, item.manifest.sha256)}


def route(*, urgency_uri: str, urgency_sha256: str) -> dict[str, dict[str, str]]:
    """Exact urgency authority ref로 proposed route aggregate를 원자 게시한다."""
    bucket, _client, object_store, _source_catalog = _runtime()
    _require_uri_bucket(urgency_uri, bucket, "urgency")
    with get_connection() as connection:
        execution = publish_rebalance_route(
            connection,
            object_store,
            urgency_manifest_uri=urgency_uri,
            urgency_manifest_sha256=urgency_sha256,
            object_base_uri=f"s3://{bucket}/{_OBJECT_PREFIX}",
        )
    if execution.result.outcome is PublicationOutcome.STALE:
        raise ContractViolation("route publication이 STALE입니다.")
    item = _single_evidence(execution.evidence, "rebalance_route")
    return {"rebalance_route": _ref(item.manifest_uri, item.manifest.sha256)}


def _runtime() -> tuple[
    str,
    Any,
    S3ImmutableObjectStore,
    S3SourceSnapshotCatalog,
]:
    """한 command에서 공유할 S3 client·store·source catalog를 만든다."""
    bucket = _required_bucket()
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
    )
    object_store = S3ImmutableObjectStore(client)
    return (
        bucket,
        client,
        object_store,
        S3SourceSnapshotCatalog(client, object_store, bucket=bucket),
    )


def _read_publication_manifest(
    object_store: S3ImmutableObjectStore,
    bucket: str,
    publication_key: str,
    uri: str,
    byte_sha256: str,
):
    """Final ref를 exact-read하고 expected publication key에 결합한다."""
    _require_uri_bucket(uri, bucket, publication_key)
    payload = object_store.read_bytes(
        uri,
        byte_sha256,
        require_canonical_json=True,
    )
    manifest = parse_publication_manifest(payload)
    if manifest.publication_key != publication_key or manifest.sha256 != byte_sha256:
        raise ContractViolation(
            f"{publication_key} exact ref가 actual manifest와 다릅니다."
        )
    return manifest


def _read_optional_ref(
    object_store: S3ImmutableObjectStore,
    uri: str | None,
    byte_sha256: str | None,
    name: str,
) -> bytes | None:
    """Optional URI·SHA 쌍을 둘 다 있을 때만 exact-read한다."""
    if (uri is None) != (byte_sha256 is None):
        raise ContractViolation(f"{name} URI와 SHA-256은 함께 지정해야 합니다.")
    if uri is None or byte_sha256 is None:
        return None
    _require_uri_bucket(uri, _required_bucket(), name)
    return object_store.read_bytes(uri, byte_sha256, require_canonical_json=True)


def _single_evidence(evidence, publication_key: str):
    """Single-key publication evidence 하나를 expected key로 검증한다."""
    if len(evidence) != 1 or evidence[0].manifest.publication_key != publication_key:
        raise ContractViolation(f"{publication_key} evidence가 정확히 하나가 아닙니다.")
    return evidence[0]


def _ref(uri: str, byte_sha256: str) -> dict[str, str]:
    """XCom 공개용 URI·SHA exact mapping을 만든다."""
    return {"byte_sha256": byte_sha256, "uri": uri}


def _required_bucket() -> str:
    """필수 S3_BUCKET을 안전한 bucket name으로 검증한다."""
    try:
        value = os.environ["S3_BUCKET"]
    except KeyError as exc:
        raise ContractViolation("필수 환경변수가 없습니다: S3_BUCKET") from exc
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or "/" in value
        or any(character.isspace() for character in value)
    ):
        raise ContractViolation("S3_BUCKET 형식이 잘못됐습니다.")
    return value


def _require_uri_bucket(uri: str, bucket: str, name: str) -> None:
    """S3 ref가 운영 bucket과 정확히 같은지 검증한다."""
    if type(uri) is not str:
        raise ContractViolation(f"{name} URI는 exact string이어야 합니다.")
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "s3"
        or parsed.netloc != bucket
        or not parsed.path.lstrip("/")
        or parsed.query
        or parsed.fragment
    ):
        raise ContractViolation(f"{name} URI가 S3_BUCKET과 다릅니다.")


def _lookback_from_env(name: str) -> timedelta:
    """명시적 환경변수의 canonical positive hour를 timedelta로 바꾼다."""
    try:
        raw = os.environ[name]
        hours = int(raw)
    except (KeyError, ValueError) as exc:
        raise ContractViolation(
            f"{name}은 canonical 양의 integer hour여야 합니다."
        ) from exc
    if type(raw) is not str or hours <= 0 or str(hours) != raw:
        raise ContractViolation(f"{name}은 canonical 양의 integer hour여야 합니다.")
    return timedelta(hours=hours)


def _utc_dttm(value: datetime) -> datetime:
    """Timezone-aware datetime을 UTC로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ContractViolation("logical_dttm은 timezone offset이 필요합니다.")
    return value.astimezone(UTC)


def _parse_dttm(raw: str) -> datetime:
    """ISO 8601 argument를 offset 필수 aware datetime으로 파싱한다."""
    try:
        return _utc_dttm(datetime.fromisoformat(raw))
    except (TypeError, ValueError) as exc:
        raise ContractViolation("--logical-dttm이 ISO 8601 시각이 아닙니다.") from exc


def _parser() -> argparse.ArgumentParser:
    """Prepare/finalize/urgency/route subcommand parser를 만든다."""
    parser = argparse.ArgumentParser(description="Gold serving publication chain")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--logical-dttm", required=True)
    prepare_parser.add_argument("--relocation-approval-uri")
    prepare_parser.add_argument("--relocation-approval-sha256")

    weather_parser = commands.add_parser("weather-ready")
    weather_parser.add_argument("--logical-dttm", required=True)

    final_parser = commands.add_parser("finalize")
    final_parser.add_argument("--plan-uri", required=True)
    final_parser.add_argument("--plan-sha256", required=True)
    final_parser.add_argument("--inference-uri", required=True)
    final_parser.add_argument("--inference-sha256", required=True)

    urgency_parser = commands.add_parser("urgency")
    for key in ("station", "demand", "stock"):
        urgency_parser.add_argument(f"--{key}-uri", required=True)
        urgency_parser.add_argument(f"--{key}-sha256", required=True)

    route_parser = commands.add_parser("route")
    route_parser.add_argument("--urgency-uri", required=True)
    route_parser.add_argument("--urgency-sha256", required=True)
    return parser


def main() -> int:
    """Subcommand를 실행하고 마지막 stdout 한 줄에 compact JSON ref만 쓴다."""
    args = _parser().parse_args()
    if args.command == "weather-ready":
        try:
            return 0 if weather_sources_ready(_parse_dttm(args.logical_dttm)) else 1
        except Exception as exc:  # noqa: BLE001 - Sensor poke 경계다.
            print(f"Weather readiness check failed: {exc}", file=sys.stderr)
            return 1
    try:
        with redirect_stdout(sys.stderr):
            if args.command == "prepare":
                result = prepare(
                    _parse_dttm(args.logical_dttm),
                    relocation_approval_uri=args.relocation_approval_uri,
                    relocation_approval_sha256=args.relocation_approval_sha256,
                )
            elif args.command == "finalize":
                result = finalize(
                    plan_uri=args.plan_uri,
                    plan_sha256=args.plan_sha256,
                    inference_uri=args.inference_uri,
                    inference_sha256=args.inference_sha256,
                )
            elif args.command == "urgency":
                result = urgency(
                    station_uri=args.station_uri,
                    station_sha256=args.station_sha256,
                    demand_uri=args.demand_uri,
                    demand_sha256=args.demand_sha256,
                    stock_uri=args.stock_uri,
                    stock_sha256=args.stock_sha256,
                )
            else:
                result = route(
                    urgency_uri=args.urgency_uri,
                    urgency_sha256=args.urgency_sha256,
                )
    except Exception as exc:  # noqa: BLE001 - CLI 경계에서 nonzero로 변환한다.
        print(f"Serving publication failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
