"""S3 읽기(living_population_grid, population_realtime silver) /
쓰기(living_population_normalized silver, manifest) / baseline 파티션 탐색.

collector/storage.py의 경로 컨벤션(`{layer}/{source_id}/dt=.../hh=.../HHMM.parquet`)을
그대로 재사용하되, collector 코드를 import하지 않고 이 파일 안에서 다시 구현한다.
"""

from __future__ import annotations

import io
import json
import os
from datetime import date, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

GRID_SOURCE_ID = "living_population_grid"
REALTIME_SOURCE_ID = "population_realtime"
NORMALIZED_SOURCE_ID = "living_population_normalized"


class PartitionNotFoundError(RuntimeError):
    """요청한 silver 파티션(날짜 또는 window)이 S3에 없을 때."""


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _bucket() -> str:
    return os.environ["S3_BUCKET"]


def _silver_key(source_id: str, window_start: datetime, ext: str = "parquet") -> str:
    return (
        f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.{ext}"
    )


def _silver_date_prefix(source_id: str, baseline_date: date) -> str:
    return f"silver/{source_id}/dt={baseline_date:%Y-%m-%d}/"


def list_partition_dates(source_id: str) -> list[date]:
    """`silver/{source_id}/` 아래 존재하는 dt= 파티션 날짜를 오름차순으로 나열한다."""
    client = _client()
    bucket = _bucket()
    prefix = f"silver/{source_id}/"

    paginator = client.get_paginator("list_objects_v2")
    dates: list[date] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            dt_segment = common_prefix["Prefix"][len(prefix):].rstrip("/")
            if dt_segment.startswith("dt="):
                dates.append(datetime.strptime(dt_segment[len("dt="):], "%Y-%m-%d").date())
    return sorted(dates)


def partition_exists(source_id: str, baseline_date: date) -> bool:
    """해당 날짜의 dt= 파티션이 존재하는지 확인한다."""
    return baseline_date in list_partition_dates(source_id)


def find_latest_partition_date(source_id: str) -> date:
    """존재하는 dt= 파티션 중 가장 최신 날짜를 반환한다(`latest` baseline 모드용)."""
    dates = list_partition_dates(source_id)
    if not dates:
        raise PartitionNotFoundError(f"{source_id}에 존재하는 dt= 파티션이 없음")
    return dates[-1]


def read_grid_silver(baseline_date: date) -> pa.Table:
    """해당 baseline 날짜의 living_population_grid silver 조각을 전부 읽어 이어붙인다.

    daily 배치가 실제로 어느 hh/HHMM 시각에 기록되는지가 이번 조사에서 확정되지
    않았으므로(Task 8 참고), 특정 키 하나를 가정하지 않고 dt= prefix 아래의 모든
    .parquet을 찾아 concat한다. 정상 운영에서는 하루 1개 파일만 있을 것으로
    예상하지만, 이 구현은 여러 개가 있어도 안전하게 동작한다.
    """
    client = _client()
    bucket = _bucket()
    prefix = _silver_date_prefix(GRID_SOURCE_ID, baseline_date)

    paginator = client.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    if not keys:
        raise PartitionNotFoundError(
            f"{GRID_SOURCE_ID}의 dt={baseline_date:%Y-%m-%d} 파티션이 없음"
        )

    tables = []
    for key in sorted(keys):
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    return pa.concat_tables(tables)


def read_realtime_silver(window_start: datetime) -> pa.Table:
    """해당 window의 population_realtime silver 파일 하나를 읽는다. 없으면 예외."""
    key = _silver_key(REALTIME_SOURCE_ID, window_start)
    client = _client()
    try:
        body = client.get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            raise PartitionNotFoundError(f"{REALTIME_SOURCE_ID} silver 파일 없음: {key}") from exc
        raise
    return pq.read_table(io.BytesIO(body))


def write_normalized_silver(window_start: datetime, table: pa.Table) -> str:
    """living_population_normalized silver를 parquet으로 저장하고 저장된 key를 반환한다."""
    key = _silver_key(NORMALIZED_SOURCE_ID, window_start)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _client().put_object(Bucket=_bucket(), Key=key, Body=buffer.getvalue())
    return key


def _manifest_key(window_start: datetime) -> str:
    return (
        f"_manifest/{NORMALIZED_SOURCE_ID}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.json"
    )


def write_manifest(window_start: datetime, data: dict) -> str:
    """해당 window의 manifest(baseline_date, baseline_date_mode 등)를 json으로 저장한다."""
    key = _manifest_key(window_start)
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    _client().put_object(Bucket=_bucket(), Key=key, Body=body)
    return key
