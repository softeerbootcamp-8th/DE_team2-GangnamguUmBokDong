"""S3/MinIO 입출력과 경로 규칙 생성.

S3와 실제로 이야기하는 유일한 창구다. 경로 문자열을 만드는 곳도 여기 하나뿐이다.
dict·bytes 단위로만 주고받고, 그 값이 무엇을 뜻하는지는 해석하지 않는다 — 해석(예:
Manifest·RetryMarker로의 변환)은 manifest.py의 몫이다.

설계 근거: docs/superpowers/specs/2026-08-13-collector-storage-manifest-design.md
"""

from __future__ import annotations

import gzip
import io
import json
import os
from datetime import datetime
from typing import Sequence

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _bucket() -> str:
    return os.environ["S3_BUCKET"]


def _bronze_prefix(source_id: str, window_start: datetime) -> str:
    return (
        f"bronze/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}/"
    )


def _bronze_part_key(source_id: str, window_start: datetime, chunk_key: str) -> str:
    return f"{_bronze_prefix(source_id, window_start)}part={chunk_key}.json.gz"


def write_bronze_part(
    source_id: str, window_start: datetime, chunk_key: str, chunk: bytes
) -> None:
    key = _bronze_part_key(source_id, window_start, chunk_key)
    _client().put_object(Bucket=_bucket(), Key=key, Body=gzip.compress(chunk))


def read_bronze(source_id: str, window_start: datetime, parts: Sequence[str]) -> list[bytes]:
    client = _client()
    result = []
    for chunk_key in parts:
        key = _bronze_part_key(source_id, window_start, chunk_key)
        body = client.get_object(Bucket=_bucket(), Key=key)["Body"].read()
        result.append(gzip.decompress(body))
    return result


def clear_bronze(source_id: str, window_start: datetime) -> None:
    client = _client()
    bucket = _bucket()
    prefix = _bronze_prefix(source_id, window_start)

    paginator = client.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    if keys:
        client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
        )


def _layer_key(layer: str, source_id: str, window_start: datetime, ext: str) -> str:
    return (
        f"{layer}/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.{ext}"
    )


def write_silver(source_id: str, window_start: datetime, table) -> str:
    key = _layer_key("silver", source_id, window_start, "parquet")
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _client().put_object(Bucket=_bucket(), Key=key, Body=buffer.getvalue())
    return key


def write_quarantine(source_id: str, window_start: datetime, rows: list[dict]) -> str | None:
    if not rows:
        return None
    key = _layer_key("quarantine", source_id, window_start, "jsonl")
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    _client().put_object(Bucket=_bucket(), Key=key, Body=body.encode("utf-8"))
    return key


def _manifest_key(source_id: str, window_start: datetime) -> str:
    return (
        f"_manifest/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.json"
    )


def _retry_marker_key(source_id: str, window_start: datetime) -> str:
    return f"_retry_queue/{source_id}/{window_start.isoformat()}.json"


def _get_json(key: str) -> dict | None:
    try:
        body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return json.loads(body)


def write_manifest(source_id: str, window_start: datetime, data: dict) -> None:
    key = _manifest_key(source_id, window_start)
    _client().put_object(Bucket=_bucket(), Key=key, Body=json.dumps(data).encode("utf-8"))


def read_manifest(source_id: str, window_start: datetime) -> dict | None:
    return _get_json(_manifest_key(source_id, window_start))


def write_retry_marker(source_id: str, window_start: datetime, data: dict) -> None:
    key = _retry_marker_key(source_id, window_start)
    _client().put_object(Bucket=_bucket(), Key=key, Body=json.dumps(data).encode("utf-8"))


def list_retry_markers(source_id: str) -> list[dict]:
    client = _client()
    bucket = _bucket()
    prefix = f"_retry_queue/{source_id}/"

    paginator = client.get_paginator("list_objects_v2")
    markers = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            markers.append(json.loads(body))
    return markers


def delete_retry_marker(source_id: str, window_start: datetime) -> None:
    key = _retry_marker_key(source_id, window_start)
    _client().delete_object(Bucket=_bucket(), Key=key)
