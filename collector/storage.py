"""S3/MinIO 입출력과 경로 규칙 생성.

S3와 실제로 이야기하는 유일한 창구다. 경로 문자열을 만드는 곳도 여기 하나뿐이다.
dict·bytes 단위로만 주고받고, 그 값이 무엇을 뜻하는지는 해석하지 않는다 — 해석(예:
Manifest·RetryMarker로의 변환)은 manifest.py의 몫이다.

설계 근거: docs/superpowers/specs/2026-08-13-collector-storage-manifest-design.md
"""

from __future__ import annotations

import gzip
import os
from datetime import datetime
from typing import Sequence

import boto3


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
