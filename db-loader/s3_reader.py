"""S3(MinIO)에서 Silver parquet을 읽는다.

collector/storage.py가 쓰는 키 규칙(`silver/{source_id}/dt=.../hh=.../{HHMM}.parquet`)과
클라이언트 생성 방식을 그대로 따른다. collector는 silver를 쓰기만 하고 읽지 않으므로,
읽기는 이 모듈이 처음 구현한다.
"""

from __future__ import annotations

import io
import os
from datetime import datetime

import boto3
import pyarrow.parquet as pq


def _client():
    """S3 호환 클라이언트를 생성한다."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _bucket() -> str:
    return os.environ["S3_BUCKET"]


def _silver_key(source_id: str, window_start: datetime) -> str:
    return (
        f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.parquet"
    )


def read_silver(source_id: str, window_start: datetime) -> pq.Table:
    """지정한 소스·윈도우의 silver parquet을 읽어 pyarrow Table로 반환한다."""
    key = _silver_key(source_id, window_start)
    body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))
