"""S3/MinIO 입출력과 경로 규칙 생성.

S3와 연결되는 창구다. 경로 문자열을 만드는 곳도 여기 하나뿐이다.
dict·bytes 단위로만 주고받고, 그 값이 무엇을 뜻하는지는 해석하지 않는다.
"""

from __future__ import annotations

import gzip
import io
import json
import os
from collections.abc import Sequence
from datetime import datetime

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def _client():
    """S3 호환 클라이언트를 생성한다."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _bucket() -> str:
    """대상 S3 버킷 이름을 환경 변수에서 읽어 반환한다."""
    return os.environ["S3_BUCKET"]


def _bronze_prefix(source_id: str, window_start: datetime) -> str:
    """bronze 조각들이 모이는 공통 prefix를 만든다."""
    return (
        f"bronze/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}/"
    )


def _bronze_part_key(source_id: str, window_start: datetime, chunk_key: str) -> str:
    """bronze 조각 하나의 전체 키를 만든다."""
    return f"{_bronze_prefix(source_id, window_start)}part={chunk_key}.json.gz"


def write_bronze_part(
    source_id: str, window_start: datetime, chunk_key: str, chunk: bytes
) -> None:
    """bronze 조각을 gzip으로 압축해 저장한다."""
    key = _bronze_part_key(source_id, window_start, chunk_key)
    _client().put_object(Bucket=_bucket(), Key=key, Body=gzip.compress(chunk))


def read_bronze(source_id: str, window_start: datetime, parts: Sequence[str]) -> list[bytes]:
    """지정된 bronze 조각들을 읽어 압축을 해제한 뒤 순서대로 반환한다."""

    client = _client()
    result = []
    for chunk_key in parts:
        key = _bronze_part_key(source_id, window_start, chunk_key)
        body = client.get_object(Bucket=_bucket(), Key=key)["Body"].read()
        result.append(gzip.decompress(body))
    return result


def clear_bronze(source_id: str, window_start: datetime) -> None:
    """해당 윈도우의 bronze 조각을 모두 삭제한다.
        수집 파이프라인이 에러로 뻗었을 때, 쓰레기 데이터가 남지 않도록 임시 조각들을 지워주는 역할."""

    client = _client()
    bucket = _bucket()
    prefix = _bronze_prefix(source_id, window_start)

    # list_objects_v2는 한 번에 최대 1000개까지만 반환하므로 paginator로 전체 조각을 모은다.
    paginator = client.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    if keys:
        # 참고: delete_objects 한 번에 삭제 가능한 키가 최대 1000개 
        client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
        )


def _layer_key(layer: str, source_id: str, window_start: datetime, ext: str) -> str:
    """silver·quarantine처럼 윈도우당 파일 하나로 떨어지는 계층의 경로를 만든다."""
    return (
        f"{layer}/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.{ext}"
    )


def write_silver(source_id: str, window_start: datetime, table: pq.Table) -> str:
    """silver 테이블을 parquet으로 직렬화해 저장하고, 저장된 키를 반환한다."""
    key = _layer_key("silver", source_id, window_start, "parquet")
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _client().put_object(Bucket=_bucket(), Key=key, Body=buffer.getvalue())
    return key


def write_quarantine(source_id: str, window_start: datetime, rows: list[dict]) -> str | None:
    """검증에 실패한 row들을 jsonl로 저장하고, 저장된 키를 반환한다."""
    if not rows:
        return None
    key = _layer_key("quarantine", source_id, window_start, "jsonl")
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    _client().put_object(Bucket=_bucket(), Key=key, Body=body.encode("utf-8"))
    return key


def _manifest_key(source_id: str, window_start: datetime) -> str:
    """해당 윈도우의 manifest 객체 키를 만든다."""
    return (
        f"_manifest/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.json"
    )


def _retry_marker_key(source_id: str, window_start: datetime) -> str:
    """해당 윈도우의 retry marker 객체 키를 만든다."""
    return f"_retry_queue/{source_id}/{window_start.isoformat()}.json"


def _get_json(key: str) -> dict | None:
    """.json 확장자로 끝나는 설정이나 이력 파일을 읽어와서 파이썬 딕셔너리로 반환해주는 내부 헬퍼 함수."""
    
    try:
        body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None       # 진짜로 파일이 없는 경우에는 조용히 None 반환
        raise
    return json.loads(body)   # 권한 오류나 통신 에러면 에러


def write_manifest(source_id: str, window_start: datetime, data: dict) -> None:
    """해당 윈도우의 manifest를 json으로 저장한다."""

    key = _manifest_key(source_id, window_start)
    _client().put_object(Bucket=_bucket(), Key=key, Body=json.dumps(data).encode("utf-8"))


def read_manifest(source_id: str, window_start: datetime) -> dict | None:
    """해당 윈도우의 manifest를 읽는다. 없으면 None을 반환한다."""

    return _get_json(_manifest_key(source_id, window_start))


def write_retry_marker(source_id: str, window_start: datetime, data: dict) -> None:
    """해당 윈도우의 retry marker를 JSON으로 저장한다."""

    key = _retry_marker_key(source_id, window_start)
    _client().put_object(Bucket=_bucket(), Key=key, Body=json.dumps(data).encode("utf-8"))


def list_retry_markers(source_id: str) -> list[dict]:
    """해당 소스에 쌓인 retry marker를 모두 읽어 반환한다."""

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
    """해당 윈도우의 retry marker를 삭제한다."""
    
    key = _retry_marker_key(source_id, window_start)
    _client().delete_object(Bucket=_bucket(), Key=key)
