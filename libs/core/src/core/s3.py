"""S3/MinIO 제네릭 입출력 모듈.

pandas DataFrame, dict(JSON), pyarrow.parquet 등의 데이터를 S3로 읽고 씁니다.
"""

from __future__ import annotations

import io
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def _client():
    """S3 호환 클라이언트를 생성한다."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


def _bucket() -> str:
    """대상 S3 버킷 이름을 환경 변수에서 읽어 반환한다."""
    return os.environ.get("S3_BUCKET", "gangnamgu")


def get_object_bytes(key: str) -> bytes | None:
    """S3 객체를 bytes로 읽는다.

    args:
        key: 읽을 객체의 전체 키
    returns:
        객체 본문 bytes, 키가 없으면 None
    raises:
        ClientError: NoSuchKey가 아닌 다른 S3 오류가 발생했을 때
    """
    try:
        return _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def put_object_bytes(key: str, body: bytes) -> None:
    """bytes를 S3 객체로 저장한다."""
    _client().put_object(Bucket=_bucket(), Key=key, Body=body)


def read_parquet(key: str, columns: list[str] | None = None, as_pandas: bool = True) -> pd.DataFrame | pq.Table | None:
    """S3의 parquet을 pandas DataFrame 또는 pyarrow Table로 읽는다 — 파일 1개짜리 객체와 다중 파트 "디렉터리" 둘 다 지원한다."""
    body = get_object_bytes(key)
    if body is not None:
        table = pq.read_table(io.BytesIO(body), columns=columns)
        return table.to_pandas() if as_pandas else table

    prefix = key if key.endswith("/") else f"{key}/"
    part_keys = sorted(k for k in list_keys(prefix) if k.endswith(".parquet"))
    if not part_keys:
        return None
    
    tables = [t for t in read_parquet_many(part_keys, columns=columns, as_pandas=as_pandas) if t is not None]
    if not tables:
        return None
        
    if as_pandas:
        return pd.concat(tables, ignore_index=True)
    else:
        import pyarrow as pa
        return pa.concat_tables(tables)


def read_parquet_many(keys: list[str], columns: list[str] | None = None, max_workers: int = 16, as_pandas: bool = True) -> list[pd.DataFrame | pq.Table | None]:
    """여러 parquet 키를 스레드로 병렬 조회한다."""
    if not keys:
        return []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(lambda key: read_parquet(key, columns=columns, as_pandas=as_pandas), keys))


def write_parquet(data: pd.DataFrame | pq.Table, key: str) -> None:
    """pandas DataFrame 또는 pyarrow Table을 parquet으로 직렬화해 S3에 저장한다."""
    buffer = io.BytesIO()
    if isinstance(data, pd.DataFrame):
        data.to_parquet(buffer, index=False)
    else:
        pq.write_table(data, buffer)
    put_object_bytes(key, buffer.getvalue())


def read_json(key: str):
    """S3의 JSON 객체를 읽는다 (dict 또는 list — JSON 최상위 값 그대로). 키가 없으면 None."""
    body = get_object_bytes(key)
    if body is None:
        return None
    return json.loads(body)


def write_json(key: str, data) -> None:
    """dict 또는 list를 JSON으로 직렬화해 S3에 저장한다."""
    put_object_bytes(key, json.dumps(data, ensure_ascii=False).encode("utf-8"))


def list_keys(prefix: str, delimiter: str = "") -> list[str]:
    """주어진 prefix 아래 모든 객체 키를 나열한다.

    args:
        prefix: 나열할 키 prefix
        delimiter: S3 폴더 구분자 (예: "/")
    returns:
        prefix로 시작하는 모든 객체 키 목록
    """
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix, Delimiter=delimiter)
        for obj in page.get("Contents", [])
    ]


@dataclass(frozen=True)
class S3Object:
    """LIST 응답이 주는 객체 메타.

    `size`·`last_modified`는 본문을 읽지 않고 변경을 감지할 때 쓴다. 같은 키를
    덮어쓴 경우 키 목록은 그대로지만 이 둘이 바뀐다.
    """

    key: str
    size: int
    last_modified: datetime


def list_objects(prefix: str, delimiter: str = "") -> list[S3Object]:
    """주어진 prefix 아래 객체를 메타와 함께 나열한다.

    `list_keys`는 키만 주므로 "내용이 바뀌었는지"를 알 수 없다. 이 함수는 LIST 응답에
    이미 들어 있는 `Size`·`LastModified`를 버리지 않고 그대로 넘긴다.

    args:
        prefix: 나열할 키 prefix
        delimiter: S3 폴더 구분자 (예: "/")
    returns:
        prefix로 시작하는 객체 메타 목록
    """
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        S3Object(key=obj["Key"], size=obj["Size"], last_modified=obj["LastModified"])
        for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix, Delimiter=delimiter)
        for obj in page.get("Contents", [])
    ]


def list_common_prefixes(prefix: str, delimiter: str = "/") -> list[str]:
    """주어진 prefix 아래의 공통 prefix(디렉터리) 목록을 반환한다."""
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        common_prefix["Prefix"]
        for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix, Delimiter=delimiter)
        for common_prefix in page.get("CommonPrefixes", [])
    ]



def object_exists(key: str) -> bool:
    """S3 객체가 존재하는지 확인한다."""
    try:
        _client().head_object(Bucket=_bucket(), Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return False
        raise


def delete_object(key: str) -> None:
    """S3 객체를 삭제한다."""
    _client().delete_object(Bucket=_bucket(), Key=key)


def delete_objects(keys: list[str]) -> None:
    """여러 S3 객체를 일괄 삭제한다."""
    if not keys:
        return
    client = _client()
    bucket = _bucket()
    # delete_objects 한 번에 최대 1000개 삭제 가능
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
