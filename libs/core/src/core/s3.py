"""S3 / MinIO 제네릭 입출력 모듈.

Pandas DataFrame, JSON(dict/list), PyArrow Parquet 등의 데이터를 S3로 읽고 씁니다.
"""

from __future__ import annotations

import io
import json
import os
from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def _client():
    """S3 호환 클라이언트를 생성하여 반환한다."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


def _bucket() -> str:
    """대상 S3 버킷 이름을 환경변수에서 읽어 반환한다."""
    return os.environ.get("S3_BUCKET", "gangnamgu")


def get_object_bytes(key: str) -> bytes | None:
    """S3 객체의 본문을 bytes로 읽어 반환한다.

    args:
        key: 읽을 객체의 S3 키
    returns:
        객체 본문 bytes (키가 존재하지 않으면 None)
    raises:
        ClientError: NoSuchKey 외의 S3 오류가 발생했을 때
    """
    try:
        return _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def put_object_bytes(key: str, body: bytes) -> None:
    """바이트 데이터를 지정된 S3 키의 객체로 저장한다.

    args:
        key: 저장할 대상 S3 키
        body: 저장할 데이터 바이트
    """
    _client().put_object(Bucket=_bucket(), Key=key, Body=body)


def read_parquet(
    key: str,
    columns: list[str] | None = None,
    as_pandas: bool = True,
) -> pd.DataFrame | pq.Table | None:
    """S3의 단일 Parquet 파일 또는 분할 디렉터리를 읽어 반환한다.

    args:
        key: 읽을 대상 S3 키 또는 디렉터리 prefix
        columns: 선택적으로 읽어올 컬럼 목록
        as_pandas: True이면 Pandas DataFrame, False이면 PyArrow Table 반환
    returns:
        읽어온 데이터 (존재하지 않으면 None)
    """
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


def read_parquet_many(
    keys: list[str],
    columns: list[str] | None = None,
    max_workers: int = 16,
    as_pandas: bool = True,
) -> list[pd.DataFrame | pq.Table | None]:
    """여러 S3 Parquet 객체를 스레드 풀을 활용해 병렬로 읽어온다.

    args:
        keys: 읽을 대상 S3 키 목록
        columns: 선택적으로 읽어올 컬럼 목록
        max_workers: 병렬 I/O 작업자 스레드 수
        as_pandas: True이면 Pandas DataFrame, False이면 PyArrow Table 반환
    returns:
        각 키에 대응하는 데이터 목록
    """
    if not keys:
        return []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(lambda key: read_parquet(key, columns=columns, as_pandas=as_pandas), keys))


def write_parquet(data: pd.DataFrame | pq.Table, key: str) -> None:
    """Pandas DataFrame 또는 PyArrow Table을 Parquet 포맷으로 직렬화하여 S3에 저장한다.

    args:
        data: 저장할 DataFrame 또는 PyArrow Table
        key: 저장할 대상 S3 키
    """
    buffer = io.BytesIO()
    if isinstance(data, pd.DataFrame):
        data.to_parquet(buffer, index=False)
    else:
        pq.write_table(data, buffer)
    put_object_bytes(key, buffer.getvalue())


def read_json(key: str):
    """S3에서 JSON 객체를 읽어 파싱된 dict 또는 list를 반환한다.

    args:
        key: 읽을 대상 S3 키
    returns:
        파싱된 JSON 데이터 (객체가 없으면 None)
    """
    body = get_object_bytes(key)
    if body is None:
        return None
    return json.loads(body)


def write_json(key: str, data) -> None:
    """Python 객체(dict 또는 list)를 JSON 문자열로 직렬화하여 S3에 저장한다.

    args:
        key: 저장할 대상 S3 키
        data: 직렬화할 데이터 객체
    """
    put_object_bytes(key, json.dumps(data, ensure_ascii=False).encode("utf-8"))


def list_keys(prefix: str, delimiter: str = "") -> list[str]:
    """주어진 prefix 하위의 모든 S3 객체 키 목록을 나열한다.

    args:
        prefix: 검색할 S3 키 prefix
        delimiter: 디렉터리 구분 기호 (기본값: 빈 문자열)
    returns:
        prefix에 매칭되는 S3 객체 키 문자열 목록
    """
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix, Delimiter=delimiter)
        for obj in page.get("Contents", [])
    ]


def list_common_prefixes(prefix: str, delimiter: str = "/") -> list[str]:
    """주어진 prefix 하위의 공통 서브디렉터리(CommonPrefixes) 목록을 반환한다.

    args:
        prefix: 검색할 S3 키 prefix
        delimiter: 디렉터리 구분 기호 (기본값: "/")
    returns:
        하위 디렉터리 prefix 문자열 목록
    """
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        common_prefix["Prefix"]
        for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix, Delimiter=delimiter)
        for common_prefix in page.get("CommonPrefixes", [])
    ]


def object_exists(key: str) -> bool:
    """지정된 키의 S3 객체가 존재하는지 확인한다.

    args:
        key: 확인할 S3 키
    returns:
        객체가 존재하면 True, 없으면 False
    """
    try:
        _client().head_object(Bucket=_bucket(), Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return False
        raise


def delete_object(key: str) -> None:
    """지정된 키의 단일 S3 객체를 삭제한다.

    args:
        key: 삭제할 대상 S3 키
    """
    _client().delete_object(Bucket=_bucket(), Key=key)


def delete_objects(keys: list[str]) -> None:
    """여러 S3 객체를 1000개 단위 배치로 일괄 삭제한다.

    args:
        keys: 삭제할 대상 S3 키 목록
    """
    if not keys:
        return
    client = _client()
    bucket = _bucket()
    # S3 delete_objects API는 한 번에 최대 1000개까지 지원
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
