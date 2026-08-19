"""S3 / MinIO 제네릭 입출력 모듈.

Pandas DataFrame, JSON(dict/list), PyArrow Parquet 등의 데이터를 S3로 읽고 씁니다.
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
    date_range: tuple[str, str] | None = None,
) -> pd.DataFrame | pq.Table | None:
    """S3의 parquet을 pandas DataFrame 또는 pyarrow Table로 읽는다 — 파일 1개짜리 객체와 다중 파트 "디렉터리" 둘 다 지원한다.

    args:
        date_range: (start, end) "YYYY-MM-DD" 문자열(둘 다 포함) — 지정하면 `key`가
            Spark `partitionBy("date")`로 쓰인 `key/date=YYYY-MM-DD/part-*.parquet`
            레이아웃이라고 보고 `_read_parquet_by_date_range()`로 위임한다. prefix
            전체를 나열하는 대신 이 범위의 date= 서브prefix만 나열/다운로드해서,
            쌓인 전체 히스토리가 아니라 실제로 필요한 기간 크기에만 비용이 비례하게
            한다(ml/training이 매달 전체를 다시 받는 문제 대응). None이면(기본)
            prefix 전체를 읽는다 — 파티션 없는 데이터셋은 계속 이 경로를 쓴다.
    """
    if date_range is not None:
        return _read_parquet_by_date_range(key, date_range, columns=columns, as_pandas=as_pandas)

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


def _read_parquet_by_date_range(
    key: str,
    date_range: tuple[str, str],
    columns: list[str] | None,
    as_pandas: bool,
) -> pd.DataFrame | pq.Table | None:
    """`date=` Hive 파티션 레이아웃에서 지정한 날짜 범위의 서브prefix만 나열/다운로드한다.

    Spark는 파티션 컬럼 값을 파일 내용에 넣지 않고 디렉터리명(`date=YYYY-MM-DD/`)
    으로만 표현한다(Hive 컨벤션) — 그래서 파일에서 읽은 결과엔 "date" 컬럼이 아예
    없다. 여기서는 그 값을 파일 내용에서 역추출할 필요 없이, 애초에 그 값으로 직접
    만든 서브prefix를 순회하는 것이므로 반복 중인 날짜 문자열을 그대로 붙인다.

    args:
        key: Spark `partitionBy("date")` 출력의 prefix(파티션 폴더들의 부모)
        date_range: (start, end) "YYYY-MM-DD" 문자열, 둘 다 포함
        columns: 읽을 컬럼(None이면 전체) — "date"가 포함돼도/안 돼도 결과에는
            항상 복원된 "date"가 있고, 지정 시 그 컬럼 순서를 그대로 맞춰 반환한다
        as_pandas: True면 DataFrame, False면 pyarrow Table 반환("date" 복원은
            내부적으로 항상 pandas로 하고 필요할 때만 마지막에 변환)
    returns:
        범위 안에 데이터가 하나도 없으면 None
    """
    start, end = date_range
    prefix = key if key.endswith("/") else f"{key}/"
    file_columns = [c for c in columns if c != "date"] if columns is not None else None

    frames = []
    for day in pd.date_range(start, end, freq="D"):
        date_str = day.strftime("%Y-%m-%d")
        part_keys = sorted(k for k in list_keys(f"{prefix}date={date_str}/") if k.endswith(".parquet"))
        if not part_keys:
            continue
        for df in read_parquet_many(part_keys, columns=file_columns, as_pandas=True):
            if df is None:
                continue
            df = df.copy()
            df["date"] = date_str
            frames.append(df)

    if not frames:
        return None
    result = pd.concat(frames, ignore_index=True)
    if columns is not None:
        result = result[columns]
    if as_pandas:
        return result
    import pyarrow as pa
    return pa.Table.from_pandas(result, preserve_index=False)

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
