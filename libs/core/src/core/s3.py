"""S3/MinIO 제네릭 입출력 모듈.

pandas DataFrame, dict(JSON), pyarrow.parquet 등의 데이터를 S3로 읽고 씁니다.
"""

from __future__ import annotations

import io
import json
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd
import pyarrow as pa
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


def read_parquet(
    key: str,
    columns: list[str] | None = None,
    as_pandas: bool = True,
    date_range: tuple[str, str] | None = None,
    dates: list[str] | None = None,
    filters: list[tuple] | None = None,
    on_complete: Callable[[int, int], None] | None = None,
) -> pd.DataFrame | pq.Table | None:
    """S3의 parquet을 pandas DataFrame 또는 pyarrow Table로 읽는다 — 파일 1개짜리 객체와 다중 파트 "디렉터리" 둘 다 지원한다.

    args:
        date_range: (start, end) "YYYY-MM-DD" 문자열(둘 다 포함) — 지정하면 `key`가
            Spark `partitionBy("date")`로 쓰인 `key/date=YYYY-MM-DD/part-*.parquet`
            레이아웃이라고 보고 `_read_parquet_by_dates()`로 위임한다. prefix
            전체를 나열하는 대신 이 범위의 date= 서브prefix만 나열/다운로드해서,
            쌓인 전체 히스토리가 아니라 실제로 필요한 기간 크기에만 비용이 비례하게
            한다(ml/training이 매달 전체를 다시 받는 문제 대응). None이면(기본)
            prefix 전체를 읽는다 — 파티션 없는 데이터셋은 계속 이 경로를 쓴다.
        dates: date_range(연속 구간) 대신 특정 날짜만 골라서 읽고 싶을 때 쓴다 —
            예: 짝수날만/특정 요일만처럼 불연속 날짜 목록. "YYYY-MM-DD" 문자열
            리스트, date_range와 동시에 줄 수 없다(training._split()의 짝/홀수
            day-of-month 표본처럼, 애초에 필요한 파티션만 골라 읽어서 로드 자체의
            메모리를 줄이는 용도 — `_split()`이 다운스트림에서 걸러도 이미 전체를
            읽어버린 뒤라 소용없다).
        filters: `date`/`dates`는 파티션(폴더) 단위 필터라 그걸로 못 거르는, 파일
            *내용* 안의 일반 컬럼 값 필터(예: `[("horizon", "<=", 6)]`) — pyarrow의
            `pq.read_table(..., filters=...)` 그대로 각 조각 파일에 적용된다(row-group
            통계로 스킵 가능하면 스킵, 아니면 읽은 뒤 걸러서 최종 결과에만 안 남김).
            여러 조건은 AND로 묶인다(`[("a", "<=", 1), ("b", ">", 2)]`). training의
            multi-horizon 테이블처럼 같은 날짜 파티션 안에 여러 horizon이 섞여 있어
            날짜만으로는 못 줄일 때 쓴다.
        on_complete: 여러 파일 중 하나를 다 읽을 때마다 `(완료 개수, 전체 개수)`로
            호출된다(`date_range`/`dates`/prefix 다중 파트처럼 파일이 여러 개일
            때만 의미가 있음 — 단일 객체 GET 한 번으로 끝나는 경로에서는 안 불림).
            대용량 학습 테이블 로드처럼 오래 걸리는 호출의 진행 상황을 로깅하고
            싶을 때만 넘기면 된다.
    """
    if date_range is not None and dates is not None:
        raise ValueError("date_range와 dates는 동시에 지정할 수 없습니다.")
    if date_range is not None:
        date_strs = [day.strftime("%Y-%m-%d") for day in pd.date_range(date_range[0], date_range[1], freq="D")]
        return _read_parquet_by_dates(
            key, date_strs, columns=columns, as_pandas=as_pandas, filters=filters, on_complete=on_complete
        )
    if dates is not None:
        return _read_parquet_by_dates(
            key, dates, columns=columns, as_pandas=as_pandas, filters=filters, on_complete=on_complete
        )

    body = get_object_bytes(key)
    if body is not None:
        table = pq.read_table(io.BytesIO(body), columns=columns, filters=filters)
        return table.to_pandas() if as_pandas else table

    prefix = key if key.endswith("/") else f"{key}/"
    part_keys = sorted(k for k in list_keys(prefix) if k.endswith(".parquet"))
    if not part_keys:
        return None

    # 조각마다 pandas DataFrame으로 각각 변환해 리스트에 쌓아뒀다가 pd.concat()하면,
    # 그 순간 "조각 전부 + 새로 만든 합본"이 동시에 메모리에 떠 있는다(최종 크기의
    # 최대 2배). 항상 Arrow Table로만 모아서(as_pandas=False) Arrow 레벨에서 먼저
    # 합치고(zero-copy — ChunkedArray가 기존 버퍼를 복사 없이 이어붙임), pandas
    # 변환은 최종 결과 하나에 대해 딱 한 번만 한다. 조각 목록은 합치자마자 바로
    # 참조를 끊어서(del) 가비지 컬렉션이 즉시 회수할 수 있게 한다.
    tables = [
        t
        for t in read_parquet_many(part_keys, columns=columns, as_pandas=False, filters=filters, on_complete=on_complete)
        if t is not None
    ]
    if not tables:
        return None
    combined = pa.concat_tables(tables)
    del tables

    return combined.to_pandas() if as_pandas else combined


def _read_parquet_by_dates(
    key: str,
    date_strs: list[str],
    columns: list[str] | None,
    as_pandas: bool,
    filters: list[tuple] | None = None,
    on_complete: Callable[[int, int], None] | None = None,
) -> pd.DataFrame | pq.Table | None:
    """`date=` Hive 파티션 레이아웃에서 지정한 날짜들의 서브prefix만 나열/다운로드한다.

    연속 구간(`date_range`)이든 불연속 목록(`dates`, 예: 짝수날만)이든 호출부
    (`read_parquet()`)가 이미 정확한 날짜 문자열 리스트로 펼쳐서 넘겨준다 — 여기는
    그 목록을 그대로 순회할 뿐 범위/불연속 여부를 신경 쓰지 않는다.

    Spark는 파티션 컬럼 값을 파일 내용에 넣지 않고 디렉터리명(`date=YYYY-MM-DD/`)
    으로만 표현한다(Hive 컨벤션) — 그래서 파일에서 읽은 결과엔 "date" 컬럼이 아예
    없다. 여기서는 그 값을 파일 내용에서 역추출할 필요 없이, 애초에 그 값으로 직접
    만든 서브prefix를 순회하는 것이므로 반복 중인 날짜 문자열을 그대로 붙인다.

    args:
        key: Spark `partitionBy("date")` 출력의 prefix(파티션 폴더들의 부모)
        date_strs: 읽을 날짜("YYYY-MM-DD") 목록 — 연속/불연속 상관없음
        columns: 읽을 컬럼(None이면 전체) — "date"가 포함돼도/안 돼도 결과에는
            항상 복원된 "date"가 있고, 지정 시 그 컬럼 순서를 그대로 맞춰 반환한다
        as_pandas: True면 DataFrame, False면 pyarrow Table 반환("date" 복원은
            내부적으로 항상 pandas로 하고 필요할 때만 마지막에 변환)
        filters: `read_parquet()` docstring 참고 — 각 날짜 파티션 파일에 그대로 적용된다
        on_complete: `read_parquet()` docstring 참고 — 날짜 전체에 걸친 파일 목록
            기준으로 진행 상황을 보고한다(날짜 경계와 무관하게 하나의 카운터)
    returns:
        지정한 날짜들에 데이터가 하나도 없으면 None
    """
    prefix = key if key.endswith("/") else f"{key}/"
    file_columns = [c for c in columns if c != "date"] if columns is not None else None

    # 날짜별 LIST부터 병렬로 끝낸다 — 순서대로 하나씩 "LIST한 뒤 그 날짜 파일들을
    # 내려받고, 그게 끝나야 다음 날짜 LIST를 시작"하는 계단식 패턴이면 학습처럼
    # 몇 달치를 한 번에 읽을 때 그 사이 대기 시간이 그대로 쌓인다 — LIST 자체는
    # 가벼운 호출이라 read_parquet_many()와 같은 동시성으로 먼저 다 끝내둔다.
    with ThreadPoolExecutor(max_workers=16) as pool:
        keys_by_date = list(
            pool.map(
                lambda date_str: sorted(k for k in list_keys(f"{prefix}date={date_str}/") if k.endswith(".parquet")),
                date_strs,
            )
        )

    # 실제 파일 다운로드는 day 단위로 나눠 부르지 않고 전체 날짜 범위를 합친
    # 키 목록 하나로 read_parquet_many()를 한 번만 호출한다 — 그래야 그 안의
    # ThreadPoolExecutor가 날짜 경계와 무관하게 전체 구간에 대해 동시성을 최대로
    # 활용한다(day마다 별도 풀을 새로 만들어 반복하면 그 사이 유휴 시간이 생김).
    part_keys: list[str] = []
    key_dates: list[str] = []
    for date_str, day_keys in zip(date_strs, keys_by_date):
        part_keys.extend(day_keys)
        key_dates.extend([date_str] * len(day_keys))

    if not part_keys:
        return None

    # read_parquet()와 같은 이유로 Arrow Table 상태로만 모은다 — "date" 컬럼도
    # pandas .copy()+할당 대신 Arrow 레벨에서 상수 컬럼으로 붙여서, 조각마다 pandas
    # DataFrame을 따로 만들지 않는다(다중 파티션 읽기라 이 함수가 가장 큰 데이터를
    # 다룰 가능성이 높음 — training의 multi-horizon 테이블 읽기).
    tables = []
    for date_str, table in zip(
        key_dates,
        read_parquet_many(part_keys, columns=file_columns, as_pandas=False, filters=filters, on_complete=on_complete),
    ):
        if table is None or table.num_rows == 0:
            continue
        date_column = pa.array([date_str] * table.num_rows)
        tables.append(table.append_column("date", date_column))

    if not tables:
        return None
    combined = pa.concat_tables(tables)
    del tables

    if columns is not None:
        combined = combined.select(columns)
    return combined.to_pandas() if as_pandas else combined


def read_parquet_many(
    keys: list[str],
    columns: list[str] | None = None,
    max_workers: int = 16,
    as_pandas: bool = True,
    filters: list[tuple] | None = None,
    on_complete: Callable[[int, int], None] | None = None,
) -> list[pd.DataFrame | pq.Table | None]:
    """여러 parquet 키를 스레드로 병렬 조회한다.

    args:
        on_complete: 키 하나를 다 읽을 때마다 `(완료 개수, 전체 개수)`로 호출된다.
            여러 워커 스레드가 동시에 끝낼 수 있어 카운터 증가는 락으로 보호한다.
            None(기본)이면 아무 것도 안 한다 — 오래 걸리는 대량 로드의 진행 상황을
            로깅하고 싶을 때만 넘기면 된다.
    """
    if not keys:
        return []

    total = len(keys)
    completed = 0
    lock = threading.Lock()

    def _read(key: str) -> pd.DataFrame | pq.Table | None:
        nonlocal completed
        result = read_parquet(key, columns=columns, as_pandas=as_pandas, filters=filters)
        if on_complete is not None:
            with lock:
                completed += 1
                current = completed
            on_complete(current, total)
        return result

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(_read, keys))


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
