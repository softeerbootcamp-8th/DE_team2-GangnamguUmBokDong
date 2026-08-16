"""S3 read/write: 아카이브(4주 lookback), collector 실측 silver 읽기, nowcast 추정 파일 쓰기/삭제.

collector/storage.py의 경로 컨벤션(`{layer}/{source_id}/dt=.../hh=.../HHMM.parquet`)을
그대로 재사용하되, collector 코드를 import하지 않고 이 파일 안에서 다시 구현한다.
"""

from __future__ import annotations

import io
import os
from datetime import date

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

GRID_SOURCE_ID = "living_population_grid"
_NOWCAST_FILENAME = "nowcast.parquet"


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _bucket() -> str:
    return os.environ["S3_BUCKET"]


def _grid_date_prefix(target_date: date) -> str:
    return f"silver/{GRID_SOURCE_ID}/dt={target_date:%Y-%m-%d}/"


def _nowcast_key(target_date: date) -> str:
    return f"{_grid_date_prefix(target_date)}hh=00/{_NOWCAST_FILENAME}"


def _archive_key(target_date: date) -> str:
    return f"archive/{GRID_SOURCE_ID}/dt={target_date:%Y-%m-%d}.parquet"


def _list_parquet_keys(prefix: str) -> list[str]:
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]


def _read_parquet(key: str) -> pa.Table:
    body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def _write_parquet(key: str, table: pa.Table) -> str:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _client().put_object(Bucket=_bucket(), Key=key, Body=buffer.getvalue())
    return key


def read_real_grid_silver(target_date: date) -> pa.Table | None:
    """해당 날짜의 collector 실측 silver를 전부 읽어 이어붙인다.

    같은 dt= 프리픽스 아래 이 모듈이 써둔 nowcast.parquet가 있으면 제외한다
    (그건 추정치이지 실측이 아니므로). 실측 파일이 하나도 없으면 None.
    """
    prefix = _grid_date_prefix(target_date)
    keys = [key for key in _list_parquet_keys(prefix) if not key.endswith(_NOWCAST_FILENAME)]
    if not keys:
        return None
    return pa.concat_tables([_read_parquet(key) for key in sorted(keys)])


def write_archive(target_date: date, table: pa.Table) -> str:
    """해당 날짜의 데이터를 아카이브 parquet으로 저장하고 저장된 key를 반환한다."""
    return _write_parquet(_archive_key(target_date), table)


def list_archive_dates() -> list[date]:
    """아카이브에 존재하는 날짜를 오름차순으로 나열한다."""
    prefix = f"archive/{GRID_SOURCE_ID}/"
    dates = []
    for key in _list_parquet_keys(prefix):
        filename = key[len(prefix):]
        dt_str = filename.removeprefix("dt=").removesuffix(".parquet")
        dates.append(date.fromisoformat(dt_str))
    return sorted(dates)


def read_archive(target_date: date) -> pa.Table | None:
    """해당 날짜의 아카이브를 읽는다. 없으면 None(4주 lookback 중 결측으로 처리)."""
    key = _archive_key(target_date)
    try:
        return _read_parquet(key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def write_nowcast(target_date: date, table: pa.Table) -> str:
    """해당 날짜의 추정치를 nowcast.parquet 고정 키에 쓴다(같은 키이므로 재실행 시 덮어써짐)."""
    return _write_parquet(_nowcast_key(target_date), table)


def nowcast_exists(target_date: date) -> bool:
    try:
        _client().head_object(Bucket=_bucket(), Key=_nowcast_key(target_date))
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return False
        raise


def delete_nowcast(target_date: date) -> None:
    """실측값이 도착한 날짜의 옛 추정 파일을 청소한다. 없어도 에러 없이 통과."""
    _client().delete_object(Bucket=_bucket(), Key=_nowcast_key(target_date))
