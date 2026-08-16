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


def _predictions_key(window_start: datetime) -> str:
    """ml/inference의 predict_single.py --all-stations 출력 키 규칙.

    `libs/ml_common/silver_schema.py:predictions_key()`와 동일한 형식이지만, 이
    저장소의 기존 컨벤션(db-loader는 collector/storage.py의 키 규칙도 import 없이
    자체 재구현한다)을 따라 여기서도 독립적으로 재구현한다 — ml_common은 lightgbm
    등 무거운 런타임 의존성을 끌고 오는데 얻는 건 이 한 줄뿐이라 트레이드오프가
    맞지 않는다. 두 구현이 갈라지지 않는지는 dev-only 계약 테스트
    (tests/test_s3_reader.py)가 ml_common.silver_schema.predictions_key()와 직접
    비교해 검증한다.
    """
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}.parquet"


def read_silver(source_id: str, window_start: datetime) -> pq.Table:
    """지정한 소스·윈도우의 silver parquet을 읽어 pyarrow Table로 반환한다."""
    key = _silver_key(source_id, window_start)
    body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def read_predictions(window_start: datetime) -> pq.Table:
    """지정한 윈도우의 ml/inference 추론 결과 parquet을 읽어 pyarrow Table로 반환한다."""
    key = _predictions_key(window_start)
    body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))
