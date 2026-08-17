"""S3에서 실버 계층 및 ML 추론 결과 Parquet 데이터를 읽어온다."""

from __future__ import annotations

import io
from datetime import datetime

# pyrefly: ignore [missing-import]
import pyarrow.parquet as pq

# pyrefly: ignore [missing-import]
from core.s3 import get_object_bytes


def _silver_key(source_id: str, window_start: datetime) -> str:
    """지정된 소스와 윈도우 시각에 대응하는 Silver Parquet S3 키를 생성한다."""
    return (
        f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.parquet"
    )


def _predictions_key(window_start: datetime) -> str:
    """지정된 윈도우 시각에 대응하는 ML 추론 결과 Parquet S3 키를 생성한다."""
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}.parquet"


def read_silver(source_id: str, window_start: datetime) -> pq.Table:
    """지정한 소스 및 윈도우 시각의 Silver Parquet 파일을 읽어 PyArrow Table로 반환한다.

    args:
        source_id: Silver 데이터 소스 식별자
        window_start: 수집 윈도우 시작 시각 (KST)
    returns:
        읽어온 PyArrow Table
    raises:
        FileNotFoundError: 해당 S3 객체가 없을 때
    """
    key = _silver_key(source_id, window_start)
    body = get_object_bytes(key)
    if body is None:
        raise FileNotFoundError(f"S3 object not found: {key}")
    return pq.read_table(io.BytesIO(body))


def read_predictions(window_start: datetime) -> pq.Table:
    """지정한 윈도우 시각의 ML 추론 결과 Parquet 파일을 읽어 PyArrow Table로 반환한다.

    args:
        window_start: 추론 윈도우 시작 시각 (KST)
    returns:
        읽어온 PyArrow Table
    raises:
        FileNotFoundError: 해당 S3 객체가 없을 때
    """
    key = _predictions_key(window_start)
    body = get_object_bytes(key)
    if body is None:
        raise FileNotFoundError(f"S3 object not found: {key}")
    return pq.read_table(io.BytesIO(body))
