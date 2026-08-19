"""reader.py의 read_urgency_result: compute_urgency가 써둔 결과를 그대로
읽어오는지, 파일이 없을 때 fail-fast하는지 검증한다."""

import io
from datetime import UTC, datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from reader import read_urgency_result
from tests.conftest import TEST_BUCKET


def test_read_urgency_result_round_trips_parquet():
    table = pa.table({"sta_id": ["101"], "urgency_score": [48.7], "bike_qty": [3]})
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    anchor = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    key = "urgency/dt=2026-08-16/hh=14/urgency_1405.parquet"
    boto3.client("s3", region_name="us-east-1").put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())

    result = read_urgency_result(anchor)

    assert isinstance(result, pd.DataFrame)
    assert result.to_dict("records") == [{"sta_id": "101", "urgency_score": 48.7, "bike_qty": 3}]


def test_read_urgency_result_requires_the_file():
    """compute_routes는 compute_urgency의 직접 downstream이므로, 그 결과 파일이
    없으면 trend-only 같은 정상 케이스가 아니라 upstream 산출물 계약 위반이다."""
    anchor = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)

    with pytest.raises(FileNotFoundError, match="urgency result parquet not found"):
        read_urgency_result(anchor)
