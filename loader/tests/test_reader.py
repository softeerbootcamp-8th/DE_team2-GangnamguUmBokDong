import io
from datetime import UTC, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from reader import (
    read_predictions,
    read_route_stops,
    read_routes,
    read_silver,
    read_urgency,
)
from tests.conftest import TEST_BUCKET


def test_read_silver_round_trips_parquet():
    table = pa.table({"stationId": ["101", "102"], "stationName": ["강남역", "역삼역"]})
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    window_start = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    key = "silver/bike_station_realtime/dt=2026-08-16/hh=00/0005.parquet"
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue()
    )

    result = read_silver("bike_station_realtime", window_start)

    assert result.to_pydict() == table.to_pydict()


def test_read_predictions_round_trips_parquet():
    table = pa.table({"station_id": ["101"], "rental_pred_mean": [3.6]})
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    key = "predictions/dt=2026-08-16/hh=14/inference_1405.parquet"
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue()
    )

    result = read_predictions(window_start)

    assert result.to_pydict() == table.to_pydict()


def test_read_urgency_round_trips_parquet():
    table = pa.table({"sta_id": ["101"], "urgency_score": [48.7]})
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    key = "urgency/dt=2026-08-16/hh=14/urgency_1405.parquet"
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue()
    )

    result = read_urgency(window_start)

    assert result.to_pydict() == table.to_pydict()


def test_read_routes_round_trips_parquet():
    table = pa.table({"route_id": ["r1"], "region": ["세종로"]})
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    key = "routes/dt=2026-08-16/hh=14/routes_1405.parquet"
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue()
    )

    result = read_routes(window_start)

    assert result.to_pydict() == table.to_pydict()


def test_read_route_stops_round_trips_parquet():
    table = pa.table({"route_id": ["r1"], "sta_id": ["101"]})
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    key = "route_stops/dt=2026-08-16/hh=14/route_stops_1405.parquet"
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue()
    )

    result = read_route_stops(window_start)

    assert result.to_pydict() == table.to_pydict()
