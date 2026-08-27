"""Gold seed publisher의 clean PostGIS reconcile·version·원자성을 검증한다."""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import psycopg
import pytest
from core.gold_publication import PublicationOutcome, S3ImmutableObjectStore
from core.gold_publication.errors import (
    PublicationDependencyError,
    PublicationTimeError,
)
from psycopg import Connection

from gold.dispatch_center import (
    load_dispatch_center_seed,
    parse_dispatch_center_seed,
    publish_dispatch_center,
)
from gold.station import (
    DispatchCenterReference,
    assign_dispatch_center_id,
)
from gold.weather_grid import (
    WEATHER_SOURCE_PATHS,
    build_weather_grid_seed,
    publish_weather_grid,
)

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적인 disposable gold151_* DB를 seed 통합 테스트마다 비운다."""
    if _DATABASE_URL is None:
        pytest.skip(
            "GOLD_PUBLICATION_TEST_DATABASE_URL이 없어 PostGIS 통합 테스트를 건너뜁니다."
        )
    connection = psycopg.connect(_DATABASE_URL)
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        row = cursor.fetchone()
    connection.rollback()
    if row is None or not row[0].startswith("gold151_"):
        connection.close()
        pytest.fail(
            "seed 통합 테스트는 이름이 gold151_로 시작하는 disposable DB만 허용합니다."
        )
    _reset_database(connection)
    try:
        yield connection
    finally:
        _reset_database(connection)
        connection.close()


def test_seed_publishers_replay_correction_stale_and_reference_rollback(
    gold_connection: Connection[Any],
) -> None:
    """두 seed의 clean publish와 replay·correction·stale·참조 차단을 실제 DB에서 검증한다."""
    object_store = S3ImmutableObjectStore(boto3.client("s3", region_name="us-east-1"))
    logical_dttm = datetime.now(UTC) - timedelta(minutes=10)
    source_payloads = _weather_source_payloads()
    future_seed = build_weather_grid_seed(
        source_payloads,
        seed_version="weather-grid-v1",
        effective_dttm=datetime.now(UTC) + timedelta(minutes=10),
    )
    with pytest.raises(PublicationTimeError, match="5분"):
        publish_weather_grid(
            gold_connection,
            object_store,
            seed=future_seed,
            object_base_uri="s3://test-bucket/gold-publication",
        )
    assert _table_count(gold_connection, "weather_grid") == 0
    assert not _state_exists(gold_connection, "weather_grid")

    weather_seed = build_weather_grid_seed(
        source_payloads,
        seed_version="weather-grid-v1",
        effective_dttm=logical_dttm,
    )
    dispatch_seed = load_dispatch_center_seed(_REPOSITORY_ROOT)

    weather_first = publish_weather_grid(
        gold_connection,
        object_store,
        seed=weather_seed,
        object_base_uri="s3://test-bucket/gold-publication",
    )
    _insert_coordinate_transition_fixture(gold_connection)
    dispatch_first = publish_dispatch_center(
        gold_connection,
        object_store,
        seed=dispatch_seed,
        object_base_uri="s3://test-bucket/gold-publication",
    )
    assert weather_first.result.outcome is PublicationOutcome.PUBLISHED
    assert dispatch_first.result.outcome is PublicationOutcome.PUBLISHED
    assert _table_count(gold_connection, "weather_grid") == 34
    assert _table_count(gold_connection, "dispatch_center") == 11
    assert _row_value(
        gold_connection,
        "SELECT dispatch_center_id FROM station WHERE sta_id = 'ST-888888'",
    ) == "isu"
    transition_center = _row_value(
        gold_connection,
        "SELECT dispatch_center_id FROM station WHERE sta_id = 'ST-1141'",
    )
    station_projection_center = _station_projection_center(
        gold_connection,
        dispatch_seed.rows,
        "ST-1141",
    )
    assert transition_center == station_projection_center == "yeongnam"
    assert _row_value(
        gold_connection,
        "SELECT count(*) FROM rebalance_route WHERE route_status_cd = 'proposed'",
    ) == 0
    assert _row_value(
        gold_connection,
        "SELECT count(*) FROM rebalance_route "
        "WHERE route_id = '77777777-7777-4777-8777-777777777777' "
        "AND route_status_cd = 'dispatched'",
    ) == 1

    dispatch_replay = publish_dispatch_center(
        gold_connection,
        object_store,
        seed=dispatch_seed,
        object_base_uri="s3://test-bucket/gold-publication",
    )
    assert dispatch_replay.result.outcome is PublicationOutcome.EXACT_REPLAY

    weather_replay = publish_weather_grid(
        gold_connection,
        object_store,
        seed=weather_seed,
        object_base_uri="s3://test-bucket/gold-publication",
    )
    assert weather_replay.result.outcome is PublicationOutcome.EXACT_REPLAY

    corrected_payloads = dict(source_payloads)
    corrected_payloads[WEATHER_SOURCE_PATHS[0]] += (
        b"\n# immutable correction evidence\n"
    )
    correction_seed = build_weather_grid_seed(
        corrected_payloads,
        seed_version="weather-grid-v1",
        effective_dttm=logical_dttm,
    )
    weather_correction = publish_weather_grid(
        gold_connection,
        object_store,
        seed=correction_seed,
        object_base_uri="s3://test-bucket/gold-publication",
    )
    assert weather_correction.result.outcome is PublicationOutcome.PUBLISHED
    assert _state_revision(gold_connection, "weather_grid") == 1

    stale_seed = build_weather_grid_seed(
        source_payloads,
        seed_version="weather-grid-v1",
        effective_dttm=logical_dttm - timedelta(minutes=1),
    )
    weather_stale = publish_weather_grid(
        gold_connection,
        object_store,
        seed=stale_seed,
        object_base_uri="s3://test-bucket/gold-publication",
    )
    assert weather_stale.result.outcome is PublicationOutcome.STALE
    assert _state_revision(gold_connection, "weather_grid") == 1

    _insert_referenced_extra_topology(gold_connection)
    blocked_payloads = dict(corrected_payloads)
    blocked_payloads[WEATHER_SOURCE_PATHS[1]] += b"\n# second correction evidence\n"
    blocked_weather_seed = build_weather_grid_seed(
        blocked_payloads,
        seed_version="weather-grid-v1",
        effective_dttm=logical_dttm,
    )
    with pytest.raises(PublicationDependencyError, match="참조 중인 weather grid"):
        publish_weather_grid(
            gold_connection,
            object_store,
            seed=blocked_weather_seed,
            object_base_uri="s3://test-bucket/gold-publication",
        )
    assert _state_revision(gold_connection, "weather_grid") == 1
    assert _row_exists(gold_connection, "weather_grid", "weather_grid_id", "99_99")

    dispatch_correction = parse_dispatch_center_seed(
        dispatch_seed.yaml_bytes + b"\n# immutable correction evidence\n"
    )
    with pytest.raises(PublicationDependencyError, match="dispatch center"):
        publish_dispatch_center(
            gold_connection,
            object_store,
            seed=dispatch_correction,
            object_base_uri="s3://test-bucket/gold-publication",
        )
    assert _state_revision(gold_connection, "dispatch_center") == 0
    assert _row_exists(
        gold_connection,
        "dispatch_center",
        "dispatch_center_id",
        "extra_center",
    )


def _weather_source_payloads() -> dict[str, bytes]:
    """repository의 두 예보 YAML bytes를 반환한다."""
    return {
        path: (_REPOSITORY_ROOT / path).read_bytes() for path in WEATHER_SOURCE_PATHS
    }


def _insert_coordinate_transition_fixture(connection: Connection[Any]) -> None:
    """구 좌표 센터와 이를 참조하는 station·proposed route를 삽입한다."""
    observed = datetime.now(UTC) - timedelta(minutes=1)
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO dispatch_center "
            "(dispatch_center_id, dispatch_center_nm, dispatch_center_point, "
            "location_accuracy_cd, location_source_desc, location_verified_dt, is_active) "
            "VALUES ('sangam', '상암', ST_SetSRID(ST_MakePoint(126.8972, 37.5683), "
            "4326), 'landmark_approximation', '좌표 전환 테스트용 구 Point', NULL, true)"
        )
        cursor.execute(
            "SELECT weather_grid_id FROM weather_grid ORDER BY weather_grid_id LIMIT 1"
        )
        weather_grid_row = cursor.fetchone()
        assert weather_grid_row is not None
        cursor.execute(
            "INSERT INTO station "
            "(sta_id, sta_nm, sta_addr, hold_cnt, sta_point, sta_point_source_cd, "
            "weather_grid_id, dispatch_center_id, master_base_dttm, last_seen_dttm, "
            "is_active) VALUES ('ST-888888', '좌표 전환 테스트 대여소', "
            "'서울시 테스트 주소', 10, ST_SetSRID(ST_MakePoint(126.982400620133, "
            "37.4837582703213), 4326), 'bike_station_master', %s, 'sangam', %s, %s, true)",
            (weather_grid_row[0], observed, observed),
        )
        cursor.execute(
            "INSERT INTO station "
            "(sta_id, sta_nm, sta_addr, hold_cnt, sta_point, sta_point_source_cd, "
            "weather_grid_id, dispatch_center_id, master_base_dttm, last_seen_dttm, "
            "is_active) VALUES ('ST-1141', '서울 외곽 제약거리 전환 대여소', "
            "'서울시 테스트 주소', 10, ST_SetSRID(ST_MakePoint(126.888458, "
            "37.475552), 4326), 'bike_station_master', %s, 'sangam', %s, %s, true)",
            (weather_grid_row[0], observed, observed),
        )
        cursor.execute(
            "INSERT INTO rebalance_route "
            "(route_id, dispatch_center_id, route_status_cd, proposed_dttm) "
            "VALUES ('88888888-8888-4888-8888-888888888888', 'sangam', 'proposed', %s)",
            (observed,),
        )
        cursor.execute(
            "INSERT INTO rebalance_route_stop "
            "(route_id, visit_no, sta_id, route_action_type_cd, bike_cnt) "
            "VALUES ('88888888-8888-4888-8888-888888888888', 1, 'ST-888888', "
            "'pickup', 1)"
        )
        cursor.execute(
            "INSERT INTO rebalance_route "
            "(route_id, dispatch_center_id, route_status_cd, proposed_dttm) "
            "VALUES ('77777777-7777-4777-8777-777777777777', 'sangam', "
            "'proposed', %s)",
            (observed - timedelta(minutes=1),),
        )
        cursor.execute(
            "INSERT INTO rebalance_route_stop "
            "(route_id, visit_no, sta_id, route_action_type_cd, bike_cnt) "
            "VALUES ('77777777-7777-4777-8777-777777777777', 1, 'ST-1141', "
            "'pickup', 1)"
        )
        cursor.execute(
            "UPDATE rebalance_route "
            "SET route_status_cd = 'dispatched', dispatched_dttm = %s "
            "WHERE route_id = '77777777-7777-4777-8777-777777777777'",
            (observed,),
        )


def _insert_referenced_extra_topology(connection: Connection[Any]) -> None:
    """candidate seed 밖 grid와 center를 active station이 참조하게 만든다."""
    observed = datetime.now(UTC) - timedelta(minutes=1)
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO weather_grid (
                weather_grid_id,
                weather_grid_x_no,
                weather_grid_y_no
            ) VALUES ('99_99', 99, 99)
            """
        )
        cursor.execute(
            """
            INSERT INTO dispatch_center (
                dispatch_center_id,
                dispatch_center_nm,
                dispatch_center_point,
                location_accuracy_cd,
                location_source_desc,
                location_verified_dt,
                is_active
            ) VALUES (
                'extra_center',
                '추가센터',
                ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326),
                'landmark_approximation',
                '통합 테스트 전용 center',
                NULL,
                true
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO station (
                sta_id,
                sta_nm,
                sta_addr,
                hold_cnt,
                sta_point,
                sta_point_source_cd,
                weather_grid_id,
                dispatch_center_id,
                master_base_dttm,
                last_seen_dttm,
                is_active
            ) VALUES (
                'ST-999999',
                '참조 테스트 대여소',
                '서울시 테스트 주소',
                10,
                ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326),
                'bike_station_master',
                '99_99',
                'extra_center',
                %s,
                %s,
                true
            )
            """,
            (observed, observed),
        )


def _table_count(connection: Connection[Any], table_name: str) -> int:
    """허용된 seed target table의 행 수를 반환한다."""
    if table_name not in {"weather_grid", "dispatch_center"}:
        raise AssertionError("unexpected table")
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT count(*) FROM {table_name}")
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0]


def _state_revision(connection: Connection[Any], publication_key: str) -> int:
    """publication key의 현재 revision을 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT revision_no FROM gold_meta.publication_state WHERE publication_key = %s",
            (publication_key,),
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0]


def _state_exists(connection: Connection[Any], publication_key: str) -> bool:
    """publication key의 state row 존재를 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM gold_meta.publication_state WHERE publication_key = %s)",
            (publication_key,),
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0]


def _row_exists(
    connection: Connection[Any],
    table_name: str,
    key_name: str,
    value: str,
) -> bool:
    """허용된 seed target의 명시적 key row 존재를 반환한다."""
    allowed = {
        ("weather_grid", "weather_grid_id"),
        ("dispatch_center", "dispatch_center_id"),
    }
    if (table_name, key_name) not in allowed:
        raise AssertionError("unexpected table or key")
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE {key_name} = %s)",
            (value,),
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0]


def _row_value(connection: Connection[Any], query: str) -> Any:
    """테스트가 고정한 read-only SQL의 단일 scalar를 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0]


def _station_projection_center(
    connection: Connection[Any],
    center_rows: tuple[Any, ...],
    station_id: str,
) -> str:
    """station projection helper로 동일 Point의 center 배정을 계산한다."""
    centers = tuple(
        DispatchCenterReference(
            row.dispatch_center_id,
            row.longitude,
            row.latitude,
            row.is_active,
        )
        for row in sorted(center_rows)
        if row.is_active
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT ST_X(station.sta_point),
                   ST_Y(station.sta_point),
                   array_agg(
                       ST_Distance(
                           station.sta_point::geography,
                           center.dispatch_center_point::geography
                       )
                       ORDER BY center.dispatch_center_id COLLATE "C"
                   )
              FROM station
             CROSS JOIN dispatch_center AS center
             WHERE station.sta_id = %s
               AND center.is_active
             GROUP BY station.sta_point
            """,
            (station_id,),
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return assign_dispatch_center_id(
        station_id=station_id,
        longitude=float(row[0]),
        latitude=float(row[1]),
        centers=centers,
        meters=tuple(float(value) for value in row[2]),
    )


def _reset_database(connection: Connection[Any]) -> None:
    """disposable DB의 Gold target과 publication state를 빈 상태로 되돌린다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            TRUNCATE TABLE
                rebalance_route_stop,
                rebalance_route,
                station_urgency,
                event,
                weather_forecast,
                station_demand_forecast,
                station_stock,
                station,
                dispatch_center,
                weather_grid,
                gold_meta.publication_state
            RESTART IDENTITY CASCADE
            """
        )
