"""ops/compose 로컬 Postgres를 대상으로 한 upsert 통합 테스트.

DATABASE_URL이 없으면(로컬 DB가 안 떠 있으면) 스킵한다.
"""

import os
from collections.abc import Iterator

import psycopg
import pytest
from core.upsert import upsert

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="DATABASE_URL 미설정: 로컬 Postgres 필요"
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection]:
    """테스트가 끝나면 변경을 롤백하는 PostgreSQL 연결을 제공한다."""
    connection = psycopg.connect(os.environ["DATABASE_URL"])
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


def test_upsert_is_idempotent(conn: psycopg.Connection) -> None:
    """#129 weather_grid 계약에서도 범용 upsert가 멱등하게 동작한다."""
    rows = [
        {
            "weather_grid_id": "32760_32760",
            "weather_grid_x_no": 32760,
            "weather_grid_y_no": 32760,
        }
    ]

    upsert(
        conn,
        "weather_grid",
        rows,
        conflict_cols=["weather_grid_id"],
        update_cols=["weather_grid_x_no", "weather_grid_y_no"],
    )
    upsert(
        conn,
        "weather_grid",
        rows,
        conflict_cols=["weather_grid_id"],
        update_cols=["weather_grid_x_no", "weather_grid_y_no"],
    )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weather_grid WHERE weather_grid_id = %s",
            ("32760_32760",),
        )
        [count] = cur.fetchone()

    assert count == 1
