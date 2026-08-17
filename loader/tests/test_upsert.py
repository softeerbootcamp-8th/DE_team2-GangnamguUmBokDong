"""ops/compose 로컬 Postgres를 대상으로 한 upsert 통합 테스트.

DATABASE_URL이 없으면(로컬 DB가 안 떠 있으면) 스킵한다.
"""

import os

import psycopg
import pytest

from core.upsert import upsert

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"), reason="DATABASE_URL 미설정: 로컬 Postgres 필요"
)


@pytest.fixture
def conn():
    connection = psycopg.connect(os.environ["DATABASE_URL"])
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


def test_upsert_is_idempotent(conn):
    rows = [{"sta_id": 999001, "sta_nm": "테스트역", "gu": "강남구", "sta_addr": "테스트역", "lat": 37.5, "lon": 127.0, "hold_cnt": 10}]

    upsert(conn, "stations", rows, conflict_cols=["sta_id"], update_cols=["sta_nm", "gu", "sta_addr", "lat", "lon", "hold_cnt"])
    upsert(conn, "stations", rows, conflict_cols=["sta_id"], update_cols=["sta_nm", "gu", "sta_addr", "lat", "lon", "hold_cnt"])

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stations WHERE sta_id = %s", (999001,))
        [count] = cur.fetchone()

    assert count == 1
