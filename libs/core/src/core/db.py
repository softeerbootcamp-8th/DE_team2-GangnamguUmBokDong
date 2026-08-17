import os
from collections.abc import Sequence
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row


def get_connection() -> Connection:
    """DATABASE_URL로 앱 Postgres(app DB)에 연결한다."""
    database_url = os.environ["DATABASE_URL"]
    return psycopg.connect(database_url)


def execute(query: str, params: Sequence[Any] | None = None) -> None:
    """쿼리를 실행하고 커밋한다."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()


def fetch_all(query: str, params: Sequence[Any] | None = None) -> list[dict]:
    """쿼리를 실행하고 모든 결과를 dict 리스트로 반환한다."""
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchall()


def fetch_one(query: str, params: Sequence[Any] | None = None) -> dict | None:
    """쿼리를 실행하고 단일 결과를 dict로 반환한다."""
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchone()
