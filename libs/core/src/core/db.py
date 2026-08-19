"""PostgreSQL(psycopg3) 데이터베이스 연결 및 기본 쿼리 실행 헬퍼 모듈."""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row


def get_connection() -> Connection:
    """DATABASE_URL을 사용해 UTC session의 PostgreSQL 연결을 반환한다."""
    database_url = os.environ["DATABASE_URL"]
    dsn_options = conninfo_to_dict(database_url).get("options")
    inherited_options = (
        dsn_options if dsn_options is not None else os.getenv("PGOPTIONS", "")
    )
    connection_options = f"{inherited_options} -c timezone=UTC".strip()
    return psycopg.connect(database_url, options=connection_options)


def execute(query: str, params: Sequence[Any] | None = None) -> None:
    """지정된 SQL 쿼리를 실행하고 트랜잭션을 커밋한다.

    args:
        query: 실행할 SQL 문자열
        params: 쿼리 바인딩 파라미터 (선택)
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()


def fetch_all(query: str, params: Sequence[Any] | None = None) -> list[dict]:
    """지정된 SELECT 쿼리를 실행하고 모든 결과 행을 딕셔너리 목록으로 반환한다.

    args:
        query: 실행할 SELECT SQL 문자열
        params: 쿼리 바인딩 파라미터 (선택)
    returns:
        컬럼명을 키로 하는 딕셔너리 레코드 목록
    """
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def fetch_one(query: str, params: Sequence[Any] | None = None) -> dict | None:
    """지정된 SELECT 쿼리를 실행하고 단일 결과 행을 딕셔너리로 반환한다.

    args:
        query: 실행할 SELECT SQL 문자열
        params: 쿼리 바인딩 파라미터 (선택)
    returns:
        단일 딕셔너리 레코드 (결과가 없으면 None)
    """
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        return cur.fetchone()
