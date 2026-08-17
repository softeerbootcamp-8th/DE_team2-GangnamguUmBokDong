"""PostgreSQL(psycopg3) 기반 범용 Upsert(INSERT ON CONFLICT DO UPDATE) 모듈."""

from __future__ import annotations

from psycopg import Connection

from core.db import get_connection


def upsert(
    conn: Connection | None,
    table: str,
    rows: list[dict],
    conflict_cols: list[str],
    update_cols: list[str],
) -> None:
    """딕셔너리 레코드 목록을 대상 테이블에 배치로 Upsert(ON CONFLICT DO UPDATE)한다.

    args:
        conn: 활성 PostgreSQL 연결 객체 (None일 경우 내부에서 새 연결을 열고 자동 커밋)
        table: 대상 테이블 이름
        rows: 삽입/갱신할 딕셔너리 레코드 목록
        conflict_cols: 중복 충돌을 감지할 고유 키(PK/Unique) 컬럼 목록
        update_cols: 충돌 발생 시 EXCLUDED 값으로 갱신할 컬럼 목록 (비어있으면 DO NOTHING)
    """
    if not rows:
        return

    cols = list(rows[0].keys())
    col_list = ", ".join(cols)
    value_list = ", ".join(f"%({c})s" for c in cols)

    if update_cols:
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        action_clause = f"DO UPDATE SET {set_clause}"
    else:
        action_clause = "DO NOTHING"

    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({value_list}) "
        f"ON CONFLICT ({', '.join(conflict_cols)}) {action_clause}"
    )

    if conn is None:
        with get_connection() as c:
            with c.cursor() as cur:
                cur.executemany(sql, rows)
            c.commit()
    else:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
