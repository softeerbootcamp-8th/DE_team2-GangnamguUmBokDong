"""psycopg3 기반 공용 upsert 실행기."""

from __future__ import annotations

from psycopg import Connection

from core.db import get_connection


def upsert(conn: Connection | None, table: str, rows: list[dict], conflict_cols: list[str], update_cols: list[str]) -> None:
    """rows를 table에 upsert한다. conflict_cols가 충돌하면 update_cols만 갱신한다.
    
    conn이 None이면 내부에서 get_connection()을 열고 커밋한다.
    """
    if not rows:
        return

    cols = list(rows[0].keys())
    col_list = ", ".join(cols)
    value_list = ", ".join(f"%({c})s" for c in cols)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({value_list}) "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {set_clause}"
    )
    
    if conn is None:
        with get_connection() as c:
            with c.cursor() as cur:
                cur.executemany(sql, rows)
            c.commit()
    else:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
