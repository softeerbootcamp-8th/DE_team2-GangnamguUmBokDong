"""psycopg3 기반 공용 upsert 실행기.

`apps/api/seed_gold.py`의 `INSERT ... ON CONFLICT DO UPDATE` 스타일을 5개 테이블에
공통으로 쓸 수 있게 일반화한다. 테이블마다 SQL을 복붙하지 않는다.
"""

from __future__ import annotations

from psycopg import Connection


def upsert(conn: Connection, table: str, rows: list[dict], conflict_cols: list[str], update_cols: list[str]) -> None:
    """rows를 table에 upsert한다. conflict_cols가 충돌하면 update_cols만 갱신한다."""
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
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
