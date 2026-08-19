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
    guard_col: str | None = None,
) -> None:
    """딕셔너리 레코드 목록을 대상 테이블에 배치로 Upsert(ON CONFLICT DO UPDATE)한다.

    args:
        conn: 활성 PostgreSQL 연결 객체 (None일 경우 내부에서 새 연결을 열고 자동 커밋)
        table: 대상 테이블 이름
        rows: 삽입/갱신할 딕셔너리 레코드 목록
        conflict_cols: 중복 충돌을 감지할 고유 키(PK/Unique) 컬럼 목록
        update_cols: 충돌 발생 시 EXCLUDED 값으로 갱신할 컬럼 목록 (비어있으면 DO NOTHING)
        guard_col: 여러 소스가 같은 conflict key로 한 테이블을 공유할 때, 이 컬럼이
            기존 행보다 뒤처지지 않은 경우에만 덮어쓰게 막는 신선도 가드(예: base_dttm).
            서로 다른 파이프라인(예: 단기예보 3h·초단기예보 30분)이 실행 순서 보장 없이
            같은 테이블에 upsert할 때, 먼저 커밋된 더 최신 발표가 나중에 커밋된 더
            오래된 발표로 덮어써지는 것을 막는다.
    """
    if not rows:
        return

    cols = list(rows[0].keys())
    col_list = ", ".join(cols)
    value_list = ", ".join(f"%({c})s" for c in cols)

    if update_cols:
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        action_clause = f"DO UPDATE SET {set_clause}"
        if guard_col is not None:
            action_clause += f" WHERE EXCLUDED.{guard_col} >= {table}.{guard_col}"
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
