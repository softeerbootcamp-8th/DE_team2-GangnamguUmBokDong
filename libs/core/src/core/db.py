import os

import psycopg
from psycopg import Connection


def get_connection() -> Connection:
    """DATABASE_URL로 앱 Postgres(app DB)에 연결한다."""
    database_url = os.environ["DATABASE_URL"]
    return psycopg.connect(database_url)
