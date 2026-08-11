#!/usr/bin/env bash
# airflow/ 프로젝트의 uv 환경을 항상 uv.lock 기준으로 맞춘 뒤,
# 인자에 따라 init(1회성 마이그레이션+admin 계정 생성) / webserver / scheduler를 실행한다.
set -euo pipefail

cd /workspace/airflow
uv sync --frozen

case "${1:-webserver}" in
    init)
        uv run airflow db migrate
        uv run airflow users create \
            --username "${AIRFLOW_ADMIN_USER:-admin}" \
            --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
            --firstname Admin \
            --lastname User \
            --role Admin \
            --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
            || echo "admin user already exists, skipping"
        ;;
    webserver)
        exec uv run airflow webserver
        ;;
    scheduler)
        exec uv run airflow scheduler
        ;;
    *)
        exec "$@"
        ;;
esac
