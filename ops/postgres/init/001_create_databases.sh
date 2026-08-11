#!/usr/bin/env bash
# postgres 공식 이미지가 최초 기동 시 docker-entrypoint-initdb.d/*.sh를 자동 실행한다.
# 앱 데이터용 DB와 Airflow 메타데이터용 DB를 인스턴스 하나에 분리 생성한다.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE "${POSTGRES_APP_DB}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_APP_DB}')\gexec

    SELECT 'CREATE DATABASE "${POSTGRES_AIRFLOW_DB}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_AIRFLOW_DB}')\gexec
EOSQL
