#!/usr/bin/env bash
# 소비자 시작 전에 #129 Gold PostGIS baseline의 핵심 객체를 읽기 전용으로 확인한다.
set -Eeuo pipefail

readonly PSQL_BIN="${PSQL_BIN:-psql}"
readonly GOLD_SCHEMA_CHECK_FILE="${GOLD_SCHEMA_CHECK_FILE:-/opt/gold/check_gold_schema.sql}"

if [[ ! -r "${GOLD_SCHEMA_CHECK_FILE}" ]]; then
    echo "[gold-postgis] schema check SQL을 읽을 수 없습니다: ${GOLD_SCHEMA_CHECK_FILE}" >&2
    exit 66
fi

export PGOPTIONS="${PGOPTIONS:+${PGOPTIONS} }-c default_transaction_read_only=on"

schema_ready="$(
    "${PSQL_BIN}" \
        --no-psqlrc \
        --set ON_ERROR_STOP=1 \
        --set "airflow_db=${POSTGRES_AIRFLOW_DB}" \
        --username "${POSTGRES_USER}" \
        --dbname "${POSTGRES_APP_DB}" \
        --tuples-only \
        --no-align \
        --quiet \
        --file "${GOLD_SCHEMA_CHECK_FILE}"
)"

if [[ "${schema_ready}" != "t" ]]; then
    cat >&2 <<'EOF'
[gold-postgis] app DB가 #129 Gold PostGIS baseline과 일치하지 않습니다.
[gold-postgis] 기존 볼륨에는 DDL을 재적용하지 않았으며 어떤 데이터도 삭제하지 않았습니다.
[gold-postgis] 기존 볼륨을 보존하려면 `make down` 후 별도 Compose 프로젝트를 사용하세요.
[gold-postgis] 예: COMPOSE_PROJECT_NAME=gold-postgis-v1 make up
EOF
    exit 78
fi

echo "[gold-postgis] #129 Gold PostGIS baseline 확인 완료."
