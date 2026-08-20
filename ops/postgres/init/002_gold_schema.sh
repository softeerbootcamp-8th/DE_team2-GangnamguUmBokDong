#!/usr/bin/env bash
# #129에서 확정한 Gold PostGIS baseline을 새 app DB에 한 번만 적용한다.
set -Eeuo pipefail

readonly GOLD_SCHEMA_FILE="${GOLD_SCHEMA_FILE:-/opt/gold/target-schema.sql}"
readonly GOLD_SCHEMA_MARKER="${PGDATA:?PGDATA must be set}/.gold-postgis-target-schema-129"
readonly PSQL_BIN="${PSQL_BIN:-psql}"

if [[ ! -r "${GOLD_SCHEMA_FILE}" ]]; then
    echo "[gold-postgis] SSOT를 읽을 수 없습니다: ${GOLD_SCHEMA_FILE}" >&2
    exit 66
fi

"${PSQL_BIN}" \
    --no-psqlrc \
    --set ON_ERROR_STOP=1 \
    --username "${POSTGRES_USER}" \
    --dbname "${POSTGRES_APP_DB}" \
    --file "${GOLD_SCHEMA_FILE}"

touch "${GOLD_SCHEMA_MARKER}"
