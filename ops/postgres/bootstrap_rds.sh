#!/usr/bin/env bash
# RDS에 app/airflow/mlflow DB를 만들고 app DB에 #129 Gold PostGIS baseline을 적용한다.
#
# 로컬 compose는 postgres 공식 이미지의 docker-entrypoint-initdb.d 훅이 이 일을 대신
# 해준다(init/001_create_databases.sh, init/002_gold_schema.sh). RDS에는 그 훅이 없으므로
# 최초 1회 수동으로 실행한다.
#
# psql만 사용한다 — PostGIS 서버 바이너리가 필요 없으므로 postgres:16-alpine 같은
# multi-arch 이미지로도 돌릴 수 있다(Graviton EC2에서 중요). Makefile의
# deploy-db-bootstrap이 컨테이너로 감싼다.
#
# 재실행 안전성: DB 생성은 \gexec + NOT EXISTS로 멱등하고, 스키마 적용은
# target-schema.sql 자체가 기존 relation을 발견하면 RAISE EXCEPTION으로 중단한다.
set -Eeuo pipefail

readonly PSQL_BIN="${PSQL_BIN:-psql}"
readonly GOLD_SCHEMA_FILE="${GOLD_SCHEMA_FILE:-/opt/gold/target-schema.sql}"

: "${PGHOST:?[bootstrap-rds] RDS 엔드포인트(PGHOST)가 필요합니다}"
: "${PGPASSWORD:?[bootstrap-rds] 마스터 비밀번호(PGPASSWORD)가 필요합니다}"

export PGPORT="${PGPORT:-5432}"
# RDS는 기본적으로 TLS를 받는다. 평문 연결로 떨어지지 않게 명시한다.
export PGSSLMODE="${PGSSLMODE:-require}"

readonly POSTGRES_USER="${POSTGRES_USER:-postgres}"
readonly POSTGRES_APP_DB="${POSTGRES_APP_DB:-app}"
readonly POSTGRES_AIRFLOW_DB="${POSTGRES_AIRFLOW_DB:-airflow}"
readonly POSTGRES_MLFLOW_DB="${POSTGRES_MLFLOW_DB:-mlflow}"

if [[ ! -r "${GOLD_SCHEMA_FILE}" ]]; then
    echo "[bootstrap-rds] Gold 스키마 SSOT를 읽을 수 없습니다: ${GOLD_SCHEMA_FILE}" >&2
    exit 66
fi

echo "[bootstrap-rds] 대상: ${PGHOST}:${PGPORT} (user=${POSTGRES_USER})"

# --- 1. 데이터베이스 3개 ---
#
# RDS가 만들어주는 초기 DB는 하나뿐이다(Terraform의 db_name=app). 나머지 둘을 여기서
# 만든다. CREATE DATABASE는 트랜잭션 안에서 못 돌기 때문에 \gexec를 쓴다 —
# init/001_create_databases.sh와 같은 방식이다.
#
# RDS에 항상 존재하는 관리용 `postgres` DB에 붙어서 실행한다.
echo "[bootstrap-rds] 데이터베이스 생성 (없을 때만)"
"${PSQL_BIN}" \
    --no-psqlrc \
    --set ON_ERROR_STOP=1 \
    --username "${POSTGRES_USER}" \
    --dbname postgres <<-EOSQL
	SELECT 'CREATE DATABASE "${POSTGRES_APP_DB}"'
	WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_APP_DB}')\gexec

	SELECT 'CREATE DATABASE "${POSTGRES_AIRFLOW_DB}"'
	WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_AIRFLOW_DB}')\gexec

	SELECT 'CREATE DATABASE "${POSTGRES_MLFLOW_DB}"'
	WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_MLFLOW_DB}')\gexec
EOSQL

# --- 2. PostGIS 가용성 확인 ---
#
# check_gold_schema.sql이 정확히 3.4를 요구한다(split_part로 major/minor를 본다).
# 스키마를 적용하기 전에 먼저 확인해 실패를 앞당긴다 — 안 맞으면 엔진 버전을 바꿔
# RDS를 재생성해야 하고, 데이터가 없는 이 시점이 가장 싸다.
#
# 3.4로 맞춘 경위: RDS PostgreSQL 16.14가 PostGIS 3.4.6만 제공한다(2026-08-21 실측).
# 로컬 이미지도 postgis/postgis:16-3.4로 내려 dev/prod를 같은 조합으로 유지한다.
# 스키마·함수 18개·트리거 35개·GiST 3개·ACL이 3.4에서 전부 통과함을 확인했다.
echo "[bootstrap-rds] PostGIS 3.4 가용성 확인"
postgis_versions="$(
    "${PSQL_BIN}" \
        --no-psqlrc --set ON_ERROR_STOP=1 --tuples-only --no-align --quiet \
        --username "${POSTGRES_USER}" --dbname "${POSTGRES_APP_DB}" \
        --command "SELECT string_agg(version, ' ') FROM pg_available_extension_versions WHERE name = 'postgis'"
)"

if [[ "${postgis_versions}" != *"3.4"* ]]; then
    cat >&2 <<EOF
[bootstrap-rds] 이 엔진 버전에 PostGIS 3.4가 없습니다.
[bootstrap-rds] 사용 가능한 버전: ${postgis_versions:-(없음)}
[bootstrap-rds] ops/postgres/check_gold_schema.sql이 정확히 3.4를 요구합니다.
[bootstrap-rds] PostGIS 버전은 엔진 버전이 올라갈수록 높아지므로, 3.4보다 낮으면
[bootstrap-rds] rds_engine_version을 올리고 높으면 내려서 재생성하세요
[bootstrap-rds] (데이터가 없는 시점이면 재생성이 쌉니다).
EOF
    exit 78
fi
echo "[bootstrap-rds] PostGIS 가용 버전: ${postgis_versions}"

# --- 3. Gold baseline 적용 ---
#
# 이 파일이 스키마의 SSOT다(ADR 0002 — Alembic 없이 raw SQL 1회 적용).
# 비어 있지 않은 DB에 실행하면 파일 앞부분의 DO 블록이 RAISE EXCEPTION으로 막는다.
echo "[bootstrap-rds] Gold PostGIS baseline 적용: ${GOLD_SCHEMA_FILE}"
"${PSQL_BIN}" \
    --no-psqlrc \
    --set ON_ERROR_STOP=1 \
    --username "${POSTGRES_USER}" \
    --dbname "${POSTGRES_APP_DB}" \
    --file "${GOLD_SCHEMA_FILE}"

echo "[bootstrap-rds] 완료. 이어서 check_gold_schema.sh로 검증하세요 (make deploy-db-check)."
