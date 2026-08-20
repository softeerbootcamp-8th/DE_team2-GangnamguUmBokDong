#!/usr/bin/env bash
# 기존 PostgreSQL 볼륨을 Gold baseline으로 자동 변환하지 않도록 시작 전에 차단한다.
set -Eeuo pipefail

readonly GOLD_SCHEMA_MARKER="${PGDATA:?PGDATA must be set}/.gold-postgis-target-schema-129"
readonly POSTGRES_IMAGE_ENTRYPOINT="${POSTGRES_IMAGE_ENTRYPOINT:-/usr/local/bin/docker-entrypoint.sh}"

if [[ -s "${PGDATA}/PG_VERSION" && ! -f "${GOLD_SCHEMA_MARKER}" ]]; then
    cat >&2 <<'EOF'
[gold-postgis] 기존 postgres-data 볼륨은 #129 Gold PostGIS baseline으로 확인되지 않았습니다.
[gold-postgis] 이 저장소는 기존 볼륨의 스키마를 자동 변경하거나 볼륨을 삭제하지 않습니다.
[gold-postgis] 기존 볼륨을 보존하려면 `make down` 후 별도 Compose 프로젝트를 사용하세요.
[gold-postgis] 예: COMPOSE_PROJECT_NAME=gold-postgis-v1 make up
[gold-postgis] 이후 logs/down 명령에도 같은 COMPOSE_PROJECT_NAME을 사용해야 합니다.
EOF
    exit 78
fi

exec "${POSTGRES_IMAGE_ENTRYPOINT}" "$@"
