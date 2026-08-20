#!/usr/bin/env bash
# Gold PostGIS 부트스트랩 래퍼의 비파괴 fail-fast 계약을 검증한다.
set -Eeuo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
readonly ENTRYPOINT="${REPO_ROOT}/ops/postgres/entrypoint.sh"
readonly SCHEMA_WRAPPER="${REPO_ROOT}/ops/postgres/init/002_gold_schema.sh"
readonly SCHEMA_CHECK="${REPO_ROOT}/ops/postgres/check_gold_schema.sh"
readonly SCHEMA_CHECK_SQL="${REPO_ROOT}/ops/postgres/check_gold_schema.sql"
readonly COMPOSE_FILE="${REPO_ROOT}/ops/compose/docker-compose.yml"
readonly APPLE_COMPOSE_FILE="${REPO_ROOT}/ops/compose/docker-compose.apple-silicon.yml"
readonly PLATFORM_ARGS="${REPO_ROOT}/ops/compose/platform_args.sh"
readonly TEMP_DIR="$(mktemp -d)"

cleanup() {
    # 이 테스트가 생성한 임시 디렉터리만 제거한다.
    rm -rf -- "${TEMP_DIR}"
}

fail() {
    # 실패 사유를 표준 오류에 남긴다.
    echo "[test-gold-bootstrap] $*" >&2
    exit 1
}

trap cleanup EXIT

bash -n "${ENTRYPOINT}"
bash -n "${SCHEMA_WRAPPER}"
bash -n "${SCHEMA_CHECK}"
bash -n "${PLATFORM_ARGS}"

grep -Fq 'command: ["postgres"]' "${COMPOSE_FILE}" || fail "custom entrypoint가 postgres 명령을 명시하지 않았습니다."
grep -Fq 'pg_isready -h 127.0.0.1' "${COMPOSE_FILE}" || fail "healthcheck가 init용 Unix socket을 제외하지 않았습니다."
grep -Fq '.gold-postgis-target-schema-129' "${COMPOSE_FILE}" || fail "healthcheck가 baseline marker를 확인하지 않았습니다."
[[ "$(grep -Fc 'platform: linux/amd64' "${APPLE_COMPOSE_FILE}")" -eq 2 ]] ||
    fail "Apple Silicon override의 두 PostGIS 서비스가 amd64 계약을 공유하지 않습니다."
mac_args="$(
    COMPOSE_HOST_OS_OVERRIDE=Darwin \
        COMPOSE_HOST_ARCH_OVERRIDE=arm64 \
        bash "${PLATFORM_ARGS}"
)"
[[ "${mac_args}" == '-f ops/compose/docker-compose.apple-silicon.yml' ]] ||
    fail "Apple Silicon 호스트가 Compose override를 선택하지 않았습니다."
linux_arm_args="$(
    COMPOSE_HOST_OS_OVERRIDE=Linux \
        COMPOSE_HOST_ARCH_OVERRIDE=aarch64 \
        bash "${PLATFORM_ARGS}"
)"
[[ -z "${linux_arm_args}" ]] ||
    fail "Linux ARM/Graviton 호스트에 Apple 전용 amd64 override가 적용됐습니다."

mkdir -p "${TEMP_DIR}/empty" "${TEMP_DIR}/legacy" "${TEMP_DIR}/ready"
printf '16\n' >"${TEMP_DIR}/legacy/PG_VERSION"

if PGDATA="${TEMP_DIR}/legacy" \
    POSTGRES_IMAGE_ENTRYPOINT=/usr/bin/printf \
    bash "${ENTRYPOINT}" delegated >"${TEMP_DIR}/legacy.out" 2>"${TEMP_DIR}/legacy.err"; then
    fail "legacy 볼륨이 차단되지 않았습니다."
else
    readonly legacy_status=$?
fi

[[ "${legacy_status}" -eq 78 ]] || fail "legacy 볼륨 종료 코드가 78이 아닙니다."
[[ ! -e "${TEMP_DIR}/legacy/.gold-postgis-target-schema-129" ]] || fail "legacy 볼륨이 변경되었습니다."
[[ "$(<"${TEMP_DIR}/legacy.err")" == *"자동 변경하거나 볼륨을 삭제하지 않습니다"* ]] || fail "legacy 볼륨 안내가 불명확합니다."

empty_output="$(
    PGDATA="${TEMP_DIR}/empty" \
        POSTGRES_IMAGE_ENTRYPOINT=/usr/bin/printf \
        bash "${ENTRYPOINT}" delegated
)"
[[ "${empty_output}" == "delegated" ]] || fail "빈 볼륨이 공식 entrypoint로 위임되지 않았습니다."

printf '16\n' >"${TEMP_DIR}/ready/PG_VERSION"
touch "${TEMP_DIR}/ready/.gold-postgis-target-schema-129"
ready_output="$(
    PGDATA="${TEMP_DIR}/ready" \
        POSTGRES_IMAGE_ENTRYPOINT=/usr/bin/printf \
        bash "${ENTRYPOINT}" delegated
)"
[[ "${ready_output}" == "delegated" ]] || fail "검증된 볼륨이 공식 entrypoint로 위임되지 않았습니다."

mkdir -p "${TEMP_DIR}/wrapper-success" "${TEMP_DIR}/wrapper-failure" "${TEMP_DIR}/wrapper-reapply"
touch "${TEMP_DIR}/target-schema.sql"
printf '#!/usr/bin/env bash\nexit 0\n' >"${TEMP_DIR}/psql-exit-0"
printf '#!/usr/bin/env bash\nexit 1\n' >"${TEMP_DIR}/psql-exit-1"
printf '#!/usr/bin/env bash\nexit 3\n' >"${TEMP_DIR}/psql-exit-3"
chmod +x "${TEMP_DIR}/psql-exit-0" "${TEMP_DIR}/psql-exit-1" "${TEMP_DIR}/psql-exit-3"

PGDATA="${TEMP_DIR}/wrapper-success" \
    GOLD_SCHEMA_FILE="${TEMP_DIR}/target-schema.sql" \
    POSTGRES_USER=test \
    POSTGRES_APP_DB=test \
    PSQL_BIN="${TEMP_DIR}/psql-exit-0" \
    bash "${SCHEMA_WRAPPER}"
[[ -f "${TEMP_DIR}/wrapper-success/.gold-postgis-target-schema-129" ]] || fail "DDL 성공 후 marker가 생성되지 않았습니다."

if PGDATA="${TEMP_DIR}/wrapper-failure" \
    GOLD_SCHEMA_FILE="${TEMP_DIR}/target-schema.sql" \
    POSTGRES_USER=test \
    POSTGRES_APP_DB=test \
    PSQL_BIN="${TEMP_DIR}/psql-exit-1" \
    bash "${SCHEMA_WRAPPER}"; then
    fail "DDL 실패가 전파되지 않았습니다."
fi
[[ ! -e "${TEMP_DIR}/wrapper-failure/.gold-postgis-target-schema-129" ]] || fail "DDL 실패 후 marker가 생성되었습니다."

touch "${TEMP_DIR}/wrapper-reapply/.gold-postgis-target-schema-129"
if PGDATA="${TEMP_DIR}/wrapper-reapply" \
    GOLD_SCHEMA_FILE="${TEMP_DIR}/target-schema.sql" \
    POSTGRES_USER=test \
    POSTGRES_APP_DB=test \
    PSQL_BIN="${TEMP_DIR}/psql-exit-3" \
    bash "${SCHEMA_WRAPPER}"; then
    fail "baseline 재적용 실패가 전파되지 않았습니다."
else
    readonly reapply_status=$?
fi
[[ "${reapply_status}" -eq 3 ]] || fail "baseline 재적용 종료 코드가 3이 아닙니다."
[[ -f "${TEMP_DIR}/wrapper-reapply/.gold-postgis-target-schema-129" ]] || fail "재적용 실패가 기존 marker를 변경했습니다."

printf '#!/usr/bin/env bash\nprintf "t\\n"\n' >"${TEMP_DIR}/psql-ready"
printf '#!/usr/bin/env bash\nprintf "f\\n"\n' >"${TEMP_DIR}/psql-invalid"
chmod +x "${TEMP_DIR}/psql-ready" "${TEMP_DIR}/psql-invalid"

POSTGRES_USER=test \
    POSTGRES_APP_DB=test \
    POSTGRES_AIRFLOW_DB=airflow \
    GOLD_SCHEMA_CHECK_FILE="${SCHEMA_CHECK_SQL}" \
    PSQL_BIN="${TEMP_DIR}/psql-ready" \
    bash "${SCHEMA_CHECK}" >"${TEMP_DIR}/check-ready.out"

if POSTGRES_USER=test \
    POSTGRES_APP_DB=test \
    POSTGRES_AIRFLOW_DB=airflow \
    GOLD_SCHEMA_CHECK_FILE="${SCHEMA_CHECK_SQL}" \
    PSQL_BIN="${TEMP_DIR}/psql-invalid" \
    bash "${SCHEMA_CHECK}" >"${TEMP_DIR}/check-invalid.out" 2>"${TEMP_DIR}/check-invalid.err"; then
    fail "불완전한 스키마가 차단되지 않았습니다."
else
    readonly check_status=$?
fi
[[ "${check_status}" -eq 78 ]] || fail "불완전한 스키마 종료 코드가 78이 아닙니다."
[[ "$(<"${TEMP_DIR}/check-invalid.err")" == *"어떤 데이터도 삭제하지 않았습니다"* ]] || fail "schema check 안내가 불명확합니다."

[[ ! -e "${REPO_ROOT}/ops/postgres/init/003_station_urgency.sh" ]] || fail "legacy 003 init이 남아 있습니다."
[[ ! -e "${REPO_ROOT}/ops/postgres/init/004_rebalance_routes.sh" ]] || fail "legacy 004 init이 남아 있습니다."

echo "[test-gold-bootstrap] 모든 검증을 통과했습니다."
