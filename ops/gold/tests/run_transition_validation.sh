#!/usr/bin/env bash
# 확정된 Gold 계약에서 현재 구현 가능한 전환 검증만 격리 환경에서 실행한다.
set -Eeuo pipefail
umask 077

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
readonly POSTGIS_IMAGE="postgis/postgis:16-3.5"
readonly SSOT_COMMIT="eadf79f925eb64386d009af71fe36854d9e56dc5"
readonly -a SSOT_FILES=(
    "docs/gold/target-schema.sql"
    "docs/gold/data-dictionary.md"
    "docs/gold/source-target-mapping.md"
    "docs/gold/publication-contract-v1.md"
)
readonly RUN_TOKEN="${BASHPID}_${RANDOM}_${RANDOM}"
readonly CONTAINER_NAME="gold-transition-${RUN_TOKEN//_/-}"
readonly BASELINE_DATABASE="gold129_transition_${RUN_TOKEN}"
readonly INTEGRATION_DATABASE="gold151_transition_${RUN_TOKEN}"
readonly POSTGRES_USER="postgres"
readonly POSTGRES_PASSWORD="gold_transition_${RUN_TOKEN}"
readonly DATA_TMPFS_OPTIONS="rw,nosuid,noexec,size=768m"

run_dir=""
container_id=""
container_owned=false
host_port=""
initial_tracked_status=""
integration_database_url=""
current_step="초기화"

fail() {
    # 현재 단계의 구체적인 실패 사유를 남기고 즉시 종료한다.
    echo "[gold-transition] ${current_step} 실패: $*" >&2
    exit 1
}

begin_step() {
    # 검증 로그에서 실패 지점을 식별할 수 있도록 단계명을 갱신한다.
    current_step=$1
    echo "[gold-transition] ==> ${current_step}"
}

cleanup() {
    # 이 실행이 만든 정확한 컨테이너와 임시 디렉터리만 정리한다.
    local exit_status=$?
    local actual_id=""

    trap - EXIT INT TERM
    set +e

    if [[ "${container_owned}" == true ]] && [[ -n "${container_id}" ]]; then
        actual_id="$(docker inspect --format '{{.Id}}' "${CONTAINER_NAME}" 2>/dev/null)"
        if [[ "${actual_id}" == "${container_id}" ]]; then
            docker stop --time 5 "${container_id}" >/dev/null 2>&1
        fi
    fi

    if [[ -n "${run_dir}" ]] \
        && [[ "${run_dir}" == /tmp/gold-transition-validation.* ]] \
        && [[ -d "${run_dir}" ]]; then
        rm -rf -- "${run_dir}"
    fi

    if [[ ${exit_status} -ne 0 ]]; then
        echo "[gold-transition] FAIL 단계=${current_step} 종료코드=${exit_status}" >&2
    fi
    exit "${exit_status}"
}

require_command() {
    # runner가 사용할 실행 파일이 PATH에 있는지 확인한다.
    local command_name=$1

    command -v "${command_name}" >/dev/null 2>&1 \
        || fail "필수 도구를 찾을 수 없습니다: ${command_name}"
}

verify_ssot() {
    # 네 SSOT 파일의 현재 bytes가 확정 commit의 blob과 같은지 확인한다.
    local file_path
    local expected_blob
    local actual_blob

    git -C "${REPO_ROOT}" cat-file -e "${SSOT_COMMIT}^{commit}" \
        || fail "확정 SSOT commit을 찾을 수 없습니다: ${SSOT_COMMIT}"

    for file_path in "${SSOT_FILES[@]}"; do
        [[ -r "${REPO_ROOT}/${file_path}" ]] \
            || fail "SSOT 파일을 읽을 수 없습니다: ${file_path}"
        expected_blob="$(git -C "${REPO_ROOT}" rev-parse "${SSOT_COMMIT}:${file_path}")"
        actual_blob="$(git -C "${REPO_ROOT}" hash-object -- "${file_path}")"
        [[ "${actual_blob}" == "${expected_blob}" ]] \
            || fail "확정 commit과 내용이 다릅니다: ${file_path}"
    done
}

wait_for_postgres() {
    # 일회성 컨테이너의 disposable baseline DB가 연결 가능해질 때까지 기다린다.
    local attempt

    for ((attempt = 1; attempt <= 120; attempt += 1)); do
        if docker exec "${container_id}" \
            pg_isready --quiet --host 127.0.0.1 --port 5432 \
            --username "${POSTGRES_USER}" \
            --dbname "${BASELINE_DATABASE}"; then
            return 0
        fi
        sleep 0.25
    done

    docker logs "${container_id}" >&2 || true
    fail "30초 안에 PostGIS가 준비되지 않았습니다."
}

verify_container_isolation() {
    # auto-remove, bind/volume 부재와 정확한 PGDATA tmpfs 설정을 확인한다.
    local actual_auto_remove
    local actual_image
    local actual_label
    local actual_mounts
    local actual_tmpfs

    actual_auto_remove="$(
        docker inspect --format '{{.HostConfig.AutoRemove}}' "${container_id}"
    )"
    actual_image="$(docker inspect --format '{{.Config.Image}}' "${container_id}")"
    actual_label="$(
        docker inspect \
            --format '{{index .Config.Labels "gold.transition.validation"}}' \
            "${container_id}"
    )"
    actual_mounts="$(docker inspect --format '{{json .Mounts}}' "${container_id}")"
    actual_tmpfs="$(
        docker inspect \
            --format '{{index .HostConfig.Tmpfs "/var/lib/postgresql/data"}}' \
            "${container_id}"
    )"

    [[ "${actual_auto_remove}" == true ]] || fail "컨테이너 AutoRemove가 아닙니다."
    [[ "${actual_image}" == "${POSTGIS_IMAGE}" ]] \
        || fail "허용되지 않은 DB image입니다: ${actual_image}"
    [[ "${actual_label}" == "${RUN_TOKEN}" ]] \
        || fail "컨테이너 ownership label이 다릅니다."
    [[ "${actual_mounts}" == "[]" ]] \
        || fail "격리 컨테이너에 bind/named volume mount가 있습니다: ${actual_mounts}"
    [[ "${actual_tmpfs}" == "${DATA_TMPFS_OPTIONS}" ]] \
        || fail "PGDATA tmpfs 설정이 다릅니다: ${actual_tmpfs}"
}

copy_database_contracts() {
    # host psql에 의존하지 않도록 검증 파일을 소유 컨테이너 내부로 복사한다.
    local file_path
    local -a contract_files=(
        "docs/gold/target-schema.sql"
        "docs/gold/target-schema-validation.sql"
        "docs/gold/target-schema-concurrency-validation.sh"
        "ops/postgres/check_gold_schema.sh"
        "ops/postgres/check_gold_schema.sql"
        "ops/gold/tests/target_edge_validation.sql"
    )

    docker exec "${container_id}" mkdir -p /tmp/gold-transition-contracts
    for file_path in "${contract_files[@]}"; do
        [[ -r "${REPO_ROOT}/${file_path}" ]] \
            || fail "DB 검증 파일을 읽을 수 없습니다: ${file_path}"
        docker cp "${REPO_ROOT}/${file_path}" \
            "${container_id}:/tmp/gold-transition-contracts/${file_path##*/}"
    done
}

container_psql() {
    # 소유 컨테이너 안의 psql로 지정한 disposable DB에만 명령을 실행한다.
    local database_name=$1
    shift

    docker exec \
        --env "PGPASSWORD=${POSTGRES_PASSWORD}" \
        "${container_id}" \
        psql -X --set ON_ERROR_STOP=1 \
        --username "${POSTGRES_USER}" \
        --dbname "${database_name}" \
        "$@"
}

apply_target_schema() {
    # final #129 DDL을 지정한 빈 disposable DB에 적용한다.
    local database_name=$1

    container_psql "${database_name}" \
        --file /tmp/gold-transition-contracts/target-schema.sql
}

reset_integration_database() {
    # package 통합 테스트 사이의 target/state/object-store fixture를 완전히 격리한다.
    docker exec \
        --env "PGPASSWORD=${POSTGRES_PASSWORD}" \
        "${container_id}" \
        dropdb --if-exists --force \
        --username "${POSTGRES_USER}" "${INTEGRATION_DATABASE}"
    docker exec \
        --env "PGPASSWORD=${POSTGRES_PASSWORD}" \
        "${container_id}" \
        createdb --username "${POSTGRES_USER}" "${INTEGRATION_DATABASE}"
    apply_target_schema "${INTEGRATION_DATABASE}"
    run_schema_check "${INTEGRATION_DATABASE}"
}

run_schema_check() {
    # 서비스 시작 전 read-only schema checker를 컨테이너 내부에서 재사용한다.
    local database_name=$1

    docker exec \
        --env "PGPASSWORD=${POSTGRES_PASSWORD}" \
        --env "POSTGRES_USER=${POSTGRES_USER}" \
        --env "POSTGRES_APP_DB=${database_name}" \
        --env "POSTGRES_AIRFLOW_DB=airflow" \
        --env "GOLD_SCHEMA_CHECK_FILE=/tmp/gold-transition-contracts/check_gold_schema.sql" \
        "${container_id}" \
        bash /tmp/gold-transition-contracts/check_gold_schema.sh
}

run_pytest_no_skips() {
    # pytest 실패뿐 아니라 예상하지 않은 skip/xfail/xpass도 실패로 처리한다.
    local suite_name=$1
    local output_file="${run_dir}/${suite_name}.pytest.log"
    local -a pipeline_status
    shift

    set +e
    "$@" 2>&1 | tee "${output_file}"
    pipeline_status=("${PIPESTATUS[@]}")
    set -e

    [[ ${pipeline_status[0]} -eq 0 ]] \
        || fail "${suite_name} pytest가 종료 코드 ${pipeline_status[0]}로 실패했습니다."
    [[ ${pipeline_status[1]} -eq 0 ]] \
        || fail "${suite_name} pytest 로그를 기록하지 못했습니다."
    if grep -Eq '(^|, )[[:digit:]]+ (skipped|xfailed|xpassed)(,| in )' \
        "${output_file}"; then
        fail "${suite_name} pytest에 예상하지 않은 skip/xfail/xpass가 있습니다."
    fi
}

run_core_tests() (
    # publication contract와 DB transaction 테스트를 focused suite로 실행한다.
    cd "${REPO_ROOT}/libs/core"
    run_pytest_no_skips "core" \
        env GOLD_PUBLICATION_TEST_DATABASE_URL="${integration_database_url}" \
        uv run --frozen pytest -q \
        tests/test_gold_publication_bindings.py \
        tests/test_gold_publication_canonical.py \
        tests/test_gold_publication_contract.py \
        tests/test_gold_publication_documents.py \
        tests/test_gold_publication_evidence.py \
        tests/test_gold_publication_exports.py \
        tests/test_gold_publication_storage.py \
        tests/test_gold_publication_transaction.py \
        tests/test_db.py \
        tests/test_inference_catalog.py \
        tests/test_inference_snapshot.py \
        tests/test_model_snapshot.py \
        tests/test_s3.py \
        tests/test_scoring_config_contract.py \
        tests/test_source_snapshot_io.py \
        tests/test_source_snapshot.py
)

run_collector_tests() (
    # immutable source authority와 completeness 관련 Collector 테스트만 실행한다.
    cd "${REPO_ROOT}/collector"
    run_pytest_no_skips "collector" uv run --frozen pytest -q \
        tests/test_compaction.py \
        tests/test_compaction_run.py \
        tests/test_kma_apihub.py \
        tests/test_pipeline.py \
        tests/test_schema.py \
        tests/test_seoul_openapi.py \
        tests/test_source_configs.py \
        tests/test_source_snapshot_manifest.py \
        tests/test_storage.py
)

run_ml_core_tests() (
    # 고정된 model pair release와 scoring artifact 경계를 검증한다.
    cd "${REPO_ROOT}/libs/ml_core"
    run_pytest_no_skips "ml-core" uv run --frozen pytest -q \
        tests/dev_pinned_scoring.py \
        tests/dev_serving_release.py
)

run_inference_tests() (
    # plan-bound inference producer, CLI와 runtime projection을 검증한다.
    cd "${REPO_ROOT}/ml/inference"
    run_pytest_no_skips "inference" uv run --frozen pytest -q \
        tests/dev_config.py \
        tests/dev_predict_common.py \
        tests/dev_predict_single_api_contract.py \
        tests/dev_predict_single_multi_horizon.py \
        tests/dev_predict_single_population_normalized.py \
        tests/dev_predict_single_rental_censoring.py \
        tests/dev_predict_single_station_master.py \
        tests/dev_predict_single_station_profile.py \
        tests/dev_predict_single_stockout_source.py \
        tests/dev_predict_single_weather_forecast.py \
        tests/dev_publication.py \
        tests/dev_publication_cli.py
)

run_training_tests() (
    # immutable model pair promotion과 pointer-last 경계를 검증한다.
    cd "${REPO_ROOT}/ml/training"
    run_pytest_no_skips "training" uv run --frozen pytest -q \
        tests/dev_promotion.py
)

run_loader_tests() (
    # source/derived publisher와 실제 PostGIS 원자성 테스트만 실행한다.
    local integration_test
    cd "${REPO_ROOT}/loader"
    run_pytest_no_skips "loader-source" \
        env GOLD_PUBLICATION_TEST_DATABASE_URL="${integration_database_url}" \
        uv run --frozen pytest -q \
        tests/gold \
        tests/test_config.py \
        tests/test_gold_cli.py \
        tests/test_gold_common.py \
        tests/test_gold_dispatch_center.py \
        tests/test_gold_event_integration.py \
        tests/test_gold_seed_integration.py \
        tests/test_gold_station_release_integration.py \
        tests/test_gold_versioning.py \
        tests/test_gold_weather_forecast_integration.py \
        tests/test_gold_weather_grid.py \
        tests/test_main.py \
        tests/test_predictions_key_contract.py \
        tests/test_serving_cli.py

    for integration_test in \
        tests/test_gold_demand_integration.py \
        tests/test_gold_serving_plan_integration.py \
        tests/test_gold_urgency_integration.py \
        tests/test_gold_rebalance_route_integration.py; do
        reset_integration_database
        run_pytest_no_skips "loader-${integration_test##*/}" \
            env GOLD_PUBLICATION_TEST_DATABASE_URL="${integration_database_url}" \
            uv run --frozen pytest -q "${integration_test}"
    done
)

run_api_tests() (
    # Gold-only query와 HTTP/PostGIS 경계 테스트만 실행한다.
    cd "${REPO_ROOT}/apps/api"
    run_pytest_no_skips "api" \
        env GOLD_API_TEST_DATABASE_URL="${integration_database_url}" \
        uv run --frozen pytest -q \
        tests/test_main.py \
        tests/test_queries.py \
        tests/test_postgis_integration.py
)

run_airflow_tests() (
    # publication CLI allowlist와 stacked DAG 의존성 테스트만 실행한다.
    mkdir -p "${run_dir}/airflow-home"
    cd "${REPO_ROOT}/airflow"
    run_pytest_no_skips "airflow" \
        env AIRFLOW_HOME="${run_dir}/airflow-home" \
        AIRFLOW__CORE__LOAD_EXAMPLES=false \
        uv run --frozen pytest -q \
        tests/test_dag_imports.py \
        tests/test_compose_runtime.py \
        tests/test_daily_population_and_events.py \
        tests/test_realtime_5min.py \
        tests/test_task_builders.py \
        tests/test_weather_10min.py \
        tests/test_weather_3h.py
)

run_web_tests() (
    # Gold DTO/UI failure-state 테스트와 production build를 실행한다.
    local output_file="${run_dir}/web.test.log"
    local -a pipeline_status

    cd "${REPO_ROOT}/apps/web"
    set +e
    npm test 2>&1 | tee "${output_file}"
    pipeline_status=("${PIPESTATUS[@]}")
    set -e
    [[ ${pipeline_status[0]} -eq 0 ]] \
        || fail "Web test가 종료 코드 ${pipeline_status[0]}로 실패했습니다."
    [[ ${pipeline_status[1]} -eq 0 ]] \
        || fail "Web test 로그를 기록하지 못했습니다."
    if grep -Eq '[[:digit:]]+ (skipped|todo|xfailed|xpassed)' "${output_file}"; then
        fail "Web test에 예상하지 않은 skip/todo/xfail/xpass가 있습니다."
    fi
    npm run build
)

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cat <<'EOF'
[gold-transition] Gold 전환 통합 검증을 시작합니다.
[gold-transition] 확정된 DDL·model/inference/publication 계약과 production 경계를 검증합니다.
[gold-transition] 운영 role/GRANT와 승인 weather seed는 별도 정책 입력이 필요하므로
[gold-transition] 실제 credential·scheduler·browser를 포함한 운영 E2E로 표방하지 않습니다.
EOF

begin_step "caller DB 환경 격리 및 필수 도구 확인"
unset DATABASE_URL GOLD_PUBLICATION_TEST_DATABASE_URL GOLD_API_TEST_DATABASE_URL
unset GOLD129_TEST_DATABASE_URL PGDATABASE PGHOST PGHOSTADDR PGPORT PGUSER PGPASSWORD
unset PGAPPNAME PGCHANNELBINDING PGCLIENTENCODING PGCONNECT_TIMEOUT PGPASSFILE
unset PGOPTIONS PGSERVICE PGSERVICEFILE PGTARGETSESSIONATTRS PGTZ
unset PGREQUIREPEER PGREQUIRESSL PGSSLCERT PGSSLCRL PGSSLCRLDIR PGSSLCOMPRESSION
unset PGSSLKEY PGSSLMAXPROTOCOLVERSION PGSSLMINPROTOCOLVERSION PGSSLMODE PGSSLSNI
for required_command in bash docker env git grep mktemp npm sed tee uv; do
    require_command "${required_command}"
done
for web_command in vitest tsc vite; do
    [[ -x "${REPO_ROOT}/apps/web/node_modules/.bin/${web_command}" ]] \
        || fail "Web 의존성이 없습니다. 먼저 apps/web에서 npm ci를 실행하세요: ${web_command}"
done
for validation_file in \
    "docs/gold/target-schema.sql" \
    "docs/gold/target-schema-validation.sql" \
    "docs/gold/target-schema-concurrency-validation.sh" \
    "ops/postgres/check_gold_schema.sh" \
    "ops/postgres/check_gold_schema.sql" \
    "ops/gold/tests/target_edge_validation.sql"; do
    [[ -r "${REPO_ROOT}/${validation_file}" ]] \
        || fail "DB 검증 파일을 읽을 수 없습니다: ${validation_file}"
done
docker info >/dev/null 2>&1 || fail "Docker daemon을 사용할 수 없습니다."

run_dir="$(mktemp -d /tmp/gold-transition-validation.XXXXXX)"
initial_tracked_status="$(
    git -C "${REPO_ROOT}" status --short --untracked-files=no
)"

begin_step "확정 SSOT bytes 및 shell 문법 확인"
verify_ssot
bash -n "${REPO_ROOT}/ops/gold/tests/run_transition_validation.sh"
bash -n "${REPO_ROOT}/ops/postgres/tests/test_bootstrap.sh"
bash -n "${REPO_ROOT}/ops/postgres/entrypoint.sh"
bash -n "${REPO_ROOT}/ops/postgres/init/002_gold_schema.sh"
bash -n "${REPO_ROOT}/ops/postgres/check_gold_schema.sh"
bash -n "${REPO_ROOT}/docs/gold/target-schema-concurrency-validation.sh"
git -C "${REPO_ROOT}" diff --check HEAD --

begin_step "DB bootstrap fail-closed 단위 검증"
bash "${REPO_ROOT}/ops/postgres/tests/test_bootstrap.sh"

begin_step "격리된 PostGIS 16-3.5 시작"
container_id="$(
    docker run --detach --rm \
        --name "${CONTAINER_NAME}" \
        --label "gold.transition.validation=${RUN_TOKEN}" \
        --tmpfs "/var/lib/postgresql/data:${DATA_TMPFS_OPTIONS}" \
        --publish 127.0.0.1::5432 \
        --env "POSTGRES_USER=${POSTGRES_USER}" \
        --env "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" \
        --env "POSTGRES_DB=${BASELINE_DATABASE}" \
        "${POSTGIS_IMAGE}"
)"
container_owned=true
[[ "${container_id}" =~ ^[[:xdigit:]]{64}$ ]] \
    || fail "docker run이 유효한 container ID를 반환하지 않았습니다."
verify_container_isolation
wait_for_postgres

port_binding="$(docker port "${container_id}" 5432/tcp)"
if [[ "${port_binding}" =~ ^127\.0\.0\.1:([0-9]+)$ ]]; then
    host_port="${BASH_REMATCH[1]}"
else
    fail "동적 localhost port를 확인할 수 없습니다: ${port_binding}"
fi
copy_database_contracts

begin_step "schema checker용 분리 Airflow DB 준비"
docker exec \
    --env "PGPASSWORD=${POSTGRES_PASSWORD}" \
    "${container_id}" \
    createdb --username "${POSTGRES_USER}" airflow

begin_step "clean baseline DDL 적용"
apply_target_schema "${BASELINE_DATABASE}"

begin_step "read-only schema check"
run_schema_check "${BASELINE_DATABASE}"

begin_step "SSOT target schema validation"
container_psql "${BASELINE_DATABASE}" \
    --file /tmp/gold-transition-contracts/target-schema-validation.sql

begin_step "Point·nonfinite·future·route edge validation"
container_psql "${BASELINE_DATABASE}" \
    --file /tmp/gold-transition-contracts/target_edge_validation.sql

begin_step "baseline 재적용 exit 3 검증"
set +e
container_psql "${BASELINE_DATABASE}" \
    --file /tmp/gold-transition-contracts/target-schema.sql \
    >"${run_dir}/baseline-reapply.stdout" \
    2>"${run_dir}/baseline-reapply.stderr"
reapply_status=$?
set -e
if [[ ${reapply_status} -ne 3 ]]; then
    sed -n '1,120p' "${run_dir}/baseline-reapply.stderr" >&2
    fail "baseline 재적용 종료 코드가 3이 아닙니다: ${reapply_status}"
fi
run_schema_check "${BASELINE_DATABASE}"

begin_step "two-session concurrency validation"
docker exec \
    --env "PGHOST=127.0.0.1" \
    --env "PGPORT=5432" \
    --env "PGUSER=${POSTGRES_USER}" \
    --env "PGPASSWORD=${POSTGRES_PASSWORD}" \
    --env "PGDATABASE=${BASELINE_DATABASE}" \
    "${container_id}" \
    bash /tmp/gold-transition-contracts/target-schema-concurrency-validation.sh

begin_step "package 통합 테스트용 clean PostGIS 준비"
docker exec \
    --env "PGPASSWORD=${POSTGRES_PASSWORD}" \
    "${container_id}" \
    createdb --username "${POSTGRES_USER}" "${INTEGRATION_DATABASE}"
apply_target_schema "${INTEGRATION_DATABASE}"
run_schema_check "${INTEGRATION_DATABASE}"
integration_database_url="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${host_port}/${INTEGRATION_DATABASE}"

begin_step "Core publication focused tests"
run_core_tests

begin_step "Collector authority focused tests"
run_collector_tests

begin_step "ML serving release focused tests"
run_ml_core_tests

begin_step "inference producer focused tests"
run_inference_tests

begin_step "model promotion focused tests"
run_training_tests

begin_step "source·derived Loader focused tests"
reset_integration_database
run_loader_tests

begin_step "API Gold/PostGIS focused tests"
reset_integration_database
run_api_tests

begin_step "Airflow publication DAG focused tests"
run_airflow_tests

begin_step "Web Gold DTO/UI focused tests"
run_web_tests

begin_step "최종 tracked worktree 무결성 확인"
git -C "${REPO_ROOT}" diff --check HEAD --
final_tracked_status="$(
    git -C "${REPO_ROOT}" status --short --untracked-files=no
)"
if [[ "${final_tracked_status}" != "${initial_tracked_status}" ]]; then
    echo "[gold-transition] 시작 tracked status:" >&2
    printf '%s\n' "${initial_tracked_status:-<clean>}" >&2
    echo "[gold-transition] 종료 tracked status:" >&2
    printf '%s\n' "${final_tracked_status:-<clean>}" >&2
    fail "검증 실행 중 tracked worktree가 변경되었습니다."
fi

current_step="전환 통합 검증 완료"
cat <<'EOF'
[gold-transition] 검증 완료: clean DDL, model/inference, source·derived publication,
[gold-transition] stale·correction·EMPTY·원자성·동시성 및 소비자 경계를 통과했습니다.
[gold-transition] 이 결과는 운영 credential·scheduler·browser를 포함한 live E2E가 아닙니다.
EOF
