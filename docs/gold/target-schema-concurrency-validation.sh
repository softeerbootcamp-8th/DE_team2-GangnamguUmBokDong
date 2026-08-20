#!/usr/bin/env bash

# Gold #129 baseline의 transaction/advisory-lock 계약을 별도 세션에서 검증한다.
# target-schema.sql을 적용한 비어 있는 gold129_* disposable DB에서만 실행한다.
# 예: PGHOST=localhost PGUSER=postgres PGDATABASE=gold129_test ./target-schema-concurrency-validation.sh

set -Eeuo pipefail

readonly PSQL_CONNECT_TIMEOUT_SEC=5
readonly SESSION_SQL_TIMEOUT="20s"
readonly SESSION_IDLE_TIMEOUT="20s"
readonly POLL_ATTEMPTS=200
readonly POLL_INTERVAL_SEC=0.05

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

pass() {
    printf 'PASS: %s\n' "$*"
}

if [[ -n "${GOLD129_TEST_DATABASE_URL:-}" ]]; then
    fail 'use standard PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE variables; URI input is refused'
fi
if [[ -z "${PGDATABASE:-}" ]] || [[ ! "${PGDATABASE}" =~ ^gold129_[[:alnum:]_]+$ ]]; then
    fail 'PGDATABASE must be a plain disposable database name matching gold129_*'
fi

# libpq 표준 환경변수만 사용해 비밀과 DSN이 argv나 로그에 나타나지 않게 한다.
export PGCONNECT_TIMEOUT="${PSQL_CONNECT_TIMEOUT_SEC}"

readonly -a PSQL_SCALAR=(psql -X --no-align --tuples-only --quiet --set=ON_ERROR_STOP=1)
readonly -a PSQL_EXEC=(psql -X --quiet --set=ON_ERROR_STOP=1)

work_dir="$(mktemp -d /tmp/gold129-concurrency.XXXXXX)"
holder_pid=''
holder_fd=''
holder_fifo=''
holder_out=''
holder_err=''
contender_pid=''

cleanup() {
    local exit_status=$?

    trap - EXIT INT TERM
    set +e

    if [[ -n "${holder_fd}" ]]; then
        exec {holder_fd}>&-
    fi
    if [[ -n "${holder_pid}" ]] && kill -0 "${holder_pid}" 2>/dev/null; then
        kill "${holder_pid}" 2>/dev/null
        wait "${holder_pid}" 2>/dev/null
    fi
    if [[ -n "${contender_pid}" ]] && kill -0 "${contender_pid}" 2>/dev/null; then
        kill "${contender_pid}" 2>/dev/null
        wait "${contender_pid}" 2>/dev/null
    fi
    if [[ "${work_dir}" == /tmp/gold129-concurrency.* ]] && [[ -d "${work_dir}" ]]; then
        rm -rf -- "${work_dir}"
    fi

    exit "${exit_status}"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

psql_scalar() {
    "${PSQL_SCALAR[@]}" "$@"
}

psql_exec() {
    "${PSQL_EXEC[@]}" "$@"
}

wait_for_holder_lock() {
    local application_name=$1
    local process_id=$2
    local lock_observed='f'
    local attempt

    for ((attempt = 1; attempt <= POLL_ATTEMPTS; attempt += 1)); do
        if ! kill -0 "${process_id}" 2>/dev/null; then
            fail "holder session ${application_name} exited before acquiring its lock"
        fi
        lock_observed="$(psql_scalar --command="
            SELECT EXISTS (
                SELECT 1
                  FROM pg_catalog.pg_locks AS l
                  JOIN pg_catalog.pg_stat_activity AS a ON a.pid = l.pid
                 WHERE a.application_name = '${application_name}'
                   AND a.state = 'idle in transaction'
                   AND l.locktype = 'advisory'
                   AND l.granted
            );
        ")"
        if [[ "${lock_observed}" == 't' ]]; then
            return 0
        fi
        sleep "${POLL_INTERVAL_SEC}"
    done

    fail "holder session ${application_name} did not become ready"
}

wait_for_contender_block() {
    local application_name=$1
    local process_id=$2
    local block_observed='f'
    local attempt

    for ((attempt = 1; attempt <= POLL_ATTEMPTS; attempt += 1)); do
        if ! kill -0 "${process_id}" 2>/dev/null; then
            fail "contender session ${application_name} exited before blocking"
        fi
        block_observed="$(psql_scalar --command="
            SELECT EXISTS (
                SELECT 1
                  FROM pg_catalog.pg_locks AS l
                  JOIN pg_catalog.pg_stat_activity AS a ON a.pid = l.pid
                 WHERE a.application_name = '${application_name}'
                   AND l.locktype = 'advisory'
                   AND NOT l.granted
            );
        ")"
        if [[ "${block_observed}" == 't' ]]; then
            return 0
        fi
        sleep "${POLL_INTERVAL_SEC}"
    done

    fail "contender session ${application_name} did not block on the expected advisory lock"
}

start_holder() {
    local application_name=$1
    local session_label=$2

    holder_fifo="${work_dir}/${session_label}.stdin"
    holder_out="${work_dir}/${session_label}.stdout"
    holder_err="${work_dir}/${session_label}.stderr"
    mkfifo "${holder_fifo}"

    PGAPPNAME="${application_name}" "${PSQL_SCALAR[@]}" \
        <"${holder_fifo}" >"${holder_out}" 2>"${holder_err}" &
    holder_pid=$!
    exec {holder_fd}>"${holder_fifo}"
}

write_holder_sql() {
    local line

    while IFS= read -r line || [[ -n "${line}" ]]; do
        printf '%s\n' "${line}"
    done >&"${holder_fd}"
}

commit_holder() {
    write_holder_sql <<'SQL'
COMMIT;
\q
SQL
    exec {holder_fd}>&-
    holder_fd=''

    if ! wait "${holder_pid}"; then
        holder_pid=''
        fail 'holder session failed while committing'
    fi
    holder_pid=''
}

wait_for_expected_failure() {
    local expected_error=$1
    local stderr_file=$2

    if wait "${contender_pid}"; then
        contender_pid=''
        fail "contender unexpectedly succeeded; expected: ${expected_error}"
    fi
    contender_pid=''

    if ! grep -Fq -- "${expected_error}" "${stderr_file}"; then
        fail "contender failed without the expected error: ${expected_error}"
    fi
}

if ! database_name="$(psql_scalar --command='SELECT current_database();')"; then
    fail 'could not connect to the disposable validation database'
fi
if [[ ! "${database_name}" =~ ^gold129_[[:alnum:]_]+$ ]]; then
    fail 'refusing to run: current database name does not match gold129_*'
fi

schema_ready="$(psql_scalar --command="
    SELECT to_regclass('public.weather_grid') IS NOT NULL
       AND to_regclass('public.dispatch_center') IS NOT NULL
       AND to_regclass('public.station') IS NOT NULL
       AND to_regclass('public.station_stock') IS NOT NULL
       AND to_regclass('public.station_demand_forecast') IS NOT NULL
       AND to_regclass('public.weather_forecast') IS NOT NULL
       AND to_regclass('public.event') IS NOT NULL
       AND to_regclass('public.station_urgency') IS NOT NULL
       AND to_regclass('public.rebalance_route') IS NOT NULL
       AND to_regclass('public.rebalance_route_stop') IS NOT NULL
       AND to_regclass('gold_meta.publication_state') IS NOT NULL
       AND to_regprocedure(
           'gold_meta.claim_publication(text,timestamp with time zone,integer,text,text,text,bigint)'
       ) IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'postgis'
       );
")"
if [[ "${schema_ready}" != 't' ]]; then
    fail 'refusing to run: target-schema.sql baseline is incomplete'
fi

existing_row_cnt="$(psql_scalar --command="
    SELECT
        (SELECT count(*) FROM weather_grid)
      + (SELECT count(*) FROM dispatch_center)
      + (SELECT count(*) FROM station)
      + (SELECT count(*) FROM station_stock)
      + (SELECT count(*) FROM station_demand_forecast)
      + (SELECT count(*) FROM weather_forecast)
      + (SELECT count(*) FROM event)
      + (SELECT count(*) FROM station_urgency)
      + (SELECT count(*) FROM rebalance_route)
      + (SELECT count(*) FROM rebalance_route_stop)
      + (SELECT count(*) FROM gold_meta.publication_state);
")"
if [[ "${existing_row_cnt}" != '0' ]]; then
    fail 'refusing to run: validation database is not empty'
fi
pass 'disposable gold129_* database and clean Gold baseline verified'

readonly run_id="${BASHPID}"
readonly publication_holder_app="gold129_pub_holder_${run_id}"
readonly publication_contender_app="gold129_pub_contender_${run_id}"
publication_contender_out="${work_dir}/publication-contender.stdout"
publication_contender_err="${work_dir}/publication-contender.stderr"

start_holder "${publication_holder_app}" 'publication-holder'
write_holder_sql <<SQL
SET statement_timeout = '${SESSION_SQL_TIMEOUT}';
SET idle_in_transaction_session_timeout = '${SESSION_IDLE_TIMEOUT}';
BEGIN;
SELECT gold_meta.claim_publication(
    'event:cultural_event',
    clock_timestamp() - INTERVAL '1 hour',
    1,
    's3://gold129-test/publication/newer.json',
    repeat('a', 64),
    repeat('b', 64),
    0
);
SQL
wait_for_holder_lock "${publication_holder_app}" "${holder_pid}"

(
    PGAPPNAME="${publication_contender_app}" "${PSQL_SCALAR[@]}" <<SQL
SET statement_timeout = '${SESSION_SQL_TIMEOUT}';
SELECT gold_meta.claim_publication(
    'event:cultural_event',
    clock_timestamp() - INTERVAL '2 hours',
    999,
    's3://gold129-test/publication/older.json',
    repeat('c', 64),
    repeat('d', 64),
    0
);
SQL
) >"${publication_contender_out}" 2>"${publication_contender_err}" &
contender_pid=$!
wait_for_contender_block "${publication_contender_app}" "${contender_pid}"
commit_holder

if ! wait "${contender_pid}"; then
    contender_pid=''
    fail 'stale publication contender failed instead of returning false'
fi
contender_pid=''
if ! grep -Eq '^[[:space:]]*f[[:space:]]*$' "${publication_contender_out}"; then
    fail 'stale publication contender did not return false after lock release'
fi
if ! grep -Fxq 't' "${holder_out}"; then
    fail 'newer publication holder did not claim the publication'
fi
pass 'newer publication serialized before an older high-revision claim; stale claim returned false'

psql_exec <<'SQL'
SET statement_timeout = '20s';
BEGIN;

INSERT INTO weather_grid (weather_grid_id, weather_grid_x_no, weather_grid_y_no)
VALUES ('61_126', 61, 126);

INSERT INTO dispatch_center (
    dispatch_center_id,
    dispatch_center_nm,
    dispatch_center_point,
    location_accuracy_cd,
    location_source_desc,
    location_verified_dt,
    is_active
)
VALUES
    (
        'gold129_center_a',
        'Gold129 검증 센터 A',
        ST_SetSRID(ST_MakePoint(127.0000, 37.5000), 4326),
        'verified_site',
        'disposable concurrency fixture',
        DATE '2026-08-20',
        true
    ),
    (
        'gold129_center_b',
        'Gold129 검증 센터 B',
        ST_SetSRID(ST_MakePoint(127.0100, 37.5100), 4326),
        'verified_site',
        'disposable concurrency fixture',
        DATE '2026-08-20',
        true
    );

INSERT INTO station (
    sta_id,
    sta_nm,
    sta_addr,
    hold_cnt,
    sta_point,
    sta_point_source_cd,
    weather_grid_id,
    dispatch_center_id,
    master_base_dttm,
    last_seen_dttm,
    is_active
)
VALUES (
    'ST-1299901',
    'Gold129 검증 대여소',
    '서울특별시 검증로 129',
    20,
    ST_SetSRID(ST_MakePoint(127.0010, 37.5010), 4326),
    'bike_station_master',
    '61_126',
    'gold129_center_a',
    clock_timestamp() - INTERVAL '1 hour',
    clock_timestamp() - INTERVAL '5 minutes',
    true
);

COMMIT;
SQL

readonly topology_holder_app="gold129_topology_holder_${run_id}"
readonly topology_contender_app="gold129_topology_contender_${run_id}"
topology_contender_out="${work_dir}/topology-contender.stdout"
topology_contender_err="${work_dir}/topology-contender.stderr"

start_holder "${topology_holder_app}" 'topology-holder'
write_holder_sql <<SQL
SET statement_timeout = '${SESSION_SQL_TIMEOUT}';
SET idle_in_transaction_session_timeout = '${SESSION_IDLE_TIMEOUT}';
BEGIN;
UPDATE station
   SET dispatch_center_id = 'gold129_center_b'
 WHERE sta_id = 'ST-1299901';
UPDATE dispatch_center
   SET is_active = false
 WHERE dispatch_center_id = 'gold129_center_a';
SQL
wait_for_holder_lock "${topology_holder_app}" "${holder_pid}"

(
    PGAPPNAME="${topology_contender_app}" "${PSQL_EXEC[@]}" <<SQL
SET statement_timeout = '${SESSION_SQL_TIMEOUT}';
INSERT INTO rebalance_route (
    route_id,
    dispatch_center_id,
    route_status_cd,
    proposed_dttm
)
VALUES (
    '12900000-0000-4000-8000-000000000001',
    'gold129_center_a',
    'proposed',
    clock_timestamp()
);
SQL
) >"${topology_contender_out}" 2>"${topology_contender_err}" &
contender_pid=$!
wait_for_contender_block "${topology_contender_app}" "${contender_pid}"
commit_holder
wait_for_expected_failure \
    'a proposed rebalance route requires an active dispatch center' \
    "${topology_contender_err}"

route_inserted="$(psql_scalar --command="
    SELECT EXISTS (
        SELECT 1
          FROM rebalance_route
         WHERE route_id = '12900000-0000-4000-8000-000000000001'
    );
")"
if [[ "${route_inserted}" != 'f' ]]; then
    fail 'route against the deactivated center survived post-lock revalidation'
fi
pass 'topology reassignment/deactivation serialized before route insert; stale route failed revalidation'

psql_exec <<'SQL'
SET statement_timeout = '20s';
BEGIN;

INSERT INTO rebalance_route (
    route_id,
    dispatch_center_id,
    route_status_cd,
    proposed_dttm
)
VALUES (
    '12900000-0000-4000-8000-000000000002',
    'gold129_center_b',
    'proposed',
    clock_timestamp() - INTERVAL '1 minute'
);

INSERT INTO rebalance_route_stop (
    route_id,
    visit_no,
    sta_id,
    route_action_type_cd,
    bike_cnt
)
VALUES (
    '12900000-0000-4000-8000-000000000002',
    1,
    'ST-1299901',
    'pickup',
    3
);

COMMIT;
SQL

readonly dispatch_holder_app="gold129_dispatch_holder_${run_id}"
readonly stop_contender_app="gold129_stop_contender_${run_id}"
stop_contender_out="${work_dir}/stop-contender.stdout"
stop_contender_err="${work_dir}/stop-contender.stderr"

start_holder "${dispatch_holder_app}" 'dispatch-holder'
write_holder_sql <<SQL
SET statement_timeout = '${SESSION_SQL_TIMEOUT}';
SET idle_in_transaction_session_timeout = '${SESSION_IDLE_TIMEOUT}';
BEGIN;
UPDATE rebalance_route
   SET route_status_cd = 'dispatched',
       dispatched_dttm = clock_timestamp()
 WHERE route_id = '12900000-0000-4000-8000-000000000002';
SQL
wait_for_holder_lock "${dispatch_holder_app}" "${holder_pid}"

(
    PGAPPNAME="${stop_contender_app}" "${PSQL_EXEC[@]}" <<SQL
SET statement_timeout = '${SESSION_SQL_TIMEOUT}';
UPDATE rebalance_route_stop
   SET bike_cnt = bike_cnt + 1
 WHERE route_id = '12900000-0000-4000-8000-000000000002'
   AND visit_no = 1;
SQL
) >"${stop_contender_out}" 2>"${stop_contender_err}" &
contender_pid=$!
wait_for_contender_block "${stop_contender_app}" "${contender_pid}"
commit_holder
wait_for_expected_failure \
    'stops of a non-proposed rebalance route are immutable' \
    "${stop_contender_err}"

route_state="$(psql_scalar --command="
    SELECT route_status_cd || ':' || rs.bike_cnt::text
      FROM rebalance_route AS r
      JOIN rebalance_route_stop AS rs USING (route_id)
     WHERE r.route_id = '12900000-0000-4000-8000-000000000002'
       AND rs.visit_no = 1;
")"
if [[ "${route_state}" != 'dispatched:3' ]]; then
    fail 'dispatch/stop serialization left an unexpected route state'
fi
pass 'route dispatch serialized before stop mutation; post-dispatch stop remained immutable'

pass 'all three two-session concurrency contracts completed without timeout or deadlock'
