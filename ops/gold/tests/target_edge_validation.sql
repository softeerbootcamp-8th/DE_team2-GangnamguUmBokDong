\set ON_ERROR_STOP on

-- #155 Gold target 경계값 검증.
-- 전제: 빈 PostgreSQL 16 + PostGIS DB에 docs/gold/target-schema.sql을 적용한 직후
-- `psql -X -v ON_ERROR_STOP=1 -f`로 실행한다.
-- fixture와 pg_temp helper는 하나의 transaction에서 만들고 마지막 ROLLBACK으로 제거한다.

BEGIN;

SET LOCAL TIME ZONE 'UTC';
SET LOCAL search_path TO public, pg_temp;

CREATE FUNCTION pg_temp.assert_true(
    assertion BOOLEAN,
    assertion_desc TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF assertion IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'edge validation assertion failed: %', assertion_desc;
    END IF;
END;
$$;

-- 각 EXECUTE는 PL/pgSQL의 내부 subtransaction(savepoint) 안에서 실행된다. 예상 오류를
-- 지나치게 한 SQLSTATE로 고정하지 않고 데이터/제약 오류 class만 허용하되, 실제 DDL의
-- SQLSTATE·constraint·메시지를 그대로 NOTICE에 남긴다. 문법·권한·미정의 객체 오류는
-- 원래 오류를 다시 던지며, 어떤 오류도 발생하지 않으면 검증 자체를 실패시킨다.
CREATE FUNCTION pg_temp.expect_rejected(
    statement_sqls TEXT[],
    assertion_desc TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    statement_sql TEXT;
    actual_sqlstate TEXT;
    actual_constraint_nm TEXT;
    actual_message TEXT;
BEGIN
    BEGIN
        FOREACH statement_sql IN ARRAY statement_sqls
        LOOP
            EXECUTE statement_sql;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                actual_sqlstate = RETURNED_SQLSTATE,
                actual_constraint_nm = CONSTRAINT_NAME,
                actual_message = MESSAGE_TEXT;

            IF left(actual_sqlstate, 2) NOT IN ('22', '23') THEN
                RAISE NOTICE
                    'UNEXPECTED: % rejected [%], constraint=%, message=%',
                    assertion_desc,
                    actual_sqlstate,
                    COALESCE(NULLIF(actual_constraint_nm, ''), '<none>'),
                    actual_message;
                RAISE;
            END IF;

            RAISE NOTICE
                'PASS: % rejected [%], constraint=%, message=%',
                assertion_desc,
                actual_sqlstate,
                COALESCE(NULLIF(actual_constraint_nm, ''), '<none>'),
                actual_message;
            RETURN;
    END;

    RAISE EXCEPTION
        'edge validation assertion failed: % (statements unexpectedly succeeded)',
        assertion_desc;
END;
$$;

-- target DDL은 업무 시각의 유한성·순서를, 공통 publisher transaction은 DB clock 기반
-- now+5분 상한을 소유한다. 이 임시 함수는 영구 구조를 추가하지 않고 후자의 SQL 경계를
-- 같은 DB clock_timestamp()로 검증한다.
CREATE FUNCTION pg_temp.validate_business_time_boundary(
    business_dttm TIMESTAMPTZ,
    business_time_nm TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF business_dttm IS NULL
       OR NOT isfinite(business_dttm)
       OR business_dttm > clock_timestamp() + INTERVAL '5 minutes' THEN
        RAISE EXCEPTION '% violates the DB-clock publication boundary', business_time_nm
            USING ERRCODE = '23514';
    END IF;
END;
$$;

-- 차량 prefix 적재량은 target table CHECK가 아니라 route publisher staging 계약이다.
-- 초기 적재량 0, capacity 20, 각 visit 뒤 0..20을 이 임시 validator로 재현한다.
CREATE FUNCTION pg_temp.validate_route_load_contract(
    action_types TEXT[],
    bike_counts INTEGER[],
    truck_capacity INTEGER
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    item_index INTEGER;
    running_load INTEGER := 0;
BEGIN
    IF cardinality(action_types) = 0
       OR cardinality(action_types) <> cardinality(bike_counts)
       OR truck_capacity <> 20 THEN
        RAISE EXCEPTION 'invalid route load validation input'
            USING ERRCODE = '23514';
    END IF;

    FOR item_index IN 1..cardinality(action_types)
    LOOP
        IF action_types[item_index] = 'pickup' THEN
            running_load := running_load + bike_counts[item_index];
        ELSIF action_types[item_index] = 'dropoff' THEN
            running_load := running_load - bike_counts[item_index];
        ELSE
            RAISE EXCEPTION 'unsupported route load action: %', action_types[item_index]
                USING ERRCODE = '23514';
        END IF;

        IF bike_counts[item_index] <= 0
           OR running_load NOT BETWEEN 0 AND truck_capacity THEN
            RAISE EXCEPTION
                'route prefix load % is outside 0..% at visit %',
                running_load,
                truck_capacity,
                item_index
                USING ERRCODE = '23514';
        END IF;
    END LOOP;
END;
$$;

DO $$
DECLARE
    target_table_nm TEXT;
    target_row_cnt BIGINT;
BEGIN
    FOREACH target_table_nm IN ARRAY ARRAY[
        'weather_grid',
        'dispatch_center',
        'station',
        'station_stock',
        'station_demand_forecast',
        'weather_forecast',
        'event',
        'station_urgency',
        'rebalance_route',
        'rebalance_route_stop'
    ]
    LOOP
        IF to_regclass('public.' || target_table_nm) IS NULL THEN
            RAISE EXCEPTION 'required Gold target table is missing: %', target_table_nm;
        END IF;
        EXECUTE format('SELECT count(*) FROM public.%I', target_table_nm)
           INTO target_row_cnt;
        PERFORM pg_temp.assert_true(
            target_row_cnt = 0,
            format('target %I must start empty', target_table_nm)
        );
    END LOOP;

    SELECT count(*) INTO target_row_cnt FROM gold_meta.publication_state;
    PERFORM pg_temp.assert_true(
        target_row_cnt = 0,
        'gold_meta.publication_state must start empty'
    );
END;
$$;

-- 정상 최소 topology와 geography index 대상 Point 세 개를 준비한다.
INSERT INTO weather_grid (
    weather_grid_id,
    weather_grid_x_no,
    weather_grid_y_no
)
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
VALUES (
    'edge_center',
    '경계 검증 센터',
    ST_SetSRID(ST_MakePoint(127.0000, 37.5000), 4326),
    'verified_site',
    'target_edge_validation fixture',
    CURRENT_DATE,
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
    'ST-9901',
    '경계 검증 대여소',
    '서울특별시 경계검증로 1',
    20,
    ST_SetSRID(ST_MakePoint(127.0005, 37.5000), 4326),
    'bike_station_master',
    '61_126',
    'edge_center',
    clock_timestamp() - INTERVAL '2 minutes',
    clock_timestamp() - INTERVAL '1 minute',
    true
);

INSERT INTO event (
    event_id,
    event_source_cd,
    source_event_id,
    event_name,
    event_spot_nm,
    event_point,
    event_point_source_cd,
    location_accuracy_cd,
    event_start_dt,
    event_end_dt,
    last_seen_dttm
)
VALUES (
    'performance_event:edge-normal',
    'performance_event',
    'edge-normal',
    '경계 검증 행사',
    '경계 검증 광장',
    ST_SetSRID(ST_MakePoint(127.0008, 37.5002), 4326),
    'curated_osm_nominatim',
    'approximate',
    CURRENT_DATE,
    CURRENT_DATE + 1,
    clock_timestamp() - INTERVAL '1 minute'
);

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        INSERT INTO dispatch_center (
            dispatch_center_id,
            dispatch_center_nm,
            dispatch_center_point,
            location_accuracy_cd,
            location_source_desc,
            is_active
        )
        VALUES (
            'edge_empty_point',
            'EMPTY Point 오류 센터',
            ST_GeomFromText('POINT EMPTY', 4326),
            'verified_site',
            'target_edge_validation fixture',
            true
        )
    $statement$],
    'POINT EMPTY must be rejected'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
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
            'ST-9902',
            'SRID 오류 대여소',
            '서울특별시 경계검증로 2',
            10,
            ST_SetSRID(ST_MakePoint(14137575.3, 4509031.4), 3857),
            'bike_station_master',
            '61_126',
            'edge_center',
            clock_timestamp() - INTERVAL '2 minutes',
            clock_timestamp() - INTERVAL '1 minute',
            true
        )
    $statement$],
    'a Point whose SRID is not 4326 must be rejected'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        INSERT INTO event (
            event_id,
            event_source_cd,
            source_event_id,
            event_name,
            event_point,
            event_point_source_cd,
            location_accuracy_cd,
            event_start_dt,
            event_end_dt,
            last_seen_dttm
        )
        VALUES (
            'performance_event:edge-outside-seoul',
            'performance_event',
            'edge-outside-seoul',
            '서울 safety box 밖 행사',
            ST_SetSRID(ST_MakePoint(128.0000, 37.5000), 4326),
            'curated_osm_nominatim',
            'approximate',
            CURRENT_DATE,
            CURRENT_DATE + 1,
            clock_timestamp() - INTERVAL '1 minute'
        )
    $statement$],
    'a Point outside the Seoul safety box must be rejected'
);

SELECT pg_temp.assert_true(
    (
        SELECT bool_and(
            NOT ST_IsEmpty(candidate.point_value)
            AND ST_GeometryType(candidate.point_value) = 'ST_Point'
            AND ST_SRID(candidate.point_value) = 4326
        )
          FROM (
              SELECT dispatch_center_point AS point_value
                FROM dispatch_center
               WHERE dispatch_center_id = 'edge_center'
              UNION ALL
              SELECT sta_point
                FROM station
               WHERE sta_id = 'ST-9901'
              UNION ALL
              SELECT event_point
                FROM event
               WHERE event_id = 'performance_event:edge-normal'
          ) AS candidate
    ),
    'all geography-indexed fixture Points must be non-empty Point/4326 values'
);

SELECT pg_temp.assert_true(
    (
        SELECT ST_DWithin(s.sta_point::geography, dc.dispatch_center_point::geography, 100)
               AND ST_Distance(
                   s.sta_point::geography,
                   dc.dispatch_center_point::geography
               ) BETWEEN 1 AND 100
          FROM station AS s
          JOIN dispatch_center AS dc USING (dispatch_center_id)
         WHERE s.sta_id = 'ST-9901'
    ),
    'normal WGS84 Points must support meter-distance geography operations'
);

SET LOCAL enable_seqscan = off;

DO $$
DECLARE
    target_table_nm TEXT;
    point_column_nm TEXT;
    point_index_nm TEXT;
    plan_line TEXT;
    plan_text TEXT;
BEGIN
    FOR target_table_nm, point_column_nm, point_index_nm IN
        SELECT *
          FROM (VALUES
              ('station', 'sta_point', 'station_point_geography_gix'),
              ('event', 'event_point', 'event_point_geography_gix'),
              (
                  'dispatch_center',
                  'dispatch_center_point',
                  'dispatch_center_point_geography_gix'
              )
          ) AS indexed_point(target_table_nm, point_column_nm, point_index_nm)
    LOOP
        plan_text := '';
        FOR plan_line IN EXECUTE format(
            'EXPLAIN (COSTS OFF) '
            'SELECT 1 FROM public.%I '
            'WHERE ST_DWithin('
            '%I::geography, '
            'ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326)::geography, '
            '1000.0)',
            target_table_nm,
            point_column_nm
        )
        LOOP
            plan_text := plan_text || E'\n' || plan_line;
        END LOOP;

        PERFORM pg_temp.assert_true(
            position(point_index_nm IN plan_text) > 0,
            format('%I geography predicate must use %I', target_table_nm, point_index_nm)
        );
    END LOOP;
END;
$$;

SET LOCAL enable_seqscan = on;

-- 정상 weather/urgency 행을 만든 뒤 모든 DOUBLE PRECISION 필드에 NaN과 ±Infinity를
-- 각각 UPDATE해 실제 CHECK가 거부하고 기존 행이 savepoint rollback으로 보존되는지 본다.
INSERT INTO weather_forecast (
    weather_grid_id,
    forecast_dttm,
    source_product_cd,
    base_dttm,
    sky_condition_cd,
    precipitation_type_cd,
    temperature,
    precipitation_prob,
    precipitation_amount,
    humidity,
    wind_speed
)
VALUES (
    '61_126',
    date_bin(
        INTERVAL '1 hour',
        clock_timestamp() + INTERVAL '2 hours',
        TIMESTAMPTZ '1970-01-01 00:00:00+00'
    ),
    'short_term',
    clock_timestamp() - INTERVAL '1 minute',
    'clear',
    'none',
    20.0,
    10.0,
    0.0,
    50.0,
    2.0
);

DO $$
DECLARE
    numeric_column_nm TEXT;
    invalid_value TEXT;
BEGIN
    FOREACH numeric_column_nm IN ARRAY ARRAY[
        'temperature',
        'precipitation_prob',
        'precipitation_amount',
        'humidity',
        'wind_speed'
    ]
    LOOP
        FOREACH invalid_value IN ARRAY ARRAY['NaN', 'Infinity', '-Infinity']
        LOOP
            PERFORM pg_temp.expect_rejected(
                ARRAY[format(
                    'UPDATE public.weather_forecast '
                    'SET %I = %L::DOUBLE PRECISION '
                    'WHERE weather_grid_id = %L',
                    numeric_column_nm,
                    invalid_value,
                    '61_126'
                )],
                format(
                    'weather_forecast.%I=%s must be rejected',
                    numeric_column_nm,
                    invalid_value
                )
            );
        END LOOP;
    END LOOP;
END;
$$;

SELECT pg_temp.assert_true(
    (
        SELECT temperature = 20.0
               AND precipitation_prob = 10.0
               AND precipitation_amount = 0.0
               AND humidity = 50.0
               AND wind_speed = 2.0
          FROM weather_forecast
         WHERE weather_grid_id = '61_126'
    ),
    'rejected non-finite weather updates must leave the valid row unchanged'
);

INSERT INTO station_urgency (
    sta_id,
    base_dttm,
    urgency_score,
    critical_remaining_min,
    rebalance_need_type_cd
)
VALUES (
    'ST-9901',
    clock_timestamp() - INTERVAL '1 minute',
    80.0,
    15,
    'supply_needed'
);

DO $$
DECLARE
    invalid_value TEXT;
BEGIN
    FOREACH invalid_value IN ARRAY ARRAY['NaN', 'Infinity', '-Infinity']
    LOOP
        PERFORM pg_temp.expect_rejected(
            ARRAY[format(
                'UPDATE public.station_urgency '
                'SET urgency_score = %L::DOUBLE PRECISION '
                'WHERE sta_id = %L',
                invalid_value,
                'ST-9901'
            )],
            format('station_urgency.urgency_score=%s must be rejected', invalid_value)
        );
    END LOOP;
END;
$$;

SELECT pg_temp.assert_true(
    (SELECT urgency_score = 80.0 FROM station_urgency WHERE sta_id = 'ST-9901'),
    'rejected non-finite urgency updates must leave the valid row unchanged'
);

-- publication logical time은 target-schema의 claim 함수에서 직접 거부한다.
SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        SELECT gold_meta.claim_publication(
            'station_urgency',
            clock_timestamp() + INTERVAL '10 minutes',
            0,
            's3://fixture/future-publication.json',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            1
        )
    $statement$],
    'a publication logical time beyond DB now+5 minutes must be rejected'
);

-- 공통 transaction executor가 검사하는 모든 업무 시각 종류를 DB clock 기준으로 고정한다.
DO $$
DECLARE
    business_time_nm TEXT;
BEGIN
    FOREACH business_time_nm IN ARRAY ARRAY[
        'station.master_base_dttm',
        'station.last_seen_dttm',
        'station_stock.base_dttm',
        'station_demand_forecast.base_dttm',
        'weather_forecast.base_dttm',
        'event.last_seen_dttm',
        'station_urgency.base_dttm',
        'rebalance_route.proposed_dttm'
    ]
    LOOP
        PERFORM pg_temp.validate_business_time_boundary(
            clock_timestamp(),
            business_time_nm
        );
        PERFORM pg_temp.expect_rejected(
            ARRAY[format(
                'SELECT pg_temp.validate_business_time_boundary('
                'clock_timestamp() + INTERVAL ''10 minutes'', %L)',
                business_time_nm
            )],
            format('%s beyond DB now+5 minutes must be rejected', business_time_nm)
        );
    END LOOP;
END;
$$;

-- Route header는 proposed로만 생성되고 commit 시 최소 한 stop을 가져야 한다.
SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        INSERT INTO rebalance_route (
            route_id,
            dispatch_center_id,
            route_status_cd,
            proposed_dttm,
            dispatched_dttm
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000102',
            'edge_center',
            'dispatched',
            clock_timestamp() - INTERVAL '1 minute',
            clock_timestamp()
        )
    $statement$],
    'a route header must not be inserted directly as dispatched'
);

SELECT pg_temp.expect_rejected(
    ARRAY[
        $statement$
            INSERT INTO rebalance_route (
                route_id,
                dispatch_center_id,
                route_status_cd,
                proposed_dttm
            )
            VALUES (
                UUID '00000000-0000-0000-0000-000000000101',
                'edge_center',
                'proposed',
                clock_timestamp() - INTERVAL '1 minute'
            )
        $statement$,
        'SET CONSTRAINTS ALL IMMEDIATE'
    ],
    'a route header without a stop must be rejected at the deferred boundary'
);

SET CONSTRAINTS ALL DEFERRED;

INSERT INTO rebalance_route (
    route_id,
    dispatch_center_id,
    route_status_cd,
    proposed_dttm
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000100',
    'edge_center',
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
    UUID '00000000-0000-0000-0000-000000000100',
    1,
    'ST-9901',
    'pickup',
    5
), (
    UUID '00000000-0000-0000-0000-000000000100',
    2,
    'ST-9901',
    'dropoff',
    3
);

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        INSERT INTO rebalance_route_stop (
            route_id, visit_no, sta_id, route_action_type_cd, bike_cnt
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000100',
            0, 'ST-9901', 'pickup', 1
        )
    $statement$],
    'route stop visit_no=0 must be rejected'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        INSERT INTO rebalance_route_stop (
            route_id, visit_no, sta_id, route_action_type_cd, bike_cnt
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000100',
            3, 'ST-9901', 'move', 1
        )
    $statement$],
    'a route stop action outside pickup/dropoff must be rejected'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        INSERT INTO rebalance_route_stop (
            route_id, visit_no, sta_id, route_action_type_cd, bike_cnt
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000100',
            3, 'ST-9901', 'pickup', 0
        )
    $statement$],
    'a route stop bike_cnt=0 must be rejected'
);

SELECT pg_temp.expect_rejected(
    ARRAY[
        $statement$
            INSERT INTO rebalance_route_stop (
                route_id, visit_no, sta_id, route_action_type_cd, bike_cnt
            )
            VALUES (
                UUID '00000000-0000-0000-0000-000000000100',
                4, 'ST-9901', 'pickup', 1
            )
        $statement$,
        'SET CONSTRAINTS ALL IMMEDIATE'
    ],
    'route stop visit_no values must remain contiguous from 1 through N'
);

SET CONSTRAINTS ALL DEFERRED;

SELECT pg_temp.validate_route_load_contract(
    ARRAY['pickup', 'dropoff'],
    ARRAY[5, 3],
    20
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        SELECT pg_temp.validate_route_load_contract(
            ARRAY['dropoff'],
            ARRAY[1],
            20
        )
    $statement$],
    'a dropoff-first route must violate the initial-zero prefix load contract'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        SELECT pg_temp.validate_route_load_contract(
            ARRAY['pickup'],
            ARRAY[21],
            20
        )
    $statement$],
    'a route prefix above truck capacity 20 must be rejected'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        UPDATE rebalance_route
           SET route_status_cd = 'completed',
               dispatched_dttm = clock_timestamp(),
               completed_dttm = clock_timestamp()
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
    $statement$],
    'a proposed route must not skip directly to completed'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        UPDATE rebalance_route
           SET route_status_cd = 'dispatched',
               dispatched_dttm = proposed_dttm - INTERVAL '1 second'
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
    $statement$],
    'dispatched_dttm before proposed_dttm must be rejected'
);

UPDATE rebalance_route
   SET route_status_cd = 'dispatched',
       dispatched_dttm = clock_timestamp()
 WHERE route_id = UUID '00000000-0000-0000-0000-000000000100';

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        UPDATE rebalance_route_stop
           SET bike_cnt = bike_cnt + 1
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
           AND visit_no = 1
    $statement$],
    'stops of a dispatched route must be immutable'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        UPDATE rebalance_route
           SET dispatched_dttm = dispatched_dttm + INTERVAL '1 second'
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
    $statement$],
    'a lifecycle timestamp must not change without a status transition'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        UPDATE rebalance_route
           SET route_status_cd = 'proposed',
               dispatched_dttm = NULL
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
    $statement$],
    'a dispatched route must not regress to proposed'
);

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        UPDATE rebalance_route
           SET route_status_cd = 'completed',
               completed_dttm = dispatched_dttm - INTERVAL '1 second'
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
    $statement$],
    'completed_dttm before dispatched_dttm must be rejected'
);

UPDATE rebalance_route
   SET route_status_cd = 'completed',
       completed_dttm = clock_timestamp()
 WHERE route_id = UUID '00000000-0000-0000-0000-000000000100';

SELECT pg_temp.expect_rejected(
    ARRAY[$statement$
        DELETE FROM rebalance_route
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
    $statement$],
    'completed route history must not be deleted'
);

SELECT pg_temp.assert_true(
    (
        SELECT route_status_cd = 'completed'
               AND dispatched_dttm IS NOT NULL
               AND completed_dttm >= dispatched_dttm
          FROM rebalance_route
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
    )
    AND (
        SELECT array_agg(visit_no ORDER BY visit_no) = ARRAY[1, 2]::SMALLINT[]
          FROM rebalance_route_stop
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000100'
    ),
    'failed route edge mutations must preserve the valid terminal aggregate'
);

SET CONSTRAINTS ALL IMMEDIATE;

ROLLBACK;

-- ROLLBACK 뒤에는 helper까지 사라지므로 독립 DO block으로 target/state 0행을 확인한다.
DO $$
DECLARE
    target_table_nm TEXT;
    target_row_cnt BIGINT;
BEGIN
    FOREACH target_table_nm IN ARRAY ARRAY[
        'weather_grid',
        'dispatch_center',
        'station',
        'station_stock',
        'station_demand_forecast',
        'weather_forecast',
        'event',
        'station_urgency',
        'rebalance_route',
        'rebalance_route_stop'
    ]
    LOOP
        EXECUTE format('SELECT count(*) FROM public.%I', target_table_nm)
           INTO target_row_cnt;
        IF target_row_cnt <> 0 THEN
            RAISE EXCEPTION
                'edge validation rollback leaked % row(s) into %',
                target_row_cnt,
                target_table_nm;
        END IF;
    END LOOP;

    SELECT count(*) INTO target_row_cnt FROM gold_meta.publication_state;
    IF target_row_cnt <> 0 THEN
        RAISE EXCEPTION
            'edge validation rollback leaked % publication_state row(s)',
            target_row_cnt;
    END IF;
END;
$$;

\echo 'PASS: Gold target edge validation completed; final target rows=0 after ROLLBACK.'
