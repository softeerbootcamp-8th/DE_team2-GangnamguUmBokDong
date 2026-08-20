\set ON_ERROR_STOP on

-- #129 Gold 목표 스키마의 격리 검증 스크립트.
-- 전제: target-schema.sql을 비어 있는 PostgreSQL 16 + PostGIS DB에 적용한 직후 실행한다.
-- 모든 fixture와 임시 함수는 마지막 ROLLBACK으로 제거한다.

BEGIN;

SET LOCAL TIME ZONE 'UTC';
SET LOCAL search_path TO public, pg_temp;

CREATE OR REPLACE FUNCTION pg_temp.assert_true(
    assertion BOOLEAN,
    assertion_desc TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF assertion IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'validation assertion failed: %', assertion_desc;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pg_temp.expect_error(
    statement_sql TEXT,
    expected_sqlstate TEXT,
    expected_constraint_nm TEXT,
    assertion_desc TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    actual_sqlstate TEXT;
    actual_constraint_nm TEXT;
BEGIN
    BEGIN
        EXECUTE statement_sql;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                actual_sqlstate = RETURNED_SQLSTATE,
                actual_constraint_nm = CONSTRAINT_NAME;

            IF actual_sqlstate <> expected_sqlstate THEN
                RAISE EXCEPTION
                    'validation assertion failed: % (expected SQLSTATE %, got %)',
                    assertion_desc,
                    expected_sqlstate,
                    actual_sqlstate;
            END IF;

            IF expected_constraint_nm IS NOT NULL
               AND COALESCE(actual_constraint_nm, '') <> expected_constraint_nm THEN
                RAISE EXCEPTION
                    'validation assertion failed: % (expected constraint %, got %)',
                    assertion_desc,
                    expected_constraint_nm,
                    COALESCE(actual_constraint_nm, '<none>');
            END IF;

            RETURN;
    END;

    RAISE EXCEPTION
        'validation assertion failed: % (statement unexpectedly succeeded)',
        assertion_desc;
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
        PERFORM pg_temp.assert_true(
            to_regclass('public.' || target_table_nm) IS NOT NULL,
            format('target table %I must exist', target_table_nm)
        );

        EXECUTE format('SELECT count(*) FROM %I', target_table_nm)
           INTO target_row_cnt;
        PERFORM pg_temp.assert_true(
            target_row_cnt = 0,
            format('target table %I must be empty before validation', target_table_nm)
        );
    END LOOP;

    FOREACH target_table_nm IN ARRAY ARRAY[
        'gu_master',
        'dong_master',
        'event_spot',
        'weather_observation'
    ]
    LOOP
        PERFORM pg_temp.assert_true(
            to_regclass('public.' || target_table_nm) IS NULL,
            format('non-serving table %I must not be in Gold', target_table_nm)
        );
    END LOOP;
END;
$$;

SELECT pg_temp.assert_true(
    (
        SELECT array_agg(c.relname ORDER BY c.relname) = ARRAY[
            'dispatch_center',
            'event',
            'rebalance_route',
            'rebalance_route_stop',
            'station',
            'station_demand_forecast',
            'station_stock',
            'station_urgency',
            'weather_forecast',
            'weather_grid'
        ]::NAME[]
          FROM pg_class AS c
         WHERE c.relnamespace = 'public'::REGNAMESPACE
           AND c.relkind = 'r'
           AND c.relname <> 'spatial_ref_sys'
    ),
    'Gold must contain exactly the ten serving tables'
);

SELECT pg_temp.assert_true(
    to_regclass('gold_meta.publication_state') IS NOT NULL
    AND (
        SELECT array_agg(c.relname ORDER BY c.relname) = ARRAY['publication_state']::NAME[]
          FROM pg_class AS c
         WHERE c.relnamespace = 'gold_meta'::REGNAMESPACE
           AND c.relkind = 'r'
    ),
    'gold_meta must contain exactly one publication control table'
);

SELECT pg_temp.expect_error(
    $statement$
        SELECT gold_meta.claim_publication(
            'station_typo',
            TIMESTAMPTZ '2026-08-19 00:00:00+00',
            0,
            's3://fixture/unknown.json',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            1
        )
    $statement$,
    '23514',
    NULL,
    'an unregistered publication key must be rejected'
);

SELECT pg_temp.expect_error(
    $statement$
        SELECT gold_meta.claim_publication(
            'station_stock',
            TIMESTAMPTZ '2026-08-19 00:00:00+00',
            0,
            's3://fixture/bad-hash.json',
            'not-a-sha256',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            1
        )
    $statement$,
    '23514',
    NULL,
    'invalid publication fingerprint metadata must be rejected'
);

SELECT pg_temp.expect_error(
    $statement$
        SELECT gold_meta.claim_publication(
            'station_stock',
            'infinity'::TIMESTAMPTZ,
            0,
            's3://fixture/infinite-time.json',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            1
        )
    $statement$,
    '23514',
    NULL,
    'an infinite publication logical time must be rejected'
);

SELECT pg_temp.expect_error(
    $statement$
        SELECT gold_meta.claim_publication(
            'station_stock',
            TIMESTAMPTZ '2099-01-01 00:00:00+00',
            0,
            's3://fixture/future-time.json',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            1
        )
    $statement$,
    '23514',
    NULL,
    'a far-future publication logical time must be rejected'
);

SELECT pg_temp.assert_true(
    gold_meta.claim_publication(
        'station_stock',
        TIMESTAMPTZ '2026-08-19 01:00:00+00',
        0,
        's3://fixture/stock-v0.json',
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        1
    ),
    'first publication version must be claimed'
);

SELECT pg_temp.assert_true(
    NOT gold_meta.claim_publication(
        'station_stock',
        TIMESTAMPTZ '2026-08-19 01:00:00+00',
        0,
        's3://fixture/stock-v0-copy.json',
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        1
    ),
    'same publication identity must be a no-op'
);

SELECT pg_temp.assert_true(
    NOT gold_meta.claim_publication(
        'station_stock',
        TIMESTAMPTZ '2026-08-19 00:00:00+00',
        99,
        's3://fixture/stock-stale.json',
        'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
        'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
        1
    ),
    'an older logical time must stay stale even with a higher revision'
);

SELECT pg_temp.expect_error(
    $statement$
        SELECT gold_meta.claim_publication(
            'station_stock',
            TIMESTAMPTZ '2026-08-19 01:00:00+00',
            0,
            's3://fixture/stock-mutated.json',
            'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            1
        )
    $statement$,
    '23514',
    NULL,
    'same publication version with different content must fail'
);

SELECT pg_temp.assert_true(
    gold_meta.claim_publication(
        'station_stock',
        TIMESTAMPTZ '2026-08-19 01:00:00+00',
        1,
        's3://fixture/stock-correction.json',
        'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
        'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
        2
    ),
    'a higher explicit revision at the same logical time must publish a correction'
);

SELECT pg_temp.assert_true(
    gold_meta.claim_publication(
        'station_stock',
        TIMESTAMPTZ '2026-08-19 02:00:00+00',
        0,
        's3://fixture/stock-next.json',
        'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
        'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
        1
    ),
    'a newer logical time may reset its revision'
);

SELECT pg_temp.expect_error(
    $statement$
        DELETE FROM gold_meta.publication_state
         WHERE publication_key = 'station_stock'
    $statement$,
    '23514',
    NULL,
    'publication state tombstones must not be deletable'
);

SELECT pg_temp.expect_error(
    $statement$
        UPDATE gold_meta.publication_state
           SET publication_key = 'station'
         WHERE publication_key = 'station_stock'
    $statement$,
    '23514',
    NULL,
    'publication keys must be immutable'
);

DO $$
DECLARE
    target_failure_rejected BOOLEAN := false;
BEGIN
    BEGIN
        PERFORM gold_meta.claim_publication(
            'event:performance_event',
            TIMESTAMPTZ '2026-08-19 03:00:00+00',
            0,
            's3://fixture/event-rollback.json',
            '5555555555555555555555555555555555555555555555555555555555555555',
            '6666666666666666666666666666666666666666666666666666666666666666',
            1
        );

        INSERT INTO event (
            event_id, event_source_cd, source_event_id, event_name,
            event_point, event_point_source_cd, location_accuracy_cd,
            event_start_dt, event_end_dt, last_seen_dttm
        )
        VALUES (
            'performance_event:rollback-fixture',
            'performance_event', 'rollback-fixture', '롤백 검증 행사',
            ST_SetSRID(ST_MakePoint(127.0000, 37.5000), 4326),
            'source_reported', 'source_reported',
            DATE '2026-08-19', DATE '2026-08-20',
            TIMESTAMPTZ '2026-08-19 03:00:00+00'
        );
    EXCEPTION
        WHEN check_violation THEN
            target_failure_rejected := true;
    END;

    PERFORM pg_temp.assert_true(
        target_failure_rejected
        AND NOT EXISTS (
            SELECT 1
              FROM gold_meta.publication_state
             WHERE publication_key = 'event:performance_event'
        ),
        'target validation failure must roll back the publication state claim'
    );
END;
$$;

SELECT pg_temp.assert_true(
    EXISTS (
        SELECT 1
          FROM pg_extension
         WHERE extname = 'postgis'
    ),
    'PostGIS extension must be installed'
);

SELECT pg_temp.assert_true(
    (
        SELECT type = 'POINT' AND srid = 4326
          FROM geometry_columns
         WHERE f_table_schema = 'public'
           AND f_table_name = 'station'
           AND f_geometry_column = 'sta_point'
    ),
    'station.sta_point must be geometry(Point, 4326)'
);

SELECT pg_temp.assert_true(
    (
        SELECT type = 'POINT' AND srid = 4326
          FROM geometry_columns
         WHERE f_table_schema = 'public'
           AND f_table_name = 'event'
           AND f_geometry_column = 'event_point'
    ),
    'event.event_point must be geometry(Point, 4326)'
);

SELECT pg_temp.assert_true(
    (
        SELECT type = 'POINT' AND srid = 4326
          FROM geometry_columns
         WHERE f_table_schema = 'public'
           AND f_table_name = 'dispatch_center'
           AND f_geometry_column = 'dispatch_center_point'
    ),
    'dispatch_center.dispatch_center_point must be geometry(Point, 4326)'
);

SELECT pg_temp.assert_true(
    lower(encode(
        ST_AsEWKB(
            ST_Force2D(ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326)),
            'XDR'
        ),
        'hex'
    )) = '0020000001000010e6405fc000000000004042c00000000000',
    'canonical Point EWKB must match the cross-runtime fingerprint test vector'
);

SELECT pg_temp.assert_true(
    (
        SELECT count(*) = 3
          FROM pg_indexes
         WHERE schemaname = 'public'
           AND indexname IN (
               'station_point_geography_gix',
               'event_point_geography_gix',
               'dispatch_center_point_geography_gix'
           )
           AND indexdef LIKE '%USING gist%'
           AND indexdef LIKE '%::geography%'
    ),
    'all meter-distance Point columns must have matching geography GiST indexes'
);

INSERT INTO weather_grid (
    weather_grid_id,
    weather_grid_x_no,
    weather_grid_y_no,
    created_dttm,
    updated_dttm
)
VALUES (
    '62_126',
    62,
    126,
    TIMESTAMPTZ '2000-01-01 00:00:00+00',
    TIMESTAMPTZ '2000-01-02 00:00:00+00'
);

SELECT pg_temp.assert_true(
    (
        SELECT created_dttm >= transaction_timestamp()
               AND updated_dttm = created_dttm
          FROM weather_grid
         WHERE weather_grid_id = '62_126'
    ),
    'DB-owned metadata must replace caller-supplied INSERT timestamps'
);

DELETE FROM weather_grid WHERE weather_grid_id = '62_126';

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
    'fixture_center',
    '검증용 배송센터',
    ST_SetSRID(ST_MakePoint(127.0000, 37.5000), 4326),
    'verified_site',
    'target-schema-validation fixture',
    DATE '2026-08-19',
    true
), (
    'fixture_center_alt',
    '검증용 대체 배송센터',
    ST_SetSRID(ST_MakePoint(127.0020, 37.5000), 4326),
    'verified_site',
    'target-schema-validation fixture',
    DATE '2026-08-19',
    true
), (
    'fixture_center_inactive',
    '검증용 비활성 배송센터',
    ST_SetSRID(ST_MakePoint(127.0040, 37.5000), 4326),
    'verified_site',
    'target-schema-validation fixture',
    DATE '2026-08-19',
    false
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO dispatch_center (
            dispatch_center_id, dispatch_center_nm, dispatch_center_point,
            location_accuracy_cd, location_source_desc, location_verified_dt
        )
        VALUES (
            'fixture_center_infinite_date', '무한 검증일 센터',
            ST_SetSRID(ST_MakePoint(127.0060, 37.5000), 4326),
            'verified_site', 'target-schema-validation fixture', 'infinity'::DATE
        )
    $statement$,
    '23514',
    'dispatch_center_verified_dt_ck',
    'an infinite center verification date must be rejected'
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
    last_seen_dttm
)
VALUES (
    'ST-9001',
    '검증용 대여소',
    '서울특별시 검증로 1',
    20,
    ST_SetSRID(ST_MakePoint(127.0005, 37.5000), 4326),
    'bike_station_master',
    '61_126',
    'fixture_center',
    TIMESTAMPTZ '2026-08-19 00:00:00+00',
    TIMESTAMPTZ '2026-08-19 00:05:00+00'
);

SELECT pg_temp.assert_true(
    (
        SELECT ST_SRID(sta_point) = 4326
               AND ST_GeometryType(sta_point) = 'ST_Point'
          FROM station
         WHERE sta_id = 'ST-9001'
    ),
    'stored station geometry must remain Point/4326'
);

SELECT pg_temp.assert_true(
    (
        SELECT ST_DWithin(
                   station.sta_point::geography,
                   dispatch_center.dispatch_center_point::geography,
                   100
               )
               AND NOT ST_DWithin(
                   station.sta_point::geography,
                   dispatch_center.dispatch_center_point::geography,
                   10
               )
          FROM station
          JOIN dispatch_center USING (dispatch_center_id)
         WHERE station.sta_id = 'ST-9001'
    ),
    'geography distance must be measured in meters'
);

DO $$
DECLARE
    inactive_center_assignment_rejected BOOLEAN := false;
BEGIN
    BEGIN
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
            last_seen_dttm
        )
        VALUES (
            'ST-9004',
            '비활성 센터 배정 오류 대여소',
            '서울특별시 검증로 4',
            10,
            ST_SetSRID(ST_MakePoint(127.0035, 37.5000), 4326),
            'bike_station_master',
            '61_126',
            'fixture_center_inactive',
            TIMESTAMPTZ '2026-08-19 00:00:00+00',
            TIMESTAMPTZ '2026-08-19 00:05:00+00'
        );

        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION
        WHEN check_violation THEN
            inactive_center_assignment_rejected := true;
    END;

    PERFORM pg_temp.assert_true(
        inactive_center_assignment_rejected,
        'active station must not reference an inactive dispatch center'
    );
    PERFORM pg_temp.assert_true(
        NOT EXISTS (SELECT 1 FROM station WHERE sta_id = 'ST-9004'),
        'rejected inactive-center assignment must leave no station row'
    );
    SET CONSTRAINTS ALL DEFERRED;
END;
$$;

-- 지연 제약이므로 센터 비활성화와 대여소 재배정을 한 트랜잭션에서 원자 적용할 수 있다.
UPDATE dispatch_center
   SET is_active = false
 WHERE dispatch_center_id = 'fixture_center';

UPDATE station
   SET dispatch_center_id = 'fixture_center_alt'
 WHERE sta_id = 'ST-9001';

SET CONSTRAINTS ALL IMMEDIATE;

SELECT pg_temp.assert_true(
    (
        SELECT NOT source_center.is_active
               AND station.dispatch_center_id = 'fixture_center_alt'
          FROM station
          JOIN dispatch_center AS source_center
            ON source_center.dispatch_center_id = 'fixture_center'
         WHERE station.sta_id = 'ST-9001'
    ),
    'same-transaction center deactivation and station reassignment must succeed'
);

SET CONSTRAINTS ALL DEFERRED;

-- 이후 route fixture가 원래 센터를 사용하도록 동일한 원자 패턴으로 복원한다.
UPDATE dispatch_center
   SET is_active = true
 WHERE dispatch_center_id = 'fixture_center';

UPDATE station
   SET dispatch_center_id = 'fixture_center'
 WHERE sta_id = 'ST-9001';

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

INSERT INTO station (
    sta_id, sta_nm, sta_addr, hold_cnt, sta_point,
    sta_point_source_cd, weather_grid_id, dispatch_center_id,
    master_base_dttm, last_seen_dttm, is_active
)
VALUES (
    'ST-9005', '대체 센터 대여소', '서울특별시 검증로 5', 10,
    ST_SetSRID(ST_MakePoint(127.0021, 37.5000), 4326),
    'bike_station_master', '61_126', 'fixture_center_alt',
    TIMESTAMPTZ '2026-08-19 00:00:00+00',
    TIMESTAMPTZ '2026-08-19 00:05:00+00', true
), (
    'ST-9006', '비활성 대여소', '서울특별시 검증로 6', 10,
    ST_SetSRID(ST_MakePoint(127.0007, 37.5000), 4326),
    'bike_station_master', '61_126', 'fixture_center',
    TIMESTAMPTZ '2026-08-19 00:00:00+00',
    TIMESTAMPTZ '2026-08-19 00:05:00+00', false
);

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO station (
            sta_id, sta_nm, sta_addr, hold_cnt, sta_point,
            sta_point_source_cd, weather_grid_id, dispatch_center_id,
            master_base_dttm, last_seen_dttm
        )
        VALUES (
            'ST-9002', '거치대 오류 대여소', '서울특별시 검증로 2', 0,
            ST_SetSRID(ST_MakePoint(127.0010, 37.5000), 4326),
            'bike_station_master', '61_126', 'fixture_center',
            TIMESTAMPTZ '2026-08-19 00:00:00+00',
            TIMESTAMPTZ '2026-08-19 00:05:00+00'
        )
    $statement$,
    '23514',
    'station_hold_cnt_ck',
    'station hold_cnt=0 must be rejected'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO station (
            sta_id, sta_nm, sta_addr, hold_cnt, sta_point,
            sta_point_source_cd, weather_grid_id, dispatch_center_id,
            master_base_dttm, last_seen_dttm
        )
        VALUES (
            'ST-9003', '주소 오류 대여소', '   ', 10,
            ST_SetSRID(ST_MakePoint(127.0010, 37.5000), 4326),
            'bike_station_master', '61_126', 'fixture_center',
            TIMESTAMPTZ '2026-08-19 00:00:00+00',
            TIMESTAMPTZ '2026-08-19 00:05:00+00'
        )
    $statement$,
    '23514',
    'station_addr_ck',
    'blank station address must be rejected'
);

INSERT INTO station_stock (
    sta_id,
    base_dttm,
    parking_bike_tot_cnt
)
VALUES (
    'ST-9001',
    TIMESTAMPTZ '2026-08-19 02:00:00+00',
    10
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO station_stock (sta_id, base_dttm, parking_bike_tot_cnt)
        VALUES ('ST-9001', 'infinity'::TIMESTAMPTZ, 1)
        ON CONFLICT (sta_id) DO UPDATE
        SET base_dttm = EXCLUDED.base_dttm,
            parking_bike_tot_cnt = EXCLUDED.parking_bike_tot_cnt
    $statement$,
    '23514',
    'station_stock_base_dttm_ck',
    'an infinite stock observation time must be rejected'
);

-- 수집 시각이 오래된 writer는 최신 serving 행을 덮어쓰지 않아야 한다.
SELECT pg_temp.assert_true(
    gold_meta.claim_publication(
        'station_stock',
        TIMESTAMPTZ '2026-08-19 03:00:00+00',
        0,
        's3://fixture/stock-0300-v0.json',
        '1111111111111111111111111111111111111111111111111111111111111111',
        '2222222222222222222222222222222222222222222222222222222222222222',
        1
    ),
    'a new stock window must be claimed before target mutation'
);

INSERT INTO station_stock AS current_stock (
    sta_id,
    base_dttm,
    parking_bike_tot_cnt
)
VALUES (
    'ST-9001',
    TIMESTAMPTZ '2026-08-19 01:00:00+00',
    99
)
ON CONFLICT (sta_id) DO UPDATE
SET base_dttm = EXCLUDED.base_dttm,
    parking_bike_tot_cnt = EXCLUDED.parking_bike_tot_cnt
WHERE EXCLUDED.base_dttm > current_stock.base_dttm;

SELECT pg_temp.assert_true(
    (
        SELECT base_dttm = TIMESTAMPTZ '2026-08-19 02:00:00+00'
               AND parking_bike_tot_cnt = 10
          FROM station_stock
         WHERE sta_id = 'ST-9001'
    ),
    'stale stock upsert must not overwrite the latest serving row'
);

INSERT INTO station_stock AS current_stock (
    sta_id,
    base_dttm,
    parking_bike_tot_cnt
)
VALUES (
    'ST-9001',
    TIMESTAMPTZ '2026-08-19 03:00:00+00',
    11
)
ON CONFLICT (sta_id) DO UPDATE
SET base_dttm = EXCLUDED.base_dttm,
    parking_bike_tot_cnt = EXCLUDED.parking_bike_tot_cnt
WHERE EXCLUDED.base_dttm > current_stock.base_dttm;

SELECT pg_temp.assert_true(
    (
        SELECT base_dttm = TIMESTAMPTZ '2026-08-19 03:00:00+00'
               AND parking_bike_tot_cnt = 11
          FROM station_stock
         WHERE sta_id = 'ST-9001'
    ),
    'newer stock upsert must replace the latest serving row'
);

SELECT pg_temp.assert_true(
    gold_meta.claim_publication(
        'station_stock',
        TIMESTAMPTZ '2026-08-19 03:00:00+00',
        1,
        's3://fixture/stock-0300-v1.json',
        '3333333333333333333333333333333333333333333333333333333333333333',
        '4444444444444444444444444444444444444444444444444444444444444444',
        1
    ),
    'a higher revision must claim an equal-base stock correction'
);

-- claim이 correction을 승인한 경우에만 equal-base authoritative 값을 바꾼다.
UPDATE station_stock
   SET parking_bike_tot_cnt = 12
 WHERE sta_id = 'ST-9001'
   AND base_dttm = TIMESTAMPTZ '2026-08-19 03:00:00+00';

SELECT pg_temp.assert_true(
    (
        SELECT base_dttm = TIMESTAMPTZ '2026-08-19 03:00:00+00'
               AND parking_bike_tot_cnt = 12
          FROM station_stock
         WHERE sta_id = 'ST-9001'
    ),
    'an approved correction must replace an equal-base stock value'
);

-- 원천 horizon 1 구간은 base부터 1시간이며 Gold 대상시각은 그 구간 종료시각이다.
INSERT INTO station_demand_forecast (
    base_dttm,
    sta_id,
    predicted_dttm,
    predicted_rent_cnt,
    predicted_rtn_cnt
)
VALUES (
    TIMESTAMPTZ '2026-08-19 04:00:00+00',
    'ST-9001',
    TIMESTAMPTZ '2026-08-19 05:00:00+00',
    3,
    2
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO station_demand_forecast (
            base_dttm, sta_id, predicted_dttm,
            predicted_rent_cnt, predicted_rtn_cnt
        )
        VALUES (
            TIMESTAMPTZ '2026-08-19 04:00:00+00',
            'ST-9001',
            TIMESTAMPTZ '2026-08-19 04:00:00+00',
            1,
            1
        )
    $statement$,
    '23514',
    'station_demand_forecast_target_ck',
    'demand interval end must be strictly after base_dttm'
);

INSERT INTO weather_forecast (
    weather_grid_id,
    forecast_dttm,
    source_product_cd,
    base_dttm,
    sky_condition_cd,
    precipitation_type_cd,
    temperature
)
VALUES (
    '61_126',
    TIMESTAMPTZ '2026-08-19 05:00:00+00',
    'ultra_short',
    TIMESTAMPTZ '2026-08-19 04:30:00+00',
    'clear',
    'none',
    28.0
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO weather_forecast (
            weather_grid_id, forecast_dttm, source_product_cd, base_dttm,
            sky_condition_cd, precipitation_type_cd, temperature
        )
        VALUES (
            '61_126',
            TIMESTAMPTZ '2026-08-19 05:30:00+00',
            'short_term',
            TIMESTAMPTZ '2026-08-19 04:30:00+00',
            'clear',
            'none',
            27.0
        )
    $statement$,
    '23514',
    'weather_forecast_full_hour_ck',
    'non-hourly weather target must be rejected'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO weather_forecast (
            weather_grid_id, forecast_dttm, source_product_cd, base_dttm,
            sky_condition_cd, precipitation_type_cd, temperature
        )
        VALUES (
            '61_126',
            TIMESTAMPTZ '2026-08-19 06:00:00+00',
            'short_term',
            TIMESTAMPTZ '2026-08-19 04:00:00+00',
            'cloudy',
            'raindrop',
            27.0
        )
    $statement$,
    '23514',
    'weather_forecast_precipitation_type_ck',
    'product-specific precipitation codes must not cross products'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO weather_forecast (
            weather_grid_id, forecast_dttm, source_product_cd, base_dttm,
            sky_condition_cd, precipitation_type_cd, temperature
        )
        VALUES (
            '61_126',
            TIMESTAMPTZ '2026-08-19 05:00:00+00',
            'short_term',
            TIMESTAMPTZ '2026-08-19 04:00:00+00',
            'clear',
            'none',
            27.0
        )
    $statement$,
    '23505',
    'weather_forecast_pk',
    'Gold weather must have one canonical row per grid and target hour'
);

SELECT pg_temp.assert_true(
    (
        SELECT count(*) = 1
               AND min(source_product_cd) = 'ultra_short'
          FROM weather_forecast
         WHERE weather_grid_id = '61_126'
           AND forecast_dttm = TIMESTAMPTZ '2026-08-19 05:00:00+00'
    ),
    'weather source products must already be resolved before Gold publication'
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
    'cultural_event:v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    'cultural_event',
    'v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    '검증용 문화행사',
    '검증 광장',
    ST_SetSRID(ST_MakePoint(127.0008, 37.5002), 4326),
    'source_reported',
    'source_reported',
    DATE '2026-08-19',
    DATE '2026-08-20',
    TIMESTAMPTZ '2026-08-19 05:00:00+00'
);

SELECT pg_temp.assert_true(
    (
        SELECT ST_SRID(event_point) = 4326
               AND ST_GeometryType(event_point) = 'ST_Point'
          FROM event
         WHERE event_id = 'cultural_event:v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    ),
    'stored event geometry must remain Point/4326'
);

SET LOCAL enable_seqscan = off;

DO $$
DECLARE
    plan_line TEXT;
    plan_text TEXT := '';
BEGIN
    FOR plan_line IN EXECUTE $query$
        EXPLAIN (COSTS OFF)
        SELECT event_id
          FROM event
         WHERE ST_DWithin(
                   event_point::geography,
                   ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326)::geography,
                   1000.0
               )
    $query$
    LOOP
        plan_text := plan_text || E'\n' || plan_line;
    END LOOP;

    PERFORM pg_temp.assert_true(
        position('event_point_geography_gix' IN plan_text) > 0,
        'nearby-event geography predicate must use its expression GiST index'
    );

    plan_text := '';
    FOR plan_line IN EXECUTE $query$
        EXPLAIN (COSTS OFF)
        SELECT sta_id, predicted_dttm
          FROM station_demand_forecast
         WHERE predicted_dttm >= TIMESTAMPTZ '2026-08-19 04:00:00+00'
         ORDER BY predicted_dttm
    $query$
    LOOP
        plan_text := plan_text || E'\n' || plan_line;
    END LOOP;

    PERFORM pg_temp.assert_true(
        position('station_demand_forecast_predicted_dttm_idx' IN plan_text) > 0,
        'forecast target-time scan must use its time index'
    );
END;
$$;

SET LOCAL enable_seqscan = on;

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO event (
            event_id, event_source_cd, source_event_id, event_name,
            event_point, event_point_source_cd, location_accuracy_cd,
            event_start_dt, event_end_dt, last_seen_dttm
        )
        VALUES (
            'cultural_event:v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            'cultural_event',
            'v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            '좌표 없는 행사',
            NULL,
            'source_reported',
            'source_reported',
            DATE '2026-08-19',
            DATE '2026-08-20',
            TIMESTAMPTZ '2026-08-19 05:00:00+00'
        )
    $statement$,
    '23502',
    NULL,
    'event without a point must be rejected'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO event (
            event_id, event_source_cd, source_event_id, event_name,
            event_point, event_point_source_cd, location_accuracy_cd,
            event_start_dt, event_end_dt, last_seen_dttm
        )
        VALUES (
            'cultural_event:v1:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
            'cultural_event',
            'v1:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
            '좌표 출처 오류 행사',
            ST_SetSRID(ST_MakePoint(127.0008, 37.5002), 4326),
            'curated_osm_nominatim',
            'approximate',
            DATE '2026-08-19',
            DATE '2026-08-20',
            TIMESTAMPTZ '2026-08-19 05:00:00+00'
        )
    $statement$,
    '23514',
    'event_location_metadata_ck',
    'event source and point lineage must be a supported pair'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO event (
            event_id, event_source_cd, source_event_id, event_name,
            event_point, event_point_source_cd, location_accuracy_cd,
            event_start_dt, event_end_dt, last_seen_dttm
        )
        VALUES (
            'cultural_event:v1:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
            'cultural_event',
            'v1:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
            '무한 날짜 행사',
            ST_SetSRID(ST_MakePoint(127.0008, 37.5002), 4326),
            'source_reported', 'source_reported',
            DATE '2026-08-19', 'infinity'::DATE,
            TIMESTAMPTZ '2026-08-19 05:00:00+00'
        )
    $statement$,
    '23514',
    'event_date_ck',
    'an infinite event date must be rejected'
);

SELECT pg_temp.assert_true(
    gold_meta.claim_publication(
        'event:cultural_event',
        TIMESTAMPTZ '2026-08-19 05:00:00+00',
        0,
        's3://fixture/cultural-v0.json',
        '7777777777777777777777777777777777777777777777777777777777777777',
        '8888888888888888888888888888888888888888888888888888888888888888',
        1
    ),
    'a non-empty cultural event projection must be claimable'
);

SELECT pg_temp.assert_true(
    gold_meta.claim_publication(
        'event:cultural_event',
        TIMESTAMPTZ '2026-08-19 06:00:00+00',
        0,
        's3://fixture/cultural-empty.json',
        '9999999999999999999999999999999999999999999999999999999999999999',
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        0
    ),
    'a newer authoritative cultural EMPTY must advance its watermark'
);

DELETE FROM event WHERE event_source_cd = 'cultural_event';

SELECT pg_temp.assert_true(
    NOT gold_meta.claim_publication(
        'event:cultural_event',
        TIMESTAMPTZ '2026-08-19 05:00:00+00',
        99,
        's3://fixture/cultural-stale.json',
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
        1
    )
    AND NOT EXISTS (SELECT 1 FROM event WHERE event_source_cd = 'cultural_event'),
    'an older event artifact must not resurrect rows after authoritative EMPTY'
);

INSERT INTO station_urgency (
    sta_id,
    base_dttm,
    urgency_score,
    critical_remaining_min,
    rebalance_need_type_cd
)
VALUES (
    'ST-9001',
    TIMESTAMPTZ '2026-08-19 05:00:00+00',
    80.0,
    15,
    'supply_needed'
);

SELECT pg_temp.assert_true(
    (
        WITH staged(visit_no, action_cd, bike_cnt) AS (
            VALUES (1, 'pickup', 5), (2, 'dropoff', 3)
        ), running AS (
            SELECT sum(
                       CASE action_cd WHEN 'pickup' THEN bike_cnt ELSE -bike_cnt END
                   ) OVER (ORDER BY visit_no) AS bike_load
              FROM staged
        )
        SELECT bool_and(bike_load BETWEEN 0 AND 20) FROM running
    ),
    'publisher staging must accept a route whose running load stays within capacity'
);

SELECT pg_temp.assert_true(
    NOT (
        WITH staged(visit_no, action_cd, bike_cnt) AS (
            VALUES (1, 'dropoff', 1)
        ), running AS (
            SELECT sum(
                       CASE action_cd WHEN 'pickup' THEN bike_cnt ELSE -bike_cnt END
                   ) OVER (ORDER BY visit_no) AS bike_load
              FROM staged
        )
        SELECT bool_and(bike_load BETWEEN 0 AND 20) FROM running
    ),
    'publisher staging must reject a dropoff-first route from an empty truck'
);

SELECT pg_temp.assert_true(
    NOT (
        WITH staged(visit_no, action_cd, bike_cnt) AS (
            VALUES (1, 'pickup', 21)
        ), running AS (
            SELECT sum(
                       CASE action_cd WHEN 'pickup' THEN bike_cnt ELSE -bike_cnt END
                   ) OVER (ORDER BY visit_no) AS bike_load
              FROM staged
        )
        SELECT bool_and(bike_load BETWEEN 0 AND 20) FROM running
    ),
    'publisher staging must reject a route above the manifest truck capacity'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO rebalance_route (
            route_id,
            dispatch_center_id,
            route_status_cd,
            proposed_dttm,
            dispatched_dttm
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000010',
            'fixture_center',
            'dispatched',
            TIMESTAMPTZ '2026-08-19 05:30:00+00',
            TIMESTAMPTZ '2026-08-19 05:40:00+00'
        )
    $statement$,
    '23514',
    NULL,
    'a route must not be inserted directly in a non-proposed state'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO rebalance_route (
            route_id,
            dispatch_center_id,
            route_status_cd,
            proposed_dttm
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000011',
            'fixture_center_inactive',
            'proposed',
            TIMESTAMPTZ '2026-08-19 05:30:00+00'
        )
    $statement$,
    '23514',
    NULL,
    'a proposed route must require an active dispatch center'
);

INSERT INTO rebalance_route (
    route_id, dispatch_center_id, route_status_cd, proposed_dttm
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000013',
    'fixture_center',
    'proposed',
    TIMESTAMPTZ '2026-08-19 05:35:00+00'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO rebalance_route_stop (
            route_id, visit_no, sta_id, route_action_type_cd, bike_cnt
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000013',
            1, 'ST-9005', 'pickup', 1
        )
    $statement$,
    '23514',
    NULL,
    'a proposed stop must belong to the route dispatch center'
);

DELETE FROM rebalance_route
 WHERE route_id = UUID '00000000-0000-0000-0000-000000000013';

INSERT INTO rebalance_route (
    route_id, dispatch_center_id, route_status_cd, proposed_dttm
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000014',
    'fixture_center',
    'proposed',
    TIMESTAMPTZ '2026-08-19 05:36:00+00'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO rebalance_route_stop (
            route_id, visit_no, sta_id, route_action_type_cd, bike_cnt
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000014',
            1, 'ST-9006', 'pickup', 1
        )
    $statement$,
    '23514',
    NULL,
    'a proposed stop must reference an active station'
);

DELETE FROM rebalance_route
 WHERE route_id = UUID '00000000-0000-0000-0000-000000000014';

-- 같은 transaction에서 잠시 만든 aggregate를 지우면 queued deferred event도 최종 부재를 허용한다.
INSERT INTO rebalance_route (
    route_id,
    dispatch_center_id,
    route_status_cd,
    proposed_dttm
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000012',
    'fixture_center',
    'proposed',
    TIMESTAMPTZ '2026-08-19 05:40:00+00'
);

INSERT INTO rebalance_route_stop (
    route_id,
    visit_no,
    sta_id,
    route_action_type_cd,
    bike_cnt
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000012',
    1,
    'ST-9001',
    'pickup',
    1
);

DELETE FROM rebalance_route
 WHERE route_id = UUID '00000000-0000-0000-0000-000000000012';

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

SELECT pg_temp.assert_true(
    NOT EXISTS (
        SELECT 1
          FROM rebalance_route
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000012'
    ),
    'a proposed aggregate created and deleted in one transaction must leave no row'
);

-- aggregate의 header와 stop은 같은 트랜잭션에서 만든 뒤 지연 제약을 확인한다.
INSERT INTO rebalance_route (
    route_id,
    dispatch_center_id,
    route_status_cd,
    proposed_dttm
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000001',
    'fixture_center',
    'proposed',
    TIMESTAMPTZ '2026-08-19 06:00:00+00'
);

INSERT INTO rebalance_route_stop (
    route_id,
    visit_no,
    sta_id,
    route_action_type_cd,
    bike_cnt
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000001',
    1,
    'ST-9001',
    'pickup',
    3
);

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

SELECT pg_temp.assert_true(
    (
        SELECT route_id::TEXT = '00000000-0000-0000-0000-000000000001'
               AND pg_typeof(route_id::TEXT) = 'text'::REGTYPE
          FROM rebalance_route
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
    ),
    'route UUID must be explicitly castable to the API text contract'
);

SELECT pg_temp.expect_error(
    $statement$
        UPDATE rebalance_route
           SET route_status_cd = 'completed',
               dispatched_dttm = TIMESTAMPTZ '2026-08-19 06:10:00+00',
               completed_dttm = TIMESTAMPTZ '2026-08-19 06:20:00+00'
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
    $statement$,
    '23514',
    NULL,
    'proposed route must not skip directly to completed'
);

DO $$
DECLARE
    affected_row_cnt BIGINT;
BEGIN
    UPDATE rebalance_route
       SET route_status_cd = 'dispatched',
           dispatched_dttm = TIMESTAMPTZ '2026-08-19 06:10:00+00'
     WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
       AND route_status_cd = 'proposed';
    GET DIAGNOSTICS affected_row_cnt = ROW_COUNT;
    PERFORM pg_temp.assert_true(
        affected_row_cnt = 1,
        'current proposed route must transition to dispatched exactly once'
    );

    -- 이전 상태를 보고 쓰는 stale worker는 행을 갱신하지 못한다.
        UPDATE rebalance_route
           SET route_status_cd = 'dispatched',
               dispatched_dttm = TIMESTAMPTZ '2026-08-19 06:11:00+00'
     WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
       AND route_status_cd = 'proposed';
    GET DIAGNOSTICS affected_row_cnt = ROW_COUNT;
    PERFORM pg_temp.assert_true(
        affected_row_cnt = 0,
        'stale route status compare-and-set must affect zero rows'
    );
END;
$$;

SELECT pg_temp.expect_error(
    $statement$
        UPDATE rebalance_route
           SET route_status_cd = 'proposed',
               dispatched_dttm = NULL
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
    $statement$,
    '23514',
    NULL,
    'dispatched route must not regress to proposed'
);

SELECT pg_temp.expect_error(
    $statement$
        UPDATE rebalance_route
           SET dispatched_dttm = TIMESTAMPTZ '2026-08-19 06:12:00+00'
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
    $statement$,
    '23514',
    NULL,
    'lifecycle timestamp must be immutable without a state transition'
);

SELECT pg_temp.expect_error(
    $statement$
        INSERT INTO rebalance_route_stop (
            route_id, visit_no, sta_id, route_action_type_cd, bike_cnt
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000001',
            2,
            'ST-9001',
            'dropoff',
            1
        )
    $statement$,
    '23514',
    NULL,
    'route stops must be immutable after dispatch'
);

DO $$
DECLARE
    affected_row_cnt BIGINT;
BEGIN
    UPDATE rebalance_route
       SET route_status_cd = 'completed',
           completed_dttm = TIMESTAMPTZ '2026-08-19 06:20:00+00'
     WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
       AND route_status_cd = 'dispatched';
    GET DIAGNOSTICS affected_row_cnt = ROW_COUNT;
    PERFORM pg_temp.assert_true(
        affected_row_cnt = 1,
        'current dispatched route must transition to completed exactly once'
    );
END;
$$;

SELECT pg_temp.expect_error(
    $statement$
        DELETE FROM rebalance_route
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
    $statement$,
    '23514',
    NULL,
    'completed route history must not be deleted'
);

DO $$
DECLARE
    missing_stop_rejected BOOLEAN := false;
BEGIN
    BEGIN
        INSERT INTO rebalance_route (
            route_id,
            dispatch_center_id,
            route_status_cd,
            proposed_dttm
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000002',
            'fixture_center',
            'proposed',
            TIMESTAMPTZ '2026-08-19 07:00:00+00'
        );

        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION
        WHEN check_violation THEN
            missing_stop_rejected := true;
    END;

    PERFORM pg_temp.assert_true(
        missing_stop_rejected,
        'route header without a stop must fail when deferred constraints are checked'
    );
    SET CONSTRAINTS ALL DEFERRED;
END;
$$;

DO $$
DECLARE
    visit_gap_rejected BOOLEAN := false;
BEGIN
    BEGIN
        INSERT INTO rebalance_route (
            route_id,
            dispatch_center_id,
            route_status_cd,
            proposed_dttm
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000004',
            'fixture_center',
            'proposed',
            TIMESTAMPTZ '2026-08-19 07:30:00+00'
        );

        INSERT INTO rebalance_route_stop (
            route_id,
            visit_no,
            sta_id,
            route_action_type_cd,
            bike_cnt
        )
        VALUES (
            UUID '00000000-0000-0000-0000-000000000004',
            1,
            'ST-9001',
            'pickup',
            1
        ), (
            UUID '00000000-0000-0000-0000-000000000004',
            3,
            'ST-9001',
            'dropoff',
            1
        );

        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION
        WHEN check_violation THEN
            visit_gap_rejected := true;
    END;

    PERFORM pg_temp.assert_true(
        visit_gap_rejected,
        'route stop visit_no values must be contiguous from 1 through N'
    );
    PERFORM pg_temp.assert_true(
        NOT EXISTS (
            SELECT 1
              FROM rebalance_route
             WHERE route_id = UUID '00000000-0000-0000-0000-000000000004'
        ),
        'rejected visit gap must leave no partial route aggregate'
    );
    SET CONSTRAINTS ALL DEFERRED;
END;
$$;

INSERT INTO rebalance_route (
    route_id,
    dispatch_center_id,
    route_status_cd,
    proposed_dttm
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000003',
    'fixture_center',
    'proposed',
    TIMESTAMPTZ '2026-08-19 08:00:00+00'
);

INSERT INTO rebalance_route_stop (
    route_id,
    visit_no,
    sta_id,
    route_action_type_cd,
    bike_cnt
)
VALUES (
    UUID '00000000-0000-0000-0000-000000000003',
    1,
    'ST-9001',
    'pickup',
    2
);

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

DO $$
DECLARE
    station_move_rejected BOOLEAN := false;
BEGIN
    BEGIN
        UPDATE station
           SET dispatch_center_id = 'fixture_center_alt'
         WHERE sta_id = 'ST-9001';
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION
        WHEN check_violation THEN
            station_move_rejected := true;
    END;

    PERFORM pg_temp.assert_true(
        station_move_rejected,
        'a station in a proposed route must not move to another center without route cleanup'
    );
    SET CONSTRAINTS ALL DEFERRED;
END;
$$;

DO $$
DECLARE
    proposed_center_deactivation_rejected BOOLEAN := false;
BEGIN
    BEGIN
        UPDATE dispatch_center
           SET is_active = false
         WHERE dispatch_center_id = 'fixture_center';
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION
        WHEN check_violation THEN
            proposed_center_deactivation_rejected := true;
    END;

    PERFORM pg_temp.assert_true(
        proposed_center_deactivation_rejected,
        'a center with a proposed route must not deactivate without aggregate cleanup'
    );
    SET CONSTRAINTS ALL DEFERRED;
END;
$$;

SELECT pg_temp.expect_error(
    $statement$
        UPDATE rebalance_route_stop
           SET created_dttm = created_dttm + INTERVAL '1 second'
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000003'
           AND visit_no = 1
    $statement$,
    '23514',
    NULL,
    'route stop created_dttm must be immutable while proposed'
);

DO $$
DECLARE
    last_stop_delete_rejected BOOLEAN := false;
BEGIN
    BEGIN
        DELETE FROM rebalance_route_stop
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000003'
           AND visit_no = 1;

        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION
        WHEN check_violation THEN
            last_stop_delete_rejected := true;
    END;

    PERFORM pg_temp.assert_true(
        last_stop_delete_rejected,
        'deleting the last route stop must fail at deferred constraint check'
    );
    SET CONSTRAINTS ALL DEFERRED;
END;
$$;

SELECT pg_temp.assert_true(
    (
        SELECT count(*) = 1
          FROM rebalance_route_stop
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000003'
    ),
    'failed last-stop deletion must leave the aggregate unchanged'
);

SELECT pg_temp.assert_true(
    gold_meta.claim_publication(
        'rebalance_route',
        TIMESTAMPTZ '2026-08-19 09:00:00+00',
        0,
        's3://fixture/route-empty.json',
        'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
        'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
        0
    ),
    'an authoritative empty proposed route aggregate must be claimable'
);

DELETE FROM rebalance_route WHERE route_status_cd = 'proposed';
SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

SELECT pg_temp.assert_true(
    NOT EXISTS (
        SELECT 1 FROM rebalance_route WHERE route_status_cd = 'proposed'
    )
    AND EXISTS (
        SELECT 1
          FROM rebalance_route
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
           AND route_status_cd = 'completed'
    )
    AND EXISTS (
        SELECT 1
          FROM rebalance_route_stop
         WHERE route_id = UUID '00000000-0000-0000-0000-000000000001'
    ),
    'route EMPTY must clear proposed aggregates and preserve completed history with stops'
);

SELECT pg_temp.assert_true(
    NOT gold_meta.claim_publication(
        'rebalance_route',
        TIMESTAMPTZ '2026-08-19 08:00:00+00',
        99,
        's3://fixture/route-stale.json',
        'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
        'abababababababababababababababababababababababababababababababab',
        1
    ),
    'an older route artifact must not resurrect proposed routes after EMPTY'
);

SET CONSTRAINTS ALL IMMEDIATE;

ROLLBACK;
