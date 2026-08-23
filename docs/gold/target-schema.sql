\set ON_ERROR_STOP on

-- #129 Gold 첫 운영 베이스라인.
-- 전제: 비어 있는 PostgreSQL 16 데이터베이스에서 실행한다. 기존 임시 스키마를
-- 변환하는 migration이 아니며, 어떤 테이블도 DROP하지 않는다.

BEGIN;

SET LOCAL search_path TO public, pg_catalog;

CREATE EXTENSION IF NOT EXISTS postgis;

DO $$
DECLARE
    existing_relation REGCLASS;
BEGIN
    SELECT c.oid::REGCLASS
      INTO existing_relation
      FROM pg_class AS c
      JOIN pg_namespace AS n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
       AND NOT EXISTS (
           SELECT 1
             FROM pg_depend AS d
             JOIN pg_extension AS e ON e.oid = d.refobjid
            WHERE d.classid = 'pg_class'::REGCLASS
              AND d.objid = c.oid
              AND d.deptype = 'e'
              AND e.extname = 'postgis'
       )
     ORDER BY c.relname
     LIMIT 1;

    IF existing_relation IS NOT NULL THEN
        RAISE EXCEPTION 'Gold baseline requires an empty target DB; relation already exists: %',
            existing_relation;
    END IF;
END;
$$;

CREATE FUNCTION gold_set_updated_dttm()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
BEGIN
    NEW.created_dttm := OLD.created_dttm;
    NEW.updated_dttm := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE FUNCTION gold_initialize_metadata_dttm()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
DECLARE
    metadata_dttm TIMESTAMPTZ := clock_timestamp();
BEGIN
    NEW.created_dttm := metadata_dttm;
    NEW.updated_dttm := metadata_dttm;
    RETURN NEW;
END;
$$;

CREATE FUNCTION gold_initialize_created_dttm()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
BEGIN
    NEW.created_dttm := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE SCHEMA gold_meta;
REVOKE ALL ON SCHEMA gold_meta FROM PUBLIC;

CREATE TABLE gold_meta.publication_state (
    publication_key TEXT PRIMARY KEY,
    logical_dttm TIMESTAMPTZ NOT NULL,
    revision_no INTEGER NOT NULL,
    manifest_uri TEXT NOT NULL,
    artifact_set_sha256 TEXT NOT NULL,
    input_fingerprint_sha256 TEXT NOT NULL,
    published_row_cnt BIGINT NOT NULL,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT publication_state_key_ck CHECK (
        publication_key IN (
            'weather_grid',
            'dispatch_center',
            'station',
            'station_stock',
            'station_demand_forecast',
            'weather_forecast',
            'event:cultural_event',
            'event:performance_event',
            'station_urgency',
            'rebalance_route'
        )
    ),
    CONSTRAINT publication_state_revision_ck CHECK (revision_no >= 0),
    CONSTRAINT publication_state_logical_dttm_ck CHECK (isfinite(logical_dttm)),
    CONSTRAINT publication_state_manifest_uri_ck CHECK (btrim(manifest_uri) <> ''),
    CONSTRAINT publication_state_artifact_sha256_ck CHECK (
        artifact_set_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT publication_state_input_sha256_ck CHECK (
        input_fingerprint_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT publication_state_row_cnt_ck CHECK (published_row_cnt >= 0),
    CONSTRAINT publication_state_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

REVOKE ALL ON TABLE gold_meta.publication_state FROM PUBLIC;

CREATE TRIGGER initialize_metadata_dttm
BEFORE INSERT ON gold_meta.publication_state
FOR EACH ROW EXECUTE FUNCTION gold_initialize_metadata_dttm();

CREATE TRIGGER set_updated_dttm
BEFORE UPDATE ON gold_meta.publication_state
FOR EACH ROW EXECUTE FUNCTION gold_set_updated_dttm();

CREATE FUNCTION gold_meta.protect_publication_state()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = gold_meta, public, pg_catalog
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'publication state rows cannot be deleted'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.publication_key IS DISTINCT FROM OLD.publication_key THEN
        RAISE EXCEPTION 'publication key is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.logical_dttm < OLD.logical_dttm
       OR (
           NEW.logical_dttm = OLD.logical_dttm
           AND NEW.revision_no <= OLD.revision_no
       ) THEN
        RAISE EXCEPTION 'publication version must advance monotonically'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER protect_publication_state
BEFORE UPDATE OR DELETE ON gold_meta.publication_state
FOR EACH ROW EXECUTE FUNCTION gold_meta.protect_publication_state();

CREATE FUNCTION gold_meta.claim_publication(
    incoming_publication_key TEXT,
    incoming_logical_dttm TIMESTAMPTZ,
    incoming_revision_no INTEGER,
    incoming_manifest_uri TEXT,
    incoming_artifact_set_sha256 TEXT,
    incoming_input_fingerprint_sha256 TEXT,
    incoming_published_row_cnt BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = gold_meta, public, pg_catalog
AS $$
DECLARE
    inserted_row_cnt BIGINT;
    current_state gold_meta.publication_state%ROWTYPE;
BEGIN
    IF incoming_publication_key IS NULL
       OR incoming_publication_key NOT IN (
           'weather_grid',
           'dispatch_center',
           'station',
           'station_stock',
           'station_demand_forecast',
           'weather_forecast',
           'event:cultural_event',
           'event:performance_event',
           'station_urgency',
           'rebalance_route'
       )
       OR incoming_logical_dttm IS NULL
       OR NOT isfinite(incoming_logical_dttm)
       OR incoming_logical_dttm > clock_timestamp() + INTERVAL '5 minutes'
       OR incoming_revision_no IS NULL OR incoming_revision_no < 0
       OR incoming_manifest_uri IS NULL OR btrim(incoming_manifest_uri) = ''
       OR incoming_artifact_set_sha256 IS NULL
       OR incoming_artifact_set_sha256 !~ '^[0-9a-f]{64}$'
       OR incoming_input_fingerprint_sha256 IS NULL
       OR incoming_input_fingerprint_sha256 !~ '^[0-9a-f]{64}$'
       OR incoming_published_row_cnt IS NULL OR incoming_published_row_cnt < 0 THEN
        RAISE EXCEPTION 'invalid publication claim metadata for %', incoming_publication_key
            USING ERRCODE = '23514';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('gold-publication:' || incoming_publication_key, 0)
    );

    INSERT INTO gold_meta.publication_state (
        publication_key,
        logical_dttm,
        revision_no,
        manifest_uri,
        artifact_set_sha256,
        input_fingerprint_sha256,
        published_row_cnt
    )
    VALUES (
        incoming_publication_key,
        incoming_logical_dttm,
        incoming_revision_no,
        incoming_manifest_uri,
        incoming_artifact_set_sha256,
        incoming_input_fingerprint_sha256,
        incoming_published_row_cnt
    )
    ON CONFLICT (publication_key) DO NOTHING;
    GET DIAGNOSTICS inserted_row_cnt = ROW_COUNT;

    IF inserted_row_cnt = 1 THEN
        RETURN true;
    END IF;

    SELECT *
      INTO STRICT current_state
      FROM gold_meta.publication_state
     WHERE publication_key = incoming_publication_key
       FOR UPDATE;

    IF incoming_logical_dttm < current_state.logical_dttm
       OR (
           incoming_logical_dttm = current_state.logical_dttm
           AND incoming_revision_no < current_state.revision_no
       ) THEN
        RETURN false;
    END IF;

    IF incoming_logical_dttm = current_state.logical_dttm
       AND incoming_revision_no = current_state.revision_no THEN
        IF incoming_artifact_set_sha256 <> current_state.artifact_set_sha256
           OR incoming_input_fingerprint_sha256 <> current_state.input_fingerprint_sha256
           OR incoming_published_row_cnt <> current_state.published_row_cnt THEN
            RAISE EXCEPTION 'same publication version has different content: % at revision %',
                incoming_publication_key,
                incoming_revision_no
                USING ERRCODE = '23514';
        END IF;
        RETURN false;
    END IF;

    UPDATE gold_meta.publication_state
       SET logical_dttm = incoming_logical_dttm,
           revision_no = incoming_revision_no,
           manifest_uri = incoming_manifest_uri,
           artifact_set_sha256 = incoming_artifact_set_sha256,
           input_fingerprint_sha256 = incoming_input_fingerprint_sha256,
           published_row_cnt = incoming_published_row_cnt
     WHERE publication_key = incoming_publication_key;

    RETURN true;
END;
$$;

REVOKE ALL ON FUNCTION gold_meta.claim_publication(
    TEXT, TIMESTAMPTZ, INTEGER, TEXT, TEXT, TEXT, BIGINT
) FROM PUBLIC;

CREATE FUNCTION gold_meta.lock_topology_shared()
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(129, 1);
END;
$$;

CREATE FUNCTION gold_meta.lock_topology_exclusive()
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(129, 1);
    PERFORM pg_advisory_xact_lock(129, 2);
END;
$$;

CREATE FUNCTION gold_meta.lock_route_operation()
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(129, 2);
END;
$$;

REVOKE ALL ON FUNCTION gold_meta.lock_topology_shared() FROM PUBLIC;
REVOKE ALL ON FUNCTION gold_meta.lock_topology_exclusive() FROM PUBLIC;
REVOKE ALL ON FUNCTION gold_meta.lock_route_operation() FROM PUBLIC;

CREATE TABLE weather_grid (
    weather_grid_id TEXT PRIMARY KEY,
    weather_grid_x_no SMALLINT NOT NULL,
    weather_grid_y_no SMALLINT NOT NULL,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT weather_grid_xy_uk UNIQUE (weather_grid_x_no, weather_grid_y_no),
    CONSTRAINT weather_grid_id_ck CHECK (
        weather_grid_id = weather_grid_x_no::TEXT || '_' || weather_grid_y_no::TEXT
    ),
    CONSTRAINT weather_grid_no_ck CHECK (
        weather_grid_x_no > 0 AND weather_grid_y_no > 0
    ),
    CONSTRAINT weather_grid_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

CREATE TABLE station (
    sta_id TEXT PRIMARY KEY,
    sta_nm TEXT NOT NULL,
    sta_addr TEXT NOT NULL,
    hold_cnt INTEGER NOT NULL,
    sta_point geometry(Point, 4326) NOT NULL,
    sta_point_source_cd TEXT NOT NULL,
    weather_grid_id TEXT NOT NULL,
    dispatch_center_id TEXT NOT NULL,
    master_base_dttm TIMESTAMPTZ NOT NULL,
    last_seen_dttm TIMESTAMPTZ NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT station_weather_grid_fk FOREIGN KEY (weather_grid_id)
        REFERENCES weather_grid (weather_grid_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT station_id_ck CHECK (sta_id ~ '^ST-[0-9]+$'),
    CONSTRAINT station_nm_ck CHECK (btrim(sta_nm) <> ''),
    CONSTRAINT station_addr_ck CHECK (btrim(sta_addr) <> ''),
    CONSTRAINT station_hold_cnt_ck CHECK (hold_cnt > 0),
    CONSTRAINT station_point_ck CHECK (
        NOT ST_IsEmpty(sta_point)
        AND ST_X(sta_point) BETWEEN 126.5 AND 127.5
        AND ST_Y(sta_point) BETWEEN 37.0 AND 38.0
    ),
    CONSTRAINT station_point_source_ck CHECK (
        sta_point_source_cd IN ('bike_station_master', 'bike_station_realtime_fallback')
    ),
    CONSTRAINT station_source_dttm_ck CHECK (
        isfinite(master_base_dttm) AND isfinite(last_seen_dttm)
    ),
    CONSTRAINT station_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

CREATE TABLE station_stock (
    sta_id TEXT PRIMARY KEY,
    base_dttm TIMESTAMPTZ NOT NULL,
    parking_bike_tot_cnt INTEGER NOT NULL,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT station_stock_station_fk FOREIGN KEY (sta_id)
        REFERENCES station (sta_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT station_stock_bike_cnt_ck CHECK (parking_bike_tot_cnt >= 0),
    CONSTRAINT station_stock_base_dttm_ck CHECK (isfinite(base_dttm)),
    CONSTRAINT station_stock_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

CREATE TABLE station_demand_forecast (
    base_dttm TIMESTAMPTZ NOT NULL,
    sta_id TEXT NOT NULL,
    predicted_dttm TIMESTAMPTZ NOT NULL,
    predicted_rent_cnt INTEGER NOT NULL,
    predicted_rtn_cnt INTEGER NOT NULL,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT station_demand_forecast_pk PRIMARY KEY (sta_id, predicted_dttm),
    CONSTRAINT station_demand_forecast_station_fk FOREIGN KEY (sta_id)
        REFERENCES station (sta_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT station_demand_forecast_target_ck CHECK (
        isfinite(base_dttm) AND isfinite(predicted_dttm)
        AND predicted_dttm > base_dttm
    ),
    CONSTRAINT station_demand_forecast_cnt_ck CHECK (
        predicted_rent_cnt >= 0 AND predicted_rtn_cnt >= 0
    ),
    CONSTRAINT station_demand_forecast_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

CREATE TABLE weather_forecast (
    weather_grid_id TEXT NOT NULL,
    forecast_dttm TIMESTAMPTZ NOT NULL,
    source_product_cd TEXT NOT NULL,
    base_dttm TIMESTAMPTZ NOT NULL,
    sky_condition_cd TEXT NOT NULL,
    precipitation_type_cd TEXT NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    precipitation_prob DOUBLE PRECISION,
    precipitation_amount DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT weather_forecast_pk PRIMARY KEY (weather_grid_id, forecast_dttm),
    CONSTRAINT weather_forecast_grid_fk FOREIGN KEY (weather_grid_id)
        REFERENCES weather_grid (weather_grid_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT weather_forecast_source_product_ck CHECK (
        source_product_cd IN ('ultra_short', 'short_term')
    ),
    CONSTRAINT weather_forecast_full_hour_ck CHECK (
        forecast_dttm = date_bin(
            INTERVAL '1 hour',
            forecast_dttm,
            TIMESTAMPTZ '1970-01-01 00:00:00+00'
        )
    ),
    CONSTRAINT weather_forecast_target_ck CHECK (
        isfinite(base_dttm) AND isfinite(forecast_dttm)
        AND forecast_dttm > base_dttm
    ),
    CONSTRAINT weather_forecast_sky_ck CHECK (
        sky_condition_cd IN ('clear', 'mostly_cloudy', 'cloudy')
    ),
    CONSTRAINT weather_forecast_precipitation_type_ck CHECK (
        (source_product_cd = 'short_term'
            AND precipitation_type_cd IN ('none', 'rain', 'rain_snow', 'snow', 'shower'))
        OR (source_product_cd = 'ultra_short'
            AND precipitation_type_cd IN (
                'none', 'rain', 'rain_snow', 'snow', 'raindrop',
                'raindrop_snow_flurry', 'snow_flurry'
            ))
    ),
    CONSTRAINT weather_forecast_temperature_ck CHECK (temperature BETWEEN -50 AND 50),
    CONSTRAINT weather_forecast_precipitation_prob_ck CHECK (
        precipitation_prob IS NULL OR precipitation_prob BETWEEN 0 AND 100
    ),
    CONSTRAINT weather_forecast_precipitation_amount_ck CHECK (
        precipitation_amount IS NULL
        OR (precipitation_amount >= 0 AND precipitation_amount < 'Infinity'::DOUBLE PRECISION)
    ),
    CONSTRAINT weather_forecast_humidity_ck CHECK (
        humidity IS NULL OR humidity BETWEEN 0 AND 100
    ),
    CONSTRAINT weather_forecast_wind_speed_ck CHECK (
        wind_speed IS NULL OR wind_speed BETWEEN 0 AND 50
    ),
    CONSTRAINT weather_forecast_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

CREATE TABLE event (
    event_id TEXT PRIMARY KEY,
    event_source_cd TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    event_spot_nm TEXT,
    event_point geometry(Point, 4326) NOT NULL,
    event_point_source_cd TEXT NOT NULL,
    location_accuracy_cd TEXT NOT NULL,
    event_start_dt DATE NOT NULL,
    event_end_dt DATE NOT NULL,
    last_seen_dttm TIMESTAMPTZ NOT NULL,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT event_source_uk UNIQUE (event_source_cd, source_event_id),
    CONSTRAINT event_source_ck CHECK (
        event_source_cd IN ('cultural_event', 'performance_event')
    ),
    CONSTRAINT event_id_ck CHECK (
        event_id = event_source_cd || ':' || source_event_id
        AND btrim(source_event_id) <> ''
        AND (
            event_source_cd <> 'cultural_event'
            OR source_event_id ~ '^v1:[0-9a-f]{64}$'
        )
    ),
    CONSTRAINT event_name_ck CHECK (btrim(event_name) <> ''),
    CONSTRAINT event_spot_nm_ck CHECK (event_spot_nm IS NULL OR btrim(event_spot_nm) <> ''),
    CONSTRAINT event_point_ck CHECK (
        NOT ST_IsEmpty(event_point)
        AND ST_X(event_point) BETWEEN 126.5 AND 127.5
        AND ST_Y(event_point) BETWEEN 37.0 AND 38.0
    ),
    CONSTRAINT event_location_metadata_ck CHECK (
        (event_source_cd = 'cultural_event'
            AND event_point_source_cd = 'source_reported'
            AND location_accuracy_cd = 'source_reported')
        OR (event_source_cd = 'performance_event'
            AND event_point_source_cd = 'curated_osm_nominatim'
            AND location_accuracy_cd = 'approximate')
    ),
    CONSTRAINT event_date_ck CHECK (
        isfinite(event_start_dt) AND isfinite(event_end_dt)
        AND event_end_dt >= event_start_dt
    ),
    CONSTRAINT event_last_seen_dttm_ck CHECK (isfinite(last_seen_dttm)),
    CONSTRAINT event_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

CREATE TABLE station_urgency (
    sta_id TEXT PRIMARY KEY,
    base_dttm TIMESTAMPTZ NOT NULL,
    urgency_score DOUBLE PRECISION NOT NULL,
    critical_remaining_min INTEGER NOT NULL,
    rebalance_need_type_cd TEXT NOT NULL,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT station_urgency_station_fk FOREIGN KEY (sta_id)
        REFERENCES station (sta_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT station_urgency_score_ck CHECK (urgency_score BETWEEN 0 AND 100),
    CONSTRAINT station_urgency_remaining_min_ck CHECK (critical_remaining_min >= 0),
    CONSTRAINT station_urgency_need_type_ck CHECK (
        rebalance_need_type_cd IN ('normal', 'supply_needed', 'retrieval_needed')
    ),
    CONSTRAINT station_urgency_base_dttm_ck CHECK (isfinite(base_dttm)),
    CONSTRAINT station_urgency_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

CREATE TABLE dispatch_center (
    dispatch_center_id TEXT PRIMARY KEY,
    dispatch_center_nm TEXT NOT NULL UNIQUE,
    dispatch_center_point geometry(Point, 4326) NOT NULL,
    location_accuracy_cd TEXT NOT NULL,
    location_source_desc TEXT NOT NULL,
    location_verified_dt DATE,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT dispatch_center_id_ck CHECK (dispatch_center_id ~ '^[a-z0-9_]+$'),
    CONSTRAINT dispatch_center_nm_ck CHECK (btrim(dispatch_center_nm) <> ''),
    CONSTRAINT dispatch_center_point_ck CHECK (
        NOT ST_IsEmpty(dispatch_center_point)
        AND ST_X(dispatch_center_point) BETWEEN 126.5 AND 127.5
        AND ST_Y(dispatch_center_point) BETWEEN 37.0 AND 38.0
    ),
    CONSTRAINT dispatch_center_accuracy_ck CHECK (
        location_accuracy_cd IN ('verified_site', 'landmark_approximation', 'administrative_centroid')
    ),
    CONSTRAINT dispatch_center_source_desc_ck CHECK (btrim(location_source_desc) <> ''),
    CONSTRAINT dispatch_center_verified_dt_ck CHECK (
        location_verified_dt IS NULL OR isfinite(location_verified_dt)
    ),
    CONSTRAINT dispatch_center_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

ALTER TABLE station
    ADD CONSTRAINT station_dispatch_center_fk FOREIGN KEY (dispatch_center_id)
    REFERENCES dispatch_center (dispatch_center_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE TABLE rebalance_route (
    route_id UUID PRIMARY KEY,
    dispatch_center_id TEXT NOT NULL,
    route_status_cd TEXT NOT NULL,
    proposed_dttm TIMESTAMPTZ NOT NULL,
    dispatched_dttm TIMESTAMPTZ,
    completed_dttm TIMESTAMPTZ,
    cancelled_dttm TIMESTAMPTZ,
    dismissed_dttm TIMESTAMPTZ,
    restored_from_route_id UUID,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT rebalance_route_dispatch_center_fk FOREIGN KEY (dispatch_center_id)
        REFERENCES dispatch_center (dispatch_center_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT rebalance_route_restored_from_fk FOREIGN KEY (restored_from_route_id)
        REFERENCES rebalance_route (route_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT rebalance_route_status_ck CHECK (
        (route_status_cd = 'proposed'
            AND dispatched_dttm IS NULL AND completed_dttm IS NULL
            AND cancelled_dttm IS NULL)
        OR (route_status_cd = 'dispatched'
            AND dispatched_dttm IS NOT NULL AND completed_dttm IS NULL
            AND cancelled_dttm IS NULL)
        OR (route_status_cd = 'completed'
            AND dispatched_dttm IS NOT NULL AND completed_dttm IS NOT NULL
            AND cancelled_dttm IS NULL)
        OR (route_status_cd = 'cancelled'
            AND dispatched_dttm IS NOT NULL AND completed_dttm IS NULL
            AND cancelled_dttm IS NOT NULL)
    ),
    CONSTRAINT rebalance_route_dttm_order_ck CHECK (
        isfinite(proposed_dttm)
        AND (dispatched_dttm IS NULL OR (
            isfinite(dispatched_dttm) AND dispatched_dttm >= proposed_dttm
        ))
        AND (completed_dttm IS NULL OR (
            isfinite(completed_dttm) AND completed_dttm >= dispatched_dttm
        ))
        AND (cancelled_dttm IS NULL OR (
            isfinite(cancelled_dttm) AND cancelled_dttm >= dispatched_dttm
        ))
    ),
    CONSTRAINT rebalance_route_dismiss_ck CHECK (
        dismissed_dttm IS NULL
        OR (isfinite(dismissed_dttm)
            AND route_status_cd IN ('completed', 'cancelled')
            AND dismissed_dttm >= COALESCE(completed_dttm, cancelled_dttm))
    ),
    CONSTRAINT rebalance_route_restored_from_self_ck CHECK (
        restored_from_route_id IS NULL OR restored_from_route_id <> route_id
    ),
    CONSTRAINT rebalance_route_metadata_dttm_ck CHECK (
        isfinite(created_dttm) AND isfinite(updated_dttm)
        AND updated_dttm >= created_dttm
    )
);

CREATE TABLE rebalance_route_stop (
    route_id UUID NOT NULL,
    visit_no SMALLINT NOT NULL,
    sta_id TEXT NOT NULL,
    route_action_type_cd TEXT NOT NULL,
    bike_cnt INTEGER NOT NULL,
    created_dttm TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT rebalance_route_stop_pk PRIMARY KEY (route_id, visit_no),
    CONSTRAINT rebalance_route_stop_route_fk FOREIGN KEY (route_id)
        REFERENCES rebalance_route (route_id) ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT rebalance_route_stop_station_fk FOREIGN KEY (sta_id)
        REFERENCES station (sta_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT rebalance_route_stop_visit_no_ck CHECK (visit_no > 0),
    CONSTRAINT rebalance_route_stop_action_ck CHECK (
        route_action_type_cd IN ('pickup', 'dropoff')
    ),
    CONSTRAINT rebalance_route_stop_bike_cnt_ck CHECK (bike_cnt > 0),
    CONSTRAINT rebalance_route_stop_created_dttm_ck CHECK (isfinite(created_dttm))
);

CREATE INDEX station_weather_grid_id_idx
    ON station (weather_grid_id);
CREATE INDEX station_dispatch_center_id_idx
    ON station (dispatch_center_id);
CREATE INDEX station_point_geography_gix
    ON station USING GIST ((sta_point::geography));
CREATE INDEX station_demand_forecast_batch_idx
    ON station_demand_forecast (base_dttm DESC);
CREATE INDEX station_demand_forecast_predicted_dttm_idx
    ON station_demand_forecast (predicted_dttm);
CREATE INDEX weather_forecast_target_idx
    ON weather_forecast (forecast_dttm);
CREATE INDEX event_point_geography_gix
    ON event USING GIST ((event_point::geography));
CREATE INDEX event_end_dt_idx
    ON event (event_end_dt);
CREATE INDEX event_last_seen_dttm_idx
    ON event (event_source_cd, last_seen_dttm);
CREATE INDEX station_urgency_base_score_idx
    ON station_urgency (base_dttm DESC, urgency_score DESC);
CREATE INDEX dispatch_center_point_geography_gix
    ON dispatch_center USING GIST ((dispatch_center_point::geography));
CREATE INDEX rebalance_route_center_status_proposed_idx
    ON rebalance_route (dispatch_center_id, route_status_cd, proposed_dttm DESC);
CREATE INDEX rebalance_route_status_proposed_idx
    ON rebalance_route (route_status_cd, proposed_dttm DESC);
CREATE INDEX rebalance_route_proposed_dttm_idx
    ON rebalance_route (proposed_dttm DESC);
CREATE INDEX rebalance_route_stop_station_idx
    ON rebalance_route_stop (sta_id);
-- 과거 복제 방식으로 생성된 데이터의 무결성을 유지하기 위한 호환 index다.
CREATE UNIQUE INDEX rebalance_route_restore_open_uk
    ON rebalance_route (restored_from_route_id)
 WHERE restored_from_route_id IS NOT NULL
   AND route_status_cd = 'proposed';

DO $$
DECLARE
    target_table REGCLASS;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'weather_grid'::REGCLASS,
        'station'::REGCLASS,
        'station_stock'::REGCLASS,
        'station_demand_forecast'::REGCLASS,
        'weather_forecast'::REGCLASS,
        'event'::REGCLASS,
        'station_urgency'::REGCLASS,
        'dispatch_center'::REGCLASS,
        'rebalance_route'::REGCLASS
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER initialize_metadata_dttm BEFORE INSERT ON %s '
            'FOR EACH ROW EXECUTE FUNCTION gold_initialize_metadata_dttm()',
            target_table
        );
        EXECUTE format(
            'CREATE TRIGGER set_updated_dttm BEFORE UPDATE ON %s '
            'FOR EACH ROW EXECUTE FUNCTION gold_set_updated_dttm()',
            target_table
        );
    END LOOP;
END;
$$;

CREATE TRIGGER initialize_created_dttm
BEFORE INSERT ON rebalance_route_stop
FOR EACH ROW EXECUTE FUNCTION gold_initialize_created_dttm();

CREATE FUNCTION gold_lock_topology_write()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    -- Statement trigger에서 row lock보다 먼저 topology -> route-operation 순서로 잠근다.
    PERFORM pg_advisory_xact_lock(129, 1);
    PERFORM pg_advisory_xact_lock(129, 2);
    RETURN NULL;
END;
$$;

CREATE FUNCTION gold_lock_route_write()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    -- Route writer는 topology snapshot을 고정한 뒤 route operation을 직렬화한다.
    PERFORM pg_advisory_xact_lock_shared(129, 1);
    PERFORM pg_advisory_xact_lock(129, 2);
    RETURN NULL;
END;
$$;

CREATE FUNCTION gold_validate_rebalance_route_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
DECLARE
    current_center_active BOOLEAN;
    invalid_stop_exists BOOLEAN;
BEGIN
    IF NEW.route_id IS DISTINCT FROM OLD.route_id
       OR NEW.dispatch_center_id IS DISTINCT FROM OLD.dispatch_center_id
       OR NEW.proposed_dttm IS DISTINCT FROM OLD.proposed_dttm
       OR NEW.restored_from_route_id IS DISTINCT FROM OLD.restored_from_route_id THEN
        RAISE EXCEPTION 'rebalance route identity, center, proposal time, and restore origin are immutable'
            USING ERRCODE = '23514';
    END IF;

    -- dismissed_dttm은 상태 전이 없이 바뀌는 유일한 컬럼이다. 같은 상태일 때만 허용한다.
    IF OLD.route_status_cd = NEW.route_status_cd THEN
        IF ROW(NEW.dispatched_dttm, NEW.completed_dttm, NEW.cancelled_dttm)
           IS DISTINCT FROM
           ROW(OLD.dispatched_dttm, OLD.completed_dttm, OLD.cancelled_dttm) THEN
            RAISE EXCEPTION 'rebalance route lifecycle timestamps cannot change without a status transition'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    -- 완료·취소 처리와 목록 삭제가 한 UPDATE에 섞이지 않게 막는다.
    IF NEW.dismissed_dttm IS DISTINCT FROM OLD.dismissed_dttm THEN
        RAISE EXCEPTION 'rebalance route dismissal cannot change during a status transition'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.route_status_cd = 'dispatched'
       AND OLD.route_status_cd IN ('proposed', 'cancelled') THEN
        IF OLD.route_status_cd = 'proposed' THEN
            IF NEW.dispatched_dttm IS NULL
               OR NEW.completed_dttm IS NOT NULL
               OR NEW.cancelled_dttm IS NOT NULL THEN
                RAISE EXCEPTION 'proposed -> dispatched requires only dispatched_dttm'
                    USING ERRCODE = '23514';
            END IF;
        ELSIF NEW.dispatched_dttm IS DISTINCT FROM OLD.dispatched_dttm
              OR NEW.completed_dttm IS NOT NULL
              OR NEW.cancelled_dttm IS NOT NULL THEN
            RAISE EXCEPTION 'cancelled -> dispatched must preserve dispatched_dttm and clear cancelled_dttm'
                USING ERRCODE = '23514';
        END IF;

        SELECT is_active
          INTO current_center_active
          FROM dispatch_center
         WHERE dispatch_center_id = NEW.dispatch_center_id;

        SELECT EXISTS (
            SELECT 1
              FROM rebalance_route_stop AS rs
              LEFT JOIN station AS s ON s.sta_id = rs.sta_id
             WHERE rs.route_id = NEW.route_id
               AND (
                   s.sta_id IS NULL
                   OR NOT s.is_active
                   OR s.dispatch_center_id <> NEW.dispatch_center_id
               )
        )
          INTO invalid_stop_exists;

        IF NOT COALESCE(current_center_active, false)
           OR invalid_stop_exists
           OR NOT EXISTS (
               SELECT 1
                 FROM rebalance_route_stop
                WHERE route_id = NEW.route_id
           ) THEN
            RAISE EXCEPTION 'dispatch requires an active center and active same-center stops'
                USING ERRCODE = '23514';
        END IF;
    ELSIF OLD.route_status_cd = 'dispatched' AND NEW.route_status_cd = 'completed' THEN
        IF NEW.dispatched_dttm IS DISTINCT FROM OLD.dispatched_dttm
           OR NEW.completed_dttm IS NULL
           OR NEW.cancelled_dttm IS NOT NULL THEN
            RAISE EXCEPTION 'dispatched -> completed must preserve dispatched_dttm and set completed_dttm'
                USING ERRCODE = '23514';
        END IF;
    ELSIF OLD.route_status_cd = 'dispatched' AND NEW.route_status_cd = 'cancelled' THEN
        IF NEW.dispatched_dttm IS DISTINCT FROM OLD.dispatched_dttm
           OR NEW.completed_dttm IS NOT NULL
           OR NEW.cancelled_dttm IS NULL THEN
            RAISE EXCEPTION 'dispatched -> cancelled must preserve dispatched_dttm and set cancelled_dttm'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid rebalance route transition: % -> %',
            OLD.route_status_cd, NEW.route_status_cd
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE FUNCTION gold_validate_rebalance_route_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
DECLARE
    center_active BOOLEAN;
BEGIN
    IF NEW.route_status_cd <> 'proposed' THEN
        RAISE EXCEPTION 'a rebalance route must be inserted as proposed'
            USING ERRCODE = '23514';
    END IF;
    SELECT is_active
      INTO center_active
      FROM dispatch_center
     WHERE dispatch_center_id = NEW.dispatch_center_id
       FOR UPDATE;

    IF NOT FOUND OR NOT center_active THEN
        RAISE EXCEPTION 'a proposed rebalance route requires an active dispatch center'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION gold_validate_station_center_assignment()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
DECLARE
    current_station_active BOOLEAN;
    current_dispatch_center_id TEXT;
    current_center_active BOOLEAN;
BEGIN
    SELECT s.is_active, s.dispatch_center_id, dc.is_active
      INTO current_station_active, current_dispatch_center_id, current_center_active
      FROM station AS s
      JOIN dispatch_center AS dc
        ON dc.dispatch_center_id = s.dispatch_center_id
     WHERE s.sta_id = NEW.sta_id
       FOR UPDATE OF dc;

    IF FOUND AND current_station_active AND NOT current_center_active THEN
        RAISE EXCEPTION 'an active station requires an active dispatch center'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM rebalance_route_stop AS rs
          JOIN rebalance_route AS r ON r.route_id = rs.route_id
         WHERE rs.sta_id = NEW.sta_id
           AND r.route_status_cd = 'proposed'
           AND (
               NOT current_station_active
               OR r.dispatch_center_id <> current_dispatch_center_id
           )
    ) THEN
        RAISE EXCEPTION 'a proposed route requires active stops assigned to its dispatch center'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION gold_validate_dispatch_center_deactivation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM dispatch_center AS dc
          JOIN station AS s
            ON s.dispatch_center_id = dc.dispatch_center_id
         WHERE dc.dispatch_center_id = NEW.dispatch_center_id
           AND NOT dc.is_active
           AND s.is_active
    ) THEN
        RAISE EXCEPTION 'an inactive dispatch center cannot retain active stations'
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM rebalance_route
         WHERE dispatch_center_id = NEW.dispatch_center_id
           AND route_status_cd = 'proposed'
           AND NOT NEW.is_active
    ) THEN
        RAISE EXCEPTION 'an inactive dispatch center cannot retain proposed routes'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION gold_protect_rebalance_route_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
BEGIN
    IF OLD.route_status_cd <> 'proposed' THEN
        RAISE EXCEPTION 'only proposed rebalance routes may be deleted'
            USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END;
$$;

CREATE FUNCTION gold_protect_rebalance_route_stop_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
DECLARE
    parent_route_id UUID;
    parent_status_cd TEXT;
    parent_dispatch_center_id TEXT;
    stop_station_active BOOLEAN;
    stop_dispatch_center_id TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        parent_route_id := NEW.route_id;
    ELSE
        parent_route_id := OLD.route_id;
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.route_id IS DISTINCT FROM OLD.route_id THEN
        RAISE EXCEPTION 'rebalance route stop cannot move between routes'
            USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.created_dttm IS DISTINCT FROM OLD.created_dttm THEN
        RAISE EXCEPTION 'rebalance route stop creation time is immutable'
            USING ERRCODE = '23514';
    END IF;

    SELECT route_status_cd, dispatch_center_id
      INTO parent_status_cd, parent_dispatch_center_id
      FROM rebalance_route
     WHERE route_id = parent_route_id
       FOR UPDATE;

    IF FOUND AND parent_status_cd <> 'proposed' THEN
        RAISE EXCEPTION 'stops of a non-proposed rebalance route are immutable'
            USING ERRCODE = '23514';
    ELSIF NOT FOUND AND TG_OP <> 'DELETE' THEN
        RAISE EXCEPTION 'parent rebalance route % does not exist', parent_route_id
            USING ERRCODE = '23503';
    END IF;

    IF TG_OP <> 'DELETE' THEN
        SELECT is_active, dispatch_center_id
          INTO stop_station_active, stop_dispatch_center_id
          FROM station
         WHERE sta_id = NEW.sta_id;

        IF NOT FOUND
           OR NOT stop_station_active
           OR stop_dispatch_center_id <> parent_dispatch_center_id THEN
            RAISE EXCEPTION 'a proposed route stop requires an active station in the same dispatch center'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION gold_ensure_rebalance_route_has_stop()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
DECLARE
    stop_cnt BIGINT;
    min_visit_no SMALLINT;
    max_visit_no SMALLINT;
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM rebalance_route
         WHERE route_id = NEW.route_id
    ) THEN
        RETURN NULL;
    END IF;

    SELECT count(*), min(visit_no), max(visit_no)
      INTO stop_cnt, min_visit_no, max_visit_no
      FROM rebalance_route_stop
     WHERE route_id = NEW.route_id;

    IF stop_cnt = 0
       OR min_visit_no <> 1
       OR max_visit_no <> stop_cnt THEN
        RAISE EXCEPTION 'rebalance route % must contain contiguous stops numbered 1..N', NEW.route_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION gold_ensure_rebalance_route_keeps_stop()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_catalog
AS $$
DECLARE
    route_to_check UUID;
    stop_cnt BIGINT;
    min_visit_no SMALLINT;
    max_visit_no SMALLINT;
BEGIN
    FOREACH route_to_check IN ARRAY CASE
        WHEN TG_OP = 'INSERT' THEN ARRAY[NEW.route_id]
        WHEN TG_OP = 'DELETE' THEN ARRAY[OLD.route_id]
        ELSE ARRAY[OLD.route_id, NEW.route_id]
    END
    LOOP
        IF EXISTS (SELECT 1 FROM rebalance_route WHERE route_id = route_to_check) THEN
            SELECT count(*), min(visit_no), max(visit_no)
              INTO stop_cnt, min_visit_no, max_visit_no
              FROM rebalance_route_stop
             WHERE route_id = route_to_check;

            IF stop_cnt = 0
               OR min_visit_no <> 1
               OR max_visit_no <> stop_cnt THEN
                RAISE EXCEPTION 'rebalance route % must contain contiguous stops numbered 1..N',
                    route_to_check
                    USING ERRCODE = '23514';
            END IF;
        END IF;
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER validate_rebalance_route_insert
BEFORE INSERT ON rebalance_route
FOR EACH ROW EXECUTE FUNCTION gold_validate_rebalance_route_insert();

CREATE TRIGGER lock_topology_write
BEFORE INSERT OR UPDATE OR DELETE ON weather_grid
FOR EACH STATEMENT EXECUTE FUNCTION gold_lock_topology_write();

CREATE TRIGGER lock_topology_write
BEFORE INSERT OR UPDATE OR DELETE ON dispatch_center
FOR EACH STATEMENT EXECUTE FUNCTION gold_lock_topology_write();

CREATE TRIGGER lock_topology_write
BEFORE INSERT OR UPDATE OR DELETE ON station
FOR EACH STATEMENT EXECUTE FUNCTION gold_lock_topology_write();

CREATE TRIGGER lock_route_write
BEFORE INSERT OR UPDATE OR DELETE ON rebalance_route
FOR EACH STATEMENT EXECUTE FUNCTION gold_lock_route_write();

CREATE TRIGGER lock_route_write
BEFORE INSERT OR UPDATE OR DELETE ON rebalance_route_stop
FOR EACH STATEMENT EXECUTE FUNCTION gold_lock_route_write();

CREATE CONSTRAINT TRIGGER validate_station_center_assignment
AFTER INSERT OR UPDATE ON station
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION gold_validate_station_center_assignment();

CREATE CONSTRAINT TRIGGER validate_dispatch_center_deactivation
AFTER INSERT OR UPDATE ON dispatch_center
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION gold_validate_dispatch_center_deactivation();

CREATE TRIGGER validate_rebalance_route_mutation
BEFORE UPDATE ON rebalance_route
FOR EACH ROW EXECUTE FUNCTION gold_validate_rebalance_route_mutation();

CREATE TRIGGER protect_rebalance_route_delete
BEFORE DELETE ON rebalance_route
FOR EACH ROW EXECUTE FUNCTION gold_protect_rebalance_route_delete();

CREATE TRIGGER protect_rebalance_route_stop_mutation
BEFORE INSERT OR UPDATE OR DELETE ON rebalance_route_stop
FOR EACH ROW EXECUTE FUNCTION gold_protect_rebalance_route_stop_mutation();

CREATE CONSTRAINT TRIGGER rebalance_route_has_stop
AFTER INSERT OR UPDATE ON rebalance_route
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION gold_ensure_rebalance_route_has_stop();

CREATE CONSTRAINT TRIGGER rebalance_route_keeps_stop
AFTER INSERT OR DELETE OR UPDATE ON rebalance_route_stop
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION gold_ensure_rebalance_route_keeps_stop();

COMMIT;
