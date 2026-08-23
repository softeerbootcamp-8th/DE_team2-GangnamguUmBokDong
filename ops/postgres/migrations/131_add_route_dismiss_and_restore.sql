-- #130 이후 Gold 볼륨에 작업 현황 소프트 삭제와 복제 되돌리기를 무손실로 추가한다.
BEGIN;

ALTER TABLE rebalance_route
    ADD COLUMN IF NOT EXISTS dismissed_dttm TIMESTAMPTZ;

ALTER TABLE rebalance_route
    ADD COLUMN IF NOT EXISTS restored_from_route_id UUID;

ALTER TABLE rebalance_route
    DROP CONSTRAINT IF EXISTS rebalance_route_restored_from_fk;

ALTER TABLE rebalance_route
    ADD CONSTRAINT rebalance_route_restored_from_fk
        FOREIGN KEY (restored_from_route_id)
        REFERENCES rebalance_route (route_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE rebalance_route
    DROP CONSTRAINT IF EXISTS rebalance_route_dismiss_ck;

ALTER TABLE rebalance_route
    ADD CONSTRAINT rebalance_route_dismiss_ck CHECK (
        dismissed_dttm IS NULL
        OR (isfinite(dismissed_dttm)
            AND route_status_cd IN ('completed', 'cancelled')
            AND dismissed_dttm >= COALESCE(completed_dttm, cancelled_dttm))
    );

ALTER TABLE rebalance_route
    DROP CONSTRAINT IF EXISTS rebalance_route_restored_from_self_ck;

ALTER TABLE rebalance_route
    ADD CONSTRAINT rebalance_route_restored_from_self_ck CHECK (
        restored_from_route_id IS NULL OR restored_from_route_id <> route_id
    );

CREATE OR REPLACE FUNCTION gold_validate_rebalance_route_mutation()
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

    IF OLD.route_status_cd = 'proposed' AND NEW.route_status_cd = 'dispatched' THEN
        IF NEW.dispatched_dttm IS NULL
           OR NEW.completed_dttm IS NOT NULL
           OR NEW.cancelled_dttm IS NOT NULL THEN
            RAISE EXCEPTION 'proposed -> dispatched requires only dispatched_dttm'
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

COMMIT;
