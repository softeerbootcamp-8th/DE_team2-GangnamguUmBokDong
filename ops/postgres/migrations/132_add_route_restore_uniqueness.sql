-- #131 이후 되돌리기 중복을 막는다. 원본 하나당 "아직 승인되지 않은 복제본"은 하나만 남긴다.
BEGIN;

-- 인덱스를 만들기 전에 기존 중복을 확인한다. 후보를 자동으로 지우지 않고 멈춘다.
-- 삭제는 운영자 판단이 필요한 데이터 손실이라 migration이 대신 결정하지 않는다.
DO $$
DECLARE
    duplicated_origins TEXT;
BEGIN
    SELECT string_agg(DISTINCT restored_from_route_id::text, ', ')
      INTO duplicated_origins
      FROM rebalance_route
     WHERE restored_from_route_id IS NOT NULL
       AND route_status_cd = 'proposed'
       AND restored_from_route_id IN (
           SELECT restored_from_route_id
             FROM rebalance_route
            WHERE restored_from_route_id IS NOT NULL
              AND route_status_cd = 'proposed'
            GROUP BY restored_from_route_id
           HAVING count(*) > 1
       );

    IF duplicated_origins IS NOT NULL THEN
        RAISE EXCEPTION
            'proposed 상태 복제본이 중복된 원본이 있습니다: %. 남길 후보 하나만 두고 다시 실행하세요.',
            duplicated_origins
            USING ERRCODE = '23505';
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS rebalance_route_restore_open_uk
    ON rebalance_route (restored_from_route_id)
 WHERE restored_from_route_id IS NOT NULL
   AND route_status_cd = 'proposed';

COMMIT;
