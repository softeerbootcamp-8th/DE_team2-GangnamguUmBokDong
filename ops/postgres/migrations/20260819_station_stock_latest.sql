-- 운영 RDS의 station_stock을 대여소별 최신 1건 구조로 전환한다.
-- 실행 전 기존 loader를 중지하고, 실행 후 새 loader를 배포해야 한다.
BEGIN;

LOCK TABLE station_stock IN ACCESS EXCLUSIVE MODE;

DO $migration$
DECLARE
    primary_key_name TEXT;
    primary_key_definition TEXT;
BEGIN
    SELECT conname, pg_get_constraintdef(oid)
      INTO primary_key_name, primary_key_definition
      FROM pg_constraint
     WHERE conrelid = 'station_stock'::regclass
       AND contype = 'p';

    IF primary_key_definition = 'PRIMARY KEY (sta_id)' THEN
        RAISE NOTICE 'station_stock은 이미 sta_id 단일 PK입니다.';
        RETURN;
    END IF;

    IF primary_key_definition IS DISTINCT FROM 'PRIMARY KEY (sta_id, observed_at)' THEN
        RAISE EXCEPTION
            '예상하지 못한 station_stock PK입니다: %',
            coalesce(primary_key_definition, '없음');
    END IF;

    DELETE FROM station_stock stock
          WHERE stock.observed_at < (
              SELECT max(observed_at)
                FROM station_stock
               WHERE sta_id = stock.sta_id
          );

    EXECUTE format(
        'ALTER TABLE station_stock DROP CONSTRAINT %I',
        primary_key_name
    );
    ALTER TABLE station_stock ADD PRIMARY KEY (sta_id);
END
$migration$;

COMMIT;

