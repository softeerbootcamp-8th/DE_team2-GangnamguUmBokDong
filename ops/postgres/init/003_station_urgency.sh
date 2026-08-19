#!/usr/bin/env bash
# station_urgency: 대여소별 재배치 긴급도(urgency_score) 배치 계산 결과.
# 002에서 만든 앱 DB($POSTGRES_APP_DB) 안에 생성한다.
#
# urgency_score는 더 이상 apps/api가 요청 시점에 계산하지 않는다 — 5분 배치
# (rebalance/)가 계산해 loader가 sta_id 기준 최신 1건만 upsert한다(이유: #124 —
# 계산은 매번 S3에서 새로 하고, 과거 배치 값을 읽는 소비자도 없어 이력을 RDS에
# 남길 필요가 없다. 원본 이력은 rebalance/main.py가 S3에 이미 영구 저장한다).
# bike_qty(실제로 옮겨야 할 대수)는 재배치 라우트 생성(#109)이 필요로 해서 추가됐다.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_APP_DB" <<-'EOSQL'

CREATE TABLE IF NOT EXISTS station_urgency (
    sta_id                   TEXT PRIMARY KEY REFERENCES stations (sta_id),
    urgency_score            DOUBLE PRECISION NOT NULL,
    minutes_until_critical   INTEGER NOT NULL,
    action_type              TEXT NOT NULL,
    bike_qty                 INTEGER NOT NULL DEFAULT 0,
    batch_run_at             TIMESTAMPTZ NOT NULL
);

-- 한때 (batch_run_at, sta_id) 복합 PK로 이력을 쌓던 버전이 존재했다(#107 리뷰
-- 중 도입, #124로 되돌림). 그 버전의 볼륨을 가진 환경은 sta_id당 여러 행이 남아
-- 있을 수 있어 단순 PK 재설정이 안 되므로, 대여소별 가장 최신 batch_run_at
-- 행만 남기고 나머지를 지운 뒤 단일 PK로 전환한다. 이미 단일 PK면 아무 작업도
-- 하지 않는다.
DO $migration$
DECLARE
    primary_key_name TEXT;
    primary_key_definition TEXT;
BEGIN
    SELECT conname, pg_get_constraintdef(oid)
      INTO primary_key_name, primary_key_definition
      FROM pg_constraint
     WHERE conrelid = 'station_urgency'::regclass
       AND contype = 'p';

    IF primary_key_definition IS DISTINCT FROM 'PRIMARY KEY (sta_id)' THEN
        DELETE FROM station_urgency u
              WHERE u.batch_run_at < (
                  SELECT max(batch_run_at) FROM station_urgency WHERE sta_id = u.sta_id
              );
        IF primary_key_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE station_urgency DROP CONSTRAINT %I', primary_key_name);
        END IF;
        ALTER TABLE station_urgency ADD PRIMARY KEY (sta_id);
    END IF;
END
$migration$;

-- bike_qty가 추가되기 전(#109 이전) 볼륨에는 이 컬럼이 없다. CREATE TABLE IF NOT
-- EXISTS는 기존 테이블에 컬럼을 더해주지 않으므로 별도로 채운다. DEFAULT 0은
-- 컬럼 자체의 NOT NULL 제약을 만족시키기 위한 임시값일 뿐 — 다음 배치 upsert가
-- 곧바로 실제 값으로 덮어쓴다.
ALTER TABLE station_urgency ADD COLUMN IF NOT EXISTS bike_qty INTEGER NOT NULL DEFAULT 0;

EOSQL
