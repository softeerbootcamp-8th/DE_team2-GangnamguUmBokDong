#!/usr/bin/env bash
# station_urgency: 대여소별 재배치 긴급도(urgency_score) 배치 계산 결과.
# 002에서 만든 앱 DB($POSTGRES_APP_DB) 안에 생성한다.
#
# urgency_score는 더 이상 apps/api가 요청 시점에 계산하지 않는다 — 5분 배치
# (rebalance/)가 계산해 loader가 (batch_run_at, sta_id) snapshot 이력으로 적재한다.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_APP_DB" <<-'EOSQL'

CREATE TABLE IF NOT EXISTS station_urgency (
    sta_id                   TEXT NOT NULL REFERENCES stations (sta_id),
    urgency_score            DOUBLE PRECISION NOT NULL,
    minutes_until_critical   INTEGER NOT NULL,
    action_type              TEXT NOT NULL,
    batch_run_at             TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (batch_run_at, sta_id)
);

-- PR 초기 버전의 기존 볼륨에는 sta_id 단일 PK가 이미 있을 수 있다. Compose의
-- postgres-schema-init가 기동마다 이 스크립트를 실행하므로 볼륨 삭제 없이 이력형
-- PK로 전환한다. 이미 복합 PK인 경우에는 아무 작업도 하지 않는다.
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

    IF primary_key_definition IS DISTINCT FROM 'PRIMARY KEY (batch_run_at, sta_id)' THEN
        IF primary_key_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE station_urgency DROP CONSTRAINT %I', primary_key_name);
        END IF;
        ALTER TABLE station_urgency ADD PRIMARY KEY (batch_run_at, sta_id);
    END IF;
END
$migration$;

EOSQL
