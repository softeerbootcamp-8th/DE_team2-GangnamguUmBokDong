#!/usr/bin/env bash
# station_urgency: 대여소별 재배치 긴급도(urgency_score) 배치 계산 결과.
# 002에서 만든 앱 DB($POSTGRES_APP_DB) 안에 생성한다.
#
# urgency_score는 더 이상 apps/api가 요청 시점에 계산하지 않는다 — 5분 배치
# (rebalance/)가 계산해 loader가 sta_id 기준 최신 1건만 upsert한다(이유: #107).
# bike_qty(실제로 옮겨야 할 대수)는 재배치 라우트 생성(#109)이 필요로 해서
# 추가됐다 — 아직 이 스키마로 실 데이터가 쌓이지 않아 새 마이그레이션 파일
# 대신 이 파일을 직접 고쳤다(ADR 0002 원칙).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_APP_DB" <<-'EOSQL'

CREATE TABLE IF NOT EXISTS station_urgency (
    sta_id                   TEXT PRIMARY KEY REFERENCES stations (sta_id),
    urgency_score            DOUBLE PRECISION NOT NULL,
    minutes_until_critical   INTEGER NOT NULL,
    action_type              TEXT NOT NULL,
    bike_qty                 INTEGER NOT NULL,
    batch_run_at             TIMESTAMPTZ NOT NULL
);

EOSQL
