#!/usr/bin/env bash
# rebalance_routes/rebalance_route_stops: 권역별 재배치 라우트 생성 배치
# (rebalance/routes.py) 결과. 003에서 만든 앱 DB($POSTGRES_APP_DB) 안에 생성한다.
#
# route_id는 배치가 매번 새로 생성하는 UUID라 upsert 충돌이 사실상 없다. 그래도
# 혹시 재실행으로 충돌하면 DO NOTHING이어야 한다 — 운영자가 대시보드에서 이미
# 바꿔놓은 status(#110이 다룸)를 배치 재실행이 실수로 덮어쓰면 안 되기 때문이다.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_APP_DB" <<-'EOSQL'

CREATE TABLE IF NOT EXISTS rebalance_routes (
    route_id        TEXT PRIMARY KEY,
    region          TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'proposed'
                        CHECK (status IN ('proposed', 'dispatched', 'completed', 'cancelled')),
    proposed_at     TIMESTAMPTZ NOT NULL,
    dispatched_at   TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS rebalance_route_stops (
    route_id        TEXT NOT NULL REFERENCES rebalance_routes (route_id),
    visit_order     INTEGER NOT NULL,
    sta_id          TEXT NOT NULL REFERENCES stations (sta_id),
    action          TEXT NOT NULL CHECK (action IN ('pickup', 'dropoff')),
    bike_cnt        INTEGER NOT NULL,
    PRIMARY KEY (route_id, visit_order)
);

EOSQL
