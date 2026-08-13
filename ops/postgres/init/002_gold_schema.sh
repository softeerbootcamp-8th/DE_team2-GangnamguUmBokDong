#!/usr/bin/env bash
# 대시보드(apps/api)가 읽고, 예측 배치(ml/predict)가 쓰는 골드 테이블을 만든다.
# 001에서 만든 앱 DB($POSTGRES_APP_DB) 안에 생성한다.
#
# urgency_score, action_type, predicted_bikes, shared_rate 같은 파생값은 테이블에
# 없다. 재고·예측이 바뀔 때마다 같이 갱신해야 하는 별도 테이블을 두는 대신,
# apps/api가 요청 시점에 station_stock + forecast_points를 조합해 계산한다.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_APP_DB" <<-'EOSQL'

CREATE TABLE IF NOT EXISTS stations (
    sta_id      INTEGER PRIMARY KEY,
    sta_nm      TEXT NOT NULL,
    gu          TEXT NOT NULL,
    sta_addr    TEXT NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    hold_cnt    INTEGER NOT NULL
);

-- 대여소별 재고 관측 이력. 수집 파이프라인이 매 주기(예: 5분)마다 새 행을 추가한다
-- (upsert 아님, sta_id 기준 최신 한 줄이 아니라 계속 쌓인다). 예측 데이터가 나오기
-- 전(1시간 이내) 구간의 위험을 최근 재고 추세로 감지하는 데 필요해서 이력으로 둔다.
-- 보관 기간 정리는 아직 강제하지 않는다(필요해지면 정리 배치를 추가한다).
CREATE TABLE IF NOT EXISTS station_stock (
    sta_id                  INTEGER NOT NULL REFERENCES stations (sta_id),
    observed_at             TIMESTAMPTZ NOT NULL,
    parking_bike_tot_cnt    INTEGER NOT NULL,
    PRIMARY KEY (sta_id, observed_at)
);

-- 예측 배치(ml/predict, 5분 주기)가 갱신한다. (sta_id, predicted_dttm) 기준으로
-- upsert해서, 같은 미래 시각에 대한 예측은 항상 가장 최근 배치 결과 하나만 남는다.
CREATE TABLE IF NOT EXISTS forecast_points (
    sta_id                  INTEGER NOT NULL REFERENCES stations (sta_id),
    predicted_dttm          TIMESTAMPTZ NOT NULL,
    predicted_rent_cnt      INTEGER NOT NULL,
    predicted_return_cnt    INTEGER NOT NULL,
    batch_run_at            TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sta_id, predicted_dttm)
);

EOSQL
