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
    sta_id      TEXT PRIMARY KEY,
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
    sta_id                  TEXT NOT NULL REFERENCES stations (sta_id),
    observed_at             TIMESTAMPTZ NOT NULL,
    parking_bike_tot_cnt    INTEGER NOT NULL,
    PRIMARY KEY (sta_id, observed_at)
);

-- 예측 배치(ml/predict, 5분 주기)가 갱신한다. (sta_id, predicted_dttm) 기준으로
-- upsert해서, 같은 미래 시각에 대한 예측은 항상 가장 최근 배치 결과 하나만 남는다.
CREATE TABLE IF NOT EXISTS forecast_points (
    sta_id                  TEXT NOT NULL REFERENCES stations (sta_id),
    predicted_dttm          TIMESTAMPTZ NOT NULL,
    predicted_rent_cnt      INTEGER NOT NULL,
    predicted_return_cnt    INTEGER NOT NULL,
    batch_run_at            TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sta_id, predicted_dttm)
);

-- 기상청 초단기 실황(현재 날씨). loader가 자치구별 최신 관측 1건만 upsert한다.
CREATE TABLE IF NOT EXISTS weather_current (
    gu              TEXT NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL,
    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    wind_speed      DOUBLE PRECISION,
    rainfall        DOUBLE PRECISION,
    pty_type        INTEGER,
    PRIMARY KEY (gu)
);

-- 기상청 단기예보(미래 날씨). 동일 (gu, forecast_dttm)에 대해 가장 최근 발표만 upsert로 남긴다.
-- 대시보드에서 상세 기상 수치(습도, 풍속, 강수량)와 발표시각까지 표시 가능하다.
CREATE TABLE IF NOT EXISTS weather_forecast (
    gu                   TEXT NOT NULL,
    forecast_dttm        TIMESTAMPTZ NOT NULL,
    sky_cond             INTEGER,
    pty_type             INTEGER,
    temperature          DOUBLE PRECISION,
    precip_prob          DOUBLE PRECISION,
    precip_amount        DOUBLE PRECISION,
    humidity             DOUBLE PRECISION,
    wind_speed           DOUBLE PRECISION,
    base_dttm            TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (gu, forecast_dttm)
);

-- 서울시 문화/공연 행사 정보. loader가 종료되지 않은 행사만 upsert한다.
CREATE TABLE IF NOT EXISTS cultural_events (
    event_id        TEXT PRIMARY KEY,
    title           TEXT NOT NULL,
    category        TEXT,
    gu              TEXT,
    place           TEXT,
    start_date      DATE,
    end_date        DATE,
    is_free         TEXT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION
);

EOSQL
