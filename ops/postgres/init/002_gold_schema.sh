#!/usr/bin/env bash
# 대시보드(apps/api)가 읽고, 예측 배치(ml/predict)가 쓰는 골드 테이블을 만든다.
# 001에서 만든 앱 DB($POSTGRES_APP_DB) 안에 생성한다.
#
# predicted_bikes, shared_rate 같은 요청 시점에만 의미 있는 파생값은 테이블에
# 없다 — apps/api가 station_stock + forecast_points를 조합해 그때그때 계산한다.
# urgency_score/action_type은 예외다 — 배치(rebalance/)가 5분마다 미리 계산해
# station_urgency 테이블(003_station_urgency.sh)에 적재하고, apps/api는 그 결과만
# 조회한다(이유: #107).
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

-- 대여소의 실제 최근접 기상 격자. weather_current/weather_forecast와 (nx, ny)로
-- 직접 조인하기 위한 컬럼이다(gu 기준 조인은 구 경계 왜곡이 커서 쓰지 않는다).
ALTER TABLE stations ADD COLUMN IF NOT EXISTS grid_nx INTEGER;
ALTER TABLE stations ADD COLUMN IF NOT EXISTS grid_ny INTEGER;

-- 대여소별 최신 재고. 과거 관측 이력은 S3 Silver에 영구 보존되고 RDS 소비자는
-- 현재값만 사용하므로, loader가 sta_id 기준으로 upsert해 최신 한 줄만 유지한다(#108).
CREATE TABLE IF NOT EXISTS station_stock (
    sta_id                  TEXT PRIMARY KEY REFERENCES stations (sta_id),
    observed_at             TIMESTAMPTZ NOT NULL,
    parking_bike_tot_cnt    INTEGER NOT NULL
);

-- 과거 버전은 (sta_id, observed_at) 복합 PK로 관측 이력을 누적했다. 기존 볼륨은
-- 대여소별 최신 관측 한 건만 남긴 뒤 단일 PK로 전환한다. 이미 단일 PK면 건너뛴다.
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

    IF primary_key_definition IS DISTINCT FROM 'PRIMARY KEY (sta_id)' THEN
        DELETE FROM station_stock stock
              WHERE stock.observed_at < (
                  SELECT max(observed_at) FROM station_stock WHERE sta_id = stock.sta_id
              );
        IF primary_key_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE station_stock DROP CONSTRAINT %I', primary_key_name);
        END IF;
        ALTER TABLE station_stock ADD PRIMARY KEY (sta_id);
    END IF;
END
$migration$;

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

-- 기상청 초단기 실황(현재 날씨). loader가 격자별 최신 관측 1건만 upsert한다.
-- gu는 표시용 파생 컬럼이며 PK가 아니다(같은 gu에 여러 격자가 걸칠 수 있다).
DROP TABLE IF EXISTS weather_current;
CREATE TABLE weather_current (
    nx              INTEGER NOT NULL,
    ny              INTEGER NOT NULL,
    gu              TEXT NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL,
    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    wind_speed      DOUBLE PRECISION,
    rainfall        DOUBLE PRECISION,
    pty_type        INTEGER,
    PRIMARY KEY (nx, ny)
);

-- 기상청 단기예보(미래 날씨). 동일 (nx, ny, forecast_dttm)에 대해 가장 최근
-- 발표만 upsert로 남긴다(guard_col: base_dttm, loader/tables.yaml 참고).
-- gu는 표시용 파생 컬럼이며 PK가 아니다.
DROP TABLE IF EXISTS weather_forecast;
CREATE TABLE weather_forecast (
    nx                   INTEGER NOT NULL,
    ny                   INTEGER NOT NULL,
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
    PRIMARY KEY (nx, ny, forecast_dttm)
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
