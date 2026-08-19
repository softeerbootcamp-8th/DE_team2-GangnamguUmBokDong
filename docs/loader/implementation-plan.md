# Gold DB 데이터 적재 파이프라인 설계 및 DDL 스펙

이 문서는 S3(또는 Collector)에서 수집되는 날씨와 행사 데이터를 Gold DB에 어떻게 정제하여 적재할 것인지에 대한 설계안입니다.
기존 스키마(`stations`, `station_stock`)와 새롭게 추가될 스키마의 전체 DDL 및 **Silver Parquet 컬럼 매핑 규칙**을 명세합니다.

---

## 1. 기존 데이터 적재 스펙 및 DDL

### 1-1. `stations` (대여소 마스터 정보)
- **S3 Silver Parquet 추출 출처**: `bike_station_realtime`
- **컬럼 매핑 (Silver -> Gold)**:
  - `stationId` (string) -> `sta_id` (대여소 ID, integer 변환)
  - `stationName` (string) -> `sta_nm` (대여소명)
  - `stationName` 파싱 등 -> `gu` (자치구명, 파싱 혹은 별도 매핑)
  - `stationName` 파싱 등 -> `sta_addr` (상세주소, 파싱 혹은 별도 매핑)
  - `stationLatitude` (double), `stationLongitude` (double) -> `lat`, `lon` (위경도)
  - `rackTotCnt` (int64) -> `hold_cnt` (총 거치대 수)

### 1-2. `station_stock` (실시간 재고 이력)
- **S3 Silver Parquet 추출 출처**: `bike_station_realtime`
- **컬럼 매핑 (Silver -> Gold)**:
  - `stationId` (string) -> `sta_id` (대여소 ID, integer 변환)
  - 파티션 시간(`dt`, `hh`) 또는 수집 시간 -> `observed_at` (관측 시간)
  - `parkingBikeTotCnt` (int64) -> `parking_bike_tot_cnt` (거치 대수)

#### 기존 DDL
```sql
CREATE TABLE IF NOT EXISTS stations (
    sta_id      INTEGER PRIMARY KEY,
    sta_nm      TEXT NOT NULL,
    gu          TEXT NOT NULL,
    sta_addr    TEXT NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    hold_cnt    INTEGER NOT NULL
);

-- 대여소별 재고 관측 이력. 수집 파이프라인이 매 주기마다 새 행을 추가한다.
CREATE TABLE IF NOT EXISTS station_stock (
    sta_id                  INTEGER NOT NULL REFERENCES stations (sta_id),
    observed_at             TIMESTAMPTZ NOT NULL,
    parking_bike_tot_cnt    INTEGER NOT NULL,
    PRIMARY KEY (sta_id, observed_at)
);
```

---

## 2. 신규 데이터 적재 스펙 (UI 상세정보용)

### 2-1. `weather_current` (초단기 실황 날씨)
- **S3 Silver Parquet 추출 출처**: `weather_ultra_short_live`
- **컬럼 매핑 (Silver -> Gold)**:
  - `nx`, `ny` (int) -> `nx`, `ny` (조인 키, 그대로 유지)
  - 격자 좌표 -> `gu` (표시용 파생 컬럼, `grid_to_gu`로 계산. PK 아님)
  - 파티션 시간(`dt`, `hh`) 또는 데이터 내부 시간 -> `observed_at` (TIMESTAMPTZ)
  - `T1H` (double) -> `temperature`
  - `REH` (double) -> `humidity`
  - `WSD` (double) -> `wind_speed`
  - `RN1` (double) -> `rainfall`
  - `PTY` (int64) -> `pty_type`
- **처리 로직**: 이상치 정제 후 `(nx, ny)`를 기준으로 최신 실황만 Upsert. `gu`는 구 경계 왜곡을 피하기 위해 조인 키로 쓰지 않는다(자세한 배경은 `docs/superpowers/specs/2026-08-19-weather-grid-matching-design.md` 참고).

### 2-2. `weather_forecast` (단기 예보 날씨)
- **S3 Silver Parquet 추출 출처**: `weather_short_term_forecast`(3시간)와 `weather_ultra_short_forecast`(30분)가 같은 물리 테이블을 공유한다.
- **컬럼 매핑 (Silver -> Gold)**:
  - `nx`, `ny` (int) -> `nx`, `ny` (조인 키)
  - 격자 좌표 -> `gu` (표시용 파생 컬럼, PK 아님)
  - 예보 대상 시간 데이터 -> `forecast_dttm` (TIMESTAMPTZ)
  - `TMP`/`T1H` (double) -> `temperature`
  - `POP` (double) -> `precip_prob`
  - `PCP`/`RN1` -> `precip_amount`
  - `SKY` (int64) -> `sky_cond`
  - `PTY` (int64) -> `pty_type`
  - `REH` (double) -> `humidity`
  - `WSD` (double) -> `wind_speed`
  - 발표 시각 -> `base_dttm` (TIMESTAMPTZ)
- **처리 로직**: 동일한 `(nx, ny, forecast_dttm)`에 대해 가장 최근에 발표된(`base_dttm`이 가장 큰) 예보로 Upsert.

### 2-3. `cultural_events` (문화/공연 행사)
- **S3 Silver Parquet 추출 출처**: `cultural_event`
- **컬럼 매핑 (Silver -> Gold)**:
  - 해시값 생성(`TITLE`+`PLACE`) -> `event_id` (PK)
  - `TITLE` (string) -> `title`
  - `CODENAME` (string) -> `category`
  - `GUNAME` (string) -> `gu`
  - `PLACE` (string) -> `place`
  - `STRTDATE` (string) -> `start_date` (DATE로 캐스팅)
  - `END_DATE` (string) -> `end_date` (DATE로 캐스팅)
  - `IS_FREE` (string) -> `is_free`
  - `LAT` (double) -> `lat`
  - `LOT` (double) -> `lon`
- **처리 로직**: 날짜 포맷 파싱 및 변환 후, 종료일(`END_DATE`)이 지나지 않은 현재/예정 행사만 Upsert.

#### 신규 추가 DDL
```sql
-- 대여소의 실제 최근접 기상 격자. weather_current/weather_forecast와 (nx, ny)로
-- 직접 조인하기 위한 컬럼이다(gu 기준 조인은 구 경계 왜곡이 커서 쓰지 않는다).
ALTER TABLE stations ADD COLUMN IF NOT EXISTS grid_nx INTEGER;
ALTER TABLE stations ADD COLUMN IF NOT EXISTS grid_ny INTEGER;

-- 기상청 초단기 실황 (현재 날씨). 격자별 최신 데이터 1건 유지(upsert).
-- gu는 표시용 파생 컬럼이며 PK가 아니다(같은 gu에 여러 격자가 걸칠 수 있다).
CREATE TABLE IF NOT EXISTS weather_current (
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

-- 기상청 단기 예보 (미래 날씨). 동일 (nx, ny, forecast_dttm)에 대해 가장 최근
-- 발표된 예보 하나만 남는다(upsert, guard_col: base_dttm).
CREATE TABLE IF NOT EXISTS weather_forecast (
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

-- 서울시 문화/공연 행사 정보.
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
```

---

## 3. 요약 (데이터 흐름)

1. **Collector 파이프라인 (Airflow)** -> S3 (Bronze -> Silver Parquet)
2. **Transform & Load 파이프라인 (ETL Batch)** -> 위 명세서에 정의된 **Silver -> Gold 컬럼 매핑** 및 정제 로직을 거쳐 적재
3. **API 서버 (apps/api)** -> 상세정보(Detail) 요청 시 `sta_id`에 해당하는 대여소의 `gu`나 `lat/lon`을 기준으로 날씨와 행사 정보를 JOIN/필터링하여 UI로 서빙.

---

