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
- **S3 Silver Parquet 추출 출처**: `weather_ultra_short_term`
- **컬럼 매핑 (Silver -> Gold)**:
  - 수집 격자 좌표 / 위치 정보 -> `gu` (자치구 이름으로 변환 매핑)
  - 파티션 시간(`dt`, `hh`) 또는 데이터 내부 시간 -> `observed_at` (TIMESTAMPTZ)
  - `T1H` (double) -> `temperature`
  - `REH` (double) -> `humidity`
  - `WSD` (double) -> `wind_speed`
  - `RN1` (double) -> `rainfall`
  - `PTY` (int64) -> `pty_type`
- **처리 로직**: 이상치 정제 후 `gu`를 기준으로 최신 실황만 Upsert.

### 2-2. `weather_forecast` (단기 예보 날씨)
- **S3 Silver Parquet 추출 출처**: `weather_short_term_forecast`
- **컬럼 매핑 (Silver -> Gold)**:
  - 수집 격자 좌표 / 위치 정보 -> `gu` (자치구 이름으로 변환 매핑)
  - 예보 대상 시간 데이터 -> `forecast_dttm` (TIMESTAMPTZ)
  - `TMP` (double) -> `temperature`
  - `POP` (double) -> `precip_prob`
  - `SKY` (int64) -> `sky_cond`
  - `PTY` (int64) -> `pty_type`
- **처리 로직**: 동일한 미래 시각(`forecast_dttm`)에 대해 가장 최근에 발표된 예보로 Upsert.

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
-- 기상청 초단기 실황 (현재 날씨). 자치구별 최신 데이터 1건 유지(upsert).
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

-- 기상청 단기 예보 (미래 날씨). 동일 예측 시간에 대해 가장 최근 발표된 예보 하나만 남는다(upsert).
CREATE TABLE IF NOT EXISTS weather_forecast (
    gu              TEXT NOT NULL,
    forecast_dttm   TIMESTAMPTZ NOT NULL,
    temperature     DOUBLE PRECISION,
    precip_prob     DOUBLE PRECISION,
    sky_cond        INTEGER,
    pty_type        INTEGER,
    PRIMARY KEY (gu, forecast_dttm)
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

