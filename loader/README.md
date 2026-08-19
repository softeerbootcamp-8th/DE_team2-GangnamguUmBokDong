# loader

S3 실버 계층(Silver Parquet) 및 머신러닝 추론 결과(ML Predictions)를 읽어와 골드 계층 스키마에 맞게 정제·변환한 후, PostgreSQL Gold 데이터베이스에 멱등성(Upsert) 있게 적재하는 모듈입니다.

---

## 1. 파일별 역할

| 파일 | 역할 |
|---|---|
| `main.py` | Airflow CLI 진입점, 테이블별 변환 실행, 다중 소스 병합(`_TABLE_ALIASES`) 및 DB 트랜잭션 Upsert 수행 |
| `tables.yaml` | 테이블별 S3 소스, 변환 함수, DB 충돌(PK/Unique) 컬럼 및 갱신 컬럼 정의 선언적 명세서 |
| `config.py` | `tables.yaml`을 로드하여 `TableSpec` 데이터클래스 레지스트리(`TABLE_SPECS`) 구성 |
| `transform.py` | Silver DataFrame $\rightarrow$ Gold DB 레코드 딕셔너리 변환 함수 모음 (타입 변환, 결측치 처리, 날짜 파싱 등) |
| `gu_mapping.py` | 위경도(WGS84) $\rightarrow$ 서울시 25개 자치구 공간 판정(Point-in-Polygon). 격자($nx, ny$) $\leftrightarrow$ 위경도 변환은 `core.weather_grid`를 재수출 |
| `reader.py` | `core.s3`를 사용하여 S3의 실버 파티션 및 ML 추론 Parquet을 PyArrow Table로 읽기 |

---

## 2. 적재 대상 Gold 테이블 명세

| 논리 스펙 키 (`tables.yaml`) | 물리 DB 테이블 | S3 소스 식별자 | DB 충돌 키 (`conflict_cols`) | 주요 갱신 컬럼 (`update_cols`) |
|---|---|---|---|---|
| `stations` | `stations` | `bike_station_realtime` | `[sta_id]` | 대여소명, 자치구, 상세주소, 위도, 경도, 거치대수 |
| `station_stock` | `station_stock` | `bike_station_realtime` | `[sta_id, observed_at]` | 실시간 거치 대수 (`parking_bike_tot_cnt`) |
| `weather_current` | `weather_current` | `weather_ultra_short_live` | `[gu]` | 관측일시, 기온(T1H), 습도(REH), 풍속(WSD), 강수량(RN1), 강수형태(PTY) |
| `weather_forecast` | `weather_forecast` | `weather_short_term_forecast` | `[gu, forecast_dttm]` | 하늘상태(SKY), 강수형태(PTY), 기온(TMP), 강수확률(POP), 강수량(PCP), 습도, 풍속 |
| `weather_forecast_ultra` | `weather_forecast` | `weather_ultra_short_forecast` | `[gu, forecast_dttm]` | 하늘상태(SKY), 강수형태(PTY), 기온(T1H), 강수량(RN1), 습도, 풍속 |
| `cultural_events` | `cultural_events` | `cultural_event` | `[event_id]` | 행사명, 카테고리, 자치구, 장소, 시작일, 종료일, 유/무료, 위도, 경도 |
| `cultural_events_performance` | `cultural_events` | `performance_event` | `[event_id]` | 공연명, 카테고리, 자치구, 장소, 시작일, 종료일, 이용료 원문, 위도, 경도 |
| `forecast_points` | `forecast_points` | `ml_predictions` | `[sta_id, predicted_dttm]` | 예측 대여량, 예측 반납량, 배치 실행 시각 |

---

## 3. 핵심 변환 및 처리 원리

### ① 서울 25개 자치구 경계 및 외래키(FK) 무결성 보호 (`gu_mapping.py`)
- **Point-in-Polygon 판정**: 서울시 25개 자치구 GeoJSON 폴리곤을 로드하여 `Shapely`의 `polygon.contains(point)`로 소속 자치구를 매핑합니다.
- **경계 외 정거장 필터링**: 서울 자치구 경계 밖의 정거장은 마스터 테이블(`stations`)에 적재되지 않으므로, 실시간 재고(`station_stock`)에서도 동일하게 제외하여 외래키 위반 에러를 원천 차단합니다.
- **기상청 5km 격자 매핑**: 람베르트 등각원추투영 공식(`core.weather_grid.latlon_to_grid`)으로 좌표를 최근접 격자로 직접 변환합니다. 이 공식은 `normalizer`(보강 station master의 `weather_nx`/`weather_ny`)와 `scripts/generate_weather_grids.py`(수집 격자 34개 목록 생성)도 같이 씁니다.

### ② 다중 소스의 단일 Gold 테이블 병합 (`_TABLE_ALIASES`)
S3의 서로 다른 데이터 소스가 Gold DB의 동일한 단일 테이블로 통합 적재되는 구조를 지원합니다:
1. **문화/공연 행사 병합**: 서울시 문화행사(`cultural_events`)와 체육시설 공연행사(`cultural_events_performance`)가 단일 **`cultural_events`** 테이블로 적재됩니다. 고유 ID는 문화행사의 경우 제목+장소+시작일 기반 SHA256 해시, 체육시설 공연행사의 경우 일정 순번(`SCH_SEQ`)을 사용합니다.
   - 체육시설 공연행사 API는 좌표를 제공하지 않으므로, 시설 코드(`SCH_CODE_B`) → 좌표 마스터(`assets/stadium_coords.json`, 11개 시설)를 조회해 `lat`/`lon`을 채우고 `gu`는 좌표에서 도출합니다. 좌표가 없으면 `apps/api`의 위경도 반경 조회에서 전 행이 걸러지기 때문입니다. 마스터에 없는 시설 코드는 좌표 없이 적재되고 경고 로그가 남습니다.
   - 카테고리·장소는 숫자 코드(`SCH_CODE_A`/`SCH_CODE_B`)가 아니라 이름 필드(`CODE_TITLE_A`/`CODE_TITLE_B`)를 씁니다.
   - `is_free`에는 원본 `USE_PAY` 문자열을 그대로 싣습니다 — 가격표·안내 URL·`"없음"` 등이 섞인 자유 텍스트라 유/무료로 정규화하지 않습니다.
2. **날씨 예보 병합**: 기상청 단기예보(`weather_forecast`)와 초단기예보(`weather_forecast_ultra`)가 단일 **`weather_forecast`** 테이블로 적재됩니다. 동일한 `(자치구, 예보 시점)`에 대해 최신 발표 일시(`base_dttm`) 데이터가 유지됩니다.

### ③ 기상청 비정형 강수량 파싱 (`_parse_precip_str`)
기상청 API가 반환하는 문자열 강수량을 DB 적재용 단일 실수(float)로 정제합니다:
- `"강수없음"`, `"적설없음"` $\rightarrow$ `0.0`
- `"1.0mm 미만"` $\rightarrow$ 소량 강수 대표값 `0.5`
- `"30.0~50.0mm"` $\rightarrow$ 단위 제거 후 하한값 `30.0`

### ④ ML 추론 수요 예측 적재 (`forecast_points_from_predictions`)
- `ml/inference`가 예측한 미래 1~6시간 대여소별 대여/반납 예측 실수치를 `round()`로 반올림하여 정수 대수로 변환합니다.
- `(sta_id, predicted_dttm)` 복합 유니크 키로 Upsert하여 최신 모델 추론 결과로 항상 갱신됩니다.

---

## 4. 실행 방법

### CLI 직접 실행
```bash
# 1. 따릉이 실시간 재고 적재
uv run python main.py --table station_stock --window-start 2026-08-16T14:05:00+09:00

# 2. 기상 실황 적재
uv run python main.py --table weather_current --window-start 2026-08-16T14:05:00+09:00

# 3. ML 수요 예측 결과 적재
uv run python main.py --table forecast_points --window-start 2026-08-16T14:05:00+09:00
```

### 테스트 실행
```bash
uv run pytest
```
