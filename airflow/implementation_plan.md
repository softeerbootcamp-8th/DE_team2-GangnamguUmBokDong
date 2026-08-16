# Unified Airflow Data Pipeline Implementation Plan

## Goal
현재 개별적으로 동작하는 `collector`, `nowcasting`, `normalizer`, `ml/inference`, `db-loader` 모듈들을 연결하여 Airflow 기반의 통합 데이터 파이프라인을 구축합니다. 원천 데이터 수집부터 결측치 보간(nowcasting), 정규화(normalization), 모델 추론(inference) 및 최종 RDB 적재까지 자동화하는 것을 목표로 합니다.

모델 추론 결과를 RDB에 로드하는 `db-loader` 기능은 아직 구현되어있지 않기 때문에 구현이 필요하다.

---

## Pipeline Schedules & Data Flow


### 1. 일 단위 주기 (1 Day Interval)
*   **수집 및 전처리:**
    *   `living_population` 수집
    *   **Nowcasting**: 수집된 `living_population` 데이터를 기반으로 결측치를 채움
*   **단일 수집:**
    *   `cultural_event` 데이터 수집

### 2. 5분 단위 주기 (5 Minute Interval) - Core Pipeline
*   **수집 (Collector):**
    *   `bike_rental_history`
    *   `bike_station_realtime`
    *   `population_realtime`
*   **정규화 (Normalizer):**
    *   수집된 실시간 데이터와 `living_population` 데이터를 기반으로 Normalization 수행
*   **모델 추론 (ML/Inference):**
    *   S3에서 추론에 필요한 피처(Feature) 데이터를 불러옴
    *   추론(Inference) 진행
    *   추론 결과를 S3에 Parquet 형식으로 저장
*   **DB 적재 (DB-Loader):**
    *   S3에 적재된 데이터들을 RDB에 맞게 변환하여 채움
    *   **[추가 구현 필요 사항]**: 모델이 S3에 적재한 추론 결과(Parquet)를 읽어와 RDB 적재하는 로직 구현 

### 3. 10분 단위 주기 (10 Minute Interval)
*   **단일 수집:**
    *   `weather_ultra_short_term` 수집

### 4. 3시간 단위 주기 (3 Hour Interval)
*   **단일 수집:**
    *   `weather_short_term_forecast` 수집

---

## 참고 사항 (References)

**모델 파일 네이밍 규칙:** `models/{model_name}_{suffix 또는 kind}.{확장자}`
*   `model_name`: `rental` 또는 `return`
*   `확장자`: 모델 파일이면 `.txt`, 메타데이터면 `.json`
*   `models/` 앞의 prefix 자체(`MODELS_PREFIX`)는 환경변수로 바꿀 수 있으며 기본값은 `"models"`입니다. 
    *   실험용으로 다른 prefix(예: `models/experiments/{run_id}`)를 넘기면 챔피언 파일을 덮어쓰지 않고 따로 저장할 수도 있게 설계되어 있습니다(`model_key`/`model_json_key`의 `models_prefix` 인자 활용).
