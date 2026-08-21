# `realtime_5min` 모델·S3 준비 가이드

이 문서는 로컬 MinIO 또는 AWS S3에서 `realtime_5min` 추론을 준비하는 최소
절차를 설명한다. 현재 검증된 방법은 **제작자가 제공한 임시 모델과 그 모델 전용
station dependency를 함께 사용하는 것**이다.

> 이 모델은 AWS 연결 검증용이다. 2025년 매월 20일만 표본 학습하고 LightGBM을
> 20 round만 실행했으므로 운영 품질 champion으로 사용하지 않는다.

## 1. 실행 구조

```text
최초 준비 또는 모델 변경
  모델 16개 + station profile + station master
    -> training.publish_serving_release
    -> models/serving-release/current.json

매 5분
  realtime_5min
    -> current.json의 고정된 모델/dependency로 추론
    -> Gold DB 게시
```

매 5분마다 학습하거나 release를 다시 게시하지 않는다. 모델을 바꿀 때만 새로운
release를 게시한다.

`prepare_serving_plan`은 pointer와 release/model manifest만 경량 검증한다. Station
profile 전체 schema·coverage 검증은 release 게시 시 수행한다. 실제 inference는
게시된 exact SHA와 Parquet footer를 확인한 뒤 현재 anchor와 1시간 전 fallback에
필요한 month/dow 조각만 읽는다. 따라서 매 5분 1천만 행대 profile을 다시
materialize하지 않는다.

## 2. 현재 사용할 파일

| 역할 | 산출물 |
|---|---|
| Rental/return 모델 16개 | `aws-temporary-model-2025-d20-h12-r20.tar.gz` |
| Station profile/master | `aws-temporary-model-2025-d20-h12-r20-serving-dependencies.tar.gz` |
| Population profile | 우리가 생성한 `processed_v2/population_hourly_profile.parquet` |
| 추론 포인터 | 게시 명령이 생성하는 `models/serving-release/current.json` |

두 압축파일은 한 세트이며 다음을 검증했다.

- 모델 입력은 현재 `develop`과 같은 13개 피처다.
- 모델 category는 정수 `station_no` 2,749개다.
- 원본 station profile은 2,749개를 모두 포함하며 누락이 없다.
- station profile은 16,396,215행, station master는 2,752행이다.
- station profile SHA-256:
  `55a5afedb7c956fcdfed40750147cfbaf214e344b943c5f8c37e5e29050f1a19`
- station crosswalk SHA-256:
  `b65e2e0166f623c5ab2a805fda0c1f547adc9ff8eeb1290538122cc717d2eabe`

사용하지 않을 파일:

- `/Users/admin/Downloads/models`의 14개 파일: PR #50 시기의 38개 피처
  모델이라 현재 `develop`과 호환되지 않는다.
- `station_hourly_profile-model-compat.parquet`: 원본을 받기 전에 누락된 63개
  station을 전역 평균으로 채운 임시 fallback이다.
- 현재 Archive로 새로 만든 일반 station profile: 임시 모델과 원천 범위가 다르다.

## 3. 공통 환경 설정

```bash
cd /Users/admin/Documents/GitHub/DE_team2-GangnamguUmBokDong
set -a
source .env
set +a

# 로컬 MinIO에서만 설정
export S3_ENDPOINT_URL=http://localhost:9000
```

AWS에서는 endpoint를 설정하지 않고 허가된 MFA profile과 region을 사용한다.
액세스 키와 세션 토큰은 문서·Git·채팅에 기록하지 않는다.

## 4. 모델 16개 업로드

```bash
mkdir -p /Users/admin/Downloads/models/temporary-model

tar -xzf /Users/admin/Downloads/aws-temporary-model-2025-d20-h12-r20.tar.gz \
  -C /Users/admin/Downloads/models/temporary-model \
  --strip-components=1

export ARCHIVE_PREFIX='models/archive/dt=2026-08-21-full-year-d20-h12-r20-emergency-v2/builtin-default'

aws --endpoint-url "$S3_ENDPOINT_URL" s3 cp \
  /Users/admin/Downloads/models/temporary-model/models/ \
  "s3://$S3_BUCKET/$ARCHIVE_PREFIX/" --recursive

aws --endpoint-url "$S3_ENDPOINT_URL" s3 ls \
  "s3://$S3_BUCKET/$ARCHIVE_PREFIX/" | wc -l
# 기대값: 16
```

필수 파일은 모델별 `poisson`, `q10`, `q50`, `q90`, `metrics`, `profile`,
`station_categories`, `conformal_correction`이다.

## 5. 원본 station dependency 업로드

압축을 풀고 체크섬을 확인한다.

```bash
mkdir -p /Users/admin/Downloads/models/serving-dependencies

tar -xzf \
  /Users/admin/Downloads/aws-temporary-model-2025-d20-h12-r20-serving-dependencies.tar.gz \
  -C /Users/admin/Downloads/models/serving-dependencies \
  --strip-components=1

cd /Users/admin/Downloads/models/serving-dependencies
shasum -a 256 -c SHA256SUMS
```

기존 산출물을 보존하도록 별도 key로 올린다.

```bash
export STATION_PROFILE_KEY='processed/features/w60_e40_t20/station_hourly_profile-d20-h12-r20.parquet'
export STATION_MASTER_KEY='processed_v2/station_master-d20-h12-r20.parquet'

aws --endpoint-url "$S3_ENDPOINT_URL" s3 cp \
  processed/features/w60_e40_t20/station_hourly_profile.parquet \
  "s3://$S3_BUCKET/$STATION_PROFILE_KEY"

aws --endpoint-url "$S3_ENDPOINT_URL" s3 cp \
  processed_v2/station_master.parquet/ \
  "s3://$S3_BUCKET/$STATION_MASTER_KEY/" --recursive
```

확인:

```bash
aws --endpoint-url "$S3_ENDPOINT_URL" s3 ls \
  "s3://$S3_BUCKET/$STATION_PROFILE_KEY"

aws --endpoint-url "$S3_ENDPOINT_URL" s3 ls \
  "s3://$S3_BUCKET/$STATION_MASTER_KEY/" --recursive
```

Station master는 Spark Parquet prefix다. `_SUCCESS`와 `part-*.parquet`을 함께
보존한다. 현재 publisher가 이 prefix를 정상적으로 읽는 것을 확인했다.

## 6. Population profile 확인

생활인구 profile은 두 압축파일에 없으므로 별도로 필요하다.

```bash
aws --endpoint-url "$S3_ENDPOINT_URL" s3 ls \
  "s3://$S3_BUCKET/processed_v2/population_hourly_profile.parquet"
```

없다면 `processed_v2/population_2025.parquet/`을 먼저 만든 뒤 실행한다.

```bash
cd /Users/admin/Documents/GitHub/DE_team2-GangnamguUmBokDong/ml

env -u VIRTUAL_ENV \
  UV_PROJECT_ENVIRONMENT="$PWD/inference/.venv" \
  uv --project inference run --frozen \
  python -m inference.build_population_profile
```

## 7. Pair serving release 게시

```bash
cd /Users/admin/Documents/GitHub/DE_team2-GangnamguUmBokDong
set -a
source .env
set +a

export S3_ENDPOINT_URL=http://localhost:9000
export ARCHIVE_PREFIX='models/archive/dt=2026-08-21-full-year-d20-h12-r20-emergency-v2/builtin-default'
export STATION_PROFILE_KEY='processed/features/w60_e40_t20/station_hourly_profile-d20-h12-r20.parquet'
export STATION_MASTER_KEY='processed_v2/station_master-d20-h12-r20.parquet'

cd ml
env -u VIRTUAL_ENV \
  UV_PROJECT_ENVIRONMENT="$PWD/training/.venv" \
  uv --project training run --frozen \
  python -m training.publish_serving_release \
  --rental-archive-prefix "$ARCHIVE_PREFIX" \
  --return-archive-prefix "$ARCHIVE_PREFIX" \
  --station-profile-key "$STATION_PROFILE_KEY" \
  --station-master-key "$STATION_MASTER_KEY"
```

`--allow-contract-change`는 기존 release와 다른 계약으로 의도적으로 migration할
때만 팀 승인 후 사용한다.

### 7.1 확인된 성공 결과

2026-08-21 로컬 MinIO에 generation 1 게시를 완료했다.

```text
release manifest SHA-256:
91e75ca517c35625a3b9d691fae36dce83a4e904d25c3e2d5bcbe6963e7d64cd

station crosswalk SHA-256:
b65e2e0166f623c5ab2a805fda0c1f547adc9ff8eeb1290538122cc717d2eabe

station master source rows: 2752
```

Pointer 확인:

```bash
aws --endpoint-url "$S3_ENDPOINT_URL" s3 cp \
  "s3://$S3_BUCKET/models/serving-release/current.json" - | jq .
```

## 8. `realtime_5min` 실행

### 8.1 신규 Gold DB Seed bootstrap

새 Postgres/RDS에는 `dispatch_center`, `weather_grid` publication state가 없으므로
`realtime_5min`을 켜기 전에 최초 한 번 게시한다. 로컬 Compose에서는 다음 명령으로
두 Seed를 함께 게시한다.

```bash
cd /Users/admin/Documents/GitHub/DE_team2-GangnamguUmBokDong
make bootstrap-gold-seeds
```

로컬 기본값은 `local-dev-weather-grid-v1`, `2026-08-19T03:15:38Z`이다. 같은
입력으로 다시 실행하면 exact replay로 처리된다. 이 작업은 기준 데이터 게시이므로
`make up`에 자동으로 포함하지 않는다.

AWS에서는 팀이 승인한 weather grid 버전과 UTC 적용 시각을 반드시 지정한다.

```bash
make bootstrap-gold-seeds \
  GOLD_WEATHER_GRID_SEED_VERSION='<승인된-version>' \
  GOLD_WEATHER_GRID_EFFECTIVE_DTTM='<승인된-UTC시각>'
```

Seed가 없으면 `prepare_serving_plan`은 다음 오류로 중단된다.

```text
Gold dependency publication state가 없습니다: ['dispatch_center', 'weather_grid']
```

먼저 필요한 object를 확인한다.

```bash
aws --endpoint-url "$S3_ENDPOINT_URL" s3 ls \
  "s3://$S3_BUCKET/models/serving-release/current.json"

aws --endpoint-url "$S3_ENDPOINT_URL" s3 ls \
  "s3://$S3_BUCKET/processed_v2/population_hourly_profile.parquet"

aws --endpoint-url "$S3_ENDPOINT_URL" s3 ls \
  "s3://$S3_BUCKET/$STATION_PROFILE_KEY"
```

Airflow UI에서 `realtime_5min`을 수동 실행하거나 다음 schedule을 기다린다.

1. `prepare_serving_plan`이 current release를 읽는지 확인한다.
2. `run_inference`가 모델, population profile과 최신 Silver 입력을 읽는지 확인한다.
3. Gold 게시 태스크와 API 결과를 확인한다.

## 9. 새 모델을 직접 만들 때 필요한 원천

제공받은 임시 모델 세트를 사용할 때는 아래 전체 재구축이 필요 없다. 새 모델
학습이나 profile 갱신 시에만 준비한다.

| 구분 | S3 key | 권장 범위 |
|---|---|---|
| 대여 이력 | `archive/bike_rental_history/dt=YYYY-MM-DD.parquet` | 학습 기간, 전 35일, 후 7일 |
| 대여소 현황 | `archive/bike_station_realtime/dt=YYYY-MM-DD.parquet` | 학습 기간 전체 |
| 날씨 | `archive/weather_ultra_short_live/dt=YYYY-MM-DD.parquet` | 학습 기간과 시작 context |
| 생활인구 | `archive/living_population_grid/dt=YYYY-MM-DD.parquet` | 학습 기간 전체 |
| 대여소 master | `silver/station_master_enriched/dt=.../hh=.../HHMM.parquet` | 최신 snapshot |

주요 생성 순서:

```text
Raw CSV/API
  -> 날짜별 archive/*.parquet
  -> station_master_enriched
  -> feature_engine.spark.run_pipeline
  -> station/population profile
  -> 모델 학습
  -> publish_serving_release
```

2025년 full build는 생활인구 365일이 필요하다. 공식 원천에도 행이 없는 날짜는
인접 값을 복제하지 않고 schema를 가진 0행 Archive로 기록한다. 확인된 예외는
대여소 현황 `2025-01-09`, `2025-01-10`과 시작 context 날씨
`2024-12-31`이다.

생활인구 CSV 변환:

```bash
cd nowcaster
for month in {01..12}; do
  S3_ENDPOINT_URL=http://localhost:9000 \
  uv run --frozen python main.py backfill-archive \
    --csv-dir "/Users/admin/Downloads/250_LOCAL_RESD_2025$month" || break
done
```

Feature/profile 생성:

```bash
cd ml
export TRAIN_WINDOW_START=2025-01-01
export TRAIN_WINDOW_END=2025-12-31

./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
./inference/.venv/bin/python -m inference.build_station_profile
./inference/.venv/bin/python -m inference.build_population_profile

unset TRAIN_WINDOW_START TRAIN_WINDOW_END
```

## 10. 오류별 확인

| 오류 | 원인 | 조치 |
|---|---|---|
| `serving release pointer가 없습니다` | `current.json` 미게시 | 7절 실행 |
| station profile이 station을 누락 | 모델과 다른 profile | 제작 당시 원본 dependency 사용 |
| population profile이 없음 | 생활인구 fallback 미생성 | 6절 실행 |
| station category int16 계약 위반 | PR #50 문자열 모델 | 13개 피처 모델로 교체 |
| LightGBM 피처 불일치 | 38개 구형 모델 | 현재 develop 모델로 교체 |
| Archive 날짜 없음 | 원천 누락 | 원천 복구 또는 근거 있는 0행 Archive 생성 |
| `Gold dependency publication state가 없습니다` | 선행 Gold projection 미게시 | 표시된 publisher를 먼저 실행하거나 DAG 선행 태스크 상태 확인 |

## 11. 관련 코드

- 실시간 DAG: `airflow/dags/realtime_5min.py`
- Pair 게시: `ml/training/publish_serving_release.py`
- Release 계약: `libs/ml_core/serving_release.py`
- Station profile: `ml/inference/build_station_profile.py`
- Population profile: `ml/inference/build_population_profile.py`
- Feature pipeline: `ml/feature_engine/spark/run_pipeline.py`
- Station master: `normalizer/station_master.py`
- 생활인구 Archive: `nowcaster/main.py`
