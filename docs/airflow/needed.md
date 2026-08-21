# AWS S3 데이터 준비부터 serving release 게시까지

이 문서는 빈 AWS S3 버킷에서 2025년 feature mart와 fallback profile을 만들고,
이미 학습된 rental/return 모델을 `realtime_5min`에 게시하는 절차를 설명한다.
기본 모델 계약은 `w60_e40_t20`이다.

- rolling window: 60분
- embargo: 40분
- 모델 grid 및 학습 anchor: 20분
- target: anchor 이후 60분의 대여/반납 건수
- 운영 호출 주기: 5분

아래의 `<BUCKET>`은 실제 AWS 버킷 이름으로 바꾼다. 현재 개발 버킷 예시는
`gng-ubd-s3-bucket`이다. 로컬 MinIO에서는 같은 object key를 사용하되 AWS CLI에
`--endpoint-url http://localhost:9000`을 추가한다.

## 1. 데이터 계층

| 계층 | 의미 | 직접 업로드 여부 |
|---|---|---|
| Raw CSV/ZIP | 서울시 등에서 내려받은 원본 | 학습 S3 경로에 그대로 올리지 않는다 |
| `archive/` | 날짜별 확정 사실 데이터 Parquet | Raw를 bootstrap/nowcaster로 변환한다 |
| `silver/` | 최신 운영 snapshot/current dimension | collector와 normalizer가 생성한다 |
| `processed_v2/`, `processed/features/` | feature/inference 파생 산출물 | Archive에서 코드로 생성한다 |
| `models/archive/` | 학습된 immutable 모델 artifact | training이 생성하거나 검증된 archive를 올린다 |
| `models/serving-release/` | 실제 추론이 읽는 pair release | `training.publish_serving_release`만 게시한다 |

Raw CSV의 확장자나 파일명만 Parquet/`dt=...`로 바꾸면 안 된다. source별 코드가
컬럼, 타입, 날짜, 중복, 마스킹 및 메타데이터를 정규화해야 한다.

## 2. 최종 S3 체크리스트

### 2.1 2025년 full build 입력

| 구분 | S3 key | 필요한 날짜 | 만드는 방법 |
|---|---|---|---|
| Archive fact | `archive/bike_rental_history/dt=YYYY-MM-DD.parquet` | target 전 35일과 뒤 7일 포함 권장. 초기학습 범위 `2024-11-27`~`2026-01-07` | collector bootstrap 또는 기존 Archive 복사 |
| Archive fact | `archive/bike_station_realtime/dt=YYYY-MM-DD.parquet` | `2025-01-01`~`2025-12-31`, 정확히 365 partition | collector bootstrap. 실제 0행 날짜도 schema 있는 empty Archive로 materialize |
| Archive fact | `archive/weather_ultra_short_live/dt=YYYY-MM-DD.parquet` | 2025년 전체; 첫 tick 재현에는 `2024-12-31` 마지막 3시간도 권장 | collector/bootstrap 또는 기존 Archive 복사 |
| Archive fact | `archive/living_population_grid/dt=YYYY-MM-DD.parquet` | `2025-01-01`~`2025-12-31`, 365 partition | 250m 생활인구 CSV를 nowcaster로 변환 |
| Current dimension | `silver/station_master_enriched/dt=.../hh=.../HHMM.parquet` | 과거 365개가 아니라 최신 snapshot 1개 이상 | station master collector + normalizer enrichment |

feature-engine은 요청 범위의 일별 Archive를 exact key로 읽고 날짜가 빠지면
fail-closed한다. `station_master_enriched`만 최신 current dimension을 사용한다.

### 2.2 추론 및 release 산출물

| 구분 | S3 key 예시 | 생성/게시 주체 |
|---|---|---|
| Station master 중간 출력 | `processed_v2/station_master.parquet/` | feature-engine Spark; multipart prefix |
| 병합 테이블 | `processed/features/w60_e40_t20/station_hour_merged_2025.parquet/` | `feature_engine.spark.run_pipeline` |
| Station fallback | `processed/features/w60_e40_t20/station_hourly_profile.parquet` | `inference.build_station_profile`; 단일 object |
| Population 중간 출력 | `processed_v2/population_2025.parquet/` | feature-engine Spark |
| Population fallback | `processed_v2/population_hourly_profile.parquet` | `inference.build_population_profile` |
| Rental 모델 8개 | `models/archive/dt=<RUN_ID>/<PROFILE>/rental_*` | training 또는 검증된 archive 업로드 |
| Return 모델 8개 | `models/archive/dt=<RUN_ID>/<PROFILE>/return_*` | training 또는 검증된 archive 업로드 |
| Serving pointer | `models/serving-release/current.json` | `training.publish_serving_release` |

Station profile은 실시간 lag/rolling 이력이 부족할 때
`station_no × minute × dow × month`의 평소 통계를 쓰는 fallback이다. 같은 grid,
target, station category 계약의 모델 버전끼리는 재사용할 수 있다.

## 3. 현재 `gng-ubd-s3-bucket` 확인 상태

2026-08-21에 확인한 상태이며 이후에는 다시 검증한다.

- 있음: `archive/bike_rental_history/` — `2024-01-01`~`2026-06-30`, 912개
- 있음: `archive/bike_station_realtime/` — 2025년 363개
- 있음: `archive/weather_ultra_short_live/` — `2025-01-01`~`2026-01-01`, 366개
- 보완 필요: `bike_station_realtime`의 2025-01-09/10. 공식 원본에도 실제 행이
  없어 schema가 있는 0행 partition으로 materialize하는 것이 확정된 처리 방식
- 보완 필요: 2024-12-31 날씨 context. 공식 ASOS 원본에도 실제 행이 없어 schema가
  있는 0행 partition으로 materialize하는 것이 확정된 처리 방식
- 부족: `archive/living_population_grid/`
- 부족: `silver/station_master_enriched/`
- 미확인/미게시: profile, 새 모델 archive, serving release pointer

대여소 현황의 2025-01-09/10과 날씨의 2024-12-31은 기존 전체 학습의 원천 완전성
검사에서도 실제 0행으로 확인됐다(`docs/ml/FULL_YEAR_RESOURCE_REPORT_2025.md`).
인접 날짜 값을 복제하지 않고 schema가 있는 0행 Archive와 manifest로 기록한다.

## 4. AWS MFA 인증

비밀값은 `.env`, Git, 문서 또는 채팅에 기록하지 않는다.

```bash
aws sts get-session-token \
  --serial-number 'arn:aws:iam::<ACCOUNT_ID>:mfa/<DEVICE_NAME>' \
  --token-code <CURRENT_6_DIGIT_CODE> \
  --duration-seconds 3600 \
  --profile gng-ubd
```

응답의 임시 자격증명 3개를 별도 `gng-ubd-mfa` 프로필에 설정한 뒤 확인한다.

```bash
aws sts get-caller-identity --profile gng-ubd-mfa
aws s3 ls 's3://gng-ubd-s3-bucket/' \
  --profile gng-ubd-mfa --region ap-northeast-2
```

SCP `explicit deny`는 사용자 IAM Allow로 우회할 수 없다. MFA 조건을 만족하거나
조직 관리자에게 허용 role/SCP 예외를 요청한다.

## 5. Raw 생활인구 CSV를 Archive로 변환

### 5.1 Raw 계약과 날짜

서울시 250m 생활인구 CSV는 `2025-01-01`~`2025-12-31` 전부 필요하다.

| Raw 컬럼 | Archive 컬럼 | 의미 |
|---|---|---|
| `일자` | `YMD` | 관측 날짜 |
| `시간` | `TT` | 00~23 |
| `행정동코드` | `H_DNG_CD` | 행정동 component |
| `250M격자` | `CELL_ID` | station `grid_id`와 결합할 격자 |
| `생활인구합계` | `SPOP` | `pop_total` 입력 |

나이·성별 컬럼도 `M00`~`M70`, `F00`~`F70`으로 바뀐다. 공식 CSV의 `*`는
결측으로 처리하며 확인된 파일 인코딩은 EUC-KR/CP949다.

월별 압축 해제 디렉터리는 아래와 같이 둔다.

```text
/Users/admin/Downloads/250_LOCAL_RESD_202501/*.csv
...
/Users/admin/Downloads/250_LOCAL_RESD_202512/*.csv
```

월별 파일 수 `31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31`, 합계
365개를 확인한다.

### 5.2 MinIO 변환·적재

```bash
cd /Users/admin/Documents/GitHub/DE_team2-GangnamguUmBokDong/nowcaster
set -a
source ../.env
set +a

for month in {01..12}; do
  echo "=== 2025-${month} 적재 ==="
  S3_ENDPOINT_URL=http://localhost:9000 \
  uv run --frozen python main.py backfill-archive \
    --csv-dir "/Users/admin/Downloads/250_LOCAL_RESD_2025${month}" \
  || break
done
```

이 코드는 컬럼/타입을 정규화하고
`is_estimated=false`, `estimation_method=actual`을 붙여 YMD별 Archive를 쓴다.

```bash
aws --endpoint-url http://localhost:9000 s3 ls \
  "s3://${S3_BUCKET}/archive/living_population_grid/" --recursive \
  | grep 'dt=2025-' | wc -l
# 기대값: 365
```

### 5.3 AWS S3 보존

AWS에서 build/재학습한다면 변환된 Archive도 AWS에 올린다. 먼저 팀에서 허용한
prefix와 `s3:PutObject` 권한을 확인한다.

```bash
mkdir -p /tmp/gng-ubd-population-archive

aws --endpoint-url http://localhost:9000 s3 cp \
  "s3://${S3_BUCKET}/archive/living_population_grid/" \
  /tmp/gng-ubd-population-archive/ --recursive

aws s3 cp /tmp/gng-ubd-population-archive/ \
  's3://gng-ubd-s3-bucket/archive/living_population_grid/' \
  --recursive --profile gng-ubd-mfa --region ap-northeast-2
```

## 6. `station_master_enriched` 생성

### 6.1 사전 데이터

과거 365개가 아니라 다음 최신 데이터를 조합한 current snapshot 한 개가 필요하다.

1. 같은 logical window의 `silver/bike_station_master/...parquet`
2. 그 시각 이전 최신 `silver/living_population_grid/.../nowcast.parquet`의 정적인
   `CELL_ID` 목록
3. 그 시각 이전 24시간 최신 `silver/bike_station_realtime/...parquet`

normalizer는 좌표를 250m CELL_ID/기상 격자에 매핑하고 `ST-<숫자>`에서
`station_no`를 만든다. CELL_ID 매핑률이 95% 미만이면 실패한다.

2번은 Archive 파일만 올린 상태로는 충족되지 않는다. station master logical date
주변의 1~4주 전 생활인구 Archive를 준비하고 nowcaster의 `estimate`를 먼저 실행해
`nowcast.parquet`을 만들어야 한다. 예:

```bash
cd nowcaster
S3_ENDPOINT_URL=http://localhost:9000 \
uv run --frozen python main.py estimate --target-date 2026-08-20
```

`estimate`가 참조할 1~4주 전 날짜가 없다면 먼저 `bootstrap-lookback`으로 공식
CSV에서 필요한 날짜를 적재한다. 운영 날짜가 달라지면 하드코딩된 예시 날짜가 아니라
실제 station master window의 KST 날짜를 사용한다.

### 6.2 생성 예시

실제 master window와 같은 값을 사용한다.

```bash
docker compose --env-file .env -p gold-postgis-v2 \
  -f ops/compose/docker-compose.yml \
  exec -T airflow-scheduler sh -lc '
    cd /workspace/normalizer &&
    env -u VIRTUAL_ENV \
      UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/normalizer \
      uv run --frozen python station_master.py \
      --window-start "2026-08-20T03:00:00+09:00"
  '
```

확인된 성공 예:

```text
station master enriched rows=3428 mapped=3345 coverage=97.579%
output=silver/station_master_enriched/dt=2026-08-20/hh=03/0300.parquet
```

## 7. Feature mart와 fallback profile 생성

### 7.1 Archive 완전성

```bash
for source in bike_rental_history bike_station_realtime \
  weather_ultra_short_live living_population_grid; do
  printf '%s: ' "$source"
  aws --endpoint-url http://localhost:9000 s3 ls \
    "s3://${S3_BUCKET}/archive/${source}/" --recursive \
    | grep 'dt=2025-' | wc -l
done
```

개수뿐 아니라 기대 날짜 집합과 key 집합도 비교한다. 현재 station realtime 363개는
먼저 복구해야 한다.

### 7.2 전체 feature-engine

```bash
cd ml
export TRAIN_WINDOW_START=2025-01-01
export TRAIN_WINDOW_END=2025-12-31

./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
```

조합 관계는 다음과 같다.

```text
bike_rental_history       -> rental_count / return_count
bike_station_realtime     -> 관측 station-time grid, capacity/stockout
station_master_enriched   -> station_id -> station_no, 좌표, grid_id
weather                   -> temp, precip
living_population_grid    -> grid_id별 pop_total
calendar                  -> dow, holiday
                           ↓
processed/features/w60_e40_t20/station_hour_merged_2025.parquet/
```

모델을 새로 학습하지 않고 profile만 만들면 multi-horizon 단계는 생략할 수 있다.
학습까지 할 때만 실행한다.

```bash
./feature_engine/.venv/bin/python -m feature_engine.spark.build_multi_horizon_features
```

### 7.3 Profile

```bash
./inference/.venv/bin/python -m inference.build_station_profile
./inference/.venv/bin/python -m inference.build_population_profile

unset TRAIN_WINDOW_START TRAIN_WINDOW_END
```

Station profile이 실제 집계하는 컬럼은 아래 다섯 개다.

```text
station_no, date, minute, rental_count, return_count
```

날씨/생활인구는 profile 통계에 직접 들어가지 않지만, 현재 구현이 전체 병합 테이블을
먼저 만들기 때문에 필요하다. 별도 profile 전용 pipeline을 만들면 대여이력,
정류소 실시간 상태, station master만으로도 가능하지만 target/window/grid 의미를
기존 feature-engine과 정확히 일치시켜야 한다.

## 8. 모델 archive

Rental과 return 각각 다음 8개, 총 16개가 필요하다.

```text
rental_poisson.txt
rental_q10.txt
rental_q50.txt
rental_q90.txt
rental_metrics.json
rental_profile.json
rental_station_categories.json
rental_conformal_correction.json

return_poisson.txt
return_q10.txt
return_q50.txt
return_q90.txt
return_metrics.json
return_profile.json
return_station_categories.json
return_conformal_correction.json
```

같은 run/profile prefix에 총 16개가 함께 있어도 된다. 파일 개수 외에 profile의
grid/window/embargo/target/horizon과 station category도 검증한다. 현재 임시 AWS
모델은 연결 검증용이며 정확도 검수 없이 production champion으로 간주하지 않는다.

## 9. Pair serving release 게시

### 9.1 Station master 형식

현재 `--station-master-key`는 **정확한 단일 Parquet 또는 canonical crosswalk JSON
object key**여야 한다. Spark multipart인 `processed_v2/station_master.parquet/`를
직접 넘길 수 없다. normalizer의 단일 enriched Parquet을 사용할 수 있다.

```text
silver/station_master_enriched/dt=2026-08-20/hh=03/0300.parquet
```

### 9.2 게시 명령

```bash
cd ml
./training/.venv/bin/python -m training.publish_serving_release \
  --rental-archive-prefix 'models/archive/dt=<RUN_ID>/<PROFILE>' \
  --return-archive-prefix 'models/archive/dt=<RUN_ID>/<PROFILE>' \
  --station-profile-key \
    'processed/features/w60_e40_t20/station_hourly_profile.parquet' \
  --station-master-key \
    'silver/station_master_enriched/dt=<DATE>/hh=<HH>/<HHMM>.parquet'
```

이 명령은 학습하지 않는다. 모델 8개씩, 두 effective contract, profile grid와
station coverage, master crosswalk, 기존 release 호환성을 검증한 뒤 마지막에
`models/serving-release/current.json`을 CAS로 게시한다.

Serving contract를 의도적으로 변경하는 최초 maintenance migration만 팀 승인 후
`--allow-contract-change`를 추가한다.

## 10. `realtime_5min` 전 최종 확인

```bash
aws --endpoint-url http://localhost:9000 s3 ls \
  "s3://${S3_BUCKET}/models/serving-release/current.json"

aws --endpoint-url http://localhost:9000 s3 ls \
  "s3://${S3_BUCKET}/processed_v2/population_hourly_profile.parquet"

aws --endpoint-url http://localhost:9000 s3 ls \
  "s3://${S3_BUCKET}/processed/features/w60_e40_t20/station_hourly_profile.parquet"
```

운영 중 매 5분마다 학습하거나 release를 다시 게시하지 않는다.

```text
최초/모델 변경 시:
모델 archive + station profile + station master
  -> publish_serving_release -> current.json 갱신

매 5분:
realtime_5min -> current.json의 고정된 pair/dependency로 추론
```

## 11. 모델/날짜 범위 변경 시

다음 변경은 기존 profile을 재사용할 수 있다.

- LightGBM 파라미터나 round 수만 변경
- 같은 `w60_e40_t20`에서 학습 sample/profile만 변경
- 새 모델 station category가 기존 profile 범위의 부분집합

다음 변경은 profile을 다시 만들어야 한다.

- grid가 20분에서 5분/60분 등으로 변경
- target horizon 또는 count 정의 변경
- 기존 profile에 없는 station category 추가
- lag/rolling fallback 정의 변경
- 최신 계절/운영 패턴으로 통계 갱신

`month`가 logical key이므로 일부 월만 사용하면 없는 달의 fallback이 빈다. 운영
profile은 최소 12개월을 포함하고 가능하면 모델 학습과 같은 확정 window를 쓴다.

## 12. 장애별 빠른 판별

| 오류 | 의미 | 조치 |
|---|---|---|
| `serving release pointer가 없습니다` | `current.json` 미게시 | 8~9절 완료 |
| `S3에 없음: ...station_hourly_profile.parquet` | station profile 미생성/잘못된 scope | 7절 실행 및 grid 확인 |
| `S3에 없음: processed_v2/population_2025.parquet` | 생활인구 Archive/build 미완료 | 5절 후 7.2 실행 |
| `S3에 없음: ...population_hourly_profile.parquet` | population fallback 미생성 | `inference.build_population_profile` 실행 |
| `station profile minute ... grid와 맞지 않습니다` | hour 또는 다른 grid profile | 활성 grid로 재생성 |
| `station profile이 model support station_no를 누락` | 모델 category가 profile 밖 | 최신 master/동일 범위로 재생성 |
| Archive exact daily partition 없음 | 날짜 누락 | 원천 복구 또는 근거 있는 empty materialization |

## 13. 관련 구현

- Archive 변환: `nowcaster/backfill.py`, `nowcaster/main.py`
- Station master enrichment: `normalizer/station_master.py`
- Archive reader: `ml/feature_engine/spark/silver_source.py`
- 병합: `ml/feature_engine/spark/build_merged_table.py`
- Station profile: `ml/inference/build_station_profile.py`
- Population profile: `ml/inference/build_population_profile.py`
- Pair 게시: `ml/training/publish_serving_release.py`
- Release 계약: `libs/ml_core/serving_release.py`
- 실시간 DAG: `airflow/dags/realtime_5min.py`
