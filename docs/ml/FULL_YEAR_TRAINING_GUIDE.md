# 2025 전체연도 모델 생성 가이드

> 현재 상태: **현행 Archive → Spark → LightGBM → serving release 경로 기준**
>
> 상세 계약: [Feature Engine](feature_engine/DESIGN.md) · [Training](training/DESIGN.md)
> · [과거 자원 실측](FULL_YEAR_MEMORY_PROBE_2026-08-22.md)

이 문서는 2025년 전체 데이터를 사용해 대여·반납 challenger를 만들고, 검수한 pair를
운영 serving release로 게시하는 순서를 설명한다. 실행 완료 전까지 기존 운영 release는
변경되지 않는다.

## 1. 기본 실행 계약

| 항목 | 기본값 |
|---|---|
| 학습 기간 | `2025-01-01`~`2025-12-31` inclusive |
| base/rolling/training grid | g20/r20/a20 |
| 서빙 주기 | 5분 |
| target 구간 | `[T, T+60분)` |
| 생성 horizon | 1..12 |
| 기본 학습 horizon | `1,2,3,4,5,6,9,12` |
| adaptive anchor | 활성화 |
| 평일/휴일 peak | 07–21시 / 08–21시 |

`feature_engine.spark.silver_source`는 historical fact(트립/재고/날씨/인구)를
날짜별 Archive에서 읽는다. 요청 구간 중 일부 날짜만 없으면(2026-08부터) 그
날짜만 건너뛰고 경고를 남긴 채 계속하고, 요청 구간 **전체**가 다 없을 때만
fail-closed한다(대여/반납처럼 타겟에 가까운 소스도 포함 — 결측 날짜가 조용히
"수요 0"으로 들어갈 수 있다는 트레이드오프를 감수하고 학습이 절대 실패하지
않는 쪽을 택한 결정).

Feature Engine은 horizon 1..12 mart를 생성하지만 Training은 기본적으로
`TRAIN_HORIZONS`의 8개 horizon만 읽는다. 또한 `ADAPTIVE_TRAIN_ANCHORS=true`이므로
모든 시간대를 같은 밀도로 학습하지 않는다. 실행 manifest에는 effective profile과
이 선택값을 반드시 함께 남긴다.

## 2. 데이터 전제 조건

다음 historical fact가 날짜별 Archive에 있어야 한다.

- `archive/bike_rental_history/dt=YYYY-MM-DD.parquet`
- `archive/bike_station_realtime/dt=YYYY-MM-DD.parquet`
- `archive/weather_ultra_short_live/dt=YYYY-MM-DD.parquet`
- `archive/living_population_grid/dt=YYYY-MM-DD.parquet`
- 최신 `silver/station_master_enriched/...` snapshot

Historical reader는 누락된 Archive 날짜를 Silver로 대체하지 않는다. 요청 구간 중
일부 날짜만 없으면 그 날짜만 건너뛰고 경고를 남긴 채 계속하고, 요청 구간
**전체**가 다 없거나 스키마가 잘못됐을 때만 실패한다. 정류소 마스터만 historical
dimension이 없어 최신 Silver를 사용한다.

대여 이력은 기본적으로 앞 35일과 뒤 7일 context가 필요하다. 날씨는 첫 target의
as-of 조회를 위해 시작 전 3시간 context가 필요하다. 실제 요구 날짜는 Feature Engine
로그의 `exact daily partitions` 출력으로 확인한다.

## 3. 원천 Archive 준비

로컬 대형 ZIP을 사용하는 경우 필요한 기간을 먼저 staging한다.

```bash
cd collector
uv run --frozen python -m bootstrap.zip_stage \
  --zip ../data/아카이브.zip \
  --bootstrap-dir ../data/issue163-full-year/bootstrap \
  --population-dir ../data/issue163-full-year/population \
  --from 2025-01-01 --to 2025-12-31 \
  --rental-context-before-days 35 \
  --rental-context-after-days 7
```

대여이력과 재고를 날짜별 Archive로 적재한다.

```bash
uv run --frozen python -m bootstrap \
  --source bike_rental_history \
  --from 2024-11-27 --to 2026-01-07 \
  --csv-dir ../data/issue163-full-year/bootstrap \
  --csv-batch-by-month

uv run --frozen python -m bootstrap \
  --source bike_station_realtime \
  --from 2025-01-01 --to 2025-12-31 \
  --csv-dir ../data/issue163-full-year/bootstrap \
  --csv-batch-by-month \
  --materialize-empty-archive
```

월별 batch는 표본 추출이 아니라 메모리 상주 범위만 줄인다.
`--materialize-empty-archive`는 실제 0행 날짜와 누락 날짜를 구분하기 위한 schema 있는
빈 Archive를 만든다. 날씨와 생활인구도 같은 기간 계약으로 별도 적재해야 한다.

전체연도 실행 전에 [로컬 학습 smoke](LOCAL_TRAINING_SMOKE.md)로 짧은 실제 원천 구간이
끝까지 통과하는지 확인한다.

## 4. 환경 준비

```bash
cd ml
(cd feature_engine && uv sync --frozen)
(cd inference && uv sync --frozen)
(cd training && uv sync --frozen)
```

S3/MinIO 접속 환경과 MLflow tracking URI를 확인한다. AWS 학습 EC2는
`terraform/compute_train.tf`의 전용 인스턴스를 사용하며 평소에는 정지 상태로 둔다.
과거 실측은 64GiB RAM을 권장했지만 현재 코드의 exact peak를 보장하는 값은 아니므로
대상 commit에서 resource probe를 다시 실행한다.

## 5. Feature mart와 fallback profile 생성

고정 window와 실행 ID를 한 shell에서 유지한다.

```bash
cd ml
export TRAIN_WINDOW_START=2025-01-01
export TRAIN_WINDOW_END=2025-12-31
export MODEL_ARCHIVE_DATE=<RUN_ID>

./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
./feature_engine/.venv/bin/python -m feature_engine.spark.build_multi_horizon_features
./inference/.venv/bin/python -m inference.build_station_profile
./inference/.venv/bin/python -m inference.build_population_profile
```

두 window 변수는 반드시 쌍으로 지정한다. Feature Engine은 마지막 날짜에서 60분
target이 완결되지 않는 anchor를 제외한다. 중간 `processed_v2/` prefix 일부는 profile
간 공유되므로 서로 다른 grid/profile 빌드는 병렬 실행하지 않는다.

## 6. 대여·반납 challenger 학습

```bash
./training/.venv/bin/python -m training.train_rental_model
./training/.venv/bin/python -m training.train_return_model
```

`--promote-if-no-champion`은 붙이지 않는다. 두 명령은 같은
`models/archive/dt=<RUN_ID>/builtin-default/`에 모델별 artifact를 쓰지만 champion이나
serving release pointer는 변경하지 않는다. 한 모델만 실패하면 같은 window·run ID로
그 모델만 다시 실행할 수 있다.

장시간 실행은 checkpoint를 활성화한다.

```bash
export TRAIN_CHECKPOINT_INTERVAL_ROUNDS=25
export TRAIN_RESUME_FROM_CHECKPOINT=true
./training/.venv/bin/python -m training.train_rental_model
./training/.venv/bin/python -m training.train_return_model
```

재개 시 데이터·split·profile·LightGBM 파라미터·코드 fingerprint가 달라지면 실패한다.
같은 checkpoint를 재사용하려면 `MODEL_ARCHIVE_DATE`를 그대로 유지해야 한다.

## 7. 자원 부족 대응 순서

먼저 전체 표본을 유지하는 옵션을 사용한다.

```bash
LGB_DEFER_VALID_DATASET=true \
  ./training/.venv/bin/python -m training.train_rental_model
```

이 모드는 valid를 학습 후 streaming 평가하므로 메모리는 줄지만 early stopping을 쓰지
않는다. 그래도 부족한 개발 검증에서만 날짜나 horizon을 줄인다.

```bash
TRAIN_DAY_DIVISOR=2 \
  ./training/.venv/bin/python -m training.train_rental_model

MAX_TRAIN_HORIZON=6 TRAIN_HORIZONS=1,2,3,4,5,6 \
  ./training/.venv/bin/python -m training.train_rental_model
```

날짜 축소는 계절·요일 표본을 줄이고 horizon 축소는 먼 시점 품질을 검증하지 못한다.
운영 후보라면 축소 내용을 별도 profile과 실행 증거에 기록해야 한다.
`TRAIN_SAMPLE_FRAC`, `VALID_SAMPLE_FRAC`, `TEST_SAMPLE_FRAC`는 지원하지 않는다.

`ADAPTIVE_TRAIN_ANCHORS`, peak 시간, `TRAIN_HORIZONS` 변경은 단순 메모리 옵션이 아니라
학습 표본 계약 변경이다. 기본 모델과 동일하다고 간주하지 말고 독립 test/backtest로
비교한다.

## 8. Resource probe

장시간 로컬 실행은 `ops/resource_probe.py`로 감싸 process tree와 system 여유를
함께 기록한다.

```bash
python ops/resource_probe.py \
  --manifest data/issue163-full-year/resource/train-rental.json \
  --label issue163-train-rental \
  --sample-seconds 1 \
  --filesystem-path . \
  --min-system-memory-available-gib 3 \
  -- bash -c 'cd ml && ./training/.venv/bin/python -m training.train_rental_model'
```

`status=resource_limit`은 학습 성공이 아니다. 동일 계약을 더 큰 인스턴스에서 다시
실행해야 한다. 과거 2026-08-22 측정값은
[메모리 실측 문서](FULL_YEAR_MEMORY_PROBE_2026-08-22.md)의 조건과 함께만 인용한다.

## 9. Artifact 검수

대여와 반납 각각 다음 8개 artifact가 있어야 한다.

- Poisson, Q10, Q50, Q90 booster 4개
- station categories
- conformal correction
- metrics
- effective profile

다음을 함께 확인한다.

1. 두 model archive prefix와 effective serving contract가 일치한다.
2. station category와 station master crosswalk가 1:1이다.
3. station fallback profile의 grid와 model contract가 같다.
4. deviance와 calibrated coverage가 승인 기준을 만족한다.
5. 5분 serving smoke와 독립 backtest를 통과한다.
6. 기존 serving release pointer가 아직 변경되지 않았다.

## 10. Pair serving release 게시

검수한 exact pair만 수동 게시한다.

```bash
cd ml
./training/.venv/bin/python -m training.publish_serving_release \
  --rental-archive-prefix 'models/archive/dt=<RUN_ID>/builtin-default' \
  --return-archive-prefix 'models/archive/dt=<RUN_ID>/builtin-default' \
  --station-profile-key 'processed/features/w60_e40_t20/station_hourly_profile.parquet' \
  --station-master-key 'processed_v2/station_master.parquet'
```

명령은 model snapshot, effective contract, station profile과 station crosswalk를
content-addressed object로 고정하고 마지막에 `models/serving-release/current.json`
pointer를 CAS로 전환한다. 실패하거나 충돌하면 기존 release를 유지한다.

현재 release와 serving feature 계약이 달라지는 승인된 maintenance migration에서만
`--allow-contract-change`를 추가한다. 성공 출력의 다음 값을 실행 증거로 보존한다.

- `generation`
- `release_manifest_uri`
- `release_manifest_byte_sha256`
- `station_crosswalk_source_fingerprint_sha256`
- source object/row count

## 11. 종료 정리

```bash
unset TRAIN_WINDOW_START TRAIN_WINDOW_END MODEL_ARCHIVE_DATE
unset TRAIN_CHECKPOINT_INTERVAL_ROUNDS TRAIN_RESUME_FROM_CHECKPOINT
```

월별 재학습은 고정 2025 window가 아니라 rolling window를 사용하므로 장기 환경에 위
변수를 남기지 않는다. AWS 학습 인스턴스는 로그·artifact·release 증거를 확인한 뒤
정지한다.
