# 2025년 전체 운영 후보·serving release 생성 가이드

> 이 문서의 과거 로컬 `processed_v2` 입력, 단일 multi-horizon 테이블,
> `TRAIN_START/END`, `*_SAMPLE_FRAC`, LightGBM 분산 실행 절차는 현재 구현과 맞지 않아
> 제거했다. 실행 계약의 기준 문서는
> [feature_engine README](../../ml/feature_engine/README.md)와
> [training README](../../ml/training/README.md)다.

목표 데이터 계약은 과거/월별 학습의 확정 사실 데이터는 `archive/{source}/dt=...`
에서 읽고, 온라인 5분 추론만 최신 `silver/`를 읽는 것이다. collector의 CSV/API
bootstrap도 과거 원천을 archive에 적재한다.

2025년 전체 운영 후보의 기본 해상도는 **g20/r20/a20 학습 + 5분 서빙**이다. g/r은
`{5, 10, 15, 20, 30, 60}`분 중 같은 값을 사용할 수 있고, formal
`TRAIN_ANCHOR_TICK_MINUTES`는 생략 시 g와 같다. anchor를 별도로 지정하면
g 이상인 배수이면서 1시간과 1일을 나눠야 한다. 아래 기본 실행은 별도
override 없이 g20/r20/a20을 사용한다.

`feature_engine.spark.silver_source`는 historical fact(트립/재고/날씨/인구)를
날짜별 Archive에서 읽고 누락 날짜를 fail-closed한다. 최신
`station_master_enriched`만 historical snapshot이 없는 current dimension으로
Silver에서 읽는 계약을 유지한다. 따라서 아래 실행 전 남은 데이터 전제는 2025와
앞뒤 context Archive partition을 실제로 모두 적재하는 것이다.

## 전제 조건

- 2025 CSV/API 원천이 source별 `archive/` partition에 모두 적재돼 있어야 한다.
- feature engine historical reader는 archive schema를 현재 feature schema로 변환하며,
  트립/재고/날씨/인구 fact에 `silver/` fallback을 두지 않는다.
- 최신 `silver/station_master_enriched`는 current station dimension 입력으로 사용할
  수 있다.
- 위 archive 경로로 작은 날짜 구간의 feature/target parity 검증이 먼저 통과해야 한다.

## 전제 충족 후 실행

각 패키지 환경을 먼저 준비한다.

```bash
cd ml
(cd feature_engine && uv sync)
(cd training && uv sync)
(cd inference && uv sync)
```

archive에 필요한 2025 원천 및 앞뒤 context 파티션이 적재된 뒤 다음 순서로 실행한다.

대형 ZIP을 쓰는 로컬 준비에서는 본 기간 2025년 자료에 대여 타깃 경계 context를
함께 staging한다. 기본 계약은 앞 35일(`INCREMENTAL_LOOKBACK_HOURS=840`), 뒤
7일(`TRAINING_SAFETY_MARGIN_DAYS=7`)이다.

```bash
cd collector
uv run --frozen python -m bootstrap.zip_stage \
  --zip ../data/아카이브.zip \
  --bootstrap-dir ../data/issue163-full-year/bootstrap \
  --population-dir ../data/issue163-full-year/population \
  --from 2025-01-01 --to 2025-12-31 \
  --rental-context-before-days 35 --rental-context-after-days 7

uv run --frozen python -m bootstrap \
  --source bike_rental_history \
  --from 2024-11-27 --to 2026-01-07 \
  --csv-dir ../data/issue163-full-year/bootstrap \
  --csv-batch-by-month

uv run --frozen python -m bootstrap \
  --source bike_station_realtime \
  --from 2025-01-01 --to 2025-12-31 \
  --csv-dir ../data/issue163-full-year/bootstrap \
  --csv-batch-by-month --materialize-empty-archive
```

월별 배치는 원천 파일이나 날짜를 표본 추출하지 않는다. 날짜별 Archive 출력은
같고, 한 번에 상주하는 Arrow 결과만 한 달로 제한한다. 날씨는 2024-12-31의 직전
3시간까지 별도 적재하고, 생활인구는 staging한 2025년 365일 전체를 적재한다.
`--materialize-empty-archive`는 실제 0행인 재고 날짜를 누락 파일과 구분되는 스키마
있는 0행 Parquet과 manifest로 남겨 exact daily Archive 계약을 만족시킨다.

```bash
cd ml
export TRAIN_WINDOW_START=2025-01-01
export TRAIN_WINDOW_END=2025-12-31
export MODEL_ARCHIVE_DATE=<RUN_DATE>

./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
./feature_engine/.venv/bin/python -m feature_engine.spark.build_multi_horizon_features
./inference/.venv/bin/python -m inference.build_station_profile
./inference/.venv/bin/python -m inference.build_population_profile
./training/.venv/bin/python -m training.train_rental_model
./training/.venv/bin/python -m training.train_return_model

unset TRAIN_WINDOW_START TRAIN_WINDOW_END MODEL_ARCHIVE_DATE
```

두 window 변수는 반드시 쌍으로 지정한다. feature 생성과 학습이 같은 값을 받아야
하며, 종료일은 inclusive다. Spark 단계는 2026년 이후 시계열이 archive에 있어도
2025년 범위 밖 행을 제외하고, 마지막 날짜에 23:00을 넘는 anchor(기본 grid의
23:20/23:40)처럼 `[T,T+60분)` 라벨이 완결되지 않는 행도 학습 테이블에서
제외해야 한다.
`station_master_enriched`만 historical snapshot이
보장되지 않는 current dimension이라 최신 Silver snapshot을 사용한다.

`<RUN_DATE>`는 두 모델에 공통으로 쓸 immutable 실행 식별자다. #163 실행은
challenger-only이므로 개별 `champion/{model_name}.json`을 바꾸는
`--promote-if-no-champion`을 붙이지 않는다. 대여가 성공하고 반납이 실패하면 같은
window/date/profile로 반납 명령만 재실행할 수 있다.

초기 실행 뒤 두 window 변수를 반드시 해제한다. 월별 재학습 오케스트레이터도 자식
프로세스에서 이 변수를 제거해 최신 rolling window를 강제하지만, 장기 배포 환경에
초기값을 남겨두지 않는 것이 명확하다.

두 모델의 16개 artifact, effective profile/source fingerprint, fallback profile을
새 process에서 검증한 뒤에만 정확한 archive prefix로 pair release를 게시한다.

```bash
cd ml
./training/.venv/bin/python -m training.publish_serving_release \
  --rental-archive-prefix 'models/archive/dt=<RUN_DATE>/builtin-default' \
  --return-archive-prefix 'models/archive/dt=<RUN_DATE>/builtin-default' \
  --station-profile-key 'processed/features/w60_e40_t20/station_hourly_profile.parquet' \
  --station-master-key 'processed_v2/station_master.parquet'
```

성공 출력의 generation, release manifest URI/SHA와 station crosswalk source
fingerprint를 실행 manifest에 보존한다. 현재 release와 feature 계약을 바꾸는 최초
maintenance migration이면 팀 승인 후에만 `--allow-contract-change`를 추가한다.
Rental/return을 개별 champion으로 순차 승격하지 않는다.

## 메모리 부족 시 지원되는 축소 방법

현재 실제 로더가 지원하는 첫 번째 비상 dial은 train 날짜의 결정적 축소다.

```bash
TRAIN_DAY_DIVISOR=2 ./training/.venv/bin/python -m training.train_rental_model
```

그래도 부족한 개발 검증에서만 `MAX_TRAIN_HORIZON=6`처럼 최대 horizon을 줄일 수
있다. 날짜 축소는 계절/요일 표본을 줄이고, horizon 축소는 먼 예측 구간의 품질을
검증하지 못하게 하므로 둘 다 전체 설정보다 품질 위험이 있다.

`TRAIN_SAMPLE_FRAC`/`VALID_SAMPLE_FRAC`/`TEST_SAMPLE_FRAC`는 구현되지 않은 과거
설정이라 사용하면 즉시 오류가 난다. multi-horizon anchor 간격은 이제
`TRAIN_ANCHOR_TICK_MINUTES`로 명시하는 정식 계약이다. 다만 anchor 변경은 학습
시각과 `minute` 분포를 바꾸는 모델 설계 변경이므로 단순 OOM 우회로 취급하지
않고 별도 프로필과 공통 5분 평가셋으로 검증한다.

feature 행렬은 날짜 청크로 읽고, 각 split의 label/exposure 사전 스캔도 날짜별로
읽은 즉시 삭제 예약된 로컬 scratch memmap에 이어 쓴다. 2025년 전체 대여 prepass를
하나의 pandas DataFrame으로 합치던 경로는 process-tree RSS 23.64GiB에서 32GB WSL의
3GiB 가용 메모리 보호선을 넘겨 제거했다. 이 변경은 날짜·horizon을 줄이지 않지만
학습 중 label/exposure/init-score 크기만큼 로컬 scratch가 추가로 필요하다.

train과 valid native Dataset의 동시 상주가 한계인 단일 머신에서는
`LGB_DEFER_VALID_DATASET=true`를 사용할 수 있다. train은 전체 날짜·horizon으로
고정 round 학습하고, 학습 Dataset을 해제한 뒤 valid 전체를 날짜별 streaming
predict해 지표와 conformal correction을 계산한다. 평가 표본은 줄지 않지만 early
stopping은 사용하지 않으므로 `LGB_NUM_BOOST_ROUND`와 이 모드를 실행 manifest에
함께 남기고 일반 기본 경로와 구분한다.

## 과거 자원 실측(참고 전용)

아래 값은 현행 configurable grid/anchor 계약 이전, 2025년 11월 일부와 과거 anchor 실험에서
얻은 수치다. 현행 용량 산정값이 아니라 상대적인 증가 폭을 이해하는 참고 자료다.

| 과거 anchor 밀도 | 한 달 학습 행 수 | 당시 peak RAM | 당시 대여 모델 학습 시간 |
|---|---:|---:|---:|
| 정각(60분) | 약 2,197만 | 약 3GB(비정밀) | 수 분 |
| 20분 | 약 6,592만 | 약 10.14GB | 약 60분 |
| 5분 | 약 2.6억 이상 | 18GB 로컬 머신에서 OOM | 미완료 |

후속 해상도 검증은 A=g20/r20/a20, B=g5/r5/a20,
C=g5/r5/a5로 분리한다. A/B의 공통 20분 anchor parity를 먼저 확인하고 세 모델을
동일한 독립 5분 test mart에서 평가한다. 각 arm의 자체 밀도 test 지표끼리는
비교하지 않으며, 실험 산출물은 별도 프로필/경로에 저장하고 자동 승격하지 않는다.

실제 2025 전체 실행 전에는 대상 머신에서 작은 날짜 범위로 smoke test하고, Spark
shuffle/스토리지와 학습 peak RSS를 관찰한다. 현재 training은 분산 LightGBM worker
구성을 완성한 상태가 아니므로, 환경변수 몇 개만 켜서 분산 실행할 수 있다고 가정하면
안 된다.

장시간 로컬 실행은 저장소 루트의 `ops/resource_probe.py`로 감싼다. 예를 들어
가용 RAM 3GiB를 WSL과 다른 프로세스에 남기고 학습만 안전 종료하려면 다음처럼
실행한다.

```bash
python ops/resource_probe.py \
  --manifest data/issue163-full-year/resource/train-rental.json \
  --label issue163-train-rental \
  --sample-seconds 1 --filesystem-path . \
  --min-system-memory-available-gib 3 \
  -- bash -c 'cd ml && ./training/.venv/bin/python -m training.train_rental_model'
```

정상·실패·signal 종료뿐 아니라 resource guard 종료도 같은 JSON에 wall time,
process-tree peak RSS/swap, system memory/swap, filesystem peak 소비량과 종료 코드를
남긴다. Guard는 데이터나 horizon을 줄이지 않고 대상 process group만 종료하므로,
`status=resource_limit`은 동일 계약을 더 큰 학습 인스턴스에서 실행해야 한다는
증거로 사용한다.
