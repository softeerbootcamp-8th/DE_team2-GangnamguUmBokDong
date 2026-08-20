# training — 실행 방법

`feature_engine`이 Spark로 만든 대여/반납 multi-horizon feature 테이블(S3,
`RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET`/`RETURN_..._PARQUET`, `libs/ml_core/paths.py`)을
읽어 대여/반납을 완전히 분리된 LightGBM 모델(Poisson+exposure, quantile
P10/50/90)로 각각 학습하고, S3 아카이브에 아티팩트를 저장한다.

설계 배경과 각 파일의 상세 로직은 [DESIGN.md](../../docs/ml/training/DESIGN.md), 실험 추적(MLflow)
세팅은 [../../docs/ml/MLFLOW_SETUP.md](../../docs/ml/MLFLOW_SETUP.md) 참고.

## 세팅

```bash
cd ml/training
uv sync   # pyproject.toml/uv.lock 기준 .venv 생성 — lightgbm/pandas/numpy/mlflow-skinny + ml_core(editable) 포함
brew install libomp   # macOS에서 LightGBM 실행에 필요
```

`feature_engine`이 먼저 multi-horizon feature 테이블을 만들어둬야 한다
([feature_engine/README.md](../feature_engine/README.md)). 로컬 개발은
`.env`의 S3 자격증명으로 MinIO(`make up`)를 거친다 — 로컬 파일시스템 폴백은 없다.

## 학습 실행

최초 챔피언은 아래처럼 feature 생성과 학습 양쪽에 같은 명시적 구간을 주어
2025년 전체를 정확히 사용한다. 두 변수는 반드시 쌍으로 지정해야 하며, 한쪽만
있거나 날짜 형식이 잘못됐거나 시작일이 종료일보다 늦으면 실행 전에 실패한다.

운영 historical fact의 확정 입력 계약은 날짜별 `archive/`다. feature engine은
트립/재고/날씨/인구를 Archive에서만 읽고 누락 날짜를 fail-closed하며, 최신 station
master만 Silver current dimension을 사용한다. 따라서 아래 실행 전에 2025와 필요한
앞뒤 context Archive partition이 모두 적재돼 있어야 한다.

```bash
cd ml
export TRAIN_WINDOW_START=2025-01-01
export TRAIN_WINDOW_END=2025-12-31
./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
./feature_engine/.venv/bin/python -m feature_engine.spark.build_multi_horizon_features
./training/.venv/bin/python -m training.train_rental_model --promote-if-no-champion
./training/.venv/bin/python -m training.train_return_model --promote-if-no-champion
unset TRAIN_WINDOW_START TRAIN_WINDOW_END
```

두 구간 변수를 지정하지 않으면 `TRAIN_LOOKBACK_MONTHS`와
`TRAINING_SAFETY_MARGIN_DAYS`로 현재 시점의 rolling window를 계산한다. 월별
재학습 subprocess는 최초학습용 고정 구간을 상위 환경에서 상속하지 않고 이 rolling
경로를 사용한다.

`--promote-if-no-champion`은 학습 결과를 아카이브에 저장한 뒤 기존 promotion의
profile contract 검증을 거쳐 최초 챔피언 포인터를 만든다. 같은 모델의 챔피언이
이미 있으면 학습 전에 오류로 중단하고 포인터를 절대 덮어쓰지 않는다. 따라서 대여
승격 후 반납 학습만 실패한 경우, 위 환경을 다시 설정하고 반납 명령만 재실행하면 된다.

각 명령은 학습 후 poisson deviance/rmse/pinball/커버리지 지표를 출력하고,
S3 아카이브 prefix(`{MODELS_ARCHIVE_PREFIX}/dt={MODEL_ARCHIVE_DATE}/{ML_PROFILE}/`,
`libs/ml_core/paths.py`의 `archive_models_prefix()`)에 아티팩트를 저장한다 —
**챔피언 자리에는 절대 직접 안 쓴다**(아래 "챔피언 승격" 참고). 같은 학습 결과가
[MLflow](../../docs/ml/MLFLOW_SETUP.md)에도 (파라미터+지표+아티팩트 사본으로)
기록된다.

환경변수:

| 변수 | 기본값 | 설명 |
|---|---|---|
| `MODEL_ARCHIVE_DATE` | 오늘(KST) | 아카이브 경로의 날짜 조각 |
| `ML_PROFILE` | 미지정(`builtin-default`) | 미지정 시 S3를 조회하지 않는 내장 g20/r20/a20 프로필. 원격 프로필은 이름을 명시하며, **feature_engine이 이 피처마트를 만들 때 쓴 프로필과 같아야 한다** |
| `GRID_TICK_MINUTES` / `ROLLING_TICK_MINUTES` | `20` / `20` | base feature/target grid와 rolling 계산 grid. 두 값은 같아야 하며 `5, 10, 15, 20, 30, 60` 중 하나 |
| `TRAIN_ANCHOR_TICK_MINUTES` | `GRID_TICK_MINUTES`와 같음 | multi-horizon 학습 행을 남기는 anchor 간격. base grid 이상인 배수이면서 1시간과 1일을 나눠야 한다 |
| `TRAIN_WINDOW_START` / `TRAIN_WINDOW_END` | 미지정(rolling) | 둘 다 `YYYY-MM-DD`로 지정하면 inclusive 고정 학습 구간. 최초 챔피언은 `2025-01-01` / `2025-12-31` 사용 |
| `TRAIN_DAY_DIVISOR` | `1` | 기본은 모든 안전한 train 날짜 사용. 로컬 검증에서만 2, 3, 5로 올려 날짜를 줄이는 비상 dial |
| `MAX_TRAIN_HORIZON` | 제한 없음(`HORIZON_COUNT`) | 읽는 시점에 `horizon <= 이 값`으로도 한 번 더 줄인다 — 그래도 OOM이면 낮출 것(단, 그 이상 horizon 예측 품질은 검증 안 됨) |
| `SPLIT_EMBARGO_DAYS` | horizon/target에서 계산(현재 `1`) | 같은 anchor가 train/valid/test에 걸치지 않도록 평가일 앞뒤에서 purge할 날짜 수. 계산된 최소값보다 낮출 수 없음 |

`SERVING_TICK_MINUTES`는 위 학습 설정과 별개인 5분 고정 코드 계약이며 환경변수
dial이 아니다. 따라서 기본 모델은 **g20/r20/a20으로 학습하고 5분마다 추론**한다.
`TRAIN_ANCHOR_TICK_MINUTES`를 생략하면 override된 base grid를 따라가므로,
g5/r5/a5를 원하면 g/r만 5로 설정해도 된다. g5/r5/a20처럼 base feature는 5분으로
만들되 학습 행만 20분 anchor로 줄이려면 a를 명시한다.

후속 A/B 실험은 다음 세 조합을 고정해 비교한다.

| arm | base/rolling/training | 목적 |
|---|---|---|
| A | g20/r20/a20 | 기존 모델 설계이자 기본값 |
| B | g5/r5/a20 | 5분 base feature가 공통 20분 anchor의 값에 영향을 주는지 확인 |
| C | g5/r5/a5 | 모든 5분 anchor를 학습했을 때 off-grid 성능 이득과 자원 비용 확인 |

세 arm 모두 같은 독립 5분 test mart에서 평가해야 하며, 각자 다른 밀도의 자체
test 지표를 직접 비교하면 안 된다. A와 B의 공통 00/20/40분 anchor는 key,
feature, label parity도 먼저 검증한다. 실험 산출물은 서로 다른 프로필/경로로
격리하고 현재 챔피언에 자동 승격하지 않는다. 현재 primary 중간 테이블 일부는
`processed_v2/` 공용 경로를 쓰므로 서로 다른 arm의 feature build를 병렬로
실행하지 말고 순차 실행한다.

base feature 산출물은 `w{window}_e{embargo}_t{grid}` namespace를 사용하고,
multi-horizon 학습 테이블만 그 아래
`training_anchor_a{TRAIN_ANCHOR_TICK_MINUTES}`로 추가 격리된다. 따라서
g5/r5/a5와 g5/r5/a20은 base feature를 재사용하되 학습 테이블은 덮어쓰지 않는다.
`FEATURE_PARAM_COMBO_ID`를 직접 지정하면 자동 base-grid 격리를 우회하므로,
서로 다른 g/r 조합에 같은 custom ID를 재사용하면 안 된다.

OOM이면 먼저 전체 horizon을 유지한 채 train 날짜를 결정적으로 줄인다. 예를 들어
`TRAIN_DAY_DIVISOR=2`는 평가/embargo 날짜를 제외한 매월 짝수 날짜만 학습에 쓴다.

```bash
TRAIN_DAY_DIVISOR=2 ./training/.venv/bin/python -m training.train_rental_model --promote-if-no-champion
```

그래도 부족한 로컬 검증에서는 마지막 수단으로 `MAX_TRAIN_HORIZON=6`을 함께 줄일
수 있다. 날짜 축소는 계절·요일 표본을 줄이고, horizon 축소는 그보다 먼 예측 구간을
아예 학습하지 않으므로 둘 다 전체 설정보다 품질 위험이 크다. 특히 horizon을 줄인
모델의 7~12시간 예측 품질은 검증되지 않는다. 과거 문서의
`TRAIN_SAMPLE_FRAC`/`VALID_SAMPLE_FRAC`/`TEST_SAMPLE_FRAC`는 실제 로더에 적용되지
않던 가짜 dial이라 제거했으며, 설정하면 이제 즉시 오류를 낸다.

남은 메모리 한계도 있다. feature 행렬은 날짜별 `lgb.Sequence`로 지연 로드하지만,
각 split의 label/date(+대여 exposure) prepass는 아직 선택 날짜 전체를 하나의 pandas
DataFrame으로 읽는다. 이 1차원 계열만으로도 메모리를 넘는 규모라면 날짜별 prepass
집계로 별도 재설계해야 하며, 현재 dial이 그 peak를 해결해 주지는 않는다.

## 산출물 (S3 아카이브)

| 키 | 내용 |
|---|---|
| `{archive_prefix}/{rental,return}_poisson.txt` | Poisson booster |
| `{archive_prefix}/{rental,return}_q{10,50,90}.txt` | quantile booster 3개씩 |
| `{archive_prefix}/{rental,return}_station_categories.json` | 학습 시 고정한 station_no 카테고리(순서 포함) — `inference`가 그대로 로드해야 함 |
| `{archive_prefix}/{rental,return}_conformal_correction.json` | split-conformal 보정값 |
| `{archive_prefix}/{rental,return}_metrics.json` | 테스트셋 평가 지표(다음 달 모니터링의 baseline) |
| `{archive_prefix}/{rental,return}_profile.json` | 환경변수 override까지 반영해 이 학습에 실제 적용된 effective profile(재현/서빙 계약 확인용) |

`archive_prefix`는 `libs/ml_core/paths.archive_models_prefix(date, profile_name)`
= `"{MODELS_ARCHIVE_PREFIX}/dt={date}/{profile_name}"`. 위 6개 파일 전부
[MLflow](../../docs/ml/MLFLOW_SETUP.md)에도 params/metrics/artifacts로 같이
기록된다(중복 저장 — MLflow가 S3 아카이브를 대체하지 않음).

## 챔피언 승격 (`training.promotion`)

학습은 항상 아카이브에만 쓴다 — "지금 서빙 중인 모델"은 `champion/{model_name}.json`
포인터가 어느 archive_prefix를 가리키는지로 정해진다(`ml_core.paths.write_champion_pointer()`/
`read_champion_prefix()`). 파일을 챔피언 자리로 복사하지 않고 포인터 하나만
원자적으로 바꾼다 — 승격 도중 booster는 새 버전, station_categories는 옛
버전인 식으로 섞인 모델이 서빙되는 사고를 원천 차단한다(자세한 근거는
`read_champion_prefix()` docstring).

`training.promotion.should_promote(challenger_metrics, champion_metrics)`이
판정한다 — **둘 다 만족해야 승격**: `poisson_deviance_test`가 챔피언보다 나쁘지
않고, `p10_p90_coverage_calibrated_test`가 목표 커버리지 ± 허용 드리프트
범위 안(둘 다 `common_config.py`의 기존 상수 재사용, 계절성 때문에 절대
임계값 대신). 챔피언이 아직 없으면 무조건 승격(부트스트랩).

지표를 통과해도 챌린저의 서빙 피처 계약이 현재 프로세스 또는 반대 모델
챔피언과 다르면 포인터를 바꾸지 않는다. 대여/반납 중 하나만 다른 rolling/window/
horizon 의미로 승격되는 것을 막기 위한 안전장치다. 최초 부트스트랩처럼 반대
챔피언이 아직 없을 때는 현재 서빙 계약만 맞으면 승격할 수 있다. 학습 기간과
LightGBM 파라미터만 다른 프로필은 호환되는 것으로 본다.

## 월별 성능 모니터링 / 재학습 트리거

```bash
./training/.venv/bin/python -m training.monitor_performance --as-of 2026-08-01 --horizon 1
# 재학습까지 자동으로:
./training/.venv/bin/python -m training.scripts.monthly_retrain_check              # 점검만 (dry-run)
./training/.venv/bin/python -m training.scripts.monthly_retrain_check --execute    # 기준 미달 시 실제 재학습 + 승격 판정까지
```

`monitor_performance`는 챔피언의 baseline(`{model_name}_metrics.json`)과 최근
`MONITOR_LOOKBACK_MONTHS`개월 실측을 비교해 재학습 필요 여부를 판정하고, 그
결과를 MLflow(`bike-demand-monitoring` experiment)에도 기록한다 — MLflow
서버가 안 떠 있어도 판정 자체는 안 막힌다.

`monthly_retrain_check.py --execute`는 재학습이 필요한 모델마다 후보 프로필들을
순서대로 시도한다 — 각 시도는 `feature_engine`(Spark, 별도 subprocess)과
`training.train_rental_model`/`train_return_model`(별도 subprocess)을 실행해
새 아카이브를 만든 뒤 `should_promote()`로 판정하고, 통과하면
`promote_challenger()`로 포인터만 전환한다(파일 덮어쓰기 없음). 어느 프로필도
기준을 못 넘으면 챔피언은 그대로 두고 조용히 종료한다.

후보 중 현재 서빙과 rolling/window/embargo/target/grid/horizon 계약이 다른
프로필은 Spark와 학습을 시작하기 전에 건너뛴다. 월별 자동 경로는 동일한 피처
계약 안에서 학습 기간이나 LightGBM 설정을 튜닝하는 용도다. 피처 계약 자체를
바꾸려면 feature 생성, 대여·반납 모델, inference를 함께 전환하는 별도 배포로
진행해야 한다.

## 검증

```bash
cd ml
./training/.venv/bin/python -m pytest training/tests/ ../libs/ml_core/tests -q
```
