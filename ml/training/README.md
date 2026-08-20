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

```bash
cd ml
./training/.venv/bin/python -m training.train_rental_model   # 대여 모델
./training/.venv/bin/python -m training.train_return_model   # 반납 모델
```

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
| `ML_PROFILE` | `default` | `libs/ml_core/profiles/{이름}.json` — tick/embargo 등 피처 프로필. **feature_engine이 이 피처마트를 만들 때 쓴 프로필과 같아야 한다** |
| `TRAIN_DAY_DIVISOR` | `2` | 학습에 쓸 날짜를 그 달의 배수일로 줄인다(짝수날=2) — 2025년 전체 multi-horizon 테이블이 로컬 RAM에 다 못 올라갈 만큼 커서(8억 행대) 도입한 메모리 절감 장치. OOM이 나면 3, 5로 올릴 것 |
| `MAX_TRAIN_HORIZON` | 제한 없음(`HORIZON_COUNT`) | 읽는 시점에 `horizon <= 이 값`으로도 한 번 더 줄인다 — 그래도 OOM이면 낮출 것(단, 그 이상 horizon 예측 품질은 검증 안 됨) |

## 산출물 (S3 아카이브)

| 키 | 내용 |
|---|---|
| `{archive_prefix}/{rental,return}_poisson.txt` | Poisson booster |
| `{archive_prefix}/{rental,return}_q{10,50,90}.txt` | quantile booster 3개씩 |
| `{archive_prefix}/{rental,return}_station_categories.json` | 학습 시 고정한 station_no 카테고리(순서 포함) — `inference`가 그대로 로드해야 함 |
| `{archive_prefix}/{rental,return}_conformal_correction.json` | split-conformal 보정값 |
| `{archive_prefix}/{rental,return}_metrics.json` | 테스트셋 평가 지표(다음 달 모니터링의 baseline) |
| `{archive_prefix}/{rental,return}_profile.json` | 이 학습에 쓴 프로필 전체(재현/서빙 조건 확인용) |

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

## 검증

```bash
cd ml
./training/.venv/bin/python -m pytest training/tests/ ../libs/ml_core/tests -q
```
