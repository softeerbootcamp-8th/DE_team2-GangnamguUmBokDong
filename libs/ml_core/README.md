# ml_core — 세 인스턴스가 공유하는 로직

`ml/feature_engine`/`ml/training`/`ml/inference`가 서로 다른 인스턴스에서 따로
배포되지만, 아래 계약(파라미터·경로·핵심 알고리즘)만큼은 세 쪽이 정확히 같은
값/로직을 써야 한다. 여기 모아두지 않으면 한쪽만 고치고 잊어버려 조용히
갈라지는(train-serving skew와 같은 종류의) 사고가 난다.

**이 프로젝트는 `ml/`과 별도로 관리되는 독립 라이브러리다** — `<repo-root>/libs/ml_core/`에
있고, `ml/`의 형제(sibling) 디렉터리다(같은 `lib/` 아래에 다른 서비스의 공유
라이브러리가 더 생길 수 있어서 `common`이 아니라 `ml_core`으로 이름을 붙였다).
`ml/feature_engine`/`ml/training`/`ml/inference`는 각자의 `pyproject.toml`에서
`ml_core`을 editable path 의존성으로 참조한다(`uv sync`로 설치).

이 폴더는 순수 pandas 로직(+ 일부 lightgbm)만 담는다 — `ml/feature_engine/spark/`는
pyspark 의존성이 있어 별도로 분리돼 있고, `common_config.py`만 예외적으로
아주 가벼운 상수 모듈이라 Spark 쪽도 함께 참조한다.

## 파일별 역할

| 파일 | 담당 | 누가 쓰나 |
|---|---|---|
| `common_config.py` | 하이퍼파라미터/censoring 파라미터 — **프로필 로더** | 전부(`feature_engine`, `feature_engine/spark`, `training`, `inference`) |
| `profiles/*.json` | 프로필 파일(파라미터 조합) | `common_config.py`가 읽음 |
| `paths.py` | `data/processed_v2/*.parquet` 산출물 경로, `MODELS_DIR` | 전부 |
| `rolling_window_features.py` | point-in-time censoring 핵심 로직(차분 배열, as-of 조회) | `feature_engine`(배치), `inference`(서빙 시뮬레이션) |
| `trip_events.py` | 대여이력 원본 로딩 + station_no 정규화 | `feature_engine`(배치), `inference`(실시간 시뮬레이션) |
| `model_contract.py` | `FEATURE_COLUMNS`(모델 입력 스키마), station_id 카테고리 저장/로드 | `training`(학습), `inference`(서빙) |
| `serving_contract.py` | 모델 아티팩트와 현재 서빙의 피처 프로필 호환성 계약 | `scoring`, `training.promotion` |
| `metrics.py` | poisson deviance, pinball loss | `training`, `ml_core/scoring.py` |
| `scoring.py` | 저장된 booster로 채점(`predict()`) | `inference`, `training/monitor_performance.py`, `training/legacy/scripts/compare_baselines.py` |

## 경로 계약(`paths.py`)에서 꼭 알아야 할 것

`ml_core`은 `ml/`의 형제 디렉터리라 자기 파일 경로(`__file__`)로는 `ml/`
위치를 알아낼 수 없다(조상 디렉터리가 아니라 아예 다른 가지에 있음). 그래서
`DATA_DIR`/`MODELS_DIR`/`ML_ROOT`의 기본값은 **현재 작업 디렉터리(cwd)** 기준이다
— 이 저장소의 모든 명령이 `cd ml` 다음에 실행되는 걸 전제로 하므로(각 폴더
README 참고) 그 컨벤션을 따르는 한 그대로 동작한다. `cd ml`을 안 지키거나 실제
배포 환경이면 `DATA_ROOT`/`MODELS_ROOT` 환경변수를 명시적으로 설정해야 한다.

## 프로필 시스템

`ML_PROFILE`을 생략하면 코드에 고정된 `builtin-default` 프로필을 사용한다. 이
기본값은 원래 모델 설계인 20분 feature/target/rolling grid와 20분 학습 anchor이며
S3를 조회하지 않으므로, 오래된
`profiles/default.json`이 새 코드의 기본 동작을 암묵적으로 덮어쓸 수 없다.
운영 추론 호출 주기는 이 모델 grid와 분리된 고정 5분 계약이다.

다른 파라미터 조합(window/embargo/LightGBM 하이퍼파라미터 등)을 쓰려면 S3의
`profiles/{ML_PROFILE}.json`에 등록하고 이름을 명시한다. 명시한 프로필이 없거나
조회/파싱에 실패하면 내장값으로 폴백하지 않고 즉시 실패한다. 모델 grid는 기본
20분이며 5/10/15/20/30/60분을 지원한다. `GRID_TICK_MINUTES`와
`ROLLING_TICK_MINUTES`는 같은 값이어야 하고 하루 및 target 구간을 정확히
나누어야 한다.

```bash
cd ml
ML_PROFILE=embargo45 ./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
```

개별 환경변수(예: `ROLLING_EMBARGO_MINUTES=45`)는 프로필 값 위에 추가로
덮어쓸 수 있다 — 우선순위는 **개별 환경변수 > 명시한 S3 프로필**. 단,
학습 grid를 바꾸려면 `GRID_TICK_MINUTES`와 `ROLLING_TICK_MINUTES`를 함께
설정해야 한다. 학습 행만 더 성기게 쓰려면 `TRAIN_ANCHOR_TICK_MINUTES`를 base
grid 이상의 배수로 지정한다(예: g5/a20). anchor를 생략하면 effective grid와 같은
값이 materialize되어 thinning하지 않는다. 한쪽 tick만 바꾸거나 호환되지 않는
간격을 쓰면 설정 검증에서 실패한다. 프로필 등록은
`ml_core.scripts.push_profile`을 사용한다.

학습 아티팩트의 `{model_name}_profile.json`에는 원본 프로필이 아니라 개별
환경변수 override까지 반영한 effective profile이 저장된다. 여기에는 실제
`TRAIN_ANCHOR_TICK_MINUTES`도 항상 포함된다. 추론은 booster를 읽기 전에
rolling/grid/horizon/anchor 계약이 현재 프로세스와 같은지 검증하고, 다르면
예측값을 내지 않는다. 공통 5분 evaluator가 생기기 전에는 서로 다른 anchor에서
측정한 metrics를 자동 승격에서 직접 비교하지 않기 위한 보수적인 제한이다. 학습
기간과 LightGBM 튜닝값 차이는 허용한다(`serving_contract.py` 참고).

학습 날짜 범위는 `common_config.training_window()`가 feature_engine과 training에
동일하게 제공한다. `TRAIN_WINDOW_START`/`TRAIN_WINDOW_END`를 함께 `YYYY-MM-DD`로
지정하면 최초 챔피언 같은 고정 inclusive 구간을 쓰고, 둘 다 없으면
`TRAIN_LOOKBACK_MONTHS`/`TRAINING_SAFETY_MARGIN_DAYS` 기반 rolling 구간을 쓴다.
부분 지정·오형식·역전은 rolling으로 폴백하지 않고 즉시 실패한다.

## 타임존

이 프로젝트의 타임존은 **KST(Asia/Seoul)**로 통일한다. Spark 세션을 새로 만드는
코드는 반드시 `TZ=Asia/Seoul` env(SparkSession 생성 **전**)와
`spark.sql.session.timeZone=Asia/Seoul`을 **같이** 설정해야 한다
(`ml/feature_engine/spark/spark_session.py` 참고). 초 단위 정수 ↔ 타임스탬프
왕복이 필요한 Spark 코드는 `F.unix_timestamp()`/`F.timestamp_seconds()`를 직접
쓰지 말고 `ml/feature_engine/spark/rolling_window_features.py`의
`_unix_seconds_ntz()`/`_seconds_to_ntz()`를 쓸 것 — 세션 타임존과 무관하게
정확한 왕복을 보장한다(모듈 docstring에 근거 설명 있음).

## 검증

```bash
cd ml
./training/.venv/bin/python -m pytest ../libs/ml_core/tests/ -q
```

`ml_core` 자체는 별도 `.venv`를 두지 않는다 — `training`/`inference`/`feature_engine`
중 어느 쪽이든 `ml_core`을 editable 의존성으로 이미 갖고 있으므로 그 `.venv`의
pytest를 그대로 쓰면 된다.
