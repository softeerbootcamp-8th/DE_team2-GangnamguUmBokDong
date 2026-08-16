# feature_engine — 실행 방법

`data/`의 원본(대여이력, 정류소, 날씨, 250m 생활인구)을 station×5분tick 단위로
병합하고 lag/rolling feature를 붙여 `training/`이 바로 학습할 수 있는
feature 테이블(`station_hour_features_2025.parquet`)을 만든다.

설계 배경과 각 파일의 상세 로직은 [DESIGN.md](DESIGN.md) 참고.

## 본 서비스 코드는 `spark/`뿐이다

**이 폴더의 pandas 코드(1차 정제 + 옛 2차정제 구현)는 전부 `feature_engine/legacy/`에
있고, 본 서비스 저장소에는 필요 없다.** 실제 배포에서는 1차 정제(원본 CSV/parquet
→ station_master/targets/status/weather/population) 자체를 이 저장소 밖의 다른
시스템이 처리한다(history.md 5번 항목) — `feature_engine`은 그 산출물이
`data/processed_v2/*`에 이미 있다고 가정하고 **2차 정제(피처마트 생성)만
Spark로** 담당한다.

| | 위치 | 실행 환경 | 역할 |
|---|---|---|---|
| **2차 정제(Spark) — 본 서비스** | `feature_engine/spark/*.py` | `feature_engine/.venv`(uv, Python 3.11, 로컬은 `local[*]` 단일 노드) 또는 EMR `spark-submit` | 1차 정제 산출물을 병합·feature화해 최종 feature 테이블 생성 |
| **1차 정제 + 옛 2차정제(pandas) — legacy, 로컬 테스트 전용** | `feature_engine/legacy/*.py` | `feature_engine/.venv` | 1차 정제 산출물을 로컬에서 재현해 위 Spark 경로를 테스트해볼 입력을 만들 뿐, 본 서비스 경로가 아님 |

Spark 구현이 이미 pandas와 정확히 같은 값을 낸다는 것을 parity 테스트
(`dev_spark_rolling_parity.py`/`dev_spark_build_features.py`/`dev_spark_incremental.py`)로
확인했으므로, 피처엔지니어링(2차정제)은 Spark 코드만 유지한다. `feature_engine/legacy/`가
`ml_common`(파라미터·경로 계약, `<repo-root>/libs/ml_common/` — `ml/`과 별도로
관리되는 공유 라이브러리)만 참조하고 `spark/`를 import하지 않으므로, Spark
쪽을 EMR에 올릴 때는 `feature_engine/spark/` + `libs/ml_common/` 디렉터리만 있으면
된다. 분류 근거는 [../LEGACY_AUDIT.md](../LEGACY_AUDIT.md) 참고.

## 세팅

```bash
cd ml/feature_engine
uv sync   # pyproject.toml/uv.lock 기준 .venv 생성 — pyspark(Python 3.11) + ml_common(editable) 포함
```

필요한 원본 데이터(이미 `ml/data/`에 있어야 함)는 [DATA_CATALOG.md](../DATA_CATALOG.md) 참고.

## 실행 — 2차 정제(Spark, 본 서비스)

```bash
cd ml
# 로컬 테스트 (local[*] 단일 노드 — 1차 정제 산출물이 data/processed_v2/에 있어야 함)
./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline

# EMR
spark-submit --deploy-mode cluster feature_engine/spark/run_pipeline.py
```

`run_pipeline.py`는 워터마크(`_watermark.json`, 파라미터 조합별)가 없으면 전체
히스토리로 처음부터 만들고, 있으면 `common_config.INCREMENTAL_LOOKBACK_HOURS`만큼만
다시 계산해서 새 행만 append한다. 파라미터 조합(window/embargo/tick)마다
`feature_engine/spark/config.py`의 `OUTPUT_ROOT`가 자동으로 분리되므로, 다른 조합을
실험할 때 챔피언 산출물을 덮어쓸 걱정 없이
`ROLLING_EMBARGO_MINUTES=45 ./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline`처럼
환경변수만 바꿔 실행하면 된다(또는 `ML_PROFILE=embargo45`로 프로필째 교체 —
[ml_common README](../../libs/ml_common/README.md) 참고).

**주의**: Spark 스크립트/테스트는 반드시 `feature_engine/.venv`(uv, Python 3.11)로
실행할 것. `training`/`inference`의 venv는 pyspark가 지원하지 않는 Python
버전을 쓴다.

## 실행 — 1차 정제(legacy pandas, 로컬 테스트 입력 준비용)

실제 배포에서는 안 쓴다 — 위 Spark 경로를 로컬에서 처음부터 테스트해보고 싶을
때, 다른 시스템이 만들어줄 1차 정제 산출물을 로컬에서 대신 만들어주는 용도다.

```bash
cd ml
./feature_engine/.venv/bin/python -m feature_engine.legacy.scripts.run_build_pipeline
```

순서대로 실행하는 단계: `build_station_master`(정류소 마스터 + grid_id) →
`build_targets`(대여/반납 sparse 타겟) → `build_station_status`(재고 스냅샷) →
`build_weather`(날씨 정제) → `build_population`(250m 생활인구 정제). 개별 단계만
다시 돌리고 싶으면 `python -m feature_engine.legacy.build_weather`처럼 모듈
하나씩 실행하면 된다.

## 검증

```bash
cd ml
./feature_engine/.venv/bin/python -m pytest feature_engine/tests/dev_spark_rolling_parity.py feature_engine/tests/dev_spark_build_features.py feature_engine/tests/dev_spark_incremental.py -q
```

`dev_spark_rolling_parity.py`/`dev_spark_incremental.py`는 pandas(`ml_common.rolling_window_features`,
이미 검증된 기준 구현)와 Spark 버전이 정확히 같은 결과를 내는지 대조하는 핵심
회귀 테스트다 — `spark/` 쪽을 고치면 반드시 다시 통과하는지 확인해야 한다.

legacy 전용 테스트(1차 정제 진단, 옛 pandas 2차정제 단위 테스트)는 기본 검증에서
뺐다 — 필요하면 `./feature_engine/.venv/bin/python -m pytest feature_engine/legacy/tests -q`로
개별 실행.

## 산출물

| 경로 | 만드는 단계 | 내용 |
|---|---|---|
| `data/processed_v2/station_master.parquet` | 1차정제(legacy) | 정류소 마스터 + grid_id |
| `data/processed_v2/targets_2025.parquet`, `return_targets_2025.parquet` | 1차정제(legacy) | 대여/반납 sparse 타겟(5분 tick) |
| `data/processed_v2/station_status_2025.parquet` | 1차정제(legacy) | 재고 스냅샷 |
| `data/processed_v2/weather_2025.parquet` | 1차정제(legacy) | 정제된 날씨 |
| `data/processed_v2/population_2025.parquet` | 1차정제(legacy) | 정제된 생활인구 |
| `data/processed_v2/spark/{PARAM_COMBO_ID}/station_hour_merged_2025.parquet` | 2차정제(Spark) | 병합 테이블 (5분 tick 그리드) |
| `data/processed_v2/spark/{PARAM_COMBO_ID}/rolling_rental_features_2025.parquet` | 2차정제(Spark) | point-in-time censored 대여 카운트(sparse) |
| `data/processed_v2/spark/{PARAM_COMBO_ID}/station_hour_features_2025.parquet` | 2차정제(Spark) | **최종 feature 테이블** — `training/`이 읽는 입력 |

`training`/`inference`는 `ml_common.paths`를 통해 이 경로를 그대로 읽는다 —
`libs/ml_common/paths.py`가 `feature_engine/spark/config.py`와 정확히 같은 공식
(`FEATURE_ENGINEERING_OUTPUT_ROOT`/`FEATURE_PARAM_COMBO_ID` 환경변수 포함)으로
`{PARAM_COMBO_ID}` 경로를 계산하므로 별도 복사/심링크가 필요 없다. 다른 파라미터
조합으로 실험할 때는 두 환경변수를 Spark 실행/training·inference 실행 양쪽에
같은 값으로 설정할 것 — 자세한 내용은 [../LEGACY_AUDIT.md](../LEGACY_AUDIT.md) 참고.
