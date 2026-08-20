# feature_engine — 실행 방법

collector가 S3(MinIO)의 Silver 레이어에 쌓은 원본(대여이력, 정류소 재고,
날씨, 250m 생활인구)을 station×20분tick 단위로 병합하고 lag/rolling feature를
붙여, `training/`이 바로 학습할 수 있는 대여/반납 multi-horizon feature 테이블
2개를 만든다.

설계 배경과 각 파일의 상세 로직은 [DESIGN.md](../../docs/ml/feature_engine/DESIGN.md) 참고.

## 이제 로컬 pandas "1차 정제" 단계가 없다

**옛날엔 `feature_engine/legacy/`(pandas)나 저장소 밖 다른 시스템이 원본을
`station_master`/`targets`/`station_status`/`weather`/`population` 중간
산출물로 미리 만들어 로컬 파일시스템에 둬야 했다 — 지금은 아니다.**
`feature_engine/spark/silver_source.py`가 S3 Silver를 직접 읽어서(`ml/data/processed_v2/*`
같은 로컬 파생 데이터는 전혀 안 봄) 그 중간 산출물을 매 실행마다 Spark로 다시
만든다(`run_pipeline.py`의 `_refresh_primary_tables()`). "processed_v2"라는
이름은 그 중간 산출물이 놓이는 **S3 키 prefix**로만 남아 있다 — 로컬
legacy pandas 폴더는 저장소에서 완전히 삭제됐다.

| 소스 | Silver 경로 |
|---|---|
| 정류소 마스터 | `silver/station/station_master.parquet` |
| 대여/반납 타겟 | `silver/bike_rental_history/...`(sparse step function으로 집계) |
| 재고 스냅샷 | `silver/bike_station_realtime/...` |
| 날씨(관측) | `silver/weather_ultra_short_live/...` |
| 생활인구 | `silver/living_population_grid/...` |

## 세팅

```bash
cd ml/feature_engine
uv sync   # pyproject.toml/uv.lock 기준 .venv 생성 — pyspark(Python 3.11) + ml_core(editable) 포함
```

로컬 개발은 실제 collector 없이 MinIO만으로 검증한다 — 저장소 루트에서
`make up`으로 MinIO/Postgres를 띄운 뒤, 샘플 원본(`ml/data/`에 있는 예시
parquet)을 Silver 스키마로 변환해 올리는 `dev/seed_s3_from_local.py`를 먼저
돌려야 한다(`dev/README.md` 참고) — collector가 실제로 운영되는 환경에서는
이 시딩 단계가 필요 없다.

## 실행

```bash
cd ml
# 0) 로컬에서만: 샘플 원본을 Silver 스키마로 MinIO에 시딩
uv run python dev/seed_s3_from_local.py --start-date 2025-01-01 --end-date 2025-12-31

# 1) Silver -> 병합 테이블 -> tick 단위 feature 테이블(horizon=1 전용)
./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline

# 2) 그 테이블을 horizon=1..HORIZON_COUNT(기본 12)로 확장 — 대여/반납 각각 별도 테이블
./feature_engine/.venv/bin/python -m feature_engine.spark.build_multi_horizon_features

# EMR
spark-submit --deploy-mode cluster feature_engine/spark/run_pipeline.py
spark-submit --deploy-mode cluster feature_engine/spark/build_multi_horizon_features.py
```

`run_pipeline.py`는 워터마크(`_watermark.json`, 파라미터 조합별)가 없으면 전체
히스토리로 처음부터 만들고, 있으면 `common_config.INCREMENTAL_LOOKBACK_HOURS`만큼만
다시 계산해서 새 행만 append한다. **`build_multi_horizon_features.py`는
`run_pipeline.py`가 자동으로 이어서 실행하지 않는다** — 별도 단계다(원본의
최대 `HORIZON_COUNT`배 행 수로 불어나는 무거운 self-join이라, tick 단위
테이블만 다시 만들고 싶을 때 매번 다시 돌릴 필요가 없도록 분리해뒀다).

파라미터 조합(window/embargo/tick)마다 `feature_engine/spark/config.py`의
`OUTPUT_ROOT`가 자동으로 분리되므로, 다른 조합을 실험할 때 챔피언 산출물을
덮어쓸 걱정 없이 `ROLLING_EMBARGO_MINUTES=45 ./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline`처럼
환경변수만 바꿔 실행하면 된다(또는 `ML_PROFILE=embargo45`로 프로필째 교체 —
[ml_core README](../../libs/ml_core/README.md)). 기본 프로필(`libs/ml_core/profiles/default.json`)
값: **20분 tick**(`GRID_TICK_MINUTES`/`ROLLING_TICK_MINUTES`), 60분 rolling
window, 40분 embargo, horizon 1~12(`HORIZON_COUNT`).

**주의**: Spark 스크립트/테스트는 반드시 `feature_engine/.venv`(uv, Python 3.11)로
실행할 것. `training`/`inference`의 venv는 pyspark가 지원하지 않는 Python
버전을 쓴다.

## 검증

```bash
cd ml
./feature_engine/.venv/bin/python -m pytest feature_engine/tests/dev_spark_rolling_parity.py feature_engine/tests/dev_spark_build_features.py feature_engine/tests/dev_spark_incremental.py feature_engine/tests/dev_spark_multi_horizon_parity.py -q
```

`dev_spark_rolling_parity.py`/`dev_spark_incremental.py`는 pandas(`ml_core.rolling_window_features`,
이미 검증된 기준 구현)와 Spark 버전이 정확히 같은 결과를 내는지 대조하는 핵심
회귀 테스트다 — `spark/` 쪽을 고치면 반드시 다시 통과하는지 확인해야 한다.
`dev_spark_multi_horizon_parity.py`는 horizon=1 확장 결과가 원본 tick 테이블의
해당 행과 완전히 같은 값을 내는지 확인한다.

## 산출물 (S3)

| 키 | 만드는 단계 | 내용 |
|---|---|---|
| `processed_v2/station_master.parquet` 등 | `run_pipeline.py`의 `_refresh_primary_tables()` | Silver를 재집계한 중간 산출물(정류소 마스터, sparse 타겟, 재고, 날씨, 인구) |
| `{OUTPUT_ROOT}/station_hour_merged_2025.parquet` | `run_pipeline.py` | station×20분tick 병합 테이블 |
| `{OUTPUT_ROOT}/rolling_rental_features_2025.parquet` | `run_pipeline.py` | point-in-time censored 대여 카운트(sparse) |
| `{OUTPUT_ROOT}/station_hour_features_2025.parquet` | `run_pipeline.py` | tick 단위 feature 테이블(horizon=1 전용, 중간 산출물) |
| `{OUTPUT_ROOT}/station_hour_features_multihorizon_rental_2025.parquet` | `build_multi_horizon_features.py` | **최종 대여 학습 테이블** — `training.train_rental_model`이 읽는 입력 |
| `{OUTPUT_ROOT}/station_hour_features_multihorizon_return_2025.parquet` | `build_multi_horizon_features.py` | **최종 반납 학습 테이블** — `training.train_return_model`이 읽는 입력 |

`OUTPUT_ROOT`는 `{FEATURE_ENGINEERING_OUTPUT_PREFIX}/{FEATURE_PARAM_COMBO_ID}`
(파라미터 조합마다 분리). `training`/`inference`는 `ml_core.paths`를 통해 이
경로를 그대로 읽는다 — `libs/ml_core/paths.py`가 `feature_engine/spark/config.py`와
정확히 같은 공식(`FEATURE_ENGINEERING_OUTPUT_ROOT`/`FEATURE_PARAM_COMBO_ID`
환경변수 포함)으로 계산하므로 별도 복사/심링크가 필요 없다. 다른 파라미터
조합으로 실험할 때는 두 환경변수를 Spark 실행/training·inference 실행 양쪽에
같은 값으로 설정할 것.

## 피처 스키마

모델이 실제로 보는 feature 목록의 단일 소스는 `libs/ml_core/common_config.py`의
`BASE_FEATURE_COLUMNS` + `libs/ml_core/model_contract.py`의
`RENTAL_FEATURE_COLUMNS`/`RETURN_FEATURE_COLUMNS`다(대여/반납 각각 자기 lag
컬럼 1개만 추가). 여기서 다시 나열하지 않는다 — 값이 바뀌면 이 문서가 아니라
그 두 파일만 고치면 되게 하려는 의도다.
