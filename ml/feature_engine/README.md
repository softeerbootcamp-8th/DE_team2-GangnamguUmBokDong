# feature_engine — 실행 방법

collector가 S3(MinIO)의 Silver 레이어에 쌓은 원본(대여이력, 정류소 재고,
날씨, 250m 생활인구)을 station×5분 tick 단위로 병합하고 lag/rolling feature를
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
| 정류소 마스터 | `silver/station_master_enriched/dt=.../hh=.../HHMM.parquet` 중 최신 snapshot |
| 대여/반납 타겟 | `silver/bike_rental_history/...`(sparse step function으로 집계) |
| 재고 스냅샷 | `silver/bike_station_realtime/...` |
| 날씨(관측) | `silver/weather_ultra_short_live/...` |
| 생활인구 | `silver/living_population_grid/...` |

날씨는 실제 수집 tick마다 유효한 서울 격자 전체를 평균낸다. 병합할 때 작은 weather
테이블만 5분 grid로 과거 방향 forward-fill하고, 다음 관측 전·최대 3시간까지만
exact join한다. 그래서 08:00 행은 08:00에 알 수 있던 값, 08:50 행은 직전 값,
08:55 행은 08:55에 새로 도착한 값을 쓰며 미래 tick이 같은 시간의 과거 행으로
역전파되지 않는다. 최초 window 시작점에도 같은 fallback을 적용하려고 weather
중간 산출물만 시작점 이전 3시간 source context를 보존한다.

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

> 운영 historical 학습의 목표 입력은 확정된 `archive/{source}/dt=...`이고,
> `silver/`는 온라인 최신 추론용이다. 현재 Spark reader는 historical fact도
> Silver에서 읽으므로 archive reader/backfill 통합 전에는 아래 최초 2025 명령을
> production bootstrap으로 간주하면 안 된다. 로컬 Silver seed는 개발 검증용이다.
> `station_master_enriched`만 최신 Silver current dimension을 계속 사용한다.

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

최초 2025년 챔피언용 feature mart를 만들 때는 두 Spark 단계 모두에
`TRAIN_WINDOW_START=2025-01-01`과 `TRAIN_WINDOW_END=2025-12-31`을 함께 지정한다.
두 값은 `training`도 같은 환경으로 실행해야 한다. 둘 다 미지정하면 현재 시점의
rolling window를 쓰며, 한쪽만 지정하거나 잘못된 날짜/역전된 구간이면 실행 전에
실패한다. 종료일은 inclusive라 내부에서는 다음날 00:00을 exclusive upper bound로
바꿔 트립/재고/날씨/인구와 최종 tick/multi-horizon 테이블에 모두 적용한다. 따라서
Silver에 2026년 행이 이미 있어도 위 최초 빌드의 최종 시계열에는 2025년 행만
들어간다. 유일한 예외는 첫 2025 target tick의 as-of 조회에 필요한 weather 중간
산출물의 2024-12-31 21:00 이후 context이며, 최종 feature 행 범위에는 포함되지 않는다.
타겟은 `[T,T+60분)` 전체가 이 경계 안에서 완결된 기준시각만 사용하므로, 2026년
outcome 없이는 정답을 완성할 수 없는 12월 31일 23:05~23:55 tick은 조용히 0으로
잘라 넣지 않고 제외한다(마지막 완결 tick은 23:00).

`run_pipeline.py`는 워터마크(`_watermark.json`, 파라미터 조합별)가 없으면 전체
구간을 처음부터 만들고, 있으면 `common_config.INCREMENTAL_LOOKBACK_HOURS`만큼
과거부터 다시 계산해 해당 날짜 파티션을 overwrite한다. 다만 명시적
`TRAIN_WINDOW_START/END` 실행은 이전 rolling 실행의 바깥 파티션을 남기지 않도록
워터마크가 있어도 전체 overwrite한다. **`build_multi_horizon_features.py`는
`run_pipeline.py`가 자동으로 이어서 실행하지 않는다** — 별도 단계다(원본의
최대 `HORIZON_COUNT`배 행 수로 불어나는 무거운 self-join이라, tick 단위
테이블만 다시 만들고 싶을 때 매번 다시 돌릴 필요가 없도록 분리해뒀다).

파라미터 조합(window/embargo/tick)마다 `feature_engine/spark/config.py`의
`OUTPUT_ROOT`가 자동으로 분리되므로, 다른 조합을 실험할 때 챔피언 산출물을
덮어쓸 걱정 없이 `ROLLING_EMBARGO_MINUTES=45 ./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline`처럼
환경변수만 바꿔 실행하면 된다(또는 `ML_PROFILE=embargo45`로 S3 프로필째 교체 —
[ml_core README](../../libs/ml_core/README.md)). `ML_PROFILE` 미지정 시 쓰는 내장
`builtin-default` 값: **5분 tick**(`GRID_TICK_MINUTES`/`ROLLING_TICK_MINUTES`), 60분 rolling
window, 40분 embargo, horizon 1~12(`HORIZON_COUNT`).

`station_master_enriched`는 2025년 당시 snapshot이 존재한다고 보장할 수 없는
serving용 current dimension이라 명시적 2025 window에서도 최신 snapshot을 사용한다.
즉 시계열 행은 2025년으로 엄격히 제한되지만, 과거 시점의 거치대 수/좌표 변경까지
복원하는 point-in-time station dimension은 아직 지원하지 않는다. historical station
dimension이 확보되면 별도 계약과 backfill이 필요하다.

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
| `{OUTPUT_ROOT}/station_hour_merged_2025.parquet` | `run_pipeline.py` | station×5분 tick 병합 테이블 |
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
