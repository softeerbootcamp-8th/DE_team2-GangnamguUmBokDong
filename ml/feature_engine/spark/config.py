"""feature_engine 패키지 전역 설정 — 경로/파라미터.

이 패키지는 EMR에 그대로 올라가는 걸 전제로 하므로, `src/`(로컬 pandas 파이프라인,
"1차 정제"와 검증용)에 의존하지 않고 독립적으로 동작한다. 로컬 개발도 항상
S3(MinIO)를 거친다 — 로컬 파일시스템 폴백은 없다. 경로는 전부 `s3a://{버킷}/{키}`
형태의 단일 URI 문자열이다(Spark의 Hadoop-S3A 필요 형식). 경로 조합에
`pathlib.Path`나 `"/".join(...)`을 쓰지 않고 f-string으로 그때그때 만든다
(`collector/storage.py`와 통일된 컨벤션).

**입력**: collector Silver(`SILVER_ROOT` 아래, `silver_source.py` 참고)뿐이다.
`ml/data/processed_v2/*` 같은 로컬 파생 데이터는 전혀 안 본다 —
station_master/targets/station_status/weather/population(예전엔 "이미 S3에
존재한다고 가정"했던 1차 정제 산출물)을 이제 이 패키지가 Silver로부터 직접
만들어(`run_pipeline._refresh_primary_tables()`) 아래 경로들에 저장한 뒤, 그걸
"피처마트 생성"(2차 정제) 단계가 읽는다 — 두 단계 다 이 패키지 안에 있다.

`src/`와 반드시 같은 값을 써야 하는 파라미터(censoring 윈도우, lag/rolling 등)는
`common_config.py`(ml/ 루트, 순수 상수 모듈 — pandas/pyspark 등 무거운 의존성 없음)에서
가져온다. 이 파일이 `src/`를 직접 import하지 않는 건 그대로 유지한다 — `common_config.py`
하나만 두 패키지가 같이 참조하는 구조라 EMR에 이 패키지만 올려도 그 작은 파일 하나만
같이 올리면 된다. 같은 이유로 `libs/ml_core/paths.py`도 import하지 않고 같은 이름의
상수를 독립적으로 다시 정의한다 — **두 파일이 가리키는 실제 키는 반드시 같아야
하므로, 한쪽을 고치면 다른 쪽도 같이 고칠 것.** 컬럼명 매핑·source_id는 대신
`libs/ml_core/silver_schema.py`를 그대로 import해서 쓴다(그건 순수 상수/문자열
모듈이라 가볍고, `inference`의 실시간 조회와 같은 스키마를 보장해야 해서 중복을
피했다).
"""

import os

from ml_core import common_config

# --- 학습기간 롤링 윈도우 (Silver glob/공휴일 계산 범위 — silver_source.py,
# build_merged_table.py) — 2026-08부터 고정 TRAIN_YEAR 대신 "오늘 기준 최근
# TRAIN_LOOKBACK_MONTHS개월"로 매번 다시 계산한다(common_config.training_window()).
# 매달 재학습 전에 feature_engine이 먼저 다시 도는데, 고정 연도면 다음 해로
# 넘어갈 때마다 코드/환경변수를 수동으로 바꿔야 했다 — 이제 프로필의
# TRAIN_LOOKBACK_MONTHS/TRAINING_SAFETY_MARGIN_DAYS 값만 바뀌면(재배포 없이,
# S3 프로필 갱신만으로) 다음 실행부터 반영된다.
WINDOW_START, WINDOW_END = common_config.training_window()

# ml_core.s3_io가 boto3 쪽에서 쓰는 것과 같은 환경변수 — 기본값은 dev/MinIO의
# 기본 버킷 이름("local-dev", dev/s3_client.py와 동일). 합성 데이터로 Spark
# DataFrame을 직접 만들어 쓰는 테스트는 이 값을 몰라도 되므로(실제 S3 I/O를
# 안 함), strict하게 요구하지 않고 소프트 기본값을 둔다.
S3_BUCKET = os.environ.get("S3_BUCKET", "local-dev")


def _s3a(key: str) -> str:
    """S3 키를 Spark Hadoop-S3A용 단일 URI 문자열로 만든다."""
    return f"s3a://{S3_BUCKET}/{key}"


# collector Silver 계층의 루트 — `silver_source.py`가 이 아래에서 소스별 glob을
# 만든다. 테스트에서 로컬 tmp_path 디렉터리로 monkeypatch하기 쉽도록 이 하나의
# 상수로 분리해뒀다(`dev_spark_incremental.py` 참고).
SILVER_ROOT = os.environ.get("SILVER_ROOT", _s3a("silver"))

# --- 1차 정제 산출물 (이제 이 패키지가 Silver로부터 직접 만들어 저장 — silver_source.py) ---
STATION_MASTER_PARQUET = os.environ.get("STATION_MASTER_PARQUET", _s3a("processed_v2/station_master.parquet"))
TARGETS_PARQUET = os.environ.get("TARGETS_PARQUET", _s3a("processed_v2/targets_2025.parquet"))
RETURN_TARGETS_PARQUET = os.environ.get("RETURN_TARGETS_PARQUET", _s3a("processed_v2/return_targets_2025.parquet"))
STATION_STATUS_PARQUET = os.environ.get("STATION_STATUS_PARQUET", _s3a("processed_v2/station_status_2025.parquet"))
WEATHER_PARQUET = os.environ.get("WEATHER_PARQUET", _s3a("processed_v2/weather_2025.parquet"))
POPULATION_PARQUET = os.environ.get("POPULATION_PARQUET", _s3a("processed_v2/population_2025.parquet"))

# --- point-in-time censoring 파라미터 (src/config.py와 반드시 같은 값을 유지 — common_config.py에서 공유) ---
ROLLING_TICK_MINUTES = common_config.ROLLING_TICK_MINUTES
ROLLING_WINDOW_MINUTES = common_config.ROLLING_WINDOW_MINUTES
ROLLING_EMBARGO_MINUTES = common_config.ROLLING_EMBARGO_MINUTES

# --- 타겟(예측 대상) 정의 (src/config.py와 동일 — common_config.py에서 공유) ---
# "T로부터 앞으로 TARGET_HORIZON_MINUTES분 동안 일어날 이벤트 수"를 GRID_TICK_MINUTES
# 간격의 모든 T에 대해 예측 (build_targets.py의 future_rolling_counts()).
TARGET_HORIZON_MINUTES = common_config.TARGET_HORIZON_MINUTES
GRID_TICK_MINUTES = common_config.GRID_TICK_MINUTES

# --- 배치예측 horizon 개수 (common_config.py에서 공유 — build_multi_horizon_features.py 참고) ---
HORIZON_COUNT = common_config.HORIZON_COUNT

EXPOSURE_STOCKOUT_VALUE = 0.05

# --- 출력: 이 패키지("2차 정제"/피처마트 생성)가 만드는 산출물 ---
# 파라미터 조합(윈도우/임바고/틱)마다 결과가 달라지므로, 산출물 경로 자체에 조합
# ID를 넣어 모델마다(=파라미터 조합마다) 서로 덮어쓰지 않도록 분리한다. 예:
# "w60_e30_t5" (챔피언), "w60_e45_t5"(챌린저) 등이 OUTPUT_ROOT 아래 각자 폴더를 갖는다.
PARAM_COMBO_ID = os.environ.get(
    "FEATURE_PARAM_COMBO_ID",
    f"w{ROLLING_WINDOW_MINUTES}_e{ROLLING_EMBARGO_MINUTES}_t{ROLLING_TICK_MINUTES}",
)
# libs/ml_core/paths.py의 FEATURE_ENGINEERING_OUTPUT_DIR과 동일 공식
# ("processed/features/{조합ID}") — dev/S3_DATA_CATALOG.md의 prefix를 따른다.
_OUTPUT_PREFIX = os.environ.get("FEATURE_ENGINEERING_OUTPUT_PREFIX", "processed/features")
# Spark가 아니라 plain boto3(ml_core.s3_io)로 읽고 쓰는 워터마크용 — s3a:// 스킴이
# 필요 없는 순수 키(watermark.py 참고, Spark 리더/라이터를 안 씀).
OUTPUT_ROOT_KEY = f"{_OUTPUT_PREFIX}/{PARAM_COMBO_ID}"
OUTPUT_ROOT = _s3a(OUTPUT_ROOT_KEY)
ROLLING_RENTAL_FEATURES_PARQUET = f"{OUTPUT_ROOT}/rolling_rental_features_2025.parquet"
MERGED_TABLE_PARQUET = f"{OUTPUT_ROOT}/station_hour_merged_2025.parquet"
# 순수 키(s3a:// 스킴 없음) 버전도 같이 둔다 — run_pipeline.py가 증분 실행 전
# ml_core.s3_io(boto3)로 이 prefix 밑에 구버전 flat parquet이 섞여 있는지
# 점검할 때 필요(Spark 리더/라이터는 FEATURES_TABLE_PARQUET(s3a://)를 그대로 씀).
FEATURES_TABLE_KEY = f"{OUTPUT_ROOT_KEY}/station_hour_features_2025.parquet"
FEATURES_TABLE_PARQUET = _s3a(FEATURES_TABLE_KEY)
# FEATURES_TABLE_PARQUET의 각 행(T0)을 horizon=1..HORIZON_COUNT로 self-join한 학습 테이블
# (build_multi_horizon_features.py). 대여/반납이 서로 다른 lag/타겟을 쓰는 완전히
# 분리된 데이터셋이라 출력도 둘로 나뉜다 — libs/ml_core/paths.py의 같은 이름
# 상수와 동일 공식.
RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET = f"{OUTPUT_ROOT}/station_hour_features_multihorizon_rental_2025.parquet"
RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET = f"{OUTPUT_ROOT}/station_hour_features_multihorizon_return_2025.parquet"
WATERMARK_PATH = f"{OUTPUT_ROOT_KEY}/_watermark.json"

# --- 증분 재생성 시 얼마나 과거까지 다시 계산해서 겹치는 구간을 보정할지 (common_config.py에서 공유) ---
# lag_168h(7일)보다 넉넉하게 잡은 안전 마진 — 이보다 짧으면 새로 추가되는 구간의
# 초반 며칠치 lag/rolling이 이전 데이터를 못 보고 결측/오류가 날 수 있다.
INCREMENTAL_LOOKBACK_HOURS = common_config.INCREMENTAL_LOOKBACK_HOURS
