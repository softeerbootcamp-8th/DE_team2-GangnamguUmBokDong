"""inference(배치 조회 CLI + 단일 시점 예측) 전용 경로·상수.

`feature_engine`이 만든 산출물(merged table, hourly profile 등)과 `training`이
저장한 모델(`ml_core/paths.py`의 `MODELS_PREFIX`)을 S3에서 읽기만 한다 — 이
패키지가 새로 만들어 쓰는 산출물(fallback 프로필 제외)은 없다.
"""

from ml_core import common_config
from ml_core.paths import (
    MERGED_TABLE_PARQUET,
    MODELS_PREFIX,
    POPULATION_HOURLY_PROFILE_PARQUET,
    POPULATION_PARQUET,
    PROCESSED_V2_PREFIX,
    RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    STATION_HOURLY_PROFILE_PARQUET,
    STATION_MASTER_PARQUET,
)

__all__ = [
    "EXPOSURE_STOCKOUT_VALUE",
    "GRID_TICK_MINUTES",
    "HORIZON_COUNT",
    "MERGED_TABLE_PARQUET",
    "MODELS_PREFIX",
    "POPULATION_HOURLY_PROFILE_PARQUET",
    "POPULATION_PARQUET",
    "PROCESSED_V2_PREFIX",
    "RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET",
    "RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET",
    "ROLLING_EMBARGO_MINUTES",
    "ROLLING_WINDOW_MINUTES",
    "STATION_HOURLY_PROFILE_PARQUET",
    "STATION_MASTER_PARQUET",
    "TARGET_HORIZON_MINUTES",
    "TEST_END",
    "TEST_START",
]

# --- point-in-time censoring 파라미터 (feature_engine와 반드시 같은 값을 유지 —
# common_config.py에서 공유) ---
ROLLING_TICK_MINUTES = common_config.ROLLING_TICK_MINUTES
ROLLING_WINDOW_MINUTES = common_config.ROLLING_WINDOW_MINUTES
ROLLING_EMBARGO_MINUTES = common_config.ROLLING_EMBARGO_MINUTES
GRID_TICK_MINUTES = common_config.GRID_TICK_MINUTES

# --- 타겟 정의(실시간 rental_count/return_count 재집계에 필요 — feature_engine의
# future_rolling_counts()와 같은 정의를 predict_single.py가 Silver rental로부터 직접
# 계산할 때 씀. common_config.py에서 공유) ---
TARGET_HORIZON_MINUTES = common_config.TARGET_HORIZON_MINUTES

# --- 배치예측 horizon 개수 (feature_engine와 동일 — common_config.py에서 공유) ---
HORIZON_COUNT = common_config.HORIZON_COUNT

EXPOSURE_STOCKOUT_VALUE = common_config.EXPOSURE_STOCKOUT_VALUE

# 배치 조회 CLI의 기본 조회 기간 — 2025 feature mart를 만든 기본 학습 split의
# TEST_DAYS_OF_MONTH={17,19} 중 실제 test 파티션 하나를 고정한다. 학습 전용 split
# 환경변수를 inference가 파싱하면 잘못된 값 하나로 실시간 추론 import까지 죽을 수
# 있으므로 두 설정의 런타임 의존성은 만들지 않는다.
TEST_START = "2025-06-17"
TEST_END = TEST_START
