"""inference(배치 조회 CLI + 단일 시점 예측) 전용 경로·상수.

`feature_engineering`이 만든 산출물(merged table, hourly profile 등)과 `training`이
저장한 모델(`ml_common/paths.py`의 `MODELS_PREFIX`)을 S3에서 읽기만 한다 — 이
패키지가 새로 만들어 쓰는 산출물(fallback 프로필 제외)은 없다.
"""

from ml_common import common_config
from ml_common.paths import (
    ANALYSIS_SUMMARY_JSON,
    MERGED_TABLE_PARQUET,
    MODELS_PREFIX,
    MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    POPULATION_HOURLY_PROFILE_PARQUET,
    POPULATION_PARQUET,
    PROCESSED_V2_PREFIX,
    STATION_HOURLY_PROFILE_PARQUET,
    STATION_MASTER_PARQUET,
    load_holidays_2025,
)

__all__ = [
    "ANALYSIS_SUMMARY_JSON",
    "EXPOSURE_STOCKOUT_VALUE",
    "GRID_TICK_MINUTES",
    "HORIZON_COUNT",
    "LAG_HOURS",
    "MERGED_TABLE_PARQUET",
    "MODELS_PREFIX",
    "MULTI_HORIZON_FEATURES_TABLE_PARQUET",
    "POPULATION_HOURLY_PROFILE_PARQUET",
    "POPULATION_PARQUET",
    "PROCESSED_V2_PREFIX",
    "ROLLING_EMBARGO_MINUTES",
    "ROLLING_WINDOWS",
    "ROLLING_WINDOW_MINUTES",
    "STATION_HOURLY_PROFILE_PARQUET",
    "STATION_MASTER_PARQUET",
    "TARGET_HORIZON_MINUTES",
    "TEST_END",
    "TEST_START",
    "load_holidays_2025",
]

# --- point-in-time censoring 파라미터 (feature_engineering와 반드시 같은 값을 유지 —
# common_config.py에서 공유) ---
ROLLING_TICK_MINUTES = common_config.ROLLING_TICK_MINUTES
ROLLING_WINDOW_MINUTES = common_config.ROLLING_WINDOW_MINUTES
ROLLING_EMBARGO_MINUTES = common_config.ROLLING_EMBARGO_MINUTES
GRID_TICK_MINUTES = common_config.GRID_TICK_MINUTES

# --- lag/rolling 피처 파라미터 (feature_engineering와 동일 — common_config.py에서 공유) ---
LAG_HOURS = common_config.LAG_HOURS
ROLLING_WINDOWS = common_config.ROLLING_WINDOWS

# --- 타겟 정의(실시간 rental_count/return_count 재집계에 필요 — feature_engineering의
# future_rolling_counts()와 같은 정의를 predict_single.py가 Silver rental로부터 직접
# 계산할 때 씀. common_config.py에서 공유) ---
TARGET_HORIZON_MINUTES = common_config.TARGET_HORIZON_MINUTES

# --- 배치예측 horizon 개수 (feature_engineering와 동일 — common_config.py에서 공유) ---
HORIZON_COUNT = common_config.HORIZON_COUNT

EXPOSURE_STOCKOUT_VALUE = common_config.EXPOSURE_STOCKOUT_VALUE

# 배치 조회 CLI의 기본 조회 기간 (테스트 기간, training/config.py와 같은 값을 유지해야
# "학습 시 나온 지표"와 "배치 조회 CLI로 재현한 지표"가 어긋나지 않는다). training이
# RAM 제약으로 학습 기간을 2025년 11월 한 달로 좁히면서(training/config.py 참고) 이
# 값도 그 TEST_START/TEST_END와 같이 좁혀야 한다 — 실제 학습 머신에서 기간을 늘리면
# 이 값도 같이 늘릴 것.
TEST_START = "2025-11-26"
TEST_END = "2025-11-30"
