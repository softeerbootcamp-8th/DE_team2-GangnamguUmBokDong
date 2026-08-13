"""`make_dataset`/`training`/`inference` 세 인스턴스가 공통으로 참조하는 산출물 경로.

세 기능이 서로 다른 인스턴스에서 따로 배포되지만(각 폴더의 README 참고),
`data/processed_v2/*.parquet` 산출물 경로만큼은 세 쪽 모두 **정확히 같은 값**을
써야 한다 — make_dataset가 만들고, training이 읽어서 학습하고, inference가 읽어서
서빙하기 때문이다. 따로 하드코딩하면 한쪽만 고치고 잊어버려 조용히 갈라지는
사고가 나므로(파라미터를 공유하는 `common_config.py`와 같은 이유), 경로도 이
파일 하나로 모았다.

**로컬 개발**: 이 파일 그대로 로컬 파일시스템 경로(`ml/data/`, `ml/training/models/`)를
쓴다. **실제 배포**: `data/`는 S3에 저장되므로(요구사항), 인스턴스별로 `DATA_ROOT`/
`MODELS_ROOT` 환경변수를 `s3://...`로 override하면 코드 변경 없이 그대로 동작한다
(`feature_engineering/config.py`가 이미 쓰던 것과 같은 패턴).
"""

import json
import os
from pathlib import Path

from . import common_config

ML_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = Path(os.environ.get("DATA_ROOT", str(ML_ROOT / "data")))
PROCESSED_V2_DIR = DATA_DIR / "processed_v2"

# make_dataset/spark/config.py의 OUTPUT_ROOT/PARAM_COMBO_ID와 정확히 같은 공식 —
# 같은 환경변수(FEATURE_ENGINEERING_OUTPUT_ROOT/FEATURE_PARAM_COMBO_ID)를 쓰면
# Spark가 실제로 쓰는 산출물 디렉터리와 항상 같은 경로를 가리킨다(LEGACY_AUDIT.md 참고).
FEATURE_ENGINEERING_OUTPUT_ROOT = Path(
    os.environ.get("FEATURE_ENGINEERING_OUTPUT_ROOT", str(PROCESSED_V2_DIR / "spark"))
)
FEATURE_PARAM_COMBO_ID = os.environ.get(
    "FEATURE_PARAM_COMBO_ID",
    f"w{common_config.ROLLING_WINDOW_MINUTES}_e{common_config.ROLLING_EMBARGO_MINUTES}_t{common_config.ROLLING_TICK_MINUTES}",
)
FEATURE_ENGINEERING_OUTPUT_DIR = FEATURE_ENGINEERING_OUTPUT_ROOT / FEATURE_PARAM_COMBO_ID

TRAIN_YEAR = 2025
TRAIN_MONTHS = [f"{TRAIN_YEAR % 100:02d}{m:02d}" for m in range(1, 13)]

# 대여이력 원본(트립 단위) parquet — make_dataset(타겟/rolling 계산)과 inference
# (predict_single.py가 실시간 point-in-time censoring 시뮬레이션에 최근 트립을
# 조회)가 같이 읽으므로 공유 경로로 둔다.
RENTAL_PARQUET_DIR = DATA_DIR / "parquet"

# training이 만들고(학습), inference가 읽는(서빙) 모델 아티팩트 — 로컬 개발 중엔
# training/models/ 그대로, 실제 배포에서는 MODELS_ROOT 환경변수로 override.
MODELS_DIR = Path(os.environ.get("MODELS_ROOT", str(ML_ROOT / "training" / "models")))

# --- make_dataset 1차 정제 산출물(pandas) ---
STATION_MASTER_PARQUET = PROCESSED_V2_DIR / "station_master.parquet"
TARGETS_PARQUET = PROCESSED_V2_DIR / "targets_2025.parquet"
RETURN_TARGETS_PARQUET = PROCESSED_V2_DIR / "return_targets_2025.parquet"
STATION_STATUS_PARQUET = PROCESSED_V2_DIR / "station_status_2025.parquet"
WEATHER_PARQUET = PROCESSED_V2_DIR / "weather_2025.parquet"
POPULATION_PARQUET = PROCESSED_V2_DIR / "population_2025.parquet"

# --- make_dataset 2차 정제 산출물(Spark, make_dataset/spark/config.py가 실제로 쓰는
# 파라미터 조합별 디렉터리) — training/inference가 그대로 읽는다. 위
# FEATURE_ENGINEERING_OUTPUT_ROOT/FEATURE_PARAM_COMBO_ID와 같은 환경변수를 쓰면
# make_dataset.spark.run_pipeline이 실제로 쓴 경로와 항상 일치한다 ---
MERGED_TABLE_PARQUET = FEATURE_ENGINEERING_OUTPUT_DIR / "station_hour_merged_2025.parquet"
FEATURES_TABLE_PARQUET = FEATURE_ENGINEERING_OUTPUT_DIR / "station_hour_features_2025.parquet"
ROLLING_RENTAL_FEATURES_PARQUET = FEATURE_ENGINEERING_OUTPUT_DIR / "rolling_rental_features_2025.parquet"

# --- inference가 만드는 fallback 프로필(위 MERGED_TABLE_PARQUET/POPULATION_PARQUET
# 기반) — 파라미터 조합과 무관하게 챔피언 경로 하나만 씀 ---
STATION_HOURLY_PROFILE_PARQUET = PROCESSED_V2_DIR / "station_hourly_profile.parquet"
POPULATION_HOURLY_PROFILE_PARQUET = PROCESSED_V2_DIR / "population_hourly_profile.parquet"

# 1차 정제 산출물(원본 CSV -> parquet) — analysis_summary.json은 make_dataset(공휴일
# 목록 재사용)과 inference(predict_single.py가 서빙 시점의 is_holiday 계산)가 같이 읽는다.
ANALYSIS_SUMMARY_JSON = DATA_DIR / "output" / "analysis_summary.json"


def load_holidays_2025() -> set[str]:
    """analysis_summary.json의 holidays_2025 목록을 'YYYY-MM-DD' 문자열 set으로 반환한다."""
    with open(ANALYSIS_SUMMARY_JSON, encoding="utf-8") as f:
        summary = json.load(f)
    return set(summary["holidays_2025"])
