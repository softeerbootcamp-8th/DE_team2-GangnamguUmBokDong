"""`feature_engine`/`training`/`inference` 세 인스턴스가 공통으로 참조하는 S3 키.

로컬 개발도 항상 S3(MinIO, `dev/start_minio.sh`)를 거친다 — 더 이상 로컬
파일시스템 폴백이 없다. 여기 정의된 값은 전부 **S3 버킷 안의 상대 키**(문자열)
이지 로컬 경로가 아니다 — 실제 버킷 이름은 `S3_BUCKET` 환경변수(`ml_core.
s3_io._bucket()`가 읽음)로 정해지고, 이 파일은 그 버킷 "안"에서 어떤 키를
쓸지만 정의한다.

키를 조합할 때 `pathlib.Path`나 `"/".join(...)`을 쓰지 않는다 — f-string으로
그때그때 직접 만든다(`collector/storage.py`와 같은 컨벤션).

**주의**: `feature_engine/spark/config.py`는 이 파일을 import하지 않고
같은 이름의 상수를 독립적으로 다시 정의한다(EMR에 그 패키지만 올릴 때
`ml_core`의 무거운 의존성 없이도 동작하게 하려는 기존 설계) — 두 파일이
가리키는 실제 키는 **반드시 같아야 하므로**, 한쪽을 고치면 다른 쪽도 같이
고칠 것.
"""

import os
from pathlib import Path

from . import common_config

# 로컬 subprocess로 형제 패키지의 venv 실행파일을 찾을 때만 쓰는 로컬 경로
# 개념(예: training/scripts/monthly_retrain_check.py가 feature_engine/training의
# .venv/bin/python을 실행) — 데이터 저장 위치와는 무관, 코드 자체는 여전히
# 로컬(또는 EMR/EC2) 프로세스로 실행되므로 이 개념만 남겨둔다.
#
# `Path.cwd()`가 아니라 이 파일 위치 기준으로 고정한다 — cwd 기준이면 스케줄러
# (cron/systemd 등)가 "ml/"로 cd하지 않고 절대경로로 이 스크립트를 실행할 때
# ML_ROOT가 엉뚱한 디렉터리가 되고, 그 아래 "feature_engine/.venv/bin/python"을
# 못 찾아 RuntimeError가 난다 — 실행 위치와 무관하게 항상 이 저장소의 "ml/"을
# 가리켜야 한다. 이 파일은 "libs/ml_core/paths.py"에 있으므로 parents[2]가
# 저장소 루트다.
ML_ROOT = Path(__file__).resolve().parents[2] / "ml"

FEATURE_PARAM_COMBO_ID = os.environ.get(
    "FEATURE_PARAM_COMBO_ID",
    f"w{common_config.ROLLING_WINDOW_MINUTES}_e{common_config.ROLLING_EMBARGO_MINUTES}_t{common_config.ROLLING_TICK_MINUTES}",
)
# feature_engine의 2차 정제 산출물 위치 — dev/S3_DATA_CATALOG.md의
# `processed/features/` prefix를 따른다. 파라미터 조합마다 결과가 달라지므로
# 조합 ID를 키에 넣어 서로 안 덮어쓰게 한다.
_FEATURE_ENGINEERING_OUTPUT_PREFIX = os.environ.get("FEATURE_ENGINEERING_OUTPUT_PREFIX", "processed/features")
FEATURE_ENGINEERING_OUTPUT_DIR = f"{_FEATURE_ENGINEERING_OUTPUT_PREFIX}/{FEATURE_PARAM_COMBO_ID}"

TRAIN_YEAR = 2025
TRAIN_MONTHS = [f"{TRAIN_YEAR % 100:02d}{m:02d}" for m in range(1, 13)]

# 대여이력 원본(트립 단위) parquet 디렉터리 — feature_engine(타겟/rolling
# 계산)이 여전히 이걸 읽는다고 가정(이번 phase는 1차정제 이전 상태 유지, 2단계
# 에서 raw Silver `rental` 조회로 대체 예정). `inference`는 이번 phase부터
# `silver_schema`를 통해 Silver `rental`을 직접 읽으므로 이 상수를 안 쓴다.
RENTAL_PARQUET_DIR = os.environ.get("RENTAL_PARQUET_DIR", "parquet")

# training이 만들고(학습), inference가 읽는(서빙) 모델 아티팩트 — dev/
# S3_DATA_CATALOG.md에 정의된 `models/` prefix를 그대로 쓴다. 이제 학습은 항상
# 아래 아카이브 prefix에 쓰고, 챌린저가 챔피언을 이길 때만(training/promotion.py)
# 이 prefix로 파일명 그대로 복사된다 — 이 prefix에 직접 학습 결과를 쓰는 코드
# 경로는 없다.
MODELS_PREFIX = os.environ.get("MODELS_PREFIX", "models")

# 학습한 모든 모델(챔피언이 됐는지와 무관하게)을 보존하는 아카이브 — 날짜/프로필별로
# 나뉘어 있어 "언제 어떤 프로필로 학습했는지"를 그대로 찾을 수 있다.
MODELS_ARCHIVE_PREFIX = os.environ.get("MODELS_ARCHIVE_PREFIX", f"{MODELS_PREFIX}/archive")


def archive_models_prefix(date: str, profile_name: str) -> str:
    """한 번의 학습 시도(날짜 + 프로필 조합)가 쓸 아카이브 prefix를 만든다.

    이 prefix를 `train_common.train_target(..., models_prefix=...)`에 그대로
    넘기면, `model_key`/`model_json_key`가 만드는 파일명(예: "rental_poisson.txt")
    자체는 챔피언 경로와 완전히 동일하게 유지되고 위치만 여기로 바뀐다 — 나중에
    챔피언으로 승격할 때 파일명을 그대로 복사만 하면 되는 이유다.

    args:
        date: "YYYY-MM-DD" — 학습을 실행한 날짜
        profile_name: 이 학습에 쓴 프로필 이름(common_config.PROFILE_NAME)
    returns:
        str: "{MODELS_ARCHIVE_PREFIX}/dt={date}/{profile_name}"
    """
    return f"{MODELS_ARCHIVE_PREFIX}/dt={date}/{profile_name}"


def model_key(model_name: str, suffix: str, models_prefix: str | None = None) -> str:
    """모델 아티팩트 하나의 S3 키를 만든다 (예: model_key("rental", "poisson") -> "models/rental_poisson.txt").

    args:
        model_name: "rental" 또는 "return"
        suffix: "poisson"/"q10"/"q50"/"q90"
        models_prefix: None이면 챔피언 prefix(MODELS_PREFIX) — 하이퍼파라미터 스윕 등
            실험 실행은 자신만의 prefix(예: "models/experiments/{run_id}")를 넘겨서
            챔피언 아티팩트를 덮어쓰지 않는다.
    """
    return f"{models_prefix or MODELS_PREFIX}/{model_name}_{suffix}.txt"


def model_json_key(model_name: str, kind: str, models_prefix: str | None = None) -> str:
    """모델 부속 JSON(conformal_correction/station_categories/metrics)의 S3 키를 만든다."""
    return f"{models_prefix or MODELS_PREFIX}/{model_name}_{kind}.json"


# --- feature_engine 1차 정제 산출물 (이번 phase는 "이미 어딘가 있다"고
# 가정 — 2단계에서 raw Silver로부터 직접 만드는 로직으로 대체될 예정. prefix
# 이름은 로컬 개발 때 쓰던 것을 그대로 유지해 마이그레이션 부담을 줄인다) ---
PROCESSED_V2_PREFIX = os.environ.get("PROCESSED_V2_PREFIX", "processed_v2")
STATION_MASTER_PARQUET = f"{PROCESSED_V2_PREFIX}/station_master.parquet"
TARGETS_PARQUET = f"{PROCESSED_V2_PREFIX}/targets_2025.parquet"
RETURN_TARGETS_PARQUET = f"{PROCESSED_V2_PREFIX}/return_targets_2025.parquet"
STATION_STATUS_PARQUET = f"{PROCESSED_V2_PREFIX}/station_status_2025.parquet"
WEATHER_PARQUET = f"{PROCESSED_V2_PREFIX}/weather_2025.parquet"
POPULATION_PARQUET = f"{PROCESSED_V2_PREFIX}/population_2025.parquet"

# --- feature_engine 2차 정제 산출물(Spark) — training/inference가 그대로
# 읽는다. `feature_engine/spark/config.py`의 같은 이름 상수와 반드시
# 같은 값이어야 한다(위 모듈 docstring 참고) ---
MERGED_TABLE_PARQUET = f"{FEATURE_ENGINEERING_OUTPUT_DIR}/station_hour_merged_2025.parquet"
FEATURES_TABLE_PARQUET = f"{FEATURE_ENGINEERING_OUTPUT_DIR}/station_hour_features_2025.parquet"
# FEATURES_TABLE_PARQUET의 각 행(T0, 5분 tick)을 horizon=1..HORIZON_COUNT만큼 self-join해
# "T0의 lag/rolling + T0+(horizon-1)시간의 날씨/캘린더/타겟"으로 조합한 학습 테이블
# (build_multi_horizon_features.py) — training이 이제 이 테이블만 읽는다.
MULTI_HORIZON_FEATURES_TABLE_PARQUET = f"{FEATURE_ENGINEERING_OUTPUT_DIR}/station_hour_features_multihorizon_2025.parquet"
ROLLING_RENTAL_FEATURES_PARQUET = f"{FEATURE_ENGINEERING_OUTPUT_DIR}/rolling_rental_features_2025.parquet"

# --- inference가 만드는 fallback 프로필(위 MERGED_TABLE_PARQUET/POPULATION_PARQUET
# 기반) — 파라미터 조합과 무관하게 챔피언 경로 하나만 씀 ---
STATION_HOURLY_PROFILE_PARQUET = f"{PROCESSED_V2_PREFIX}/station_hourly_profile.parquet"
POPULATION_HOURLY_PROFILE_PARQUET = f"{PROCESSED_V2_PREFIX}/population_hourly_profile.parquet"

# 1차 정제 산출물(원본 CSV -> parquet) — analysis_summary.json은 feature_engine(공휴일
# 목록 재사용)과 inference(predict_single.py가 서빙 시점의 is_holiday 계산)가 같이 읽는다.
ANALYSIS_SUMMARY_JSON = f"{PROCESSED_V2_PREFIX}/output/analysis_summary.json"


def load_holidays_2025() -> set[str]:
    """analysis_summary.json의 holidays_2025 목록을 'YYYY-MM-DD' 문자열 set으로 반환한다."""
    from core import s3 as s3_io

    summary = s3_io.read_json(ANALYSIS_SUMMARY_JSON)
    if summary is None:
        raise FileNotFoundError(f"S3에 없음: {ANALYSIS_SUMMARY_JSON}")
    return set(summary["holidays_2025"])
