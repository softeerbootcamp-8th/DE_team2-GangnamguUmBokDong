"""feature_engineering 패키지 전역 설정 — 경로/파라미터.

이 패키지는 EMR에 그대로 올라가는 걸 전제로 하므로, `src/`(로컬 pandas 파이프라인,
"1차 정제"와 검증용)에 의존하지 않고 독립적으로 동작한다. 경로는 전부 환경변수로
override 가능하게 해서, 로컬 개발 중엔 local 파일시스템을, EMR에선 S3 URI를 그대로
쓸 수 있다 (Spark의 read/write는 `s3://...`/`s3a://...` 경로를 로컬 경로와 동일하게
받아들인다 — Hadoop-AWS 설정만 EMR 쪽에 돼있으면 코드는 안 바뀐다).

**입력**: "1차 정제"(원본 CSV -> parquet) 결과물 — station_master/targets/
station_status/weather/population parquet과 대여이력 원본(트립 단위) parquet.
이 패키지는 그 산출물이 이미 존재한다고 가정하고, "피처마트 생성"(2차 정제)만
담당한다.

`src/`와 반드시 같은 값을 써야 하는 파라미터(censoring 윈도우, lag/rolling 등)는
`common_config.py`(ml/ 루트, 순수 상수 모듈 — pandas/pyspark 등 무거운 의존성 없음)에서
가져온다. 이 파일이 `src/`를 직접 import하지 않는 건 그대로 유지한다 — `common_config.py`
하나만 두 패키지가 같이 참조하는 구조라 EMR에 이 패키지만 올려도 그 작은 파일 하나만
같이 올리면 된다.
"""

import os

from ml_common import common_config

# --- 데이터 루트: 로컬 개발 시 ml/data(이 파일 기준 ../../data), EMR/S3에선 DATA_ROOT
# 환경변수로 override. 이 파일은 ml/make_dataset/spark/config.py에 있으므로 dirname을
# 세 번 타야 ml/에 닿는다(예전 feature_engineering/config.py 시절엔 두 번이면 됐던
# 게 폴더 재편으로 한 단계 더 깊어짐 — LEGACY_AUDIT.md 참고, dirname 두 번짜리는
# 실제로는 ml/make_dataset/data라는 존재하지 않는 경로를 가리키는 버그였다) ---
_DEFAULT_LOCAL_DATA_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data"
)
DATA_ROOT = os.environ.get("DATA_ROOT", _DEFAULT_LOCAL_DATA_ROOT).rstrip("/")


def _path(*parts: str) -> str:
    """DATA_ROOT 기준 하위 경로를 만든다 (local 경로/S3 URI 모두 안전 — pathlib 대신 문자열 join).

    args:
        *parts: DATA_ROOT 아래 하위 경로 조각들
    returns:
        str: "/"로 이어붙인 전체 경로 (S3 URI의 "s3://" 프리픽스가 pathlib에 의해
            깨지는 걸 방지하기 위해 일부러 pathlib.Path를 쓰지 않음)
    """
    return "/".join([DATA_ROOT, *parts])


# --- 입력: "1차 정제" 산출물 (이미 존재한다고 가정) ---
RENTAL_PARQUET_DIR = os.environ.get("RENTAL_PARQUET_DIR", _path("parquet"))  # 대여이력 원본(트립 단위) parquet 디렉터리
STATION_MASTER_PARQUET = os.environ.get("STATION_MASTER_PARQUET", _path("processed_v2", "station_master.parquet"))
TARGETS_PARQUET = os.environ.get("TARGETS_PARQUET", _path("processed_v2", "targets_2025.parquet"))
RETURN_TARGETS_PARQUET = os.environ.get(
    "RETURN_TARGETS_PARQUET", _path("processed_v2", "return_targets_2025.parquet")
)
STATION_STATUS_PARQUET = os.environ.get("STATION_STATUS_PARQUET", _path("processed_v2", "station_status_2025.parquet"))
WEATHER_PARQUET = os.environ.get("WEATHER_PARQUET", _path("processed_v2", "weather_2025.parquet"))
POPULATION_PARQUET = os.environ.get("POPULATION_PARQUET", _path("processed_v2", "population_2025.parquet"))
ANALYSIS_SUMMARY_JSON = os.environ.get("ANALYSIS_SUMMARY_JSON", _path("output", "analysis_summary.json"))

# --- 학습 대상 연도/월 (지금은 2025년 단일 연도 — 다년 확장은 후속 과제) ---
TRAIN_YEAR = int(os.environ.get("TRAIN_YEAR", "2025"))
TRAIN_MONTHS = [f"{TRAIN_YEAR % 100:02d}{m:02d}" for m in range(1, 13)]

# --- point-in-time censoring 파라미터 (src/config.py와 반드시 같은 값을 유지 — common_config.py에서 공유) ---
ROLLING_TICK_MINUTES = common_config.ROLLING_TICK_MINUTES
ROLLING_WINDOW_MINUTES = common_config.ROLLING_WINDOW_MINUTES
ROLLING_EMBARGO_MINUTES = common_config.ROLLING_EMBARGO_MINUTES

# --- 타겟(예측 대상) 정의 (src/config.py와 동일 — common_config.py에서 공유) ---
# "T로부터 앞으로 TARGET_HORIZON_MINUTES분 동안 일어날 이벤트 수"를 GRID_TICK_MINUTES
# 간격의 모든 T에 대해 예측 (build_targets.py의 future_rolling_counts()).
TARGET_HORIZON_MINUTES = common_config.TARGET_HORIZON_MINUTES
GRID_TICK_MINUTES = common_config.GRID_TICK_MINUTES

# --- lag/rolling 피처 파라미터 (src/config.py와 동일 — common_config.py에서 공유) ---
LAG_HOURS = common_config.LAG_HOURS
ROLLING_WINDOWS = common_config.ROLLING_WINDOWS

EXPOSURE_STOCKOUT_VALUE = 0.05

# --- 출력: 이 패키지("2차 정제"/피처마트 생성)가 만드는 산출물 ---
# 파라미터 조합(윈도우/임바고/틱)마다 결과가 달라지므로, 산출물 경로 자체에 조합
# ID를 넣어 모델마다(=파라미터 조합마다) 서로 덮어쓰지 않도록 분리한다. 예:
# "w60_e30_t5" (챔피언), "w60_e45_t5"(챌린저) 등이 OUTPUT_ROOT 아래 각자 폴더를 갖는다.
PARAM_COMBO_ID = os.environ.get(
    "FEATURE_PARAM_COMBO_ID",
    f"w{ROLLING_WINDOW_MINUTES}_e{ROLLING_EMBARGO_MINUTES}_t{ROLLING_TICK_MINUTES}",
)
OUTPUT_ROOT = "/".join(
    [os.environ.get("FEATURE_ENGINEERING_OUTPUT_ROOT", _path("processed_v2", "spark")), PARAM_COMBO_ID]
)
ROLLING_RENTAL_FEATURES_PARQUET = "/".join([OUTPUT_ROOT, "rolling_rental_features_2025.parquet"])
MERGED_TABLE_PARQUET = "/".join([OUTPUT_ROOT, "station_hour_merged_2025.parquet"])
FEATURES_TABLE_PARQUET = "/".join([OUTPUT_ROOT, "station_hour_features_2025.parquet"])
WATERMARK_PATH = "/".join([OUTPUT_ROOT, "_watermark.json"])

# --- 증분 재생성 시 얼마나 과거까지 다시 계산해서 겹치는 구간을 보정할지 (common_config.py에서 공유) ---
# lag_168h(7일)보다 넉넉하게 잡은 안전 마진 — 이보다 짧으면 새로 추가되는 구간의
# 초반 며칠치 lag/rolling이 이전 데이터를 못 보고 결측/오류가 날 수 있다.
INCREMENTAL_LOOKBACK_HOURS = common_config.INCREMENTAL_LOOKBACK_HOURS
