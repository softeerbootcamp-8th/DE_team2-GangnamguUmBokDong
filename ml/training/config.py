"""training(LightGBM 학습) 전용 경로·상수.

`feature_engine`이 만든 multi-horizon feature 테이블을 S3에서 읽어
(`RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET`/`RETURN_..._PARQUET`, `ml_core/`에서
공유) `MODELS_PREFIX`(S3 키 prefix, `models/`)에 학습 결과를 저장한다. `inference`도
같은 `MODELS_PREFIX`를 읽어야 하므로 그 값 자체는 `ml_core/paths.py`가 소유하고,
여기서는 학습에만 쓰는 값(split 기준, quantile 목록 등)을 정의한다.
"""

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from ml_core import common_config
from ml_core.paths import (
    MODELS_PREFIX,
    PROCESSED_V2_PREFIX,
    RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
)

__all__ = [
    "MODELS_PREFIX",
    "PROCESSED_V2_PREFIX",
    "RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET",
    "RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET",
    "today_kst",
]

_KST = ZoneInfo("Asia/Seoul")


def today_kst() -> date:
    """KST(Asia/Seoul) 기준 오늘 날짜.

    원본 데이터(트립 시각 등) 자체가 한국 로컬 wall-clock이라
    `feature_engine/spark/spark_session.py`와 같은 이유로 KST로 통일한다 —
    `date.today()`(시스템 타임존에 의존)를 그대로 쓰면 배포 환경의 타임존에 따라
    "오늘"의 경계가 달라질 수 있다.
    """
    return datetime.now(_KST).date()


# --- 학습/검증/평가 split (day-of-month 기준) ---
# 예전엔 TRAIN/VALID/TEST를 시간 순(walk-forward)으로 연속 구간(20/5/5일)만 뽑아
# 썼다 — multi-horizon 테이블이 원본의 최대 HORIZON_COUNT배 행 수라 단일 머신
# LightGBM이 1년 전체를 못 받았기 때문(history.md 18번 항목). 이제 대여/반납을
# 완전히 분리하고 lag도 1개만 남겨(공통 feature 24->12, lag/rolling 14->2) 행당
# 크기가 크게 줄어서, 표본 추출(TRAIN/VALID/TEST_SAMPLE_FRAC)과 조합하면 연중
# 특정 날짜(매달 3/20일=valid, 7/24일=test, 나머지=train)를 흩뿌려 뽑는 방식으로
# TRAIN_YEAR 1년 전체를 다 학습에 쓸 수 있다 — 계절성을 연속 구간 하나보다 훨씬
# 고르게 커버한다.
#
# 대여이력은 반납이 완료돼야 Silver에 나타난다(feature_engine/spark/run_pipeline.py의
# 날짜 파티션 overwrite 보정과 같은 이유) — 그래서 가장 최근 TRAINING_SAFETY_MARGIN_DAYS
# (기본 7일)는 rental_count 집계가 아직 안 끝났을 수 있어 학습/평가 라벨로 쓰지 않는다
# (TRAIN_YEAR가 이미 지난 해라면 사실상 영향 없음 — "오늘"에 가까운 올해를 학습할 때만
# 실제로 걸러진다). feature_engine의 INCREMENTAL_LOOKBACK_HOURS(35일 — 사후 보정을
# 계속 반영하기 위한 feature mart 쪽 마진)와는 목적이 다른, "이 정도 지났으면 라벨을
# 믿고 학습해도 된다"는 별도의(더 짧은) 마진이다.
TRAINING_SAFETY_MARGIN_DAYS = int(os.environ.get("TRAINING_SAFETY_MARGIN_DAYS", "7"))
TRAIN_YEAR = int(os.environ.get("TRAIN_YEAR", "2025"))
VALID_DAYS_OF_MONTH = frozenset(int(d) for d in os.environ.get("VALID_DAYS_OF_MONTH", "3,20").split(","))
TEST_DAYS_OF_MONTH = frozenset(int(d) for d in os.environ.get("TEST_DAYS_OF_MONTH", "7,24").split(","))


def safety_cutoff_date(as_of: date | None = None) -> date:
    """`as_of - TRAINING_SAFETY_MARGIN_DAYS` — 이 날짜를 넘는 라벨은 아직 확정되지
    않았을 수 있어 학습/평가에 쓰지 않는다.

    args:
        as_of: 기준 날짜(기본 오늘) — 테스트에서 날짜를 고정하기 위한 override
    """
    as_of = as_of or today_kst()
    return as_of - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)


QUANTILE_ALPHAS = [0.1, 0.5, 0.9]

# multi-horizon 테이블은 원본 feature 테이블의 최대 HORIZON_COUNT(기본 12)배 행 수라(T0
# 앵커를 tick 전체로 유지 — feature_engine/spark/build_multi_horizon_features.py
# 참고), 학습 머신 RAM에 안 맞으면 OOM이 난다(history.md 18번 항목이 실제로 겪은 문제와
# 같은 종류 — 그때는 train/valid/test 각각 다른 비율로 표본을 뽑아 해결했다). 기본값은
# "표본 없음"(1.0)이라 실행해보고 OOM이 나면 실제 학습 머신 스펙에 맞춰 낮출 것 — 정확한
# 안전 값은 이 저장소만으로는 알 수 없다.
TRAIN_SAMPLE_FRAC = float(os.environ.get("TRAIN_SAMPLE_FRAC", "1.0"))
VALID_SAMPLE_FRAC = float(os.environ.get("VALID_SAMPLE_FRAC", "1.0"))
TEST_SAMPLE_FRAC = float(os.environ.get("TEST_SAMPLE_FRAC", "1.0"))

CATEGORICAL_FEATURES = ["station_no"]

# LightGBM 하이퍼파라미터 (common_config.py에서 공유 — feature_engine/spark의 SynapseML
# 학습도 참고할 수 있게)
LGB_PARAMS_COMMON = common_config.LGB_PARAMS_COMMON
LGB_NUM_BOOST_ROUND = common_config.LGB_NUM_BOOST_ROUND
LGB_EARLY_STOPPING_ROUNDS = common_config.LGB_EARLY_STOPPING_ROUNDS

# P10~P90 목표 커버리지 (conformal 보정 기준)
CONFORMAL_TARGET_COVERAGE = common_config.CONFORMAL_TARGET_COVERAGE

# --- 월별 성능 모니터링 / 재학습 트리거 (monitor_performance.py) — common_config.py에서
# 근거와 함께 정의됨(계절성 때문에 절대 임계값 대신 baseline 대비 상대 악화율을 씀) ---
PERFORMANCE_DEGRADATION_THRESHOLD = common_config.PERFORMANCE_DEGRADATION_THRESHOLD
COVERAGE_DRIFT_THRESHOLD = common_config.COVERAGE_DRIFT_THRESHOLD
MONITOR_LOOKBACK_MONTHS = common_config.MONITOR_LOOKBACK_MONTHS

# --- LightGBM 분산 학습 (Socket 기반) — training.DESIGN.md §1 참고 ---
# 워커 IP/포트 등 인프라는 배포 환경마다 다르므로 프로필 파일이 아니라 환경변수로만
# 설정한다. 기본값(tree_learner="serial")은 지금까지와 동일한 단일 머신 로컬 학습이라
# 인프라가 준비되기 전까지 기존 동작을 그대로 유지한다.
LGB_TREE_LEARNER = os.environ.get("LGB_TREE_LEARNER", "serial")  # serial | data | voting | feature
LGB_NUM_MACHINES = int(os.environ.get("LGB_NUM_MACHINES", "1"))
LGB_MACHINE_RANK = int(os.environ.get("LGB_MACHINE_RANK", "0"))  # 0-based, 이 프로세스가 몇 번째 머신인지
# "host:port,host:port,..." 형식 — machine_list_filename 대신 이 문자열을 쓰면 워커마다
# 파일을 따로 배포할 필요가 없다.
LGB_MACHINES = os.environ.get("LGB_MACHINES")
LGB_LOCAL_LISTEN_PORT = int(os.environ.get("LGB_LOCAL_LISTEN_PORT", "12400"))
LGB_TIME_OUT = int(os.environ.get("LGB_TIME_OUT", "120"))  # 분 단위, 다른 머신 연결 대기 타임아웃
