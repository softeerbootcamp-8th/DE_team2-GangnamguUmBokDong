"""training(LightGBM 학습) 전용 경로·상수.

`feature_engine`이 만든 multi-horizon feature 테이블을 S3에서 읽어
(`MULTI_HORIZON_FEATURES_TABLE_PARQUET`, `ml_core/`에서 공유) `MODELS_PREFIX`
(S3 키 prefix, `models/`)에 학습 결과를 저장한다. `inference`도 같은
`MODELS_PREFIX`를 읽어야 하므로 그 값 자체는 `ml_core/paths.py`가 소유하고,
여기서는 학습에만 쓰는 값(split 기간, quantile 목록 등)을 정의한다.
"""

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from ml_core import common_config
from ml_core.paths import (
    MODELS_PREFIX,
    MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    PROCESSED_V2_PREFIX,
)

__all__ = ["MODELS_PREFIX", "MULTI_HORIZON_FEATURES_TABLE_PARQUET", "PROCESSED_V2_PREFIX", "today_kst"]

_KST = ZoneInfo("Asia/Seoul")


def today_kst() -> date:
    """KST(Asia/Seoul) 기준 오늘 날짜.

    원본 데이터(트립 시각 등) 자체가 한국 로컬 wall-clock이라
    `feature_engine/spark/spark_session.py`와 같은 이유로 KST로 통일한다 —
    `date.today()`(시스템 타임존에 의존)를 그대로 쓰면 배포 환경의 타임존에 따라
    "오늘"의 경계가 달라질 수 있다.
    """
    return datetime.now(_KST).date()

# --- 학습/검증/평가 기간 (시간 순 split, walk-forward) ---
# multi-horizon 테이블은 원본 feature 테이블의 최대 HORIZON_COUNT(기본 12)배 행 수라
# (T0 앵커를 5분 tick 전체로 유지하는 채로) 기존처럼 10~12개월을 통째로 쓰면 단일 머신
# LightGBM이 감당 못 한다(history.md 18번 항목이 실제로 겪은 OOM과 같은 종류) — 그래서
# TRAIN/VALID/TEST 합쳐 한 달 분량(기본 20/5/5일)만 쓴다. 예전엔 이 구간이 "2025-11"로
# 고정돼 있었는데, 매달 자동으로 재학습하는 지금은 그러면 다음 달 재학습도 계속 같은
# 옛날 데이터만 보게 된다 — 그래서 매번 "오늘 기준" 안전한 최근 구간으로 슬라이딩한다
# (아래 _default_window()). 실제 학습 머신에서 기간을 늘리거나 재현용으로 특정 구간을
# 고정하고 싶으면 TRAIN_START 등 6개 환경변수로 이 계산값을 그대로 override할 수 있다
# (TRAIN_SAMPLE_FRAC 등(아래)과 조합해서 씀).
#
# 대여이력은 반납이 완료돼야 Silver에 나타난다(feature_engine/spark/run_pipeline.py의
# 날짜 파티션 overwrite 보정과 같은 이유) — 그래서 가장 최근 TRAINING_SAFETY_MARGIN_DAYS
# (기본 7일)는 rental_count 집계가 아직 안 끝났을 수 있어 학습/평가 라벨로 쓰지 않는다.
# feature_engine의 INCREMENTAL_LOOKBACK_HOURS(35일 — 사후 보정을 계속 반영하기 위한
# feature mart 쪽 마진)와는 목적이 다른, "이 정도 지났으면 라벨을 믿고 학습해도 된다"는
# 별도의(더 짧은) 마진이다.
TRAINING_SAFETY_MARGIN_DAYS = int(os.environ.get("TRAINING_SAFETY_MARGIN_DAYS", "7"))
TRAIN_DAYS = int(os.environ.get("TRAIN_DAYS", "20"))
VALID_DAYS = int(os.environ.get("VALID_DAYS", "5"))
TEST_DAYS = int(os.environ.get("TEST_DAYS", "5"))


def _default_window(as_of: date | None = None) -> tuple[str, str, str, str, str, str]:
    """"as_of - TRAINING_SAFETY_MARGIN_DAYS"를 안전 상한으로 TRAIN/VALID/TEST를
    시간 순(TRAIN이 가장 오래됨)으로 슬라이딩 배분한다.

    args:
        as_of: 기준 날짜(기본 오늘) — 테스트에서 날짜를 고정하기 위한 override
    returns:
        tuple[str, str, str, str, str, str]: (train_start, train_end, valid_start,
            valid_end, test_start, test_end) "YYYY-MM-DD"
    """
    as_of = as_of or today_kst()
    test_end = as_of - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)
    test_start = test_end - timedelta(days=TEST_DAYS - 1)
    valid_end = test_start - timedelta(days=1)
    valid_start = valid_end - timedelta(days=VALID_DAYS - 1)
    train_end = valid_start - timedelta(days=1)
    train_start = train_end - timedelta(days=TRAIN_DAYS - 1)
    return (
        train_start.isoformat(),
        train_end.isoformat(),
        valid_start.isoformat(),
        valid_end.isoformat(),
        test_start.isoformat(),
        test_end.isoformat(),
    )


_DEFAULT_TRAIN_START, _DEFAULT_TRAIN_END, _DEFAULT_VALID_START, _DEFAULT_VALID_END, _DEFAULT_TEST_START, _DEFAULT_TEST_END = (
    _default_window()
)
TRAIN_START = os.environ.get("TRAIN_START", _DEFAULT_TRAIN_START)
TRAIN_END = os.environ.get("TRAIN_END", _DEFAULT_TRAIN_END)
VALID_START = os.environ.get("VALID_START", _DEFAULT_VALID_START)
VALID_END = os.environ.get("VALID_END", _DEFAULT_VALID_END)
TEST_START = os.environ.get("TEST_START", _DEFAULT_TEST_START)
TEST_END = os.environ.get("TEST_END", _DEFAULT_TEST_END)

QUANTILE_ALPHAS = [0.1, 0.5, 0.9]

# multi-horizon 테이블은 원본 feature 테이블의 최대 HORIZON_COUNT(기본 12)배 행 수라(T0
# 앵커를 5분 tick 전체로 유지 — feature_engine/spark/build_multi_horizon_features.py
# 참고), 학습 머신 RAM에 안 맞으면 OOM이 난다(history.md 18번 항목이 실제로 겪은 문제와
# 같은 종류 — 그때는 train/valid/test 각각 다른 비율로 표본을 뽑아 해결했다). 기본값은
# "표본 없음"(1.0)이라 실행해보고 OOM이 나면 실제 학습 머신 스펙에 맞춰 낮출 것 — 정확한
# 안전 값은 이 저장소만으로는 알 수 없다.
TRAIN_SAMPLE_FRAC = float(os.environ.get("TRAIN_SAMPLE_FRAC", "1.0"))
VALID_SAMPLE_FRAC = float(os.environ.get("VALID_SAMPLE_FRAC", "1.0"))
TEST_SAMPLE_FRAC = float(os.environ.get("TEST_SAMPLE_FRAC", "1.0"))

CATEGORICAL_FEATURES = ["station_id"]

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
