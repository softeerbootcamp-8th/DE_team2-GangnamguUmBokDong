"""training(LightGBM 학습) 전용 경로·상수.

`feature_engineering`이 만든 feature 테이블을 읽어(`FEATURES_TABLE_PARQUET`, `ml_common/`에서
공유) `MODELS_DIR`(이 폴더 아래 `models/`)에 학습 결과를 저장한다. `inference`도
같은 `MODELS_DIR`을 읽어야 하므로 그 경로 자체는 `ml_common/paths.py`가 소유하고
(로컬 개발 시 기본값이 `training/models/`), 여기서는 학습에만 쓰는 값(split 기간,
quantile 목록 등)을 정의한다.
"""

import os

from ml_common import common_config
from ml_common.paths import FEATURES_TABLE_PARQUET, MODELS_DIR, PROCESSED_V2_DIR

__all__ = ["FEATURES_TABLE_PARQUET", "MODELS_DIR", "PROCESSED_V2_DIR"]

# --- 학습/검증/평가 기간 (2025년 내에서 시간 순 split, walk-forward) ---
TRAIN_START = "2025-01-01"
TRAIN_END = "2025-10-31"
VALID_START = "2025-11-01"
VALID_END = "2025-11-30"
TEST_START = "2025-12-01"
TEST_END = "2025-12-31"

QUANTILE_ALPHAS = [0.1, 0.5, 0.9]

CATEGORICAL_FEATURES = ["station_id"]

# LightGBM 하이퍼파라미터 (common_config.py에서 공유 — feature_engineering/spark의 SynapseML
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
