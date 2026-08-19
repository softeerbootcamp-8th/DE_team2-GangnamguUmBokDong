"""training(LightGBM 학습) 전용 경로·상수.

`feature_engine`이 만든 multi-horizon feature 테이블을 S3에서 읽어
(`RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET`/`RETURN_..._PARQUET`, `ml_core/`에서
공유) `MODELS_PREFIX`(S3 키 prefix, `models/`)에 학습 결과를 저장한다. `inference`도
같은 `MODELS_PREFIX`를 읽어야 하므로 그 값 자체는 `ml_core/paths.py`가 소유하고,
여기서는 학습에만 쓰는 값(split 기준, quantile 목록 등)을 정의한다.
"""

import os
from datetime import date, timedelta

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

# KST 기준 "오늘"은 이제 common_config.py 소유다(feature_engine도 학습기간 롤링
# 윈도우 계산에 필요해져서 공유 위치로 옮김) — 여기는 하위 호환을 위해 그대로 재수출.
today_kst = common_config.today_kst

# --- 학습/검증/평가 split (day-of-month 기준) ---
# 예전엔 TRAIN/VALID/TEST를 시간 순(walk-forward)으로 연속 구간(20/5/5일)만 뽑아
# 썼다 — multi-horizon 테이블이 원본의 최대 HORIZON_COUNT배 행 수라 단일 머신
# LightGBM이 1년 전체를 못 받았기 때문(history.md 18번 항목).
#
# **2026-08 실측 + 이후 정책 변경**: 대여/반납 분리 + lag 1개로 줄인 뒤에도(피처
# 축소 이후) 20분 tick·1년 전체 multi-horizon 테이블이 8억 행이라 로컬
# (RAM 18GB)에서 pandas로 한 번에 못 읽어서(에러 메시지도 없이 SIGKILL), 한때
# day-of-month 배수로 날짜 자체를 줄이는(`TRAIN_DAY_DIVISOR`, 기본 2=짝수날만)
# 임시 조치를 도입했었다. **20분(또는 그 이하) tick 앵커 밀도는 유지해야 한다는
# 요구사항이 확정되면서(minute 단위 서빙 요청을 실제로 커버해야 함 — 시간
# 단위로 앵커를 줄이면 모델이 그 minute 값들을 아예 학습에서 못 봄), 날짜를
# 솎아내는 방식으로 메모리를 줄이는 건 기본값에서 뺐다** — `TRAIN_DAY_DIVISOR`
# 기본값을 다시 1(=날짜 필터 없음, 전체 윈도우 사용)로 되돌렸다. 대신 실제 메모리
# 문제는 `lazy_train_dataset.py`의 날짜 파티션 단위 스트리밍 학습으로 푼다 —
# 그게 어려운 특수 상황(예: 로컬에서 급하게 뭔가 검증)에서만 `TRAIN_DAY_DIVISOR`를
# 다시 2, 3, 5로 올리는 임시 dial로 남겨둔다.
#
# train은 **`TRAIN_DAY_DIVISOR`의 배수인 날 중 VALID/TEST로 안 뽑힌 날**
# (기본 1 = 사실상 전체 날짜), valid/test는 `VALID_DAYS_OF_MONTH`/
# `TEST_DAYS_OF_MONTH`로 지정한 날짜만 쓴다 — `train_common._dates_for_split()`이
# valid/test 여부를 먼저 확정하고(`elif`) 그 나머지 중에서만 train 배수 조건을 보므로,
# TRAIN_DAY_DIVISOR가 1이라 "모든 날짜가 배수"인 경우에도 valid/test 날짜가
# train으로 새지 않는다.
TRAIN_DAY_DIVISOR = int(os.environ.get("TRAIN_DAY_DIVISOR", "1"))
VALID_DAYS_OF_MONTH = frozenset(int(d) for d in os.environ.get("VALID_DAYS_OF_MONTH", "11,13").split(","))
TEST_DAYS_OF_MONTH = frozenset(int(d) for d in os.environ.get("TEST_DAYS_OF_MONTH", "17,19").split(","))

# --- 학습기간 롤링 윈도우 (2026-08부터, 고정 TRAIN_YEAR 폐지) ---
# `common_config.training_window()`가 프로필의 TRAIN_LOOKBACK_MONTHS/
# TRAINING_SAFETY_MARGIN_DAYS로 "오늘 기준 최근 N개월"을 계산한다 — 매달 재학습
# 전에 이 프로세스가 새로 뜨므로, 매번 새로 계산된 값을 그대로 쓰면 코드 변경
# 없이 다음 실행부터 최신 구간이 반영된다(feature_engine/spark/config.py와 동일
# 함수를 공유 — 둘이 다른 값을 보면 학습기간과 실제 존재하는 feature mart 구간이
# 어긋난다).
#
# TRAINING_SAFETY_MARGIN_DAYS는 대여이력이 반납 완료 시에만 Silver에 나타나는
# 것과 같은 이유로(feature_engine/spark/run_pipeline.py의 날짜 파티션 overwrite
# 보정 참고) 최근 며칠은 라벨이 아직 안 굳었을 수 있어 학습/평가에서 뺀다 — 예전엔
# 이 파일 전용 환경변수였는데, feature_engine도 같은 마진이 필요해져서
# common_config로 승격됐다(값은 여기서 재수출해 기존 `config.TRAINING_SAFETY_MARGIN_DAYS`
# 참조/monkeypatch 하위 호환을 유지).
TRAINING_SAFETY_MARGIN_DAYS = common_config.TRAINING_SAFETY_MARGIN_DAYS
TRAIN_WINDOW_START, TRAIN_WINDOW_END = common_config.training_window()


def safety_cutoff_date(as_of: date | None = None) -> date:
    """`as_of - TRAINING_SAFETY_MARGIN_DAYS` — 이 날짜를 넘는 라벨은 아직 확정되지
    않았을 수 있어 학습/평가에 쓰지 않는다.

    `monitor_performance.py`가 여전히 이 함수를 쓴다(월별 실측 성능 구간 계산,
    학습기간 자체와는 다른 용도) — `TRAIN_WINDOW_END`는 이미 이 마진이 적용된
    값이라 `train_common.py`는 이 함수를 직접 부르지 않고 `TRAIN_WINDOW_END`를
    그대로 쓴다.

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

# **2026-08**: 같은 날짜 파티션 안에 horizon 1~HORIZON_COUNT이 전부 섞여 있어
# 날짜 필터만으론 못 줄이는 배율이라, 필요하면 읽는 시점에
# `horizon <= MAX_TRAIN_HORIZON` 필터를 걸(core.s3.read_parquet의 filters=,
# row-group 단위로 걸러져서 날짜 필터와 같은 원리로 로드 자체를 줄인다) 수
# 있게 남겨둔 dial이다. **기본값은 제한 없음(HORIZON_COUNT=12 그대로, 즉 항상
# 전체 horizon으로 학습)** — horizon도 낮추지 않는 게 확정된 정책이라(위
# TRAIN_DAY_DIVISOR 주석 참고), OOM 대응은 이 값을 낮추는 대신 실제
# 스트리밍/분산 학습 쪽으로 푼다. 로컬에서 급하게 뭔가 검증할 때만 임시로
# 낮출 것 — 낮추면 그 이상 horizon에 대한 예측 품질은 검증되지 않는다(모델이
# 그 구간의 실제 예를 아예 못 봄).
MAX_TRAIN_HORIZON = int(os.environ.get("MAX_TRAIN_HORIZON", str(common_config.HORIZON_COUNT)))

# **2026-08**: divisor=2+horizon<=6로 줄여도 로드가 8시간 넘게 걸리다 디스크
# 스와핑(STAT=U, %CPU 급락)으로 판단해 강제 종료한 사건 이후 도입 — 그 전까지는
# 로드가 실제로 진행 중인지 멈춘 것인지 `ps`의 경과시간을 수동으로 재확인하는 것
# 말고는 알 방법이 없었다. `train_common`/`lazy_train_dataset`이 S3 파일이나 날짜
# 청크를 읽을 때마다 완료 개수(또는 청크)와 그 시점까지의 peak RSS(MB)를 이 파일에
# 이어쓴다(표준출력과 별개 — 표준출력이 다른 곳으로 리다이렉트/버퍼링돼도 이 파일만
# tail 하면 진행 상황을 확인할 수 있다).
TRAIN_PROGRESS_LOG_PATH = os.environ.get("TRAIN_PROGRESS_LOG_PATH", "training_progress.log")
TRAIN_PROGRESS_LOG_INTERVAL_SECONDS = float(os.environ.get("TRAIN_PROGRESS_LOG_INTERVAL_SECONDS", "5"))

# MLflow(ops/compose의 mlflow 서비스, ml_core.mlflow_tracking이 접속을 담당)에
# 이 실험 이름으로 run을 남긴다 — divisor/horizon 조합을 바꿔가며 여러 번 학습을
# 시도할 때(2026-08 OOM 대응 이력) 같은 실험 아래 run들을 나란히 비교하기 위함.
MLFLOW_EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "bike-demand-training")
# monitor_performance.py의 월별 성능 점검 결과 — 학습 run과 섞이면 MLflow UI에서
# "이번 달 드리프트 추이"를 보기 번거로워져 별도 experiment로 분리한다.
MLFLOW_MONITORING_EXPERIMENT_NAME = os.environ.get("MLFLOW_MONITORING_EXPERIMENT_NAME", "bike-demand-monitoring")

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
