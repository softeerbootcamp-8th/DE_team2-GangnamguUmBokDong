"""`src/`(로컬 pandas 학습·추론)와 `feature_engine/`(EMR Spark 배포용)이 공유하는
파라미터·기준값 — 이 파일 하나만 두 패키지가 같이 참조한다.

**왜 따로 뺐는가**: 두 패키지는 서로를 import하면 안 된다 — `feature_engine/`은
EMR에 단독으로 올라가는 걸 전제로 하고, `src/`는 로컬 pandas/LightGBM 스택이라
pyspark가 없어도 돌아가야 한다. 그런데 point-in-time censoring 윈도우(embargo 등)나
LightGBM 하이퍼파라미터처럼 **두 쪽이 반드시 같은 값을 써야 하는** 상수들이 있다 —
따로 하드코딩해두면 한쪽만 고치고 잊어버려 조용히 갈라지는(train-serving skew와
똑같은 종류의) 사고가 난다. 그래서 이 파일은 **pandas/pyspark 등 무거운 의존성을
전혀 import하지 않는 순수 상수 모듈**로 만들어서, 어느 쪽 venv에서 import해도
안전하다.

경로(로컬 파일시스템 vs S3)처럼 두 쪽이 원래 다를 수밖에 없는 값은 각자의
`config.py`에 그대로 둔다 — 여기 옮기는 건 "반드시 같아야 하는 값"만이다.

**프로필 시스템**: 위 상수들은 `ml/profiles/{ML_PROFILE}.json`(기본 프로필명
"default")에서 값을 읽어온다. 여러 파라미터 조합(예: embargo 30분 챔피언 vs 45분
챌린저)을 프로필 파일로 미리 만들어두고, `ML_PROFILE` 환경변수 하나로 전체 조합을
바꿔 낄 수 있다. 기존에 있던 개별 환경변수 override(예: `ROLLING_EMBARGO_MINUTES=45`,
`scripts/run_embargo_sweep.py`류 스크립트가 실험 중 값을 임시로 바꿀 때 사용)는
프로필 값 위에 한 번 더 덮어쓸 수 있게 유지한다 — 우선순위는
"개별 환경변수 > 프로필 파일 > (프로필 파일도 없을 때만 쓰는 하드코드 기본값 없음,
프로필 파일이 최종 소스)". `src/config.py`/`feature_engine/spark/config.py`는 지금처럼
`common_config.XXX`를 그대로 참조하면 되고 인터페이스는 바뀌지 않는다.
"""

import json
import os
from pathlib import Path

PROFILES_DIR = Path(__file__).resolve().parent / "profiles"


def _load_profile() -> dict:
    name = os.environ.get("ML_PROFILE", "default")
    path = PROFILES_DIR / f"{name}.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def list_profile_names() -> list[str]:
    """`PROFILES_DIR` 밑의 프로필 이름 목록을 반환한다(확장자 제외, 정렬됨).

    `training/scripts/monthly_retrain_check.py`가 챌린저 재시도 때 어떤 프로필을
    돌아가며 시도할지 정할 때 쓴다 — 새 프로필 파일을 추가하기만 하면 이 목록에
    자동으로 잡힌다(코드 수정 불필요).
    """
    return sorted(p.stem for p in PROFILES_DIR.glob("*.json"))


# 이 프로세스가 실제로 어떤 프로필을 쓰고 있는지 — 모델 아티팩트에 "어떤 프로필로
# 학습됐는지"를 그대로 남겨야 해서(model_contract.py의 profile.json) public으로 둔다.
PROFILE_NAME = os.environ.get("ML_PROFILE", "default")
PROFILE = _load_profile()
_PROFILE = PROFILE  # 기존 참조(아래) 호환용 별칭


def _int_env(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float_env(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


# --- point-in-time censoring 파라미터 (REALTIME_FEATURES.md) ---
# "5분 단위"는 서빙 갱신 주기일 뿐 윈도우 폭이 아니다. 실제 feature:
# "[T-embargo-window, T-embargo) 구간에 시작되고 end_dt<=T인 대여 수".
ROLLING_TICK_MINUTES = _int_env("ROLLING_TICK_MINUTES", _PROFILE["ROLLING_TICK_MINUTES"])
ROLLING_WINDOW_MINUTES = _int_env("ROLLING_WINDOW_MINUTES", _PROFILE["ROLLING_WINDOW_MINUTES"])
ROLLING_EMBARGO_MINUTES = _int_env("ROLLING_EMBARGO_MINUTES", _PROFILE["ROLLING_EMBARGO_MINUTES"])

# --- 타겟(예측 대상) 정의 ---
# "기준 시각 T로부터 앞으로 TARGET_HORIZON_MINUTES분 동안 일어날 이벤트 수"를
# GRID_TICK_MINUTES 간격의 모든 T에 대해 예측한다 (build_targets.py의
# future_rolling_counts()). GRID_TICK_MINUTES는 ROLLING_TICK_MINUTES와 값은
# 같지만(둘 다 5분) 의미가 다르다 — 하나는 "입력 피처 갱신 주기", 하나는
# "타겟/전체 그리드 간격"이라 나중에 독립적으로 바꿀 수 있게 따로 뺀다.
TARGET_HORIZON_MINUTES = _int_env("TARGET_HORIZON_MINUTES", _PROFILE["TARGET_HORIZON_MINUTES"])
GRID_TICK_MINUTES = _int_env("GRID_TICK_MINUTES", _PROFILE["GRID_TICK_MINUTES"])

# --- 배치예측 horizon(몇 시간 뒤까지 한 번에 예측하는지) ---
# lag/rolling(직전 실적)은 항상 "지금(T0)" 기준으로 고정하고, horizon(1..HORIZON_COUNT)을
# feature로 모델에 직접 알려준다 — horizon마다 별도 모델을 두거나 재귀 예측(오차 누적)을
# 쓰지 않는 이유는 history.md 18번 항목 참고. feature_engine의 multi-horizon self-join
# 범위와 inference의 기본 예측 구간 수가 이 값 하나를 같이 참조한다.
HORIZON_COUNT = _int_env("HORIZON_COUNT", _PROFILE["HORIZON_COUNT"])

# --- 모델 입력 feature 스키마(lag 제외) — feature_engine이 만들고, training이
# 학습에 쓰고, inference가 동일 순서로 맞춰야 하는 "모델 계약"의 일부라 공유한다.
#
# 피처 중요도 분석 결과 gain이 낮거나(wind/humidity/pop 세부분류) 다른 피처와
# 중복 정보였던(month, is_weekend/is_next_day_off/is_prev_day_off, hour/dow의
# sin·cos 순환 인코딩) 컬럼을 정리하고 핵심만 남겼다:
# - `is_holiday` 하나가 주말+공휴일을 전부 흡수(과거의 is_weekend/is_next_day_off/
#   is_prev_day_off를 대체) — 다음날/전날 조회가 없어져 연도 경계 처리도 단순해짐.
# - `hour`/`dow`를 sin/cos로 순환 인코딩하지 않고 원값 그대로 둔다 — 순환
#   인코딩은 "휴일 여부가 날마다 완전히 다른 패턴을 만든다"는 걸 모델에 감춘다.
# - `month`/`date` 대신 `day`(2000-01-01 기준 경과일수, ml_core.day_index)를 쓴다
#   — month 순환 인코딩과 달리 연도 경계(작년 12월과 올해 1월이 가깝고, 같은 해
#   1월과 12월은 멀다)를 올바르게 표현한다.
# - `station_id`(텍스트, "ST-2565") 대신 station master의 station_no(정수 일련번호)를
#   쓴다 — station_id는 출력/S3 키 등 식별 용도로는 계속 쓰이지만, 모델 feature
#   자체를 정수로 두면 학습 데이터 읽기(core.s3.read_parquet)에서 Parquet dictionary
#   encoding이 pandas Categorical로 안 살아나 매번 object dtype 문자열 배열을
#   통째로 만드는 비용을 원천적으로 피한다(정수 컬럼은 그 디코딩 자체가 없음).
BASE_FEATURE_COLUMNS = [
    "station_no",
    "capacity",
    "lat",
    "lon",
    "temp",
    "precip",
    "pop_total",
    "hour",
    "dow",
    "is_holiday",
    "day",
    "horizon",
]

# --- LightGBM 공통 하이퍼파라미터 (train_common.py, 향후 SynapseML 쪽도 이 값을 참고) ---
_LGB_PROFILE = _PROFILE["LGB_PARAMS_COMMON"]
LGB_PARAMS_COMMON = {
    "num_leaves": _int_env("LGB_NUM_LEAVES", _LGB_PROFILE["num_leaves"]),
    "learning_rate": _float_env("LGB_LEARNING_RATE", _LGB_PROFILE["learning_rate"]),
    "feature_fraction": _float_env("LGB_FEATURE_FRACTION", _LGB_PROFILE["feature_fraction"]),
    "bagging_fraction": _float_env("LGB_BAGGING_FRACTION", _LGB_PROFILE["bagging_fraction"]),
    "bagging_freq": _int_env("LGB_BAGGING_FREQ", _LGB_PROFILE["bagging_freq"]),
    "min_data_in_leaf": _int_env("LGB_MIN_DATA_IN_LEAF", _LGB_PROFILE["min_data_in_leaf"]),
    "verbose": -1,
    "num_threads": 0,  # 0 = LightGBM이 사용 가능한 코어 수만큼 자동 사용
}
LGB_NUM_BOOST_ROUND = _int_env("LGB_NUM_BOOST_ROUND", _PROFILE["LGB_NUM_BOOST_ROUND"])
LGB_EARLY_STOPPING_ROUNDS = _int_env("LGB_EARLY_STOPPING_ROUNDS", _PROFILE["LGB_EARLY_STOPPING_ROUNDS"])

# P10~P90 목표 커버리지 (conformal 보정 기준)
CONFORMAL_TARGET_COVERAGE = _float_env("CONFORMAL_TARGET_COVERAGE", _PROFILE["CONFORMAL_TARGET_COVERAGE"])

# 재고 스냅샷 결측(~1.1%)은 "알 수 없음"이므로 exposure=1(정상 운영)로 간주.
# 품절(stockout) 시간대는 대여가 사실상 불가능하지만 완전히 0은 아니므로 작은 값으로
# 근사한다. feature_engine(학습 데이터의 exposure 계산)와 inference(서빙 시점
# rental_exposure 계산)가 정확히 같은 값을 써야 하므로 공유한다.
EXPOSURE_STOCKOUT_VALUE = 0.05

# --- 증분 피처마트 생성 (feature_engine/spark/run_pipeline.py) ---
# lag_168h(7일)보다 넉넉한 안전 마진 — 짧으면 신규 구간 초반 며칠의 lag/rolling이
# 과거를 못 보고 결측/오류가 날 수 있다.
INCREMENTAL_LOOKBACK_HOURS = _int_env("INCREMENTAL_LOOKBACK_HOURS", _PROFILE["INCREMENTAL_LOOKBACK_HOURS"])

# --- 월별 성능 모니터링 / 재학습 트리거 (src/monitor_performance.py) ---
# 판단 기준을 "절대 수치"가 아니라 "학습 시점 baseline 대비 상대적 악화율"로 두는 이유:
# Poisson deviance/RMSE는 계절성 때문에 달마다 자연스럽게 오르내린다(실측
# 1월 대비 6월 대여량이 약 2.44배) — 절대 임계값은 겨울엔 걸핏하면 오탐, 여름엔 못 잡는
# 식으로 계절과 뒤섞인다. 반면 상대 악화율은 "그 달 자체의 계절 수준에서 모델이 얼마나
# 못 맞추는가"라 계절성과 어느 정도 분리된다.
#
# 임계값 10%의 근거: 실측한 노이즈 바닥이 이보다 훨씬 낮다 —
#   - 같은 코드로 재학습만 다시 돌렸을 때(LightGBM 자체 run-to-run 편차): deviance
#     0.3~0.5% 수준 차이 (반납 모델 재학습 전후 0.913->0.920 등)
#   - embargo 파라미터를 0~60분으로 바꿔가며 스윕했을 때: deviance 0.9598~0.9659
#     (약 0.6% 범위)
# 즉 10%는 이 노이즈 바닥보다 훨씬 위라 순수 랜덤/파라미터 변동으로는 거의 안 걸리고,
# 진짜 성능 저하(수요 패턴 변화, 신규 station 급증 등)일 때만 걸리도록 잡은 초기값이다.
# 실제 운영 데이터가 몇 달 쌓이면 이 값을 다시 보정해야 한다 — 지금은 근거 있는 추정치.
PERFORMANCE_DEGRADATION_THRESHOLD = _float_env(
    "PERFORMANCE_DEGRADATION_THRESHOLD", _PROFILE["PERFORMANCE_DEGRADATION_THRESHOLD"]
)

# P10~P90 커버리지 드리프트 임계값: 목표(0.80) 대비가 아니라 baseline 학습 시점의
# 실측 커버리지 대비로 비교한다 — 지금 모델도 이미 0.80이 아니라 0.828~0.865
# 수준에서 "정상"으로 받아들여지고 있어서, 목표치 대비로 재면 항상 걸린다.
COVERAGE_DRIFT_THRESHOLD = _float_env("COVERAGE_DRIFT_THRESHOLD", _PROFILE["COVERAGE_DRIFT_THRESHOLD"])

# 매달 점검할 때 "최근 몇 개월"을 실측 성능 구간으로 볼지
MONITOR_LOOKBACK_MONTHS = _int_env("MONITOR_LOOKBACK_MONTHS", _PROFILE["MONITOR_LOOKBACK_MONTHS"])
