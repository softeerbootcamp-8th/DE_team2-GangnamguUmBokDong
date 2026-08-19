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

**프로필 시스템(2026-08 S3 이관)**: 위 상수들은 S3의 `profiles/{ML_PROFILE}.json`
(`ml_core.paths.profile_path()`, 기본 프로필명 "default")에서 값을 읽어온다. 예전엔
저장소에 커밋된 로컬 JSON 파일이었는데, feature_engine/training/inference가 각자
다른 서버에 배포되므로 로컬 파일로는 "값 하나 바꾸면 파이프라인 전체에 반영"이
불가능했다(서버마다 코드 재배포가 필요) — S3는 이미 세 서비스가 공유하는 유일한
인프라라 여기로 옮겼다. 여러 파라미터 조합(예: embargo 30분 챔피언 vs 45분 챌린저)을
프로필로 미리 만들어 S3에 두고, `ML_PROFILE` 환경변수 하나로 전체 조합을 바꿔 낄 수
있다(`ml_core.profile_registry.push_profile()`로 생성/수정 — MLflow에도 같이 기록되어
변경 이력을 볼 수 있지만, 실제 런타임 조회는 항상 S3 직접 조회다). 기존에 있던 개별
환경변수 override(예: `ROLLING_EMBARGO_MINUTES=45`)는 프로필 값 위에 한 번 더 덮어쓸
수 있게 유지한다 — 우선순위는 "개별 환경변수 > S3 프로필 > (S3 조회 실패 시에만 쓰는
내장 기본값 `_DEFAULT_PROFILE`)". `training/config.py`/`feature_engine/spark/config.py`는
지금처럼 `common_config.XXX`를 그대로 참조하면 되고 인터페이스는 바뀌지 않는다.

**왜 `core.s3`를 안 쓰고 boto3를 직접 쓰는가**: `core.s3`는 parquet 처리를 위해
pandas/pyarrow를 무조건 import한다 — 이 파일은 위에서 말한 "pandas/pyspark 등 무거운
의존성을 전혀 import하지 않는 순수 상수 모듈" 원칙을 지켜야 해서, boto3만 쓰는 최소
구현을 따로 둔다(`_fetch_profile_from_s3()`).
"""

import json
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

# S3에 아직 아무 프로필도 안 올라갔거나(첫 부트스트랩) S3가 응답하지 않을 때만 쓰는
# 안전망 — 예전 `libs/ml_core/profiles/default.json`과 내용이 같다. 로컬 JSON 파일은
# 이제 없다(위 docstring 참고) — 이 파이썬 리터럴이 유일한 "로컬" 기본값이다.
_DEFAULT_PROFILE = {
    "ROLLING_TICK_MINUTES": 20,
    "ROLLING_WINDOW_MINUTES": 60,
    "ROLLING_EMBARGO_MINUTES": 40,
    "TARGET_HORIZON_MINUTES": 60,
    "GRID_TICK_MINUTES": 20,
    "HORIZON_COUNT": 12,
    "LGB_PARAMS_COMMON": {
        "num_leaves": 63,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "min_data_in_leaf": 100,
    },
    "LGB_NUM_BOOST_ROUND": 800,
    "LGB_EARLY_STOPPING_ROUNDS": 50,
    "CONFORMAL_TARGET_COVERAGE": 0.80,
    "INCREMENTAL_LOOKBACK_HOURS": 840,
    "PERFORMANCE_DEGRADATION_THRESHOLD": 0.10,
    "COVERAGE_DRIFT_THRESHOLD": 0.15,
    "MONITOR_LOOKBACK_MONTHS": 1,
    # --- 2026-08 추가: 학습기간 롤링 윈도우 (TRAIN_YEAR 고정값 폐지) ---
    "TRAIN_LOOKBACK_MONTHS": 12,
    "TRAINING_SAFETY_MARGIN_DAYS": 7,
}


def _s3_client(timeout_seconds: float):
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        config=BotoConfig(connect_timeout=timeout_seconds, read_timeout=timeout_seconds, retries={"max_attempts": 1}),
    )


def _fetch_profile_from_s3(name: str, timeout_seconds: float = 2.0) -> dict | None:
    """`profiles/{name}.json`(`ml_core.paths.profile_path()`와 같은 키 규칙, 순환
    import를 피하려고 문자열을 직접 조립한다 — paths.py가 이미 이 모듈을 import하므로
    반대 방향 import는 불가능)을 읽는다. 없으면 None, S3가 아예 응답하지 않으면
    예외를 그대로 던진다(호출부 `_load_profile()`가 무조건 폴백으로 받는다).

    기본 boto3 재시도 정책으로 존재하지 않는 엔드포인트를 조회하면 실측 8.5초가
    걸린다 — 프로필 조회는 부가 기능이지 핵심 데이터 경로가 아니므로, 짧은
    timeout+재시도 없음으로 빠르게 실패하고 내장 기본값으로 넘어가게 한다.
    """
    bucket = os.environ.get("S3_BUCKET", "gangnamgu")
    try:
        body = _s3_client(timeout_seconds).get_object(Bucket=bucket, Key=f"profiles/{name}.json")["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return json.loads(body)


def _load_profile() -> dict:
    name = os.environ.get("ML_PROFILE", "default")
    try:
        profile = _fetch_profile_from_s3(name)
    except Exception as exc:  # noqa: BLE001 — S3 조회 실패 사유와 무관하게 무조건 폴백
        print(f"[common_config] ERROR: S3에서 프로필 '{name}' 조회 실패({exc}) — 내장 기본값 사용", file=sys.stderr)
        return _DEFAULT_PROFILE
    if profile is None:
        print(f"[common_config] ERROR: S3에 프로필 '{name}' 없음(profiles/{name}.json) — 내장 기본값 사용", file=sys.stderr)
        return _DEFAULT_PROFILE
    print(f"[common_config] 프로필 '{name}'을 S3에서 읽음")
    return profile


def list_profile_names() -> list[str]:
    """S3 `profiles/` 밑의 프로필 이름 목록을 반환한다(확장자 제외, 정렬됨).

    `training/scripts/monthly_retrain_check.py`가 챌린저 재시도 때 어떤 프로필을
    돌아가며 시도할지 정할 때 쓴다 — `ml_core.profile_registry.push_profile()`로 새
    프로필을 올리기만 하면 이 목록에 자동으로 잡힌다(코드 수정 불필요). S3 조회
    실패 시 빈 리스트(호출부가 "시도할 챌린저 프로필 없음"으로 자연스럽게 처리).
    """
    bucket = os.environ.get("S3_BUCKET", "gangnamgu")
    try:
        client = _s3_client(timeout_seconds=5.0)
        keys = [
            obj["Key"]
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="profiles/")
            for obj in page.get("Contents", [])
        ]
    except Exception as exc:  # noqa: BLE001
        print(f"[common_config] S3 profiles/ 목록 조회 실패({exc})")
        return []
    return sorted(k.removeprefix("profiles/").removesuffix(".json") for k in keys if k.endswith(".json"))


_KST = ZoneInfo("Asia/Seoul")


def today_kst() -> date:
    """KST(Asia/Seoul) 기준 오늘 날짜.

    원본 데이터(트립 시각 등) 자체가 한국 로컬 wall-clock이라 이 기준으로 통일한다
    — `date.today()`(시스템 타임존 의존)를 쓰면 배포 환경 타임존에 따라 "오늘"의
    경계가 달라질 수 있다. 예전엔 `training/config.py`에만 있었는데, 이제
    feature_engine도 학습기간 롤링 윈도우 계산에 "오늘"이 필요해서 공유 위치로 옮겼다
    (`training/config.py`는 하위 호환을 위해 재수출).
    """
    return datetime.now(_KST).date()


def _subtract_months(d: date, months: int) -> date:
    """`d`에서 `months`개월 전 날짜 — 말일 근처는 대상 월의 실제 말일로 자동 보정한다.

    dateutil 등 새 의존성을 추가하지 않으려고 표준 라이브러리(datetime/calendar)만으로
    구현한다.
    """
    import calendar

    total_months = d.year * 12 + (d.month - 1) - months
    year, month = divmod(total_months, 12)
    month += 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def training_window(as_of: date | None = None) -> tuple[date, date]:
    """(start, end) — 학습에 쓸 롤링 날짜 구간.

    `end = as_of(기본 오늘 KST) - TRAINING_SAFETY_MARGIN_DAYS`(최근 며칠은 라벨이
    아직 확정 안 됐을 수 있어 제외 — 대여이력은 반납이 끝나야 Silver에 나타남),
    `start = end - TRAIN_LOOKBACK_MONTHS개월`. `TRAIN_YEAR` 같은 고정 연도 대신
    이 함수 하나로 feature_engine(Silver 조회 범위)과 training(train/valid/test
    split 범위)이 항상 같은 롤링 윈도우를 보게 한다 — 프로필의 `TRAIN_LOOKBACK_MONTHS`/
    `TRAINING_SAFETY_MARGIN_DAYS` 값만 바꾸면 재배포 없이 다음 실행부터 반영된다
    (feature_engine/training 둘 다 매번 새로 뜨는 배치 프로세스라 "다음 실행부터"로
    충분하다).

    args:
        as_of: 기준 날짜(기본 오늘 KST) — 테스트에서 날짜를 고정하기 위한 override
    """
    as_of = as_of or today_kst()
    end = as_of - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)
    start = _subtract_months(end, TRAIN_LOOKBACK_MONTHS)
    return start, end


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
# - `dow`를 sin/cos로 순환 인코딩하지 않고 원값 그대로 둔다 — 순환 인코딩은
#   "휴일 여부가 날마다 완전히 다른 패턴을 만든다"는 걸 모델에 감춘다.
# - `month`/`date` 대신 `day`(2000-01-01 기준 경과일수, ml_core.day_index)를 쓴다
#   — month 순환 인코딩과 달리 연도 경계(작년 12월과 올해 1월이 가깝고, 같은 해
#   1월과 12월은 멀다)를 올바르게 표현한다.
# - `station_id`(텍스트, "ST-2565") 대신 station master의 station_no(정수 일련번호)를
#   쓴다 — station_id는 출력/S3 키 등 식별 용도로는 계속 쓰이지만, 모델 feature
#   자체를 정수로 두면 학습 데이터 읽기(core.s3.read_parquet)에서 Parquet dictionary
#   encoding이 pandas Categorical로 안 살아나 매번 object dtype 문자열 배열을
#   통째로 만드는 비용을 원천적으로 피한다(정수 컬럼은 그 디코딩 자체가 없음).
# - `hour`(0~23) 대신 `minute`(자정 기준 경과분 0~1439, ml_core.minute_of_day) 하나로
#   시각을 나타낸다 — 그리드 자체가 20분 tick인데 hour만 쓰면 같은 시간 안의
#   17:00/17:20/17:40이 모델에 전부 같은 값으로 보인다. minute은 그 tick 구분을
#   그대로 담으면서 hour가 주던 정보(시간대별 패턴)도 당연히 포함한다. `hour`는
#   출력/CLI 조회 등 식별 용도로는 계속 남아있지만 더 이상 모델 feature가 아니다.
BASE_FEATURE_COLUMNS = [
    "station_no",
    "capacity",
    "lat",
    "lon",
    "temp",
    "precip",
    "pop_total",
    "minute",
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

# --- 학습기간 롤링 윈도우 (2026-08, TRAIN_YEAR 고정 연도 폐지 — training_window() 참고) ---
# 매달 재학습할 때마다 "최근 TRAIN_LOOKBACK_MONTHS개월"을 다시 계산해야 하므로 고정
# 연도 대신 개월 수로 둔다. TRAINING_SAFETY_MARGIN_DAYS는 예전엔 training/config.py
# 전용이었는데, feature_engine도 이제 같은 마진으로 Silver 조회 범위를 정해야 해서
# 공유 프로필로 승격했다(대여이력은 반납이 끝나야 Silver에 나타나므로 최근 며칠은
# 아직 라벨이 안 굳었을 수 있음).
TRAIN_LOOKBACK_MONTHS = _int_env("TRAIN_LOOKBACK_MONTHS", _PROFILE["TRAIN_LOOKBACK_MONTHS"])
TRAINING_SAFETY_MARGIN_DAYS = _int_env("TRAINING_SAFETY_MARGIN_DAYS", _PROFILE["TRAINING_SAFETY_MARGIN_DAYS"])
