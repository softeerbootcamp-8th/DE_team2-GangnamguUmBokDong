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

**프로필 시스템(2026-08 S3 이관)**: `ML_PROFILE`을 명시하면 S3의
`profiles/{ML_PROFILE}.json`(`ml_core.paths.profile_path()`)에서 값을 읽어온다. 예전엔
저장소에 커밋된 로컬 JSON 파일이었는데, feature_engine/training/inference가 각자
다른 서버에 배포되므로 로컬 파일로는 "값 하나 바꾸면 파이프라인 전체에 반영"이
불가능했다(서버마다 코드 재배포가 필요) — S3는 이미 세 서비스가 공유하는 유일한
인프라라 여기로 옮겼다. 여러 파라미터 조합(예: embargo 30분 챔피언 vs 45분 챌린저)을
프로필로 미리 만들어 S3에 두고, `ML_PROFILE` 환경변수 하나로 전체 조합을 바꿔 낄 수
있다(`ml_core.profile_registry.push_profile()`로 생성/수정 — MLflow에도 같이 기록되어
변경 이력을 볼 수 있지만, 실제 런타임 조회는 항상 S3 직접 조회다). 기존에 있던 개별
환경변수 override(예: `ROLLING_EMBARGO_MINUTES=45`)는 프로필 값 위에 한 번 더 덮어쓸
수 있게 유지한다 — 우선순위는 "개별 환경변수 > 명시적으로 선택한 S3 프로필"이다.
`ML_PROFILE`을 생략한 실행만 예약 이름 `builtin-default`의 내장 기본값을 사용한다.
명시한 S3 프로필이 없거나 조회/파싱에 실패하면 다른 설정으로 조용히 진행하지 않고
즉시 실패한다. `training/config.py`/`feature_engine/spark/config.py`는
지금처럼 `common_config.XXX`를 그대로 참조하면 되고 인터페이스는 바뀌지 않는다.

**왜 `core.s3`를 안 쓰고 boto3를 직접 쓰는가**: `core.s3`는 parquet 처리를 위해
pandas/pyarrow를 무조건 import한다 — 이 파일은 위에서 말한 "pandas/pyspark 등 무거운
의존성을 전혀 import하지 않는 순수 상수 모듈" 원칙을 지켜야 해서, boto3만 쓰는 최소
구현을 따로 둔다(`_fetch_profile_from_s3()`).
"""

import json
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from . import profile_contract

# `ML_PROFILE`을 생략한 실행이 쓰는 내장 프로필이다. S3의 `profiles/default.json`과
# 이름을 분리해, 오래된 원격 객체가 코드에 고정된 운영 기본값을 암묵적으로 덮어쓰지
# 못하게 한다. 원격 프로필을 쓰려면 반드시 `ML_PROFILE=<name>`을 명시해야 한다.
BUILTIN_PROFILE_NAME = profile_contract.BUILTIN_PROFILE_NAME
REQUIRED_GRID_TICK_MINUTES = profile_contract.REQUIRED_GRID_TICK_MINUTES
_DEFAULT_PROFILE = profile_contract.DEFAULT_PROFILE


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
    반대 방향 import는 불가능)을 읽는다. 없으면 None, S3가 아예 응답하지 않거나
    JSON이 깨졌으면 예외를 그대로 던진다(호출부도 fail-closed로 전파한다).

    기본 boto3 재시도 정책으로 존재하지 않는 엔드포인트를 조회하면 실측 8.5초가
    걸린다 — 프로필 조회 실패는 잘못된 설정으로 학습/추론하는 것보다 즉시 실패하는
    편이 안전하므로, 짧은 timeout+재시도 없음으로 원인을 빠르게 드러낸다.
    """
    bucket = os.environ.get("S3_BUCKET", "gangnamgu")
    try:
        body = _s3_client(timeout_seconds).get_object(Bucket=bucket, Key=f"profiles/{name}.json")["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return json.loads(body)


def _selected_profile_name() -> str:
    """이번 프로세스가 사용할 실제 프로필 이름을 반환한다.

    `ML_PROFILE`을 생략하면 S3를 조회하지 않는 예약 이름 `builtin-default`를
    반환한다. 빈 문자열을 명시한 경우는 운영 설정 오타이므로 기본값으로 간주하지
    않고 즉시 실패한다.
    """
    raw_name = os.environ.get("ML_PROFILE")
    if raw_name is None:
        return BUILTIN_PROFILE_NAME
    name = raw_name.strip()
    if not name:
        raise ValueError("ML_PROFILE은 빈 문자열일 수 없습니다")
    return name


def _validate_profile(profile: dict, name: str) -> None:
    """하위 호환을 위해 순수 프로필 계약 검증 함수를 재노출한다."""
    profile_contract.validate_profile(profile, name)


def _load_profile(name: str | None = None) -> dict:
    """선택된 프로필을 결정적으로 로드한다.

    `builtin-default`는 S3를 전혀 조회하지 않는다. 그 외 이름은 명시적으로 선택한
    원격 프로필이므로, 객체가 없거나 S3/JSON 오류가 나면 예외를 그대로 드러내
    요청하지 않은 내장 값으로 학습·추론하는 fail-open을 막는다.

    args:
        name: 직접 지정할 프로필 이름. None이면 `_selected_profile_name()` 결과 사용
    returns:
        dict: 내장 기본 키와 병합되고 검증된 새 프로필 사본
    raises:
        FileNotFoundError: 명시한 S3 프로필이 없을 때
        TypeError: 프로필 구조가 잘못됐을 때
        ValueError: 예약 메타데이터나 5분 grid 계약이 잘못됐을 때
    """
    name = name or _selected_profile_name()
    if name == BUILTIN_PROFILE_NAME:
        profile = profile_contract.merge_and_validate_profile({}, name)
    else:
        loaded = _fetch_profile_from_s3(name)
        if loaded is None:
            raise FileNotFoundError(f"S3에 프로필 '{name}'이 없습니다: profiles/{name}.json")
        profile = profile_contract.merge_and_validate_profile(loaded, name)
        print(f"[common_config] 프로필 '{name}'을 S3에서 읽음")

    # 내장 기본값과 병합한다(S3 값이 우선) — 이 프로필이 이번 PR 이전에 올려둔
    # 것이거나 push_profile()로 사람이 손으로 만든 것이면 신규 키(TRAIN_LOOKBACK_
    # MONTHS 등)가 없을 수 있는데, 그대로 반환하면 이 파일 끝부분의
    # `_PROFILE["TRAIN_LOOKBACK_MONTHS"]` 같은 곳에서 KeyError가 나 feature_engine/
    # training/inference가 전부 import 시점에 죽는다(리뷰 지적) — 병합하면 누락된
    # 키만 내장 기본값으로 자연히 메워진다.
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
    """학습에 쓸 명시적 또는 롤링 날짜 구간을 반환한다.

    `TRAIN_WINDOW_START`와 `TRAIN_WINDOW_END`를 둘 다 지정하면 해당 inclusive
    구간을 그대로 쓴다. 최초 챔피언처럼 과거 한 해를 정확히 재현할 때 쓰며,
    feature_engine과 training이 이 함수를 공유하므로 두 단계가 같은 범위를 본다.
    한쪽만 지정하거나 ISO 날짜가 잘못됐거나 시작이 종료보다 늦으면 import 시점에
    실패해 서로 다른 범위의 산출물을 만드는 것을 막는다.

    두 환경변수가 모두 없으면 기존 rolling 규칙을 유지한다. 즉 `end =
    as_of(기본 오늘 KST) - TRAINING_SAFETY_MARGIN_DAYS`, `start = end -
    TRAIN_LOOKBACK_MONTHS개월`이다. 월별 재학습은 명시적 구간 변수를 설정하지 않고
    이 경로를 써서 최신 데이터가 포함된 윈도우를 매번 다시 계산한다.

    args:
        as_of: rolling 계산 기준 날짜(기본 오늘 KST). 명시적 구간에서는 사용하지 않음
    raises:
        ValueError: 명시적 구간이 쌍으로 없거나 YYYY-MM-DD 형식이 아니거나 역전됐을 때
    """
    raw_start = os.environ.get("TRAIN_WINDOW_START")
    raw_end = os.environ.get("TRAIN_WINDOW_END")
    if raw_start is not None or raw_end is not None:
        if raw_start is None or raw_end is None:
            raise ValueError("TRAIN_WINDOW_START와 TRAIN_WINDOW_END는 반드시 함께 지정해야 합니다")

        def _parse_explicit_date(name: str, value: str) -> date:
            """명시적 학습 경계 하나를 엄격한 YYYY-MM-DD 형식으로 파싱한다."""
            try:
                parsed = date.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(f"{name}은 YYYY-MM-DD 형식이어야 합니다: {value!r}") from exc
            if parsed.isoformat() != value:
                raise ValueError(f"{name}은 YYYY-MM-DD 형식이어야 합니다: {value!r}")
            return parsed

        start = _parse_explicit_date("TRAIN_WINDOW_START", raw_start)
        end = _parse_explicit_date("TRAIN_WINDOW_END", raw_end)
        if start > end:
            raise ValueError(
                "TRAIN_WINDOW_START는 TRAIN_WINDOW_END보다 늦을 수 없습니다: "
                f"start={start.isoformat()}, end={end.isoformat()}"
            )
        return start, end

    as_of = as_of or today_kst()
    end = as_of - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)
    start = _subtract_months(end, TRAIN_LOOKBACK_MONTHS)
    return start, end


# 이 프로세스가 실제로 어떤 프로필을 쓰고 있는지 — 모델 아티팩트에 "어떤 프로필로
# 학습됐는지"를 그대로 남겨야 해서(model_contract.py의 profile.json) public으로 둔다.
# 요청한 S3 프로필을 못 읽으면 import 자체가 실패하므로, PROFILE_NAME과 PROFILE이
# 서로 다른 출처를 가리키는 상태는 생기지 않는다.
PROFILE_NAME = _selected_profile_name()
PROFILE = _load_profile(PROFILE_NAME)
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
if ROLLING_TICK_MINUTES != REQUIRED_GRID_TICK_MINUTES or GRID_TICK_MINUTES != REQUIRED_GRID_TICK_MINUTES:
    raise ValueError(
        "ROLLING_TICK_MINUTES와 GRID_TICK_MINUTES는 운영 계약에 따라 모두 "
        f"{REQUIRED_GRID_TICK_MINUTES}여야 합니다: rolling={ROLLING_TICK_MINUTES}, grid={GRID_TICK_MINUTES}"
    )

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
#   시각을 나타낸다 — 그리드 자체가 5분 tick인데 hour만 쓰면 같은 시간 안의
#   17:00/17:05/17:10 등이 모델에 전부 같은 값으로 보인다. minute은 그 tick 구분을
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


def _build_lgb_params(profile_params: dict) -> dict:
    """원격 LGB 파라미터 전체를 보존하고 알려진 환경변수 override를 적용한다.

    `max_bin`처럼 아직 전용 환경변수가 없는 새 LightGBM 파라미터도 원격 프로필에
    추가하면 Dataset construct와 `lgb.train()`까지 그대로 전달돼야 한다. 알려진
    키만 새 dict로 재작성하면 이런 미래 키가 조용히 사라지므로, 원본을 복사한 뒤
    현재 지원하는 override만 덮는다.
    """
    params = dict(profile_params)
    params.update(
        {
            "num_leaves": _int_env("LGB_NUM_LEAVES", profile_params["num_leaves"]),
            "learning_rate": _float_env("LGB_LEARNING_RATE", profile_params["learning_rate"]),
            "feature_fraction": _float_env("LGB_FEATURE_FRACTION", profile_params["feature_fraction"]),
            "bagging_fraction": _float_env("LGB_BAGGING_FRACTION", profile_params["bagging_fraction"]),
            "bagging_freq": _int_env("LGB_BAGGING_FREQ", profile_params["bagging_freq"]),
            "min_data_in_leaf": _int_env("LGB_MIN_DATA_IN_LEAF", profile_params["min_data_in_leaf"]),
            "verbose": _int_env("LGB_VERBOSE", profile_params.get("verbose", -1)),
            # 0 = LightGBM이 사용 가능한 코어 수만큼 자동 사용
            "num_threads": _int_env("LGB_NUM_THREADS", profile_params.get("num_threads", 0)),
        }
    )
    return params


LGB_PARAMS_COMMON = _build_lgb_params(_LGB_PROFILE)
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


def effective_profile() -> dict:
    """환경변수 override까지 반영된 이번 프로세스의 프로필 snapshot을 반환한다.

    원격/내장 원문 `PROFILE`을 그대로 모델 옆에 저장하면 실제 실행에 적용된 개별
    환경변수와 아티팩트 기록이 달라질 수 있다. 알려지지 않은 미래 top-level 키는
    보존하고, 런타임이 해석하는 키만 최종 상수로 덮어 재현 가능한 사본을 만든다.
    반환 dict와 중첩 LGB dict는 새 객체라 호출자가 바꿔도 모듈 전역 설정은 변하지
    않는다. `profile_name`은 값 자체가 아니라 선택 메타데이터이므로 호출부가
    `PROFILE_NAME`을 별도 필드로 마지막에 추가한다.
    """
    return {
        **PROFILE,
        "ROLLING_TICK_MINUTES": ROLLING_TICK_MINUTES,
        "ROLLING_WINDOW_MINUTES": ROLLING_WINDOW_MINUTES,
        "ROLLING_EMBARGO_MINUTES": ROLLING_EMBARGO_MINUTES,
        "TARGET_HORIZON_MINUTES": TARGET_HORIZON_MINUTES,
        "GRID_TICK_MINUTES": GRID_TICK_MINUTES,
        "HORIZON_COUNT": HORIZON_COUNT,
        "LGB_PARAMS_COMMON": dict(LGB_PARAMS_COMMON),
        "LGB_NUM_BOOST_ROUND": LGB_NUM_BOOST_ROUND,
        "LGB_EARLY_STOPPING_ROUNDS": LGB_EARLY_STOPPING_ROUNDS,
        "CONFORMAL_TARGET_COVERAGE": CONFORMAL_TARGET_COVERAGE,
        "INCREMENTAL_LOOKBACK_HOURS": INCREMENTAL_LOOKBACK_HOURS,
        "PERFORMANCE_DEGRADATION_THRESHOLD": PERFORMANCE_DEGRADATION_THRESHOLD,
        "COVERAGE_DRIFT_THRESHOLD": COVERAGE_DRIFT_THRESHOLD,
        "MONITOR_LOOKBACK_MONTHS": MONITOR_LOOKBACK_MONTHS,
        "TRAIN_LOOKBACK_MONTHS": TRAIN_LOOKBACK_MONTHS,
        "TRAINING_SAFETY_MARGIN_DAYS": TRAINING_SAFETY_MARGIN_DAYS,
    }
