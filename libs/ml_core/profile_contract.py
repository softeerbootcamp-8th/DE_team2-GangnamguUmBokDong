"""런타임 로딩과 독립적인 ML 프로필 기본값·검증 계약이다.

이 모듈은 boto3, S3 경로 모듈, pandas 같은 런타임 의존성을 import하지 않는다.
따라서 `ML_PROFILE`이 존재하지 않거나 깨져 `common_config` import가 실패하는
상황에서도 profile 관리 CLI가 이 계약으로 새 원격 프로필을 검증하고 복구할 수 있다.
"""

BUILTIN_PROFILE_NAME = "builtin-default"
REQUIRED_GRID_TICK_MINUTES = 5
PROFILES_PREFIX = "profiles"

DEFAULT_PROFILE = {
    "ROLLING_TICK_MINUTES": REQUIRED_GRID_TICK_MINUTES,
    "ROLLING_WINDOW_MINUTES": 60,
    "ROLLING_EMBARGO_MINUTES": 40,
    "TARGET_HORIZON_MINUTES": 60,
    "GRID_TICK_MINUTES": REQUIRED_GRID_TICK_MINUTES,
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
    "TRAIN_LOOKBACK_MONTHS": 12,
    "TRAINING_SAFETY_MARGIN_DAYS": 7,
}


def profile_path(name: str) -> str:
    """프로필 이름에 대응하는 S3 상대 키를 반환한다."""
    return f"{PROFILES_PREFIX}/{name}.json"


def validate_profile(profile: dict, name: str) -> None:
    """병합된 프로필 구조와 고정된 5분 grid 계약을 검증한다.

    알려지지 않은 일반 키는 이후 코드가 새 파라미터를 도입할 수 있도록 허용하되,
    런타임이 직접 기록하는 `profile_name` 메타데이터는 원격 입력에서 거부한다.

    args:
        profile: 기본값과 원격 override를 병합한 프로필
        name: 오류 메시지에 표시할 프로필 이름
    raises:
        TypeError: 프로필 또는 LGB_PARAMS_COMMON이 객체가 아닐 때
        ValueError: 예약 메타데이터나 5분 grid 계약이 잘못됐을 때
    """
    if not isinstance(profile, dict):
        raise TypeError(f"프로필 '{name}'의 최상위 JSON 값은 객체여야 합니다")
    if "profile_name" in profile:
        raise ValueError(f"프로필 '{name}'은 예약 메타데이터 키 profile_name을 포함할 수 없습니다")
    if not isinstance(profile.get("LGB_PARAMS_COMMON"), dict):
        raise TypeError(f"프로필 '{name}'의 LGB_PARAMS_COMMON은 객체여야 합니다")
    for key in ("GRID_TICK_MINUTES", "ROLLING_TICK_MINUTES"):
        try:
            value = int(profile[key])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"프로필 '{name}'의 {key}는 정수여야 합니다") from exc
        if value != REQUIRED_GRID_TICK_MINUTES:
            raise ValueError(
                f"프로필 '{name}'의 {key}={value}는 운영 계약({REQUIRED_GRID_TICK_MINUTES}분)과 다릅니다"
            )


def merge_and_validate_profile(overrides: dict, name: str) -> dict:
    """부분 프로필을 기본값과 깊이 1단계 병합하고 검증해 새 dict로 반환한다.

    `LGB_PARAMS_COMMON`은 별도로 병합해 `max_bin` 같은 미래 키와 기존 기본 키를
    모두 보존한다.
    """
    if not isinstance(overrides, dict):
        raise TypeError(f"프로필 '{name}'의 최상위 JSON 값은 객체여야 합니다")
    lgb_overrides = overrides.get("LGB_PARAMS_COMMON", {})
    if not isinstance(lgb_overrides, dict):
        raise TypeError(f"프로필 '{name}'의 LGB_PARAMS_COMMON은 객체여야 합니다")
    merged = {
        **DEFAULT_PROFILE,
        **overrides,
        "LGB_PARAMS_COMMON": {
            **DEFAULT_PROFILE["LGB_PARAMS_COMMON"],
            **lgb_overrides,
        },
    }
    validate_profile(merged, name)
    return merged
