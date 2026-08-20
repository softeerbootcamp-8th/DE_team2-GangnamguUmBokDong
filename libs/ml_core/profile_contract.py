"""런타임 로딩과 독립적인 ML 프로필 기본값·검증 계약이다.

이 모듈은 boto3, S3 경로 모듈, pandas 같은 런타임 의존성을 import하지 않는다.
따라서 `ML_PROFILE`이 존재하지 않거나 깨져 `common_config` import가 실패하는
상황에서도 profile 관리 CLI가 이 계약으로 새 원격 프로필을 검증하고 복구할 수 있다.
"""

BUILTIN_PROFILE_NAME = "builtin-default"
SERVING_TICK_MINUTES = 5
DEFAULT_MODEL_GRID_TICK_MINUTES = 20
SUPPORTED_MODEL_GRID_TICK_MINUTES = (5, 10, 15, 20, 30, 60)
PROFILES_PREFIX = "profiles"

DEFAULT_PROFILE = {
    "ROLLING_TICK_MINUTES": DEFAULT_MODEL_GRID_TICK_MINUTES,
    "ROLLING_WINDOW_MINUTES": 60,
    "ROLLING_EMBARGO_MINUTES": 40,
    "TARGET_HORIZON_MINUTES": 60,
    "GRID_TICK_MINUTES": DEFAULT_MODEL_GRID_TICK_MINUTES,
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


def validate_model_grid_contract(
    grid_tick: int,
    rolling_tick: int,
    target_horizon: int,
    name: str,
) -> None:
    """모델 feature/target grid 조합이 지원되는지 검증한다.

    args:
        grid_tick: feature/target anchor 간격(분)
        rolling_tick: rolling feature 계산 anchor 간격(분)
        target_horizon: 한 target이 포함하는 미래 구간(분)
        name: 오류 메시지에 표시할 설정 또는 프로필 이름
    raises:
        ValueError: 지원하지 않는 tick이거나 간격 간 계약이 맞지 않을 때
    """
    ticks = {
        "GRID_TICK_MINUTES": grid_tick,
        "ROLLING_TICK_MINUTES": rolling_tick,
    }
    for key, value in ticks.items():
        if value not in SUPPORTED_MODEL_GRID_TICK_MINUTES:
            raise ValueError(
                f"프로필 '{name}'의 {key}={value}는 지원하는 모델 grid"
                f"{SUPPORTED_MODEL_GRID_TICK_MINUTES} 중 하나여야 합니다"
            )
    if grid_tick != rolling_tick:
        raise ValueError(
            f"프로필 '{name}'의 GRID_TICK_MINUTES와 ROLLING_TICK_MINUTES는 같아야 합니다: "
            f"grid={grid_tick}, rolling={rolling_tick}"
        )
    if target_horizon <= 0:
        raise ValueError(f"프로필 '{name}'의 TARGET_HORIZON_MINUTES는 양수여야 합니다: {target_horizon}")
    if grid_tick % SERVING_TICK_MINUTES != 0:
        raise ValueError(
            f"프로필 '{name}'의 모델 grid({grid_tick}분)는 서빙 주기({SERVING_TICK_MINUTES}분)의 배수여야 합니다"
        )
    if 1440 % grid_tick != 0 or target_horizon % grid_tick != 0:
        raise ValueError(
            f"프로필 '{name}'의 모델 grid({grid_tick}분)는 하루(1440분)와 "
            f"타겟 구간({target_horizon}분)을 모두 나누어야 합니다"
        )


def validate_train_anchor_contract(grid_tick: int, anchor_tick: int, name: str) -> None:
    """학습 anchor 밀도가 base 모델 grid와 호환되는지 검증한다.

    5분 base feature에서 20분마다 학습하는 식의 thinning은 허용하지만, 원본에
    존재하지 않는 중간 시각을 요구하거나 시간마다 anchor 위상이 달라지는 조합은
    허용하지 않는다.

    args:
        grid_tick: feature/target base grid 간격(분)
        anchor_tick: 실제 학습에 남길 anchor 간격(분)
        name: 오류 메시지에 표시할 설정 또는 프로필 이름
    raises:
        ValueError: anchor 간격이 base grid와 호환되지 않을 때
    """
    if anchor_tick < grid_tick or anchor_tick % grid_tick != 0:
        raise ValueError(
            f"프로필 '{name}'의 TRAIN_ANCHOR_TICK_MINUTES={anchor_tick}는 "
            f"GRID_TICK_MINUTES={grid_tick} 이상의 배수여야 합니다"
        )
    if anchor_tick % SERVING_TICK_MINUTES != 0 or 60 % anchor_tick != 0 or 1440 % anchor_tick != 0:
        raise ValueError(
            f"프로필 '{name}'의 TRAIN_ANCHOR_TICK_MINUTES={anchor_tick}는 "
            f"{SERVING_TICK_MINUTES}분 배수이면서 한 시간과 하루를 나누어야 합니다"
        )


def validate_profile(profile: dict, name: str) -> None:
    """병합된 프로필 구조와 모델 학습 grid 계약을 검증한다.

    알려지지 않은 일반 키는 이후 코드가 새 파라미터를 도입할 수 있도록 허용하되,
    런타임이 직접 기록하는 `profile_name` 메타데이터는 원격 입력에서 거부한다.
    운영 추론은 5분마다 실행하지만, 학습 feature/target grid는 기본 20분이고
    한 시간을 정확히 나누는 5분 배수도 명시적으로 허용한다. rolling과 target
    grid가 서로 다르면 같은 anchor의 feature/label 의미가 갈라지므로 반드시
    같아야 한다. 선택적인 학습 anchor thinning은 base grid와 별도로 검증한다.

    args:
        profile: 기본값과 원격 override를 병합한 프로필
        name: 오류 메시지에 표시할 프로필 이름
    raises:
        TypeError: 프로필 또는 LGB_PARAMS_COMMON이 객체가 아닐 때
        ValueError: 예약 메타데이터나 모델 학습 grid 계약이 잘못됐을 때
    """
    if not isinstance(profile, dict):
        raise TypeError(f"프로필 '{name}'의 최상위 JSON 값은 객체여야 합니다")
    if "profile_name" in profile:
        raise ValueError(f"프로필 '{name}'은 예약 메타데이터 키 profile_name을 포함할 수 없습니다")
    if not isinstance(profile.get("LGB_PARAMS_COMMON"), dict):
        raise TypeError(f"프로필 '{name}'의 LGB_PARAMS_COMMON은 객체여야 합니다")
    ticks: dict[str, int] = {}
    for key in ("GRID_TICK_MINUTES", "ROLLING_TICK_MINUTES"):
        try:
            value = int(profile[key])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"프로필 '{name}'의 {key}는 정수여야 합니다") from exc
        ticks[key] = value

    grid_tick = ticks["GRID_TICK_MINUTES"]
    rolling_tick = ticks["ROLLING_TICK_MINUTES"]
    try:
        target_horizon = int(profile["TARGET_HORIZON_MINUTES"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"프로필 '{name}'의 TARGET_HORIZON_MINUTES는 정수여야 합니다") from exc
    validate_model_grid_contract(grid_tick, rolling_tick, target_horizon, name)

    try:
        anchor_tick = int(profile.get("TRAIN_ANCHOR_TICK_MINUTES", grid_tick))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"프로필 '{name}'의 TRAIN_ANCHOR_TICK_MINUTES는 정수여야 합니다") from exc
    validate_train_anchor_contract(grid_tick, anchor_tick, name)


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
    # 오래된 프로필에는 anchor 키가 없다. 명시하지 않은 경우 그 프로필의 실제
    # base grid를 그대로 써서 thinning 없는 기존 동작을 보존하고, 반환값에는
    # 항상 materialize해 월별 preflight와 model artifact가 같은 계약을 보게 한다.
    if "TRAIN_ANCHOR_TICK_MINUTES" not in overrides:
        merged["TRAIN_ANCHOR_TICK_MINUTES"] = merged["GRID_TICK_MINUTES"]
    validate_profile(merged, name)
    return merged
