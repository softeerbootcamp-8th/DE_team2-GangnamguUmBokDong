"""학습 아티팩트와 실시간 서빙이 공유해야 하는 피처 프로필 계약.

프로필에는 LightGBM 학습 파라미터, 재학습 기간, 모니터링 임계값처럼 모델마다
달라도 서빙 피처의 의미를 바꾸지 않는 값도 들어 있다. 이 모듈은 그중 피처 값과
타깃의 의미를 직접 바꾸는 키만 추려 비교한다. 따라서 학습 전용 설정 차이는
허용하면서, 같은 컬럼 이름이 서로 다른 의미를 갖는 train-serve skew는 막는다.
"""

from __future__ import annotations

from collections.abc import Mapping

from core import s3 as s3_io

from .paths import model_json_key

SERVING_FEATURE_PROFILE_KEYS = (
    "ROLLING_TICK_MINUTES",
    "ROLLING_WINDOW_MINUTES",
    "ROLLING_EMBARGO_MINUTES",
    "TARGET_HORIZON_MINUTES",
    "GRID_TICK_MINUTES",
    "HORIZON_COUNT",
)


class ServingProfileContractError(RuntimeError):
    """서빙 피처 프로필이 없거나 서로 호환되지 않을 때 발생한다."""


def extract_serving_feature_contract(profile: Mapping, *, source: str) -> dict[str, object]:
    """전체 프로필에서 서빙 피처 의미를 결정하는 키만 추출한다.

    args:
        profile: effective profile 또는 모델 옆에 저장된 profile.json 객체
        source: 오류 메시지에서 프로필 출처를 식별할 이름
    returns:
        dict[str, object]: 비교 가능한 서빙 피처 계약
    raises:
        ServingProfileContractError: 프로필이 객체가 아니거나 필수 키가 없을 때
    """
    if not isinstance(profile, Mapping):
        raise ServingProfileContractError(f"{source} 프로필은 JSON 객체여야 합니다")

    missing = [key for key in SERVING_FEATURE_PROFILE_KEYS if key not in profile]
    if missing:
        raise ServingProfileContractError(
            f"{source} 프로필에 서빙 계약 키가 없습니다: {', '.join(missing)}"
        )
    return {key: profile[key] for key in SERVING_FEATURE_PROFILE_KEYS}


def assert_serving_profiles_compatible(
    expected_profile: Mapping,
    actual_profile: Mapping,
    *,
    expected_source: str,
    actual_source: str,
) -> dict[str, object]:
    """두 프로필의 서빙 피처 계약이 같지 않으면 상세 오류를 발생시킨다.

    LightGBM 파라미터나 학습 기간처럼 ``SERVING_FEATURE_PROFILE_KEYS``에 없는
    값은 의도적으로 비교하지 않는다.

    returns:
        dict[str, object]: 호환성이 확인된 actual_profile의 서빙 피처 계약
    raises:
        ServingProfileContractError: 필수 키가 없거나 값이 하나라도 다를 때
    """
    expected = extract_serving_feature_contract(expected_profile, source=expected_source)
    actual = extract_serving_feature_contract(actual_profile, source=actual_source)
    mismatches = {
        key: (expected[key], actual[key])
        for key in SERVING_FEATURE_PROFILE_KEYS
        if expected[key] != actual[key]
    }
    if mismatches:
        details = ", ".join(
            f"{key}: {expected_source}={expected_value!r}, {actual_source}={actual_value!r}"
            for key, (expected_value, actual_value) in mismatches.items()
        )
        raise ServingProfileContractError(f"서빙 피처 프로필이 호환되지 않습니다: {details}")
    return actual


def load_model_profile(model_name: str, archive_prefix: str) -> dict:
    """모델 아카이브의 effective profile 아티팩트를 읽는다.

    raises:
        ServingProfileContractError: profile.json이 없거나 객체가 아닐 때
    """
    key = model_json_key(model_name, "profile", archive_prefix)
    profile = s3_io.read_json(key)
    if profile is None:
        raise ServingProfileContractError(f"모델 effective profile이 없습니다: {key}")
    if not isinstance(profile, dict):
        raise ServingProfileContractError(f"모델 effective profile은 JSON 객체여야 합니다: {key}")
    return profile
