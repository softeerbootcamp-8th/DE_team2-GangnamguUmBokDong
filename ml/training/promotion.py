"""챌린저 모델이 챔피언을 대체할 만한지 판정하고, 대체할 때 챔피언 포인터를
챌린저의 아카이브 prefix로 원자적으로 전환한다.

**왜 재학습이 아니라 아카이브를 그대로 쓰는가**: LightGBM 학습은 `LGB_PARAMS_COMMON`에
고정 시드가 없어 완전히 결정적이라고 보장할 수 없다 — 같은 데이터로 다시 학습하면
챌린저와 미묘하게 다른 모델이 나올 수 있다. `train_rental_model.py`/
`train_return_model.py`가 이미 아카이브에 써둔 파일을 그대로 챔피언으로 삼아야만
"승격된 챔피언"이 "방금 평가했던 챌린저"와 바이트 단위로 동일함을 보장한다.

**왜 파일 복사가 아니라 포인터 전환인가**: 예전엔 archive 밑의 파일 8개
(booster 4개 + station_categories/conformal_correction/metrics/profile)를
챔피언 prefix로 하나씩 복사했다 — S3는 여러 키에 걸친 트랜잭션을 지원하지
않으므로, 복사가 절반쯤 끝난 순간 inference가 실행되면 booster는 새 버전인데
station_categories는 옛 버전인 식으로 섞인 모델을 읽을 수 있었다(station_id
카테고리 코드가 학습 시점의 정렬 순서에 의존해서, 섞이면 성능 저하가 아니라
엉뚱한 정류소에 대한 예측이 조용히 나감). archive 자체는 학습이 끝난 뒤 다시
안 바뀌는 immutable 산출물이므로, `ml_core.paths.write_champion_pointer()`로
"지금 챔피언은 이 archive_prefix다"라는 포인터 객체 하나만 원자적으로 바꾸면
파일을 복사할 필요가 아예 없다(자세한 근거는 `read_champion_prefix()` docstring 참고).

**승격 기준**(둘 다 만족해야 승격): `poisson_deviance_test`가 챔피언보다 나쁘지
않고(작거나 같고), `p10_p90_coverage_calibrated_test`가 목표 커버리지
(`common_config.CONFORMAL_TARGET_COVERAGE`) ± 허용 드리프트
(`common_config.COVERAGE_DRIFT_THRESHOLD`) 범위 안. 둘 다 이미 있는 상수를 그대로
쓴다 — 승격 전용의 새 절대 임계값을 따로 만들지 않는다(계절성 때문에 절대
임계값이 적합하지 않다는 근거는 common_config.py 참고).
"""

from __future__ import annotations

from ml_core import common_config
from ml_core.paths import read_champion_prefix, write_champion_pointer
from ml_core.scoring import (
    load_boosters,
    load_conformal_correction,
    validate_champion_serving_contract,
)
from ml_core.serving_contract import (
    assert_serving_profiles_compatible,
    load_model_profile,
)

_MODEL_NAMES = {"rental", "return"}


class ChampionAlreadyExistsError(RuntimeError):
    """부트스트랩이 기존 챔피언을 덮어쓰려 할 때 발생한다."""


def ensure_champion_absent(model_name: str) -> None:
    """지정 모델에 챔피언 포인터가 아직 없는지 검증한다.

    최초 학습 CLI가 `--promote-if-no-champion`으로 긴 학습을 시작하기 전에 빠르게
    확인할 때 쓴다. 실제 포인터 쓰기 직전에는 `bootstrap_challenger()`가 다시
    확인하므로, 이 사전 검사는 주로 불필요한 재학습 비용을 막는 역할이다.

    raises:
        ValueError: 지원하지 않는 모델 이름일 때
        ChampionAlreadyExistsError: 이미 챔피언 포인터가 있을 때
    """
    if model_name not in _MODEL_NAMES:
        raise ValueError(f"알 수 없는 모델 이름입니다: {model_name}")
    try:
        existing_prefix = read_champion_prefix(model_name)
    except FileNotFoundError:
        return
    raise ChampionAlreadyExistsError(
        f"{model_name} 챔피언이 이미 존재합니다: {existing_prefix}. "
        "부트스트랩은 기존 챔피언을 덮어쓰지 않습니다"
    )


def should_promote(challenger_metrics: dict, champion_metrics: dict | None) -> tuple[bool, list[str]]:
    """챌린저가 챔피언을 대체할 만한지 판정한다.

    args:
        challenger_metrics: 방금 학습한 챌린저의 train_target() 반환값
        champion_metrics: 현재 챔피언의 metrics.json (아직 챔피언이 없으면 None)
    returns:
        tuple[bool, list[str]]: (승격 여부, 판단 근거 문자열 목록 — 승격이든 반려든
            항상 채워짐, 알림 로그에 그대로 쓴다)
    """
    if champion_metrics is None:
        return True, ["챔피언이 아직 없음 — 최초 학습 결과를 그대로 승격(부트스트랩)"]

    reasons = []
    challenger_deviance = challenger_metrics["poisson_deviance_test"]
    champion_deviance = champion_metrics["poisson_deviance_test"]
    deviance_ok = challenger_deviance <= champion_deviance
    reasons.append(
        f"deviance: 챌린저 {challenger_deviance:.4f} {'<=' if deviance_ok else '>'} "
        f"챔피언 {champion_deviance:.4f} ({'통과' if deviance_ok else '미달'})"
    )

    coverage = challenger_metrics["p10_p90_coverage_calibrated_test"]
    lo = common_config.CONFORMAL_TARGET_COVERAGE - common_config.COVERAGE_DRIFT_THRESHOLD
    hi = common_config.CONFORMAL_TARGET_COVERAGE + common_config.COVERAGE_DRIFT_THRESHOLD
    coverage_ok = lo <= coverage <= hi
    reasons.append(
        f"coverage: 챌린저 {coverage:.3f} {'in' if coverage_ok else 'out of'} "
        f"허용 범위 [{lo:.3f}, {hi:.3f}] ({'통과' if coverage_ok else '미달'})"
    )

    return deviance_ok and coverage_ok, reasons


def promote_challenger(
    model_name: str,
    archive_prefix: str,
    *,
    require_no_champion: bool = False,
) -> dict:
    """model_name의 챔피언이 archive_prefix를 가리키도록 포인터를 원자적으로 전환한다.

    더 이상 archive 밑의 파일을 챔피언 자리로 복사하지 않는다(모듈 docstring
    참고) — 포인터 하나만 바꾸면 승격이 끝난다.

    **2026-08**: `write_champion_pointer()` 자신은 캐시를 안 비운다(그 함수
    docstring 참고 — `read_champion_prefix()`만 비우면 `load_boosters()`/
    `load_conformal_correction()`은 옛 값에 머물러 서로 다른 archive를
    가리키는 불일치가 생긴다, 실측 확인됨). 관련 로더를 다 아는 지점이 여기라서,
    포인터를 쓴 직후 포인터·booster·보정값·프로필 검증 캐시를 함께 비운다 — "학습해봤더니 구려서 같은
    프로세스 안에서 재학습→재승격"을 반복하는 코드가 있다면, 재승격 직후
    다음 채점부터 booster/correction/station_categories가 전부 새 archive
    하나로 일관되게 나온다.

    args:
        model_name: "rental" 또는 "return"
        archive_prefix: 이번에 승격할 학습 결과가 있는 아카이브 prefix
            (`ml_core.paths.archive_models_prefix()`가 만든 값)
        require_no_champion: True면 계약 검증 후 포인터 쓰기 직전에 기존 챔피언
            존재 여부를 확인해 부트스트랩이 기존 포인터를 덮어쓰지 않게 함
    returns:
        dict: `write_champion_pointer()`가 실제로 기록한 포인터 내용(로그/알림용)
    raises:
        ChampionAlreadyExistsError: require_no_champion인데 이미 챔피언이 있을 때
        ServingProfileContractError: 챌린저 프로필이 없거나 현재 서빙/반대 모델
            챔피언의 피처 계약과 다를 때
    """
    if model_name not in _MODEL_NAMES:
        raise ValueError(f"알 수 없는 모델 이름입니다: {model_name}")

    challenger_profile = load_model_profile(model_name, archive_prefix)
    assert_serving_profiles_compatible(
        common_config.effective_profile(),
        challenger_profile,
        expected_source="현재 서빙",
        actual_source=f"{model_name} 챌린저({archive_prefix})",
    )

    # 대여/반납은 한 실시간 feature row를 공유한다. 다른 모델이 이미 챔피언이면
    # 그 계약과도 같아야 한쪽 포인터만 먼저 바뀌는 순차 승격이 안전하다. 최초
    # 부트스트랩에서는 다른 챔피언이 아직 없는 것이 정상이라 비교를 생략한다.
    other_model_name = "return" if model_name == "rental" else "rental"
    try:
        other_archive_prefix = read_champion_prefix(other_model_name)
    except FileNotFoundError:
        other_archive_prefix = None
    if other_archive_prefix is not None:
        other_profile = load_model_profile(other_model_name, other_archive_prefix)
        assert_serving_profiles_compatible(
            other_profile,
            challenger_profile,
            expected_source=f"{other_model_name} 챔피언({other_archive_prefix})",
            actual_source=f"{model_name} 챌린저({archive_prefix})",
        )

    if require_no_champion:
        # 긴 학습을 시작하기 전 엔트리포인트에서도 한 번 확인하지만, 그 뒤 다른
        # 프로세스가 먼저 부트스트랩했을 수 있으므로 실제 PUT 직전에 다시 본다.
        read_champion_prefix.cache_clear()
        ensure_champion_absent(model_name)

    record = write_champion_pointer(model_name, archive_prefix)
    read_champion_prefix.cache_clear()
    load_boosters.cache_clear()
    load_conformal_correction.cache_clear()
    validate_champion_serving_contract.cache_clear()
    return record


def bootstrap_challenger(model_name: str, archive_prefix: str) -> dict:
    """챔피언이 없을 때만 계약 검증을 통과한 첫 아카이브를 승격한다.

    일반 재학습의 지표 비교를 우회할 수 있는 최초 배포 전용 진입점이다. 내부적으로
    `promote_challenger()`를 그대로 거치므로 현재 서빙 및 반대 모델 챔피언과의
    effective profile 계약 검증은 생략되지 않는다.
    """
    ensure_champion_absent(model_name)
    return promote_challenger(model_name, archive_prefix, require_no_champion=True)
