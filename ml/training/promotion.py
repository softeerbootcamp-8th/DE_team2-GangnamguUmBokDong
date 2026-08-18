"""챌린저 모델이 챔피언을 대체할 만한지 판정하고, 대체할 때 아카이브 사본을
챔피언 자리(`ml_core.paths.MODELS_PREFIX`)로 복사한다.

**왜 재학습이 아니라 복사인가**: LightGBM 학습은 `LGB_PARAMS_COMMON`에 고정
시드가 없어 완전히 결정적이라고 보장할 수 없다 — 같은 데이터로 다시 학습하면
챌린저와 미묘하게 다른 모델이 나올 수 있다. `train_rental_model.py`/
`train_return_model.py`가 이미 아카이브에 써둔 파일을 그대로 복사해야만 "승격된
챔피언"이 "방금 평가했던 챌린저"와 바이트 단위로 동일함을 보장한다.

**승격 기준**(둘 다 만족해야 승격): `poisson_deviance_test`가 챔피언보다 나쁘지
않고(작거나 같고), `p10_p90_coverage_calibrated_test`가 목표 커버리지
(`common_config.CONFORMAL_TARGET_COVERAGE`) ± 허용 드리프트
(`common_config.COVERAGE_DRIFT_THRESHOLD`) 범위 안. 둘 다 이미 있는 상수를 그대로
쓴다 — 승격 전용의 새 절대 임계값을 따로 만들지 않는다(계절성 때문에 절대
임계값이 적합하지 않다는 근거는 common_config.py 참고).
"""

from __future__ import annotations

from core import s3 as s3_io
from ml_core import common_config
from ml_core.paths import MODELS_PREFIX


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


def promote_challenger(model_name: str, archive_prefix: str) -> list[str]:
    """`archive_prefix` 아래 `{model_name}_`로 시작하는 아티팩트를 챔피언 자리로 복사한다.

    파일명은 그대로 유지한다("rental_poisson.txt"는 챔피언에서도 "rental_poisson.txt")
    — 챔피언 전용 이름을 따로 만들지 않는다. 같은 prefix에 다른 model_name의
    아티팩트가 섞여 있어도(예: rental/return을 같은 archive_prefix에 같이 학습해
    저장하는 경우) model_name이 다른 파일은 건드리지 않는다.

    args:
        model_name: "rental" 또는 "return"
        archive_prefix: 이번에 승격할 학습 결과가 있는 아카이브 prefix
            (`ml_core.paths.archive_models_prefix()`가 만든 값)
    returns:
        list[str]: 챔피언 자리로 복사된 파일명 목록(로그/알림용)
    """
    prefix = archive_prefix if archive_prefix.endswith("/") else f"{archive_prefix}/"
    copied = []
    for key in s3_io.list_keys(prefix):
        filename = key[len(prefix):]
        if not filename.startswith(f"{model_name}_"):
            continue
        body = s3_io.get_object_bytes(key)
        s3_io.put_object_bytes(f"{MODELS_PREFIX}/{filename}", body)
        copied.append(filename)
    return copied
