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
from ml_core.scoring import load_boosters, load_conformal_correction


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


def promote_challenger(model_name: str, archive_prefix: str) -> dict:
    """model_name의 챔피언이 archive_prefix를 가리키도록 포인터를 원자적으로 전환한다.

    더 이상 archive 밑의 파일을 챔피언 자리로 복사하지 않는다(모듈 docstring
    참고) — 포인터 하나만 바꾸면 승격이 끝난다.

    **2026-08**: `write_champion_pointer()` 자신은 캐시를 안 비운다(그 함수
    docstring 참고 — `read_champion_prefix()`만 비우면 `load_boosters()`/
    `load_conformal_correction()`은 옛 값에 머물러 셋이 서로 다른 archive를
    가리키는 불일치가 생긴다, 실측 확인됨). 셋 다 아는 유일한 지점이 여기라서,
    포인터를 쓴 직후 세 캐시를 한꺼번에 비운다 — "학습해봤더니 구려서 같은
    프로세스 안에서 재학습→재승격"을 반복하는 코드가 있다면, 재승격 직후
    다음 채점부터 booster/correction/station_categories가 전부 새 archive
    하나로 일관되게 나온다.

    args:
        model_name: "rental" 또는 "return"
        archive_prefix: 이번에 승격할 학습 결과가 있는 아카이브 prefix
            (`ml_core.paths.archive_models_prefix()`가 만든 값)
    returns:
        dict: `write_champion_pointer()`가 실제로 기록한 포인터 내용(로그/알림용)
    """
    record = write_champion_pointer(model_name, archive_prefix)
    read_champion_prefix.cache_clear()
    load_boosters.cache_clear()
    load_conformal_correction.cache_clear()
    return record
