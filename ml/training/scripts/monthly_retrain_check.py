"""매달 챔피언 모델(대여/반납) 성능을 점검하고, 기준 미달이면 챌린저를 학습해
챔피언보다 나을 때만 교체한다.

**기본은 dry-run이다** — 리포트만 찍고 아무것도 바꾸지 않는다. 실제로 Spark
피처마트 생성(수십 초~수분)과 LightGBM 재학습(모델당 ~25분)을 트리거하려면
`--execute`를 명시해야 한다 — 매달 자동으로 돌리는 운영 환경에서는 그
스케줄러(cron/EMR step 등)가 `--execute`로 호출하면 된다.

기준(어느 정도 악화되면 재학습할지)은 [common_config.py](../../../libs/ml_core/common_config.py)에서
관리한다 — 여기서는 그 기준을 적용만 한다.

**챌린저/챔피언 흐름**: 학습은 항상 아카이브(`ml_core.paths.archive_models_prefix()`
— 날짜+프로필별로 분리)에 쓰고, 챔피언 경로(`models/`)에는 booster/JSON을 직접
쓰지 않는다. `training.promotion.should_promote()`가 챌린저(방금 학습한 것)와
챔피언(현재 챔피언 포인터가 가리키는 archive의 `metrics.json`)을 비교해, 챌린저가
기준을 만족할 때만 `promotion.promote_challenger()`로 챔피언 포인터
(`models/champion/{model_name}.json`)가 그 아카이브 prefix를 가리키도록 원자적으로
전환한다 — 파일을 복사하지 않는다(승격 도중 파일이 부분적으로만 바뀌어 서로 다른
버전이 섞이는 문제를 피하기 위함, `ml_core.paths.read_champion_prefix()` docstring
참고).

**재시도 순서(`_candidate_profiles()`)**: 1차는 챔피언이 실제로 학습됐던 프로필의
하이퍼파라미터(임베고/앵커/LGB 파라미터 등)를 그대로 쓰되 학습기간만 지금의
기본 롤링 윈도우(`TRAIN_LOOKBACK_MONTHS`, 최신 증분 포함)로 갱신해서 재시도한다
— "성능이 나빠졌으니 최신 데이터로 다시 학습해보자"가 먼저지, 하이퍼파라미터
자체를 바꾸는 건 별개 문제이기 때문이다. 그래도 못 넘으면 미리 등록해둔 다른
프로필(S3 `profiles/*.json` — `ml_core.common_config.list_profile_names()`가 나열,
`ml_core.profile_registry.push_profile()`로 생성)을 이름순으로 순차 시도한다.
단, 자동 승격은 현재 서빙과 같은 피처 계약 안에서 LightGBM/학습 기간만 튜닝하는
경우에 한한다. rolling/window/horizon처럼 피처 의미가 다른 후보는 무거운 Spark·
학습을 시작하기 전에 건너뛴다. 그런 계약 변경은 feature 생성·두 모델·inference를
함께 전환하는 별도 배포가 필요하다. 호환 후보를 전부 써도 못 넘으면 챔피언을
그대로 두고 다음 달을 기약한다(정상 종료 — 예외를 던지지 않는다).

**프로필마다 별도 프로세스가 필요한 이유**: `ml_core.common_config`는 프로세스가
시작할 때 `ML_PROFILE` 환경변수로 프로필 값을 한 번만 읽어 모듈 전역 상수로
고정한다 — 같은 프로세스 안에서 프로필을 바꿔가며 반복할 수 없다. 그래서 프로필
하나를 시도할 때마다 "feature 파이프라인 subprocess + 학습 subprocess"를
`ML_PROFILE=<profile>` 환경변수와 함께 새로 띄운다(기존에 `_trigger_feature_pipeline()`
이 이미 쓰던 subprocess 패턴을 학습에도 그대로 적용한 것).

**알림**: 레포에 Slack/이메일 등 실제 알림 채널이 없어, 구조화된 콘솔 출력
(`_notify()`)으로 학습 시작/성공/실패/승격/미승격을 전부 남긴다 — 나중에 실제
채널이 생기면 `_notify()` 하나만 그 채널로 바꾸면 된다.

실행 예:
    ./.venv/bin/python -m training.scripts.monthly_retrain_check              # 점검만 (dry-run)
    ./.venv/bin/python -m training.scripts.monthly_retrain_check --execute    # 기준 미달 시 실제 재학습 시도
"""

import argparse
import os
import subprocess
import sys

from core import s3 as s3_io
from ml_core import common_config, profile_contract
from ml_core.paths import (
    ML_ROOT,
    archive_models_prefix,
    model_json_key,
    read_champion_prefix,
)
from ml_core.serving_contract import (
    SERVING_FEATURE_PROFILE_KEYS,
    ServingProfileContractError,
    assert_serving_profiles_compatible,
)

from ..config import unique_archive_date
from ..monitor_performance import _load_baseline_metrics, check_all_models
from ..promotion import promote_challenger, should_promote

SPARK_PYTHON = ML_ROOT / "feature_engine" / ".venv" / "bin" / "python"

_TRAIN_SCRIPTS = {"rental": "training.train_rental_model", "return": "training.train_return_model"}
_EXPLICIT_TRAIN_WINDOW_ENV = ("TRAIN_WINDOW_START", "TRAIN_WINDOW_END")


def _notify(message: str) -> None:
    """학습/승격 진행 상황을 알린다 — 지금은 표준 출력뿐이지만, 나중에 실제
    알림 채널(Slack 등)이 생기면 이 함수만 바꾸면 된다."""
    print(f"[monthly_retrain] {message}", flush=True)


def _monthly_subprocess_env(profile_name: str, env_overrides: dict[str, str]) -> dict[str, str]:
    """월별 재학습용 rolling window 환경을 만든다.

    최초 2025년 챔피언 생성 때 사용한 명시적 `TRAIN_WINDOW_START/END`가 상위
    셸이나 장기 실행 스케줄러에 남아 있어도 월별 자식 프로세스가 상속하면 안 된다.
    두 값을 제거해 `common_config.training_window()`의 최신 rolling 경로를 강제하고,
    나머지 프로필 및 시도별 override는 그대로 전달한다.
    """
    env = dict(os.environ)
    for name in _EXPLICIT_TRAIN_WINDOW_ENV:
        env.pop(name, None)
    rolling_overrides = {
        name: value
        for name, value in env_overrides.items()
        if name not in _EXPLICIT_TRAIN_WINDOW_ENV
    }
    env.update({"ML_PROFILE": profile_name, **rolling_overrides})
    return env


def _print_report(results: list[dict]) -> None:
    print("=== 월별 성능 점검 ===")
    for r in results:
        status = "재학습 필요" if r["needs_retrain"] else "정상"
        print(f"[{r['model_name']}] {status} — {r['period']['start']}~{r['period']['end']} ({r['n_rows']:,}행)")
        print(
            f"    deviance: baseline={r['baseline_deviance']:.4f} 현재={r['current_deviance']:.4f} "
            f"({r['deviance_relative_change']:+.1%})"
        )
        print(
            f"    coverage: baseline={r['baseline_coverage']:.3f} 현재={r['current_coverage']:.3f} "
            f"(drift={r['coverage_drift']:.1%}p)"
        )
        for reason in r["reasons"]:
            print(f"    - {reason}")


def _champion_profile_name(model_name: str) -> str | None:
    """지금 챔피언이 실제로 학습될 때 쓴 프로필 이름 — 그 프로필의 하이퍼파라미터
    (임베고/앵커/LGB 파라미터 등)를 재학습 1차 시도에서 그대로 재사용하기 위함
    (`_candidate_profiles()` 참고). `train_common.train_target()`이 학습 시점에
    저장해두는 `{model_name}_profile.json`(`profile_name` 필드 포함)에서 읽는다.

    챔피언이 아직 없거나(최초 학습 전) 그 기록을 못 찾으면 None — 호출부가
    `common_config.PROFILE_NAME`(이 프로세스의 기본 프로필)으로 대체한다.
    """
    try:
        archive_prefix = read_champion_prefix(model_name)
    except FileNotFoundError:
        return None
    payload = s3_io.read_json(model_json_key(model_name, "profile", archive_prefix))
    if payload is None:
        print(
            f"[monthly_retrain] ERROR: [{model_name}] 챔피언 프로필 기록을 못 찾음({archive_prefix}) "
            "— 현재 프로세스 기본 프로필로 대체",
            file=sys.stderr,
        )
        return None
    return payload["profile_name"]


def _candidate_profiles(model_name: str) -> list[tuple[str, dict[str, str]]]:
    """시도할 (프로필 이름, 이 시도에만 덮어쓸 환경변수) 순서.

    **1차**: 챔피언이 실제로 학습됐던 프로필을 그대로 쓰되, 학습기간만 지금
    프로세스의 기본 롤링 윈도우(`TRAIN_LOOKBACK_MONTHS`, 최신 증분 포함)로 갱신해서
    재시도한다.
    **2차**: 해당 모델 전용 프로필(`{model_name}_*` 형식, 예: `rental_embargo45`).
    **3차**: 내장 기본값(`builtin-default`) 및 일반 공통 프로필.
    단, 타 모델 전용 프로필(예: 대여 모델 시도 시 `return_*`)은 후보에서 제외한다.
    """
    primary = _champion_profile_name(model_name) or common_config.PROFILE_NAME
    other_model = "return" if model_name == "rental" else "rental"

    remote_profiles = common_config.list_profile_names()
    # 타 모델 전용 프로필 제외
    valid_remotes = [p for p in remote_profiles if not p.startswith(f"{other_model}_")]

    # 해당 모델 전용 프로필을 일반 공통 프로필보다 우선 시도
    model_specific = sorted([p for p in valid_remotes if p.startswith(f"{model_name}_")])
    general_remotes = sorted([p for p in valid_remotes if not p.startswith(f"{model_name}_")])

    ordered_names = [
        primary,
        *model_specific,
        common_config.BUILTIN_PROFILE_NAME,
        *general_remotes,
    ]
    unique_names = list(dict.fromkeys(ordered_names))
    refreshed_period = {"TRAIN_LOOKBACK_MONTHS": str(common_config.TRAIN_LOOKBACK_MONTHS)}
    return [
        (name, refreshed_period if index == 0 else {})
        for index, name in enumerate(unique_names)
    ]


def _trigger_feature_pipeline(profile_name: str, env_overrides: dict[str, str]) -> None:
    """feature_engine/spark의 증분 파이프라인 + multi-horizon 테이블 생성을 지정한
    프로필로 Spark 전용 venv(Python 3.11)에서 실행한다.

    rental/return 두 모델이 같은 multi-horizon feature mart(파라미터 조합 하나)를
    같이 쓰므로, 두 모델이 같은 프로필을 시도하면 이 파이프라인이 두 번 실행될 수
    있다 — 워터마크 덕분에 두 번째 실행은 재계산 낭비가 없고, 월 1회 배치라 비용
    문제도 아니라 단순하게 모델별로 각자 실행한다.

    **주의(1단계 한계)**: `build_multi_horizon_features`는 아직 증분(watermark)을 지원하지
    않아 매번 전체를 다시 만든다(feature_engine/spark/build_multi_horizon_features.py
    docstring 참고) — multi-horizon 테이블이 원본의 최대 HORIZON_COUNT배라 이 단계가
    가장 오래 걸리는 부분이 될 수 있다.

    args:
        env_overrides: `ML_PROFILE=profile_name` 위에 이 시도에서만 덮어쓸 환경변수
            (`_candidate_profiles()` 참고 — 챔피언 프로필 재시도에서 학습기간만
            갱신할 때 씀. 빈 dict면 프로필 값 그대로).
    """
    if not SPARK_PYTHON.exists():
        raise RuntimeError(f"{SPARK_PYTHON}가 없습니다 — feature_engine/에서 'uv sync'를 먼저 실행해야 합니다")
    env = _monthly_subprocess_env(profile_name, env_overrides)
    _notify(f"'{profile_name}' 프로필로 feature_engine.spark.run_pipeline 실행 중...")
    subprocess.run([str(SPARK_PYTHON), "-m", "feature_engine.spark.run_pipeline"], cwd=ML_ROOT, check=True, env=env)
    _notify(f"'{profile_name}' 프로필로 feature_engine.spark.build_multi_horizon_features 실행 중...")
    subprocess.run(
        [str(SPARK_PYTHON), "-m", "feature_engine.spark.build_multi_horizon_features"],
        cwd=ML_ROOT,
        check=True,
        env=env,
    )


def _validate_candidate_serving_contract(profile_name: str, env_overrides: dict[str, str]) -> None:
    """후보 프로필이 현재 서빙 계약과 같은지 무거운 작업 전에 검증한다.

    feature/training subprocess는 현재 환경을 상속한 뒤 ``env_overrides``를 마지막에
    덮는다. 여기서도 같은 우선순위를 적용해야 preflight와 실제 학습 프로필이
    어긋나지 않는다. 서빙 계약 키는 모두 분 단위 또는 개수 정수다.

    raises:
        ServingProfileContractError: 후보를 읽거나 해석할 수 없거나 현재 서빙
            피처 계약과 다를 때
    """
    try:
        loaded_profile = common_config._load_profile(profile_name)
        candidate_profile = loaded_profile.copy()
        subprocess_env = _monthly_subprocess_env(profile_name, env_overrides)
        for key in SERVING_FEATURE_PROFILE_KEYS:
            if key == "TRAIN_ANCHOR_TICK_MINUTES":
                continue
            raw_value = subprocess_env.get(key)
            if raw_value is not None:
                candidate_profile[key] = int(raw_value)
        candidate_profile["TRAIN_ANCHOR_TICK_MINUTES"] = common_config._resolved_train_anchor_tick(
            loaded_profile,
            int(candidate_profile["GRID_TICK_MINUTES"]),
            env=subprocess_env,
        )
        profile_contract.validate_model_grid_contract(
            int(candidate_profile["GRID_TICK_MINUTES"]),
            int(candidate_profile["ROLLING_TICK_MINUTES"]),
            int(candidate_profile["TARGET_HORIZON_MINUTES"]),
            f"후보 {profile_name}",
        )
        profile_contract.validate_train_anchor_contract(
            int(candidate_profile["GRID_TICK_MINUTES"]),
            int(candidate_profile["TRAIN_ANCHOR_TICK_MINUTES"]),
            f"후보 {profile_name}",
        )
    # 한 후보의 S3/파싱 실패는 다음 후보로 격리하되 KeyboardInterrupt/SystemExit은
    # Exception 바깥이라 정상적으로 전파한다.
    except Exception as exc:
        raise ServingProfileContractError(
            f"후보 프로필 '{profile_name}'을 서빙 계약으로 해석할 수 없습니다: {exc}"
        ) from exc

    assert_serving_profiles_compatible(
        common_config.effective_profile(),
        candidate_profile,
        expected_source="현재 서빙",
        actual_source=f"후보 프로필 '{profile_name}'",
    )


def _run_training_subprocess(
    model_name: str, profile_name: str, archive_date: str, env_overrides: dict[str, str]
) -> dict:
    """`model_name`을 지정한 프로필로 학습하는 subprocess를 띄우고, 그 결과로
    아카이브에 쓰인 metrics를 다시 읽어 반환한다.

    subprocess의 표준출력을 파싱하지 않는다 — `train_target()`이 학습 과정에서
    다른 진행 로그도 같이 찍기 때문에 정확히 마지막 JSON만 골라내는 게 불안정하다.
    대신 이미 알고 있는 아카이브 경로(`archive_models_prefix()`, 학습 스크립트가
    쓰는 것과 정확히 같은 공식)에서 `metrics.json`을 S3로 직접 다시 읽는다 — 더
    견고하고, 아카이브 자체가 이미 "진실의 원천"이라 자연스럽다.

    args:
        model_name: "rental" 또는 "return"
        profile_name: 이 시도에 쓸 프로필 이름(ML_PROFILE로 subprocess에 전달)
        archive_date: "YYYY-MM-DD-{실행 유니크 접미사}" — 이 시도 전체(feature
            파이프라인 포함)가 공유하는 값. 자정을 넘겨 실행되더라도 아카이브
            경로가 어긋나지 않게, 그리고 같은 날 다시 실행해도 이전 시도의
            archive_prefix와 절대 안 겹치게(`_attempt_promotion()` 참고) 오케스트레이터가
            한 번만 계산해서 넘긴다. `archive_models_prefix()`는 이 문자열의 형식을
            검사하지 않으므로 순수 날짜가 아니어도 문제없다.
        env_overrides: `_trigger_feature_pipeline()` 참고 — 같은 시도 안에서 feature
            파이프라인과 반드시 같은 값을 써야 학습기간이 어긋나지 않는다.
    returns:
        dict: train_target()이 저장한 metrics.json
    raises:
        subprocess.CalledProcessError: 학습 자체가 실패했을 때
        RuntimeError: 학습은 성공했다고 나왔는데 metrics.json을 못 찾았을 때(버그 신호)
    """
    env = _monthly_subprocess_env(profile_name, env_overrides)
    env["MODEL_ARCHIVE_DATE"] = archive_date
    _notify(f"[{model_name}] '{profile_name}' 프로필로 학습 중...")
    subprocess.run([sys.executable, "-m", _TRAIN_SCRIPTS[model_name]], cwd=ML_ROOT, check=True, env=env)

    archive_prefix = archive_models_prefix(archive_date, profile_name)
    metrics = s3_io.read_json(model_json_key(model_name, "metrics", archive_prefix))
    if metrics is None:
        raise RuntimeError(f"[{model_name}] 학습은 끝났는데 metrics를 못 찾음: {archive_prefix}")
    return metrics


def _attempt_promotion(
    model_name: str,
    champion_metrics: dict | None,
    *,
    skip_feature_pipeline: bool = False,
    target_profile: str | None = None,
    archive_date: str | None = None,
) -> bool:
    """후보 프로필을 시도하며 챔피언을 대체할 최적의 챌린저를 선정하여 승격한다.

    1. 완전 기준(Deviance 개선 및 Coverage 정상)을 충족하는 후보 중 Deviance가 가장 낮은 후보로 승격한다.
    2. 완전 기준에 미달하더라도 챔피언보다 Deviance가 우수한 후보가 있다면 그중 최선의 후보로 차선책 승격한다.
    3. 모든 후보가 챔피언보다 열세라면 챔피언을 그대로 유지하고 실패 로그를 남긴다.

    args:
        model_name: "rental" 또는 "return"
        champion_metrics: 현재 챔피언의 metrics.json (없으면 None)
        skip_feature_pipeline: True면 EMR이 이미 피처를 생성했다고 보고 Spark 생략
        target_profile: 특정 프로필만 단독 실행할 때 프로필 이름
        archive_date: 아카이브 날짜 접미사 (None이면 새로 생성)
    returns:
        bool: 승격이 일어났는지
    """
    exec_archive_date = archive_date or unique_archive_date()
    candidates = (
        [(target_profile, {})]
        if target_profile
        else _candidate_profiles(model_name)
    )

    champion_deviance = (
        champion_metrics["poisson_deviance_test"]
        if champion_metrics and "poisson_deviance_test" in champion_metrics
        else float("inf")
    )

    evaluated_candidates: list[dict] = []

    for profile_name, env_overrides in candidates:
        try:
            _validate_candidate_serving_contract(profile_name, env_overrides)
        except ServingProfileContractError as exc:
            _notify(
                f"[{model_name}] '{profile_name}' 후보 건너뜀(현재 서빙 계약과 불일치: {exc}) "
                "— feature 생성/학습을 시작하지 않음"
            )
            continue

        try:
            if not skip_feature_pipeline:
                _trigger_feature_pipeline(profile_name, env_overrides)
            challenger_metrics = _run_training_subprocess(
                model_name, profile_name, exec_archive_date, env_overrides
            )
        except subprocess.CalledProcessError as exc:
            _notify(f"[{model_name}] '{profile_name}' 시도 실패(subprocess 오류: {exc}) — 다음 프로필로 넘어감")
            continue
        except (RuntimeError, OSError, ValueError) as exc:
            _notify(f"[{model_name}] '{profile_name}' 실행 중 오류 발생: {exc}")
            continue

        promote, reasons = should_promote(challenger_metrics, champion_metrics)
        for reason in reasons:
            _notify(f"[{model_name}] '{profile_name}' 판정 — {reason}")

        challenger_deviance = challenger_metrics.get("poisson_deviance_test", float("inf"))
        is_better = challenger_deviance < champion_deviance
        archive_prefix = archive_models_prefix(exec_archive_date, profile_name)

        evaluated_candidates.append(
            {
                "profile_name": profile_name,
                "archive_prefix": archive_prefix,
                "metrics": challenger_metrics,
                "deviance": challenger_deviance,
                "fully_qualified": promote,
                "better_than_champion": is_better,
                "reasons": reasons,
            }
        )

        # 단일 프로필 지정 모드이고 완전 충족 시 바로 승격 시도
        if target_profile and promote:
            break

    if not evaluated_candidates:
        _notify(f"[{model_name}] 실행 가능한 후보 프로필이 없음 — 챔피언 유지")
        return False

    # 1순위: 완전 충족 후보 중 최저 deviance 순서로 시도
    fully_qualified = sorted(
        [c for c in evaluated_candidates if c["fully_qualified"]],
        key=lambda c: c["deviance"],
    )
    for best in fully_qualified:
        try:
            promote_challenger(model_name, best["archive_prefix"])
            _notify(
                f"[{model_name}] '{best['profile_name']}' 완전 승격 기준 충족 "
                f"(deviance={best['deviance']:.4f}) — 챔피언으로 승격 ({best['archive_prefix']})"
            )
            return True
        except ServingProfileContractError as exc:
            _notify(f"[{model_name}] '{best['profile_name']}' 승격 거부(서빙 계약 불일치: {exc}) — 다음 후보 시도")

    # 2순위: 완전 기준(Coverage 등)은 미달했으나 챔피언보다 Deviance가 우수한 후보 중 최저 deviance 순서로 시도
    better_candidates = sorted(
        [c for c in evaluated_candidates if c["better_than_champion"]],
        key=lambda c: c["deviance"],
    )
    for best in better_candidates:
        try:
            promote_challenger(model_name, best["archive_prefix"])
            _notify(
                f"[{model_name}] '{best['profile_name']}' 완전 기준(Coverage 등)에는 미달했으나 "
                f"기존 챔피언보다 성능 우수 (deviance {best['deviance']:.4f} < {champion_deviance:.4f}) "
                f"— 차선책으로 챔피언 교체 ({best['archive_prefix']})"
            )
            return True
        except ServingProfileContractError as exc:
            _notify(f"[{model_name}] '{best['profile_name']}' 승격 거부(서빙 계약 불일치: {exc}) — 다음 후보 시도")

    # 3순위: 챔피언보다 뛰어난 후보가 없음 -> 유지
    best_attempt = min(evaluated_candidates, key=lambda c: c["deviance"])
    _notify(
        f"[{model_name}] 가능한 프로필({len(evaluated_candidates)}개)을 모두 시도했지만 "
        f"챔피언보다 뛰어난 모델이 없음 (최선 deviance={best_attempt['deviance']:.4f} >= 챔피언 {champion_deviance:.4f}) "
        "— 기존 챔피언 유지, 다음 달에 재시도"
    )
    return False


def main() -> list[dict]:
    """월별 성능 점검 및 챌린저 모델 재학습/승격 프로세스를 실행한다."""
    parser = argparse.ArgumentParser(description="매달 챔피언 모델 성능 점검 + (옵션) 챌린저 재학습/승격 시도")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="기준 미달 모델이 있으면 실제로 챌린저 학습을 시도한다 (기본은 리포트만 찍는 dry-run)",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="성능 점검만 수행하고 재학습은 일체 진행하지 않는다",
    )
    parser.add_argument(
        "--skip-feature-pipeline",
        action="store_true",
        help="EMR 등에서 피처마트가 이미 생성되었다고 가정하고 Spark 생성을 건너뛴다",
    )
    parser.add_argument(
        "--profile-name",
        default=None,
        help="특정 프로필 하나만 지정하여 학습/평가를 수행한다",
    )
    parser.add_argument(
        "--models",
        default=None,
        help="쉼표로 구분된 대상 모델 목록 (예: 'rental,return' 또는 'rental')",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="점검 또는 실행 결과를 JSON 문자열로 출력한다",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="기준 날짜(YYYY-MM-DD) override — 기본은 오늘",
    )
    args = parser.parse_args()

    results = check_all_models(as_of=args.as_of)
    if not args.json_output:
        _print_report(results)

    requested_models = (
        [m.strip() for m in args.models.split(",") if m.strip()]
        if args.models
        else None
    )
    relevant_results = (
        [r for r in results if r["model_name"] in requested_models]
        if requested_models
        else results
    )
    retrain_needed = [r for r in relevant_results if r["needs_retrain"]]
    target_models = [r["model_name"] for r in retrain_needed]

    if args.check_only or not args.execute:
        summary = {
            "needs_retrain": len(retrain_needed) > 0,
            "retrain_models": [r["model_name"] for r in retrain_needed],
            "candidate_profiles": list(dict.fromkeys(
                name for r in retrain_needed for name, _ in _candidate_profiles(r["model_name"])
            )) if retrain_needed else [],
            "results": relevant_results,
        }
        if args.json_output:
            import json
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            if not retrain_needed:
                _notify("모든 모델이 기준 이내 — 재학습 필요 없음")
            else:
                _notify(
                    f"기준 미달 모델 {len(retrain_needed)}개 — 실제 재학습은 --execute로 다시 실행하세요 "
                    "(지금은 dry-run/check-only라 아무것도 바꾸지 않았습니다)"
                )
        return relevant_results

    if not target_models:
        _notify("재학습 대상 모델이 없음 — 종료")
        return results

    _notify(f"=== 챌린저 재학습 시도 시작 ({len(target_models)}개 모델: {target_models}) ===")
    for model_name in target_models:
        try:
            champion_metrics = _load_baseline_metrics(model_name)
        except FileNotFoundError:
            champion_metrics = None
        _attempt_promotion(
            model_name,
            champion_metrics,
            skip_feature_pipeline=args.skip_feature_pipeline,
            target_profile=args.profile_name,
        )

    return results


if __name__ == "__main__":
    main()
