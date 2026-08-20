"""저장된 booster로 추론하는 공유 스코어링 로직.

`inference/`(배치 조회 CLI, 단일 시점 예측)와 `training/monitor_performance.py`
(매달 실측 성능 재평가), `training/scripts/compare_baselines.py`(베이스라인 비교)가
전부 "저장된 booster 4개(poisson/q10/q50/q90)로 feature 행을 채점"하는 이 로직을
공유한다 — 서빙 경로와 모니터링/평가 경로가 각자 채점 로직을 따로 구현하면
train-serve skew와 같은 종류의 사고(두 경로가 조용히 다른 값을 냄)가 날 수 있다.

입력 DataFrame은 반드시 `feature_engine`의 `build_features.build_rental_features()`/
`build_return_features()`를 거친 스키마여야 한다(`ml_core.model_contract`의
`RENTAL_FEATURE_COLUMNS`/`RETURN_FEATURE_COLUMNS` 포함).
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import cache
from types import MappingProxyType

import lightgbm as lgb
import numpy as np
import pandas as pd
from core import s3 as s3_io

from . import common_config, metrics, model_io
from .model_contract import (
    RENTAL_FEATURE_COLUMNS,
    RETURN_FEATURE_COLUMNS,
    load_station_dtype,
    station_dtype_from_payload,
)
from .paths import model_json_key, model_key, read_champion_prefix
from .serving_contract import (
    assert_serving_profiles_compatible,
    load_model_profile,
)

BOOSTER_SUFFIXES = ["poisson", "q10", "q50", "q90"]
_FEATURE_COLUMNS_BY_MODEL = {"rental": RENTAL_FEATURE_COLUMNS, "return": RETURN_FEATURE_COLUMNS}


@dataclass(frozen=True, slots=True)
class PinnedScoringModel:
    """한 model snapshot에서 메모리로 로드한 exact scoring artifact다."""

    boosters: Mapping[str, lgb.Booster]
    conformal_correction: float
    station_dtype: pd.CategoricalDtype

    def __post_init__(self) -> None:
        """Booster role 집합, correction과 category dtype을 검증하고 mapping을 고정한다."""
        if not isinstance(self.boosters, Mapping):
            raise TypeError("pinned boosters는 mapping이어야 합니다.")
        values = dict(self.boosters)
        if set(values) != set(BOOSTER_SUFFIXES):
            raise ValueError(
                "pinned boosters는 poisson/q10/q50/q90을 정확히 가져야 합니다."
            )
        object.__setattr__(self, "boosters", MappingProxyType(values))
        correction = self.conformal_correction
        if (
            type(correction) not in {int, float}
            or not math.isfinite(correction)
            or correction < 0
        ):
            raise ValueError(
                "pinned conformal correction은 finite nonnegative number여야 합니다."
            )
        object.__setattr__(self, "conformal_correction", float(correction))
        if type(self.station_dtype) is not pd.CategoricalDtype:
            raise TypeError(
                "pinned station dtype은 exact CategoricalDtype이어야 합니다."
            )


_PINNED_SCORING_MODELS: ContextVar[Mapping[str, PinnedScoringModel] | None] = (
    ContextVar(
        "ml_core_pinned_scoring_models",
        default=None,
    )
)


def build_pinned_scoring_model(
    artifact_payloads: Mapping[str, bytes],
) -> PinnedScoringModel:
    """검증된 model snapshot payload를 legacy pointer 없이 메모리 scorer로 로드한다."""
    if not isinstance(artifact_payloads, Mapping):
        raise TypeError("model artifact payloads는 mapping이어야 합니다.")
    required = {
        "booster_poisson",
        "booster_q10",
        "booster_q50",
        "booster_q90",
        "conformal_correction",
        "station_categories",
    }
    missing = required.difference(artifact_payloads)
    if missing:
        raise ValueError(f"pinned scoring artifact가 누락됐습니다: {sorted(missing)}")
    payloads: dict[str, bytes] = {}
    for role in required:
        payload = artifact_payloads[role]
        if type(payload) is not bytes:
            raise TypeError(f"pinned scoring artifact {role}은 bytes여야 합니다.")
        payloads[role] = payload

    boosters: dict[str, lgb.Booster] = {}
    for suffix in BOOSTER_SUFFIXES:
        role = f"booster_{suffix}"
        try:
            model_text = payloads[role].decode("utf-8")
            boosters[suffix] = lgb.Booster(model_str=model_text)
        except (
            UnicodeDecodeError,
            TypeError,
            ValueError,
            lgb.basic.LightGBMError,
        ) as exc:
            raise ValueError(
                f"pinned LightGBM artifact를 읽을 수 없습니다: {role}"
            ) from exc

    try:
        correction_document = json.loads(payloads["conformal_correction"])
        correction = correction_document["correction"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValueError(
            "pinned conformal_correction JSON을 읽을 수 없습니다."
        ) from exc
    return PinnedScoringModel(
        boosters=boosters,
        conformal_correction=correction,
        station_dtype=station_dtype_from_payload(payloads["station_categories"]),
    )


@contextmanager
def use_pinned_scoring_models(
    models: Mapping[str, PinnedScoringModel],
) -> Iterator[None]:
    """현재 run의 rental/return scorer를 exact pinned snapshot 쌍으로 제한한다."""
    if _PINNED_SCORING_MODELS.get() is not None:
        raise RuntimeError("pinned scoring context는 중첩할 수 없습니다.")
    if not isinstance(models, Mapping) or set(models) != {"rental", "return"}:
        raise ValueError(
            "pinned scoring models는 rental/return을 정확히 가져야 합니다."
        )
    frozen = MappingProxyType(dict(models))
    if any(type(value) is not PinnedScoringModel for value in frozen.values()):
        raise TypeError("pinned scoring model은 exact PinnedScoringModel이어야 합니다.")
    token = _PINNED_SCORING_MODELS.set(frozen)
    try:
        yield
    finally:
        _PINNED_SCORING_MODELS.reset(token)


@cache
def validate_champion_serving_contract(model_name: str) -> dict[str, object]:
    """챔피언 아티팩트와 현재 프로세스의 서빙 피처 계약이 같은지 검증한다.

    검증 결과를 모델별로 캐시해 요청마다 S3의 profile.json을 다시 읽지 않는다.
    챔피언 승격 시 ``training.promotion.promote_challenger()``가 booster/보정값과
    함께 이 캐시도 비운다.

    returns:
        dict[str, object]: 검증된 챔피언의 서빙 피처 계약
    raises:
        ServingProfileContractError: 프로필 아티팩트가 없거나 현재 서빙 설정과
            호환되지 않을 때
    """
    archive_prefix = read_champion_prefix(model_name)
    champion_profile = load_model_profile(model_name, archive_prefix)
    return assert_serving_profiles_compatible(
        common_config.effective_profile(),
        champion_profile,
        expected_source="현재 서빙",
        actual_source=f"{model_name} 챔피언({archive_prefix})",
    )


@cache
def load_boosters(model_name: str) -> dict[str, lgb.Booster]:
    """model_name의 booster 4개(poisson, q10, q50, q90)를 챔피언 archive에서 로드한다.

    `read_champion_prefix()`로 "지금 챔피언이 가리키는 archive_prefix"를 구한 뒤
    거기서 읽는다 — booster를 챔피언 자리로 따로 복사해두지 않는다(그 이유는
    `read_champion_prefix()` docstring 참고: 파일 여러 개를 복사하면 승격 도중
    inference가 신/구 버전을 섞어 읽을 수 있어서, archive를 immutable하게 두고
    포인터만 원자적으로 바꾸는 방식으로 바꿨다).

    `@cache`로 프로세스당 model_name 하나에 한 번만 S3에서 읽는다 —
    `predict()`가 배치/단일 조회 어느 경로든 호출마다 이걸 다시 읽고 있어서,
    같은 프로세스에서 반복 호출(예: 여러 정류소×여러 시간대 예측)이 많을 때
    불필요한 S3 GET이 병목이 됐다. **가정**: 이 프로세스가 살아있는 동안
    챔피언이 안 바뀐다 — 지금 이 함수를 부르는 곳(배치/단일 시점 예측,
    모니터링) 중 "같은 프로세스 안에서 재학습 후 바로 다시 채점"하는 코드는
    없어서 안전하다. `read_champion_prefix()`도 같은 프로세스 안에서 이
    함수·`load_conformal_correction()`·`load_station_dtype()`이 전부 같은
    archive_prefix를 보도록 캐시를 공유한다(그 함수 docstring 참고). 그런
    코드를 나중에 추가한다면 `load_boosters.cache_clear()`로 캐시를 비울 것.

    args:
        model_name: "rental" 또는 "return"
    returns:
        dict[str, lgb.Booster]: {"poisson": ..., "q10": ..., "q50": ..., "q90": ...}
    """
    archive_prefix = read_champion_prefix(model_name)
    validate_champion_serving_contract(model_name)
    return {
        suffix: model_io.download_and_load_booster(model_key(model_name, suffix, archive_prefix))
        for suffix in BOOSTER_SUFFIXES
    }


@cache
def load_conformal_correction(model_name: str) -> float:
    """학습 시 저장해둔 split-conformal 보정값을 챔피언 archive에서 불러온다.

    `load_boosters()`와 같은 이유로 `read_champion_prefix()`를 거치고, 같은
    이유로 캐시한다(위 docstring 참고).

    args:
        model_name: "rental" 또는 "return"
    returns:
        float: P10/P90 구간에 적용할 보정값 (training/train_common._conformal_correction 참고)
    """
    archive_prefix = read_champion_prefix(model_name)
    key = model_json_key(model_name, "conformal_correction", archive_prefix)
    data = s3_io.read_json(key)
    if data is None:
        raise FileNotFoundError(f"conformal_correction 없음: {key}")
    return data["correction"]


def predict(df: pd.DataFrame, model_name: str, exposure_col: str | None = None) -> pd.DataFrame:
    """station×tick feature 행마다 point(poisson) + quantile(P10/50/90, conformal 보정 적용) 예측.

    args:
        df: feature_engine의 build_features.build_rental_features()/build_return_features()와
            동일한 스키마의 DataFrame (station_no, date, hour + model_name에 맞는
            RENTAL_FEATURE_COLUMNS/RETURN_FEATURE_COLUMNS 포함) — feature_engine의
            multi-horizon 테이블엔 station_id(텍스트)가 아예 없다(용량 절감,
            build_multi_horizon_features.py 모듈 docstring 참고) — 사람이 보는
            station_id가 필요한 호출부는 station_master로 직접 join해서 붙일 것
            (`inference/predict_common.py` 참고)
        model_name: "rental" 또는 "return"
        exposure_col: Poisson exposure 컬럼명. None이면 exposure=1로 간주 (반납 모델)
    returns:
        pd.DataFrame: station_no, date, hour, pred_mean, pred_p10, pred_p50, pred_p90
    """
    pinned_models = _PINNED_SCORING_MODELS.get()
    pinned = None if pinned_models is None else pinned_models.get(model_name)
    if pinned_models is not None and pinned is None:
        raise ValueError(f"pinned scoring context에 model이 없습니다: {model_name}")

    if pinned is None:
        # Legacy offline/monitor 경로는 기존 champion profile 검증과 lazy load를
        # 유지한다. Authority 경로는 이미 pinned release 계약을 검증했으므로 이
        # branch에 들어오지 않아 run 도중 mutable champion pointer를 다시 읽지 않는다.
        validate_champion_serving_contract(model_name)
        station_dtype = load_station_dtype(model_name)
        boosters = load_boosters(model_name)
        correction = load_conformal_correction(model_name)
    else:
        station_dtype = pinned.station_dtype
        boosters = pinned.boosters
        correction = pinned.conformal_correction

    X = df[_FEATURE_COLUMNS_BY_MODEL[model_name]].copy()
    X["station_no"] = X["station_no"].astype(station_dtype)

    exposure = df[exposure_col].to_numpy() if exposure_col is not None else np.ones(len(df))

    pred_mean = exposure * boosters["poisson"].predict(X)
    pred_p10 = np.clip(boosters["q10"].predict(X) - correction, 0, None)
    pred_p50 = np.clip(boosters["q50"].predict(X), 0, None)  # count는 음수가 될 수 없음
    pred_p90 = boosters["q90"].predict(X) + correction

    out = df[["station_no", "date", "hour"]].copy()
    out["pred_mean"] = pred_mean
    out["pred_p10"] = pred_p10
    out["pred_p50"] = pred_p50
    out["pred_p90"] = pred_p90
    return out


def print_metrics(preds: pd.DataFrame) -> None:
    """예측 결과 DataFrame(actual 컬럼 포함)으로 평가 지표를 계산해 출력한다.

    args:
        preds: predict()의 결과에 "actual" 컬럼을 추가한 DataFrame
    """
    y = preds["actual"].to_numpy()
    deviance = metrics.poisson_deviance(y, preds["pred_mean"].to_numpy())
    rmse = float(np.sqrt(np.mean((y - preds["pred_mean"].to_numpy()) ** 2)))
    pinball10 = metrics.pinball_loss(y, preds["pred_p10"].to_numpy(), 0.1)
    pinball90 = metrics.pinball_loss(y, preds["pred_p90"].to_numpy(), 0.9)
    coverage = float(np.mean((y >= preds["pred_p10"]) & (y <= preds["pred_p90"])))
    print(f"[검증] poisson_deviance={deviance:.4f} rmse={rmse:.4f}")
    print(f"[검증] pinball_p10={pinball10:.4f} pinball_p90={pinball90:.4f}")
    print(f"[검증] P10~P90 커버리지={coverage:.3f}")
