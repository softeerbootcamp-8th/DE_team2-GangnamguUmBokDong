"""저장된 booster로 추론하는 공유 스코어링 로직.

`inference/`(배치 조회 CLI, 단일 시점 예측)와 `training/monitor_performance.py`
(매달 실측 성능 재평가), `training/scripts/compare_baselines.py`(베이스라인 비교)가
전부 "저장된 booster 4개(poisson/q10/q50/q90)로 feature 행을 채점"하는 이 로직을
공유한다 — 서빙 경로와 모니터링/평가 경로가 각자 채점 로직을 따로 구현하면
train-serve skew와 같은 종류의 사고(두 경로가 조용히 다른 값을 냄)가 날 수 있다.

입력 DataFrame은 반드시 `make_dataset`의 `features.build_features()`를 거친
스키마여야 한다(`ml_common.model_contract.FEATURE_COLUMNS` 포함).
"""

import json
from functools import lru_cache

import lightgbm as lgb
import numpy as np
import pandas as pd

from . import metrics
from .model_contract import FEATURE_COLUMNS, load_station_dtype
from .paths import MODELS_DIR

BOOSTER_SUFFIXES = ["poisson", "q10", "q50", "q90"]


@lru_cache(maxsize=None)
def load_boosters(model_name: str) -> dict[str, lgb.Booster]:
    """model_name의 booster 4개(poisson, q10, q50, q90)를 로드한다.

    `lru_cache`로 프로세스당 model_name 하나에 한 번만 디스크에서 읽는다 —
    `predict()`가 배치/단일 조회 어느 경로든 호출마다 이걸 다시 읽고 있어서,
    같은 프로세스에서 반복 호출(예: 여러 정류소×여러 시간대 예측)이 많을 때
    불필요한 파일 I/O가 병목이 됐다. **가정**: 이 프로세스가 살아있는 동안
    챔피언 모델 파일이 안 바뀐다 — 지금 이 함수를 부르는 곳(배치/단일 시점
    예측, 모니터링, 베이스라인 비교) 중 "같은 프로세스 안에서 재학습 후
    바로 다시 채점"하는 코드는 없어서 안전하다. 그런 코드를 나중에 추가한다면
    `load_boosters.cache_clear()`로 캐시를 비울 것.

    args:
        model_name: "rental" 또는 "return"
    returns:
        dict[str, lgb.Booster]: {"poisson": ..., "q10": ..., "q50": ..., "q90": ...}
    """
    return {
        suffix: lgb.Booster(model_file=str(MODELS_DIR / f"{model_name}_{suffix}.txt"))
        for suffix in BOOSTER_SUFFIXES
    }


@lru_cache(maxsize=None)
def load_conformal_correction(model_name: str) -> float:
    """학습 시 저장해둔 split-conformal 보정값을 불러온다.

    `load_boosters()`와 같은 이유로 캐시한다(위 docstring 참고).

    args:
        model_name: "rental" 또는 "return"
    returns:
        float: P10/P90 구간에 적용할 보정값 (training/train_common._conformal_correction 참고)
    """
    path = MODELS_DIR / f"{model_name}_conformal_correction.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)["correction"]


def predict(df: pd.DataFrame, model_name: str, exposure_col: str | None = None) -> pd.DataFrame:
    """station×tick feature 행마다 point(poisson) + quantile(P10/50/90, conformal 보정 적용) 예측.

    args:
        df: make_dataset의 features.build_features()와 동일한 스키마의 DataFrame
            (station_id, date, hour, ml_common.model_contract.FEATURE_COLUMNS 포함)
        model_name: "rental" 또는 "return"
        exposure_col: Poisson exposure 컬럼명. None이면 exposure=1로 간주 (반납 모델)
    returns:
        pd.DataFrame: station_id, date, hour, pred_mean, pred_p10, pred_p50, pred_p90
    """
    station_dtype = load_station_dtype(model_name)
    X = df[FEATURE_COLUMNS].copy()
    X["station_id"] = X["station_id"].astype(station_dtype)

    boosters = load_boosters(model_name)
    correction = load_conformal_correction(model_name)

    exposure = df[exposure_col].to_numpy() if exposure_col is not None else np.ones(len(df))

    pred_mean = exposure * boosters["poisson"].predict(X)
    pred_p10 = np.clip(boosters["q10"].predict(X) - correction, 0, None)
    pred_p50 = np.clip(boosters["q50"].predict(X), 0, None)  # count는 음수가 될 수 없음
    pred_p90 = boosters["q90"].predict(X) + correction

    out = df[["station_id", "date", "hour"]].copy()
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
