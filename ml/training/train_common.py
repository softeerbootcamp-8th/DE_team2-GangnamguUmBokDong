"""대여/반납 공통 학습 로직: Poisson(+exposure offset) 1개 + quantile(P10/50/90) 3개.

대여 모델은 품절 시간대 censoring을 exposure offset(init_score=log(exposure))으로
보정한다. 반납은 거치대 상태와 무관하게 항상 성공하므로 exposure=1인 순수 Poisson과
동일 — exposure_col=None으로 호출하면 offset 없이 학습한다.

offset 트릭 주의: LightGBM은 init_score를 모델에 저장하지 않는다. 학습 시
label에 대해 eta = init_score + tree(x) 로 적합되지만, predict()가 반환하는 값은
tree(x)의 objective 역변환(Poisson이면 exp(tree(x)))일 뿐 init_score를 포함하지
않는다. 따라서 실제 예측값은 항상 `exposure * booster.predict(X)`로 복원해야 한다.
"""

import json
import zlib

import lightgbm as lgb
import numpy as np
import pandas as pd
from core import s3 as s3_io
from ml_core import model_io
from ml_core.metrics import pinball_loss as _pinball_loss
from ml_core.metrics import poisson_deviance as _poisson_deviance
from ml_core.model_contract import (
    FEATURE_COLUMNS,
    load_station_dtype,
    station_categories_path,
)
from ml_core.paths import model_json_key, model_key

from . import config

__all__ = [
    "FEATURE_COLUMNS",
    "load_station_dtype",
    "load_training_table",
    "station_categories_path",
    "train_target",
]


def load_training_table() -> pd.DataFrame:
    """multi-horizon feature 테이블에서 학습에 필요한 컬럼만 읽는다.

    `pd.read_parquet(..., columns=[...])`로 필요한 컬럼만 골라 읽는다 — 전체 컬럼을 읽은
    뒤 `df[FEATURE_COLUMNS]`로 다시 골라내면 안 쓸 컬럼까지 한 번 더 메모리에 올렸다가
    버리는 이중 보유가 생긴다(history.md 18번 항목, multi-horizon 실험에서 실측된 병목).
    multi-horizon 테이블은 원본 feature 테이블의 최대 HORIZON_COUNT배 행 수라 이 절약이
    특히 중요하다.

    returns:
        pd.DataFrame: FEATURE_COLUMNS + rental_count/return_count(라벨) +
            rental_exposure(대여 exposure offset) + date(`_split()` 경계 기준)
    """
    needed = sorted(set(FEATURE_COLUMNS) | {"rental_count", "return_count", "rental_exposure", "date"})
    df = s3_io.read_parquet(config.MULTI_HORIZON_FEATURES_TABLE_PARQUET, columns=needed)
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {config.MULTI_HORIZON_FEATURES_TABLE_PARQUET}")
    return df


def _split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """config의 TRAIN/VALID/TEST 기간으로 시간 순 split한다 (랜덤 split 금지 — lag feature 누출 방지).

    구간을 먼저 시간 순으로 확정한 뒤에만 `config.{TRAIN,VALID,TEST}_SAMPLE_FRAC`으로
    행을 표본 추출한다(기본 1.0=표본 없음) — multi-horizon 테이블은 원본의 최대
    HORIZON_COUNT배라 학습 머신 RAM에 안 맞을 수 있다(history.md 18번 항목이 실제로
    겪은 OOM과 같은 종류). 시간 경계를 먼저 자르고 그 안에서만 무작위 표본을 뽑으므로
    표본 추출 자체가 누출을 만들지는 않는다.

    args:
        df: date 컬럼(YYYY-MM-DD)을 포함한 feature 테이블
    returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (train, valid, test)
    """
    train = df[(df["date"] >= config.TRAIN_START) & (df["date"] <= config.TRAIN_END)]
    valid = df[(df["date"] >= config.VALID_START) & (df["date"] <= config.VALID_END)]
    test = df[(df["date"] >= config.TEST_START) & (df["date"] <= config.TEST_END)]

    if config.TRAIN_SAMPLE_FRAC < 1.0:
        train = train.sample(frac=config.TRAIN_SAMPLE_FRAC, random_state=42)
    if config.VALID_SAMPLE_FRAC < 1.0:
        valid = valid.sample(frac=config.VALID_SAMPLE_FRAC, random_state=42)
    if config.TEST_SAMPLE_FRAC < 1.0:
        test = test.sample(frac=config.TEST_SAMPLE_FRAC, random_state=42)
    return train, valid, test


def _prepare_xy(
    df: pd.DataFrame, target_col: str, station_dtype: pd.CategoricalDtype
) -> tuple[pd.DataFrame, pd.Series]:
    """feature/label을 분리하고 station_id를 지정된 CategoricalDtype으로 맞춘다.

    args:
        df: feature 테이블의 한 split (train/valid/test 중 하나)
        target_col: "rental_count" 또는 "return_count"
        station_dtype: train/valid/test 전체에서 동일해야 하는 station_id 카테고리
            (split마다 따로 astype("category")하면 LightGBM 카테고리 코드가 어긋남)
    returns:
        tuple[pd.DataFrame, pd.Series]: (X, y)
    """
    X = df[FEATURE_COLUMNS].copy()
    X["station_id"] = X["station_id"].astype(station_dtype)
    y = df[target_col]
    return X, y


def _distributed_params() -> dict:
    """LightGBM Socket 분산 학습 파라미터. config.LGB_TREE_LEARNER="serial"(기본값)이면
    빈 dict를 반환해 지금까지와 동일한 단일 머신 학습으로 동작한다 — 워커 인프라
    (머신 목록)가 준비되기 전까지 기존 동작을 깨지 않기 위함.
    """
    if config.LGB_TREE_LEARNER == "serial":
        return {}
    params = {
        "tree_learner": config.LGB_TREE_LEARNER,
        "num_machines": config.LGB_NUM_MACHINES,
        "local_listen_port": config.LGB_LOCAL_LISTEN_PORT,
        "time_out": config.LGB_TIME_OUT,
    }
    if config.LGB_MACHINES:
        params["machines"] = config.LGB_MACHINES
    return params


def _shard_for_this_machine(df: pd.DataFrame) -> pd.DataFrame:
    """data-parallel 분산 학습용으로 station_id 기준 이 머신의 몫만 남긴다.

    LightGBM의 socket 기반 분산 학습(tree_learner="data"/"voting")은 전체 데이터를
    자동으로 나눠주지 않는다 — 각 머신이 미리 자기 몫만 들고 lgb.train()을 호출해야
    한다. station_id를 머신 수로 나눠 배정하면 날짜 범위는 모든 머신에 동일하게
    유지되면서 station 집합만 갈라져 train/valid split 로직을 그대로 재사용할 수 있다.
    `hash()`는 프로세스마다(PYTHONHASHSEED) 값이 달라 머신마다 다른 배정이 나올 수
    있으므로 대신 `zlib.crc32`로 고정된 배정을 쓴다.
    """
    if config.LGB_NUM_MACHINES <= 1:
        return df
    station_rank = df["station_id"].astype(str).map(lambda s: zlib.crc32(s.encode()) % config.LGB_NUM_MACHINES)
    return df[station_rank == config.LGB_MACHINE_RANK]


def _conformal_correction(y_valid: np.ndarray, lower: np.ndarray, upper: np.ndarray, target_coverage: float) -> float:
    """Split-conformal 보정값 계산 (Romano et al., CQR).

    검증셋에서 conformity score(구간 밖으로 벗어난 정도, 안이면 음수)를 구하고 그
    (1-miscoverage) 분위수를 correction으로 삼는다. 최종 구간은
    [lower - correction, upper + correction] — correction이 음수면 오히려 구간이
    좁아진다 (지금처럼 커버리지가 목표보다 넓을 때).

    args:
        y_valid: 검증셋 실제값
        lower: 검증셋 P10 예측값
        upper: 검증셋 P90 예측값
        target_coverage: 목표 커버리지 (예: 0.80)
    returns:
        float: [lower-correction, upper+correction]에 적용할 보정값
    """
    scores = np.maximum(lower - y_valid, y_valid - upper)
    n = len(scores)
    miscoverage = 1 - target_coverage
    q_level = min(1.0, np.ceil((n + 1) * (1 - miscoverage)) / n)
    return float(np.quantile(scores, q_level))


def train_target(
    df: pd.DataFrame,
    target_col: str,
    model_name: str,
    exposure_col: str | None = None,
    models_prefix: str | None = None,
) -> dict:
    """대여 또는 반납 하나를 학습한다 — Poisson(+exposure offset) 1개 + quantile(P10/50/90) 3개.

    학습된 booster 4개, station_id 카테고리 목록, conformal correction을 S3의
    models_prefix 아래 저장하고, 2025-12 테스트셋 기준 평가 지표를 반환한다.

    args:
        df: features.build_features()를 거친 전체 feature 테이블
        target_col: "rental_count" 또는 "return_count"
        model_name: 저장 파일명 접두사 ("rental" 또는 "return")
        exposure_col: Poisson exposure offset으로 쓸 컬럼명. None이면 offset 없이
            (exposure=1) 학습 — 반납 모델은 항상 None
        models_prefix: 저장할 S3 키 prefix. None이면 챔피언 prefix(config.MODELS_PREFIX)
            — 하이퍼파라미터 스윕 등 실험 실행은 자신만의 prefix를 넘겨서 챔피언
            아티팩트를 덮어쓰지 않는다.
    returns:
        dict: poisson_deviance_test, rmse_test, best_iteration, pinball_test_q{10,50,90},
            p10_p90_coverage_raw_test, conformal_correction, p10_p90_coverage_calibrated_test
    """
    models_prefix = models_prefix or config.MODELS_PREFIX
    is_primary = config.LGB_MACHINE_RANK == 0  # 분산 학습 시 최종 평가/아티팩트 저장은 이 머신만 담당
    train_df, valid_df, test_df = _split(df)

    # station_categories는 샤딩 전 전체 df 기준이어야 한다 — 그래야 카테고리 코드가
    # 모든 머신에서 동일하게 매겨져(station_id -> code) 분산 학습 시 어긋나지 않는다.
    station_categories = sorted(df["station_id"].unique())
    station_dtype = pd.CategoricalDtype(categories=station_categories)

    train_df = _shard_for_this_machine(train_df)
    valid_df = _shard_for_this_machine(valid_df)
    print(
        f"[{model_name}] machine={config.LGB_MACHINE_RANK}/{config.LGB_NUM_MACHINES} "
        f"train={len(train_df):,} valid={len(valid_df):,} test={len(test_df):,}"
    )

    X_train, y_train = _prepare_xy(train_df, target_col, station_dtype)
    X_valid, y_valid = _prepare_xy(valid_df, target_col, station_dtype)
    X_test, y_test = _prepare_xy(test_df, target_col, station_dtype)

    if is_primary:
        s3_io.write_json(station_categories_path(model_name, models_prefix), station_categories)

    metrics: dict = {"model_name": model_name}

    # 1) Poisson (+ exposure offset)
    if exposure_col is not None:
        exposure_train = train_df[exposure_col].to_numpy()
        exposure_valid = valid_df[exposure_col].to_numpy()
        exposure_test = test_df[exposure_col].to_numpy()
        init_train, init_valid = np.log(exposure_train), np.log(exposure_valid)
    else:
        exposure_train = np.ones(len(train_df))
        exposure_valid = np.ones(len(valid_df))
        exposure_test = np.ones(len(test_df))
        init_train = init_valid = None

    train_set = lgb.Dataset(
        X_train, label=y_train, init_score=init_train, categorical_feature=config.CATEGORICAL_FEATURES
    )
    valid_set = lgb.Dataset(
        X_valid, label=y_valid, init_score=init_valid, reference=train_set,
        categorical_feature=config.CATEGORICAL_FEATURES,
    )
    poisson_params = {
        **config.LGB_PARAMS_COMMON, **_distributed_params(), "objective": "poisson", "metric": "poisson",
    }
    booster = lgb.train(
        poisson_params,
        train_set,
        num_boost_round=config.LGB_NUM_BOOST_ROUND,
        valid_sets=[valid_set],
        callbacks=[lgb.early_stopping(config.LGB_EARLY_STOPPING_ROUNDS, verbose=False), lgb.log_evaluation(0)],
    )
    # 분산 학습 시 lgb.train()은 모든 머신이 동시에(같은 파라미터·같은 횟수) 호출해야
    # 소켓 핸드셰이크가 맞아떨어진다 — 그래서 여기서 리턴하지 않고 quantile 학습까지
    # 전 머신이 그대로 진행한다. 다만 boosting이 끝나면 모든 머신의 booster가 동일하므로
    # (매 라운드 gradient를 네트워크로 동기화) 파일 저장은 대표 머신(rank 0)만 하면 되고,
    # 나머지 머신이 같은 경로에 동시에 쓰면 경합이 생길 수 있어 그 부분만 막는다.
    if is_primary:
        model_io.stage_and_upload_booster(booster, model_key(model_name, "poisson", models_prefix))

    mu_test = exposure_test * booster.predict(X_test, num_iteration=booster.best_iteration)
    metrics["poisson_deviance_test"] = _poisson_deviance(y_test.to_numpy(), mu_test)
    metrics["rmse_test"] = float(np.sqrt(np.mean((y_test.to_numpy() - mu_test) ** 2)))
    metrics["best_iteration"] = booster.best_iteration
    print(
        f"  [poisson] best_iter={booster.best_iteration} "
        f"test_deviance={metrics['poisson_deviance_test']:.4f} test_rmse={metrics['rmse_test']:.4f}"
    )

    # 2) Quantile P10/P50/P90 (exposure offset 미적용 — quantile loss는 offset 해석이 표준적이지 않음)
    train_set_q = lgb.Dataset(X_train, label=y_train, categorical_feature=config.CATEGORICAL_FEATURES)
    valid_set_q = lgb.Dataset(
        X_valid, label=y_valid, reference=train_set_q, categorical_feature=config.CATEGORICAL_FEATURES
    )

    quantile_preds_test = {}
    quantile_preds_valid = {}
    for alpha in config.QUANTILE_ALPHAS:
        q_params = {**config.LGB_PARAMS_COMMON, **_distributed_params(), "objective": "quantile", "alpha": alpha}
        q_booster = lgb.train(
            q_params,
            train_set_q,
            num_boost_round=config.LGB_NUM_BOOST_ROUND,
            valid_sets=[valid_set_q],
            callbacks=[
                lgb.early_stopping(config.LGB_EARLY_STOPPING_ROUNDS, verbose=False),
                lgb.log_evaluation(0),
            ],
        )
        if is_primary:
            model_io.stage_and_upload_booster(q_booster, model_key(model_name, f"q{int(alpha * 100)}", models_prefix))
        pred_test = q_booster.predict(X_test, num_iteration=q_booster.best_iteration)
        pred_valid = q_booster.predict(X_valid, num_iteration=q_booster.best_iteration)
        quantile_preds_test[alpha] = pred_test
        quantile_preds_valid[alpha] = pred_valid
        metrics[f"pinball_test_q{int(alpha * 100)}"] = _pinball_loss(y_test.to_numpy(), pred_test, alpha)
        print(f"  [q{int(alpha * 100)}] best_iter={q_booster.best_iteration} pinball={metrics[f'pinball_test_q{int(alpha * 100)}']:.4f}")

    p10_test, p90_test = quantile_preds_test[0.1], quantile_preds_test[0.9]
    raw_coverage = float(np.mean((y_test.to_numpy() >= p10_test) & (y_test.to_numpy() <= p90_test)))
    metrics["p10_p90_coverage_raw_test"] = raw_coverage
    print(f"  [calibration] 보정 전 P10~P90 커버리지 = {raw_coverage:.3f} (이론값 {config.CONFORMAL_TARGET_COVERAGE})")

    # split-conformal 보정: 검증셋에서 목표 커버리지에 맞는 correction을 구해 저장, 테스트셋에 적용해 재평가
    # 주의(분산 학습 시 알려진 근사): y_valid/quantile_preds_valid는 이 머신의 station 샤드
    # 뿐이라, correction이 전체 검증셋이 아니라 rank 0 머신 몫만으로 계산된다 — station을
    # 머신에 무작위로(crc32) 배정하므로 심하게 편향되진 않지만 정확히 전체 검증셋과 같지는
    # 않다. 여러 머신의 conformity score를 모아 합치는 건 LightGBM 소켓 프로토콜 밖의
    # 별도 집계 단계가 필요해 지금은 범위 밖으로 남겨둔다.
    correction = _conformal_correction(
        y_valid.to_numpy(), quantile_preds_valid[0.1], quantile_preds_valid[0.9], config.CONFORMAL_TARGET_COVERAGE
    )
    if is_primary:
        s3_io.write_json(
            model_json_key(model_name, "conformal_correction", models_prefix),
            {"correction": correction, "target_coverage": config.CONFORMAL_TARGET_COVERAGE},
        )

    p10_calibrated = np.clip(p10_test - correction, 0, None)  # count는 음수가 될 수 없음
    p90_calibrated = p90_test + correction
    calibrated_coverage = float(
        np.mean((y_test.to_numpy() >= p10_calibrated) & (y_test.to_numpy() <= p90_calibrated))
    )
    metrics["conformal_correction"] = correction
    metrics["p10_p90_coverage_calibrated_test"] = calibrated_coverage
    print(
        f"  [calibration] correction={correction:.4f} 적용 후 커버리지 = {calibrated_coverage:.3f}"
    )

    # monitor_performance.py가 "이 모델이 학습/검수 시점에 어느 정도였는지"를 알아야
    # 매달 실측 성능과 비교할 baseline을 잡을 수 있다 — 그 baseline을 여기서 남긴다.
    if is_primary:
        s3_io.write_json(model_json_key(model_name, "metrics", models_prefix), metrics)

    return metrics


if __name__ == "__main__":
    df = load_training_table()

    rental_metrics = train_target(df, "rental_count", "rental", exposure_col="rental_exposure")
    return_metrics = train_target(df, "return_count", "return", exposure_col=None)

    print(json.dumps({"rental": rental_metrics, "return": return_metrics}, indent=2, ensure_ascii=False))
