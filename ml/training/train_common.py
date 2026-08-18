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
from datetime import date

import lightgbm as lgb
import numpy as np
import pandas as pd
from core import s3 as s3_io
from ml_core import common_config, model_io
from ml_core.day_index import DAY_INDEX_EPOCH, day_index
from ml_core.metrics import pinball_loss as _pinball_loss
from ml_core.metrics import poisson_deviance as _poisson_deviance
from ml_core.model_contract import (
    RENTAL_FEATURE_COLUMNS,
    RETURN_FEATURE_COLUMNS,
    load_station_dtype,
    station_categories_path,
)
from ml_core.paths import archive_models_prefix, model_json_key, model_key

from . import config

__all__ = [
    "RENTAL_FEATURE_COLUMNS",
    "RETURN_FEATURE_COLUMNS",
    "load_station_dtype",
    "load_training_table",
    "run_and_notify_on_failure",
    "station_categories_path",
    "train_target",
]

_FEATURE_COLUMNS_BY_MODEL = {"rental": RENTAL_FEATURE_COLUMNS, "return": RETURN_FEATURE_COLUMNS}
_TARGET_COL_BY_MODEL = {"rental": "rental_count", "return": "return_count"}
_TRAINING_TABLE_BY_MODEL = {
    "rental": config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    "return": config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
}


def run_and_notify_on_failure(label: str, main_fn):
    """`main_fn()`을 실행하고, 실패하면 알아보기 쉬운 한 줄을 표준출력에 남긴 뒤 다시 던진다.

    `train_rental_model.py`/`train_return_model.py`는 `monthly_retrain_check.py`가
    subprocess로 띄운다 — 그 표준출력이 그대로 오케스트레이터 로그에 스트리밍되므로,
    오케스트레이터 쪽은 "다음 프로필로 넘어감" 정도로만 요약해도 실제 실패 사유(예:
    feature mart에 학습 구간 데이터가 아직 없음)가 이 한 줄로 로그에 분명히 남는다 —
    파이썬 기본 traceback만 믿으면 로그를 끝까지 스크롤해야 원인을 알 수 있다.

    args:
        label: 로그에 남길 스크립트 이름(예: "train_rental_model")
        main_fn: 실행할 함수(인자 없음)
    returns:
        main_fn()의 반환값
    """
    try:
        return main_fn()
    except Exception as exc:
        print(f"[{label}] 실패: {exc}", flush=True)
        raise


def load_training_table(model_name: str) -> pd.DataFrame:
    """model_name(대여/반납)에 맞는 multi-horizon feature 테이블에서 학습에 필요한
    컬럼·기간만 읽는다.

    대여/반납은 완전히 분리된 데이터셋이라(서로 상대방의 lag를 안 봄) 읽는 테이블
    경로와 feature 컬럼 목록이 model_name에 따라 다르다.

    `pd.read_parquet(..., columns=[...])`로 필요한 컬럼만 골라 읽는다 — 전체 컬럼을 읽은
    뒤 `df[feature_columns]`로 다시 골라내면 안 쓸 컬럼까지 한 번 더 메모리에 올렸다가
    버리는 이중 보유가 생긴다(history.md 18번 항목, multi-horizon 실험에서 실측된 병목).
    multi-horizon 테이블은 원본 feature 테이블의 최대 HORIZON_COUNT배 행 수라 이 절약이
    특히 중요하다.

    `date_range=(TRAIN_YEAR-01-01, 안전 상한)`도 같이 넘긴다 — 테이블이 `date` 파티션으로
    쌓여있으므로(`feature_engineering/spark/build_multi_horizon_features.py`) 이
    학습 구간에 해당하는 파티션만 나열/다운로드한다. 안 그러면 그동안 쌓인 전체
    히스토리를 매번 다 받은 뒤 `_split()`에서 대부분 버리게 된다 — 쌓인 기간이
    늘어날수록 이 낭비가 계속 커진다. 이 `date_range`는 S3 키 경로(`date=YYYY-MM-DD/`)
    기준 파티션 선택일 뿐이고, `date`는 Spark가 파일 내용엔 안 넣는 파티션 컬럼이라
    (`core.s3._read_parquet_by_date_range()` docstring 참고) `columns=`에 넣으면
    읽을 때마다 그 문자열을 행 수만큼 새로 복제해 만들어야 한다 — `_split()`이 이제
    `day`(이미 BASE_FEATURE_COLUMNS에 있어 어차피 읽음)만으로 경계를 가르므로
    `date`는 아예 요청하지 않는다.

    args:
        model_name: "rental" 또는 "return"
    returns:
        pd.DataFrame: feature_columns + 라벨(rental_count/return_count) +
            (rental이면) rental_exposure — `day`가 feature_columns에 이미 있어
            `_split()`의 경계 판정에 그대로 쓰인다
    """
    feature_columns = _FEATURE_COLUMNS_BY_MODEL[model_name]
    extra = {_TARGET_COL_BY_MODEL[model_name]}
    if model_name == "rental":
        extra.add("rental_exposure")
    needed = sorted(set(feature_columns) | extra)

    table_path = _TRAINING_TABLE_BY_MODEL[model_name]
    train_year_end = f"{config.TRAIN_YEAR}-12-31"
    safe_end = min(train_year_end, config.safety_cutoff_date().isoformat())
    df = s3_io.read_parquet(table_path, columns=needed, date_range=(f"{config.TRAIN_YEAR}-01-01", safe_end))
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {table_path}")
    return df


def _split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """config.TRAIN_YEAR 1년치를 day-of-month 기준으로 train/valid/test로 나눈다
    (랜덤 split 금지 — lag feature 누출 방지).

    매달 `config.VALID_DAYS_OF_MONTH`(기본 3, 20일)는 valid, `config.TEST_DAYS_OF_MONTH`
    (기본 7, 24일)는 test, 나머지는 train — 12개월 전체를 계절성 편중 없이 학습에
    쓰면서도, 같은 날짜가 반복되므로 연중 패턴을 고르게 검증/평가할 수 있다.
    `config.safety_cutoff_date()`를 넘는(아직 라벨이 확정 안 됐을 수 있는) 날짜는
    셋 다에서 제외한다.

    구간을 먼저 확정한 뒤에만 `config.{TRAIN,VALID,TEST}_SAMPLE_FRAC`으로 행을
    표본 추출한다(기본 1.0=표본 없음) — multi-horizon 테이블은 원본의 최대
    HORIZON_COUNT배라 학습 머신 RAM에 안 맞을 수 있다(history.md 18번 항목이 실제로
    겪은 OOM과 같은 종류). 날짜로 먼저 나누고 그 안에서만 무작위 표본을 뽑으므로
    표본 추출 자체가 누출을 만들지는 않는다.

    args:
        df: `day`(2000-01-01 기준 경과일수, ml_core.day_index) 컬럼을 포함한 feature 테이블
    returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (train, valid, test)
    raises:
        ValueError: 세 구간 중 하나라도 행이 0개일 때 — feature mart가 TRAIN_YEAR
            구간까지 아직 안 쌓였으면 조용히 빈 데이터로 학습을 시도하다 lgb.train()
            안에서 알아보기 힘든 에러로 죽을 수 있다 — 여기서 먼저 걸러서 원인을
            바로 알 수 있게 한다(monitor_performance.evaluate_recent_performance()의
            같은 패턴 참고).
    """
    start_day = day_index(date(config.TRAIN_YEAR, 1, 1))
    safe_end = min(date(config.TRAIN_YEAR, 12, 31), config.safety_cutoff_date())
    safe_end_day = day_index(safe_end)
    df = df[(df["day"] >= start_day) & (df["day"] <= safe_end_day)]

    # day(정수)를 실제 달력 날짜로 되돌려 일(day-of-month)만 뽑는다 — day_index는
    # 연 경계를 위해 단조증가하게 설계된 값이라 "그 달의 며칠인지"는 선형식으로
    # 못 구하고 실제 날짜로 복원해야 한다(월마다 일수가 달라서).
    real_dates = pd.Timestamp(DAY_INDEX_EPOCH) + pd.to_timedelta(df["day"], unit="D")
    day_of_month = real_dates.dt.day
    valid_mask = day_of_month.isin(config.VALID_DAYS_OF_MONTH)
    test_mask = day_of_month.isin(config.TEST_DAYS_OF_MONTH)

    train = df[~(valid_mask | test_mask)]
    valid = df[valid_mask]
    test = df[test_mask]

    if train.empty or valid.empty or test.empty:
        raise ValueError(
            f"학습 구간에 데이터가 없음 — train {len(train)}행, valid {len(valid)}행, "
            f"test {len(test)}행 ({config.TRAIN_YEAR}년, ~{safe_end.isoformat()}까지) — "
            "feature mart가 이 구간까지 쌓였는지 확인하세요"
        )

    if config.TRAIN_SAMPLE_FRAC < 1.0:
        train = train.sample(frac=config.TRAIN_SAMPLE_FRAC, random_state=42)
    if config.VALID_SAMPLE_FRAC < 1.0:
        valid = valid.sample(frac=config.VALID_SAMPLE_FRAC, random_state=42)
    if config.TEST_SAMPLE_FRAC < 1.0:
        test = test.sample(frac=config.TEST_SAMPLE_FRAC, random_state=42)
    return train, valid, test


def _prepare_xy(
    df: pd.DataFrame, target_col: str, station_dtype: pd.CategoricalDtype, feature_columns: list[str]
) -> tuple[pd.DataFrame, pd.Series]:
    """feature/label을 분리하고 station_no를 지정된 CategoricalDtype으로 맞춘다.

    args:
        df: feature 테이블의 한 split (train/valid/test 중 하나)
        target_col: "rental_count" 또는 "return_count"
        station_dtype: train/valid/test 전체에서 동일해야 하는 station_no 카테고리
            (split마다 따로 astype("category")하면 LightGBM 카테고리 코드가 어긋남)
        feature_columns: RENTAL_FEATURE_COLUMNS 또는 RETURN_FEATURE_COLUMNS
    returns:
        tuple[pd.DataFrame, pd.Series]: (X, y)
    """
    X = df[feature_columns].copy()
    X["station_no"] = X["station_no"].astype(station_dtype)
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
    """data-parallel 분산 학습용으로 station_no 기준 이 머신의 몫만 남긴다.

    LightGBM의 socket 기반 분산 학습(tree_learner="data"/"voting")은 전체 데이터를
    자동으로 나눠주지 않는다 — 각 머신이 미리 자기 몫만 들고 lgb.train()을 호출해야
    한다. station_no를 머신 수로 나눠 배정하면 날짜 범위는 모든 머신에 동일하게
    유지되면서 station 집합만 갈라져 train/valid split 로직을 그대로 재사용할 수 있다.
    `hash()`는 프로세스마다(PYTHONHASHSEED) 값이 달라 머신마다 다른 배정이 나올 수
    있으므로 대신 `zlib.crc32`로 고정된 배정을 쓴다.
    """
    if config.LGB_NUM_MACHINES <= 1:
        return df
    station_rank = df["station_no"].astype(str).map(lambda s: zlib.crc32(s.encode()) % config.LGB_NUM_MACHINES)
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
    models_prefix: str,
    exposure_col: str | None = None,
) -> dict:
    """대여 또는 반납 하나를 학습한다 — Poisson(+exposure offset) 1개 + quantile(P10/50/90) 3개.

    학습된 booster 4개, station_id 카테고리 목록, conformal correction을 S3의
    models_prefix 아래 저장하고, 2025-12 테스트셋 기준 평가 지표를 반환한다.

    args:
        df: features.build_features()를 거친 전체 feature 테이블
        target_col: "rental_count" 또는 "return_count"
        model_name: 저장 파일명 접두사 ("rental" 또는 "return")
        models_prefix: 저장할 S3 키 prefix — 항상 명시적으로 줘야 한다(기본값 없음).
            학습은 항상 아카이브(`ml_core.paths.archive_models_prefix()`)에 쓰고
            챔피언 자리(`config.MODELS_PREFIX`)에는 절대 직접 안 쓴다 — 챔피언은
            `ml_core.paths.write_champion_pointer()`가 archive_prefix를 가리키는
            포인터만 원자적으로 바꿔서 정해지므로(`training/promotion.py` 참고),
            여기서 `models_prefix`에 `config.MODELS_PREFIX`를 넘기면 그 밑에 아무도
            안 읽는 고아 파일이 생긴다.
        exposure_col: Poisson exposure offset으로 쓸 컬럼명. None이면 offset 없이
            (exposure=1) 학습 — 반납 모델은 항상 None
    returns:
        dict: poisson_deviance_test, rmse_test, best_iteration, pinball_test_q{10,50,90},
            p10_p90_coverage_raw_test, conformal_correction, p10_p90_coverage_calibrated_test
    """
    is_primary = config.LGB_MACHINE_RANK == 0  # 분산 학습 시 최종 평가/아티팩트 저장은 이 머신만 담당
    feature_columns = _FEATURE_COLUMNS_BY_MODEL[model_name]
    train_df, valid_df, test_df = _split(df)

    # station_categories는 샤딩 전 전체 df 기준이어야 한다 — 그래야 카테고리 코드가
    # 모든 머신에서 동일하게 매겨져(station_no -> code) 분산 학습 시 어긋나지 않는다.
    # int()로 감싸는 이유: station_no 컬럼은 model_contract.NATIVE_COLUMN_DTYPES에 따라
    # int16이라 .unique()가 numpy.int16 배열을 반환한다 — 이걸 그대로
    # s3_io.write_json()에 넘기면 json.dumps가 numpy 정수 타입을 직렬화하지 못해 죽는다.
    station_categories = sorted(int(s) for s in df["station_no"].unique())
    station_dtype = pd.CategoricalDtype(categories=station_categories)

    train_df = _shard_for_this_machine(train_df)
    valid_df = _shard_for_this_machine(valid_df)
    print(
        f"[{model_name}] machine={config.LGB_MACHINE_RANK}/{config.LGB_NUM_MACHINES} "
        f"train={len(train_df):,} valid={len(valid_df):,} test={len(test_df):,}"
    )

    X_train, y_train = _prepare_xy(train_df, target_col, station_dtype, feature_columns)
    X_valid, y_valid = _prepare_xy(valid_df, target_col, station_dtype, feature_columns)
    X_test, y_test = _prepare_xy(test_df, target_col, station_dtype, feature_columns)

    # exposure 컬럼은 원본 DataFrame에서 뽑아쓰는 마지막 용도다 — 뽑자마자 바로
    # del해서 X_*/y_*로 이미 변환된 train_df/valid_df/test_df를 이중으로 메모리에
    # 붙들고 있지 않게 한다(아래로는 어디서도 이 세 DataFrame을 참조하지 않음).
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
    del train_df, valid_df, test_df

    if is_primary:
        s3_io.write_json(station_categories_path(model_name, models_prefix), station_categories)

    metrics: dict = {"model_name": model_name}

    # 1) Poisson (+ exposure offset)
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

    # train_set/train_set_q는 X_train/y_train을 쓰는 마지막 Dataset이다 — 지금
    # construct()로 bin 압축을 강제로 끝내두면(각 Dataset이 압축된 자체 사본을
    # 가짐) 그 뒤로는 원본 X_train/y_train이 필요 없으므로 del로 참조를 끊는다.
    # X_valid/X_test는 뒤에서 predict()에 그대로 쓰이므로 지우지 않는다.
    train_set.construct()
    train_set_q.construct()
    del X_train, y_train

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
        # 임베고 등 프로필 값이 바뀌면 이 모델을 서빙할 feature_engine/inference도
        # 같은 프로필을 써야 한다 — 어떤 프로필로 학습됐는지를 모델 파일 옆에 그대로
        # 남겨서, 나중에 이 모델을 찾았을 때 재현/서빙 조건을 바로 알 수 있게 한다
        # (챔피언으로 승격되면 training/promotion.py가 이 archive_prefix를 포인터로
        # 가리키므로, 이 파일도 그 포인터를 통해 그대로 조회된다 — 별도 복사 없음).
        s3_io.write_json(
            model_json_key(model_name, "profile", models_prefix),
            {"profile_name": common_config.PROFILE_NAME, **common_config.PROFILE},
        )

    return metrics


if __name__ == "__main__":
    # 대여/반납은 이제 완전히 분리된 데이터셋이라 테이블을 따로 읽는다 — 실제
    # 운영 엔트리포인트는 train_rental_model.py/train_return_model.py(모델
    # 아카이브 경로/프로필까지 처리)이고, 이 블록은 로컬 ad-hoc 실행용이다.
    # models_prefix에 기본값이 없으므로(챔피언 자리에 직접 못 씀) 여기서도
    # 실제 엔트리포인트와 똑같이 아카이브 경로를 명시적으로 계산해서 넘긴다.
    _archive_prefix = archive_models_prefix(config.today_kst().isoformat(), common_config.PROFILE_NAME)

    rental_df = load_training_table("rental")
    rental_metrics = train_target(rental_df, "rental_count", "rental", _archive_prefix, exposure_col="rental_exposure")
    del rental_df

    return_df = load_training_table("return")
    return_metrics = train_target(return_df, "return_count", "return", _archive_prefix, exposure_col=None)

    print(json.dumps({"rental": rental_metrics, "return": return_metrics}, indent=2, ensure_ascii=False))
