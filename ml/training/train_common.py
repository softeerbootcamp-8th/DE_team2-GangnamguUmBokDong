"""대여/반납 공통 학습 로직: Poisson(+exposure offset) 1개 + quantile(P10/50/90) 3개.

대여 모델은 품절 시간대 censoring을 exposure offset(init_score=log(exposure))으로
보정한다. 반납은 거치대 상태와 무관하게 항상 성공하므로 exposure=1인 순수 Poisson과
동일 — exposure_col=None으로 호출하면 offset 없이 학습한다.

offset 트릭 주의: LightGBM은 init_score를 모델에 저장하지 않는다. 학습 시
label에 대해 eta = init_score + tree(x) 로 적합되지만, predict()가 반환하는 값은
tree(x)의 objective 역변환(Poisson이면 exp(tree(x)))일 뿐 init_score를 포함하지
않는다. 따라서 실제 예측값은 항상 `exposure * booster.predict(X)`로 복원해야 한다.
"""

import contextlib
import json
import resource
import sys
import time
import zlib
from datetime import UTC, date, datetime

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from core import s3 as s3_io
from ml_core import common_config, mlflow_tracking, model_io
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


def _validate_valid_test_days_dont_overlap_train() -> None:
    """VALID_DAYS_OF_MONTH/TEST_DAYS_OF_MONTH에 TRAIN_DAY_DIVISOR의 배수가 섞여 있으면 바로 에러를 낸다.

    train이 "TRAIN_DAY_DIVISOR의 배수인 날 전부"라(`_split()` 참고), 여기 그 배수가
    하나라도 섞이면 그 날짜가 train과 valid/test 양쪽에 동시에 들어가는 누출이
    생긴다. `load_training_table()`(읽기 단계에서 날짜를 미리 거름)과 `_split()`
    (읽은 뒤 다시 확인) 둘 다 같은 검증을 쓴다.
    """
    conflicting = {
        d for d in (*config.VALID_DAYS_OF_MONTH, *config.TEST_DAYS_OF_MONTH) if d % config.TRAIN_DAY_DIVISOR == 0
    }
    if conflicting:
        raise ValueError(
            f"VALID_DAYS_OF_MONTH/TEST_DAYS_OF_MONTH에 TRAIN_DAY_DIVISOR({config.TRAIN_DAY_DIVISOR})의 "
            f"배수가 섞여 있음(train과 겹쳐 누출): {sorted(conflicting)}"
        )


def _wanted_dates(start: date, end: date) -> list[str]:
    """TRAIN_DAY_DIVISOR의 배수인 날 전부 + VALID/TEST_DAYS_OF_MONTH에 해당하는 날짜만 "YYYY-MM-DD"로 나열한다.

    `_split()`이 어차피 이 날짜들만 남기고 나머지(valid/test로도 안 뽑힌 날)는
    버리므로, 그 필터링을 읽기 전에 미리 해서 애초에 안 쓸 파티션을 S3에서
    받아오지도 않게 한다 — multi-horizon 테이블이 2025년 전체 기준 8억 행이라
    (2026-08 실측) 다 읽은 뒤에 걸러서는 이미 늦다(그 시점에 이미 OOM).
    """
    wanted_days_of_month = config.VALID_DAYS_OF_MONTH | config.TEST_DAYS_OF_MONTH
    return [
        d.strftime("%Y-%m-%d")
        for d in pd.date_range(start, end, freq="D")
        if d.day % config.TRAIN_DAY_DIVISOR == 0 or d.day in wanted_days_of_month
    ]


def _peak_rss_mb() -> float:
    """이 프로세스가 시작한 이후 지금까지의 최고 RSS(MB).

    `resource.getrusage(RUSAGE_SELF).ru_maxrss`는 매 호출 시점의 순간 RSS가 아니라
    그때까지 관측된 최댓값을 계속 누적해서 돌려주므로, 그냥 주기적으로 이 값만
    읽어도 "지금까지 RAM을 최대 얼마나 썼는지"를 알 수 있다 — 별도로 폴링
    스레드를 띄워 직접 최댓값을 추적할 필요가 없다. 단위가 플랫폼마다 달라서
    (Linux는 KB, macOS는 바이트) 나눠주는 값도 다르게 맞춘다.
    """
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return max_rss / (1024 * 1024 if sys.platform == "darwin" else 1024)


def _append_progress_log(message: str) -> None:
    """`config.TRAIN_PROGRESS_LOG_PATH`에 타임스탬프를 붙여 한 줄 이어쓴다.

    표준출력(`print(..., flush=True)`)과는 별도 채널이다 — 표준출력이 파이프/
    리다이렉트로 버퍼링되거나 다른 로그와 섞여도, 이 파일만 tail하면 오래 걸리는
    로드가 실제로 진행 중인지 바로 확인할 수 있다(2026-08, 8시간 넘게 걸린 로드를
    `ps`의 경과시간을 수동으로 다시 확인하고서야 스와핑 중이었음을 알아챈 사건 이후
    도입 — training/config.py의 TRAIN_PROGRESS_LOG_PATH 참고). 매 호출마다 열고
    닫아 즉시 디스크에 반영한다 — 프로세스가 이후 강제 종료돼도 이미 쓰인 줄은 남는다.
    """
    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
    with open(config.TRAIN_PROGRESS_LOG_PATH, "a") as f:
        f.write(f"{timestamp} {message}\n")


def _progress_on_complete(label: str):
    """`core.s3.read_parquet(..., on_complete=...)`에 넘길, 시간 기준으로 스로틀된 콜백을 만든다.

    파일이 수백~수천 개인 대량 로드에서 완료될 때마다 그대로 로깅하면 로그
    자체가 병목/노이즈가 되므로, 마지막 로깅 이후 `config.
    TRAIN_PROGRESS_LOG_INTERVAL_SECONDS`가 지났을 때만(단, 마지막 파일은 무조건)
    진행 개수와 그 시점의 peak RSS를 남긴다.
    """
    last_logged_at = 0.0

    def _on_complete(done: int, total: int) -> None:
        nonlocal last_logged_at
        now = time.monotonic()
        if done < total and now - last_logged_at < config.TRAIN_PROGRESS_LOG_INTERVAL_SECONDS:
            return
        last_logged_at = now
        _append_progress_log(f"[{label}] {done}/{total}개 파일 완료, peak_rss={_peak_rss_mb():.0f}MB")

    return _on_complete


def load_training_table(model_name: str) -> pd.DataFrame:
    """model_name(대여/반납)에 맞는 multi-horizon feature 테이블에서 학습에 필요한
    컬럼·날짜만 읽는다.

    대여/반납은 완전히 분리된 데이터셋이라(서로 상대방의 lag를 안 봄) 읽는 테이블
    경로와 feature 컬럼 목록이 model_name에 따라 다르다.

    `pd.read_parquet(..., columns=[...])`로 필요한 컬럼만 골라 읽는다 — 전체 컬럼을 읽은
    뒤 `df[feature_columns]`로 다시 골라내면 안 쓸 컬럼까지 한 번 더 메모리에 올렸다가
    버리는 이중 보유가 생긴다(history.md 18번 항목, multi-horizon 실험에서 실측된 병목).
    multi-horizon 테이블은 원본 feature 테이블의 최대 HORIZON_COUNT배 행 수라 이 절약이
    특히 중요하다.

    `dates=_wanted_dates(...)`도 같이 넘긴다 — 테이블이 `date` 파티션으로 쌓여있으므로
    (`feature_engineering/spark/build_multi_horizon_features.py`) `_split()`이 결국
    남길 날짜(TRAIN_DAY_DIVISOR의 배수 전부 + VALID/TEST_DAYS_OF_MONTH)만 미리 골라서
    나열/다운로드한다
    (2026-08부터 — 8억 행짜리 2025년 전체를 date_range로 통째로 받으면 `_split()`에서
    거르기도 전에 로드 자체가 OOM으로 죽는다, `training/config.py` 참고). `date`
    컬럼 자체는 Spark가 파일 내용엔 안 넣는 파티션 컬럼이라(`core.s3._read_parquet_by_dates()`
    docstring 참고) `columns=`에 넣지 않는다 — `_split()`이 `day`(이미
    BASE_FEATURE_COLUMNS에 있어 어차피 읽음)만으로 경계를 가른다.

    args:
        model_name: "rental" 또는 "return"
    returns:
        pd.DataFrame: feature_columns + 라벨(rental_count/return_count) +
            (rental이면) rental_exposure — `day`가 feature_columns에 이미 있어
            `_split()`의 경계 판정에 그대로 쓰인다
    """
    _validate_valid_test_days_dont_overlap_train()
    feature_columns = _FEATURE_COLUMNS_BY_MODEL[model_name]
    extra = {_TARGET_COL_BY_MODEL[model_name]}
    if model_name == "rental":
        extra.add("rental_exposure")
    needed = sorted(set(feature_columns) | extra)

    table_path = _TRAINING_TABLE_BY_MODEL[model_name]
    safe_end = min(date(config.TRAIN_YEAR, 12, 31), config.safety_cutoff_date())
    wanted = _wanted_dates(date(config.TRAIN_YEAR, 1, 1), safe_end)
    _append_progress_log(f"[{model_name}] 로드 시작 — 날짜 {len(wanted)}개, max_horizon={config.MAX_TRAIN_HORIZON}")
    # horizon도 같은 날짜 파티션 안에 1..HORIZON_COUNT가 전부 섞여 있어 날짜
    # 필터만으론 못 줄인다 — config.MAX_TRAIN_HORIZON(기본값 = 제한 없음)로 읽는
    # 시점에 한 번 더 걸러서 그 배율 자체를 줄인다(training/config.py 참고).
    df = s3_io.read_parquet(
        table_path,
        columns=needed,
        dates=wanted,
        filters=[("horizon", "<=", config.MAX_TRAIN_HORIZON)],
        on_complete=_progress_on_complete(model_name),
    )
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {table_path}")
    _append_progress_log(f"[{model_name}] 로드 완료 — {len(df):,}행, peak_rss={_peak_rss_mb():.0f}MB")
    return df


def _split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """config.TRAIN_YEAR 1년치를 day-of-month 배수 기준으로 train/valid/test로 나눈다
    (랜덤 split 금지 — lag feature 누출 방지).

    **train은 `config.TRAIN_DAY_DIVISOR`의 배수인 날 전부**(기본 2=짝수날), valid/test는
    `config.VALID_DAYS_OF_MONTH`(기본 11, 13일)/`config.TEST_DAYS_OF_MONTH`(기본 17,
    19일) — TRAIN_DAY_DIVISOR의 배수가 아닌 날짜만 지정해야 한다(training/config.py
    참고, 겹치면 train과 겹쳐 누출이 생긴다). valid/test로도 안 뽑힌 나머지 날짜
    (대부분)는 셋 어디에도 안 들어가고 버려진다 — multi-horizon 테이블이 2025년
    전체로는 8억 행이라(2026-08 실측, `TRAIN_SAMPLE_FRAC` 같은 행 단위 표본
    추출과 별개로) 애초에 읽어들이는 총 행 수 자체를 줄이는 용도다.
    `config.safety_cutoff_date()`를 넘는(아직 라벨이 확정 안 됐을 수 있는) 날짜는
    셋 다에서 제외한다.

    구간을 먼저 확정한 뒤에만 `config.{TRAIN,VALID,TEST}_SAMPLE_FRAC`으로 행을
    한 번 더 표본 추출한다(기본 1.0=표본 없음). 날짜로 먼저 나누고 그 안에서만
    무작위 표본을 뽑으므로 표본 추출 자체가 누출을 만들지는 않는다.

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
    _validate_valid_test_days_dont_overlap_train()

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
    train_mask = day_of_month % config.TRAIN_DAY_DIVISOR == 0

    train = df[train_mask]
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
    _append_progress_log(f"[{model_name}] split/학습 시작 — {len(df):,}행, peak_rss={_peak_rss_mb():.0f}MB")
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

    # 분산 학습 워커(rank>0)는 run을 안 연다 — 대표 머신(rank 0) 하나만 기록해야
    # 실험 하나당 run 하나가 되고, 다른 서버(EMR 등)에서 도는 워커가 tracking
    # server에 접근 못 해도 학습 자체엔 영향이 없다. run_name은 archive_prefix(날짜+
    # 프로필)가 아니라 실제로 실험마다 바뀌는 divisor/horizon을 담는다 — 같은 날
    # 같은 프로필로 여러 조합을 재시도한 이력(2026-08 OOM 대응 폴백 체인)이 전부
    # archive_prefix는 같아서(S3 모델 파일이 서로 덮어써짐) 그것만으로는 MLflow UI
    # 목록에서 어느 시도가 어느 조합이었는지 구분이 안 됐다.
    #
    # `with mlflow.start_run(...)`로 여는 이유: 예전엔 성공 경로 끝에서만
    # mlflow.end_run()을 불렀다 — lgb.train()/S3 업로드/평가 도중 예외가 나면 run이
    # RUNNING 상태로 영구 방치되고, 같은 프로세스가 다음 모델(반납 등)을 학습할 때
    # 활성 run과 충돌할 수 있었다. `with` 블록은 예외가 나도 자동으로 FAILED 처리하고
    # 종료한다. 분산 학습 워커(rank>0)는 run 자체를 안 열므로(위 이유) 진짜
    # context manager 대신 `contextlib.nullcontext()`를 써서 이 블록 구조를 공유한다.
    if is_primary:
        mlflow_tracking.configure(config.MLFLOW_EXPERIMENT_NAME)
    run_name = f"{model_name}_{common_config.PROFILE_NAME}_d{config.TRAIN_DAY_DIVISOR}_h{config.MAX_TRAIN_HORIZON}"
    mlflow_run = mlflow.start_run(run_name=run_name) if is_primary else contextlib.nullcontext()

    with mlflow_run:
        if is_primary:
            mlflow.log_params({
                "model_name": model_name,
                "train_year": config.TRAIN_YEAR,
                "train_day_divisor": config.TRAIN_DAY_DIVISOR,
                "max_train_horizon": config.MAX_TRAIN_HORIZON,
                "valid_days_of_month": sorted(config.VALID_DAYS_OF_MONTH),
                "test_days_of_month": sorted(config.TEST_DAYS_OF_MONTH),
                "train_sample_frac": config.TRAIN_SAMPLE_FRAC,
                "valid_sample_frac": config.VALID_SAMPLE_FRAC,
                "test_sample_frac": config.TEST_SAMPLE_FRAC,
                "profile_name": common_config.PROFILE_NAME,
                "models_prefix": models_prefix,
                "train_rows": len(train_df),
                "valid_rows": len(valid_df),
                "test_rows": len(test_df),
                "feature_columns": ",".join(feature_columns),
                "lgb_num_boost_round": config.LGB_NUM_BOOST_ROUND,
                "lgb_early_stopping_rounds": config.LGB_EARLY_STOPPING_ROUNDS,
                **config.LGB_PARAMS_COMMON,
            })

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
            mlflow.log_dict(station_categories, "station_categories.json")

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
            model_io.stage_and_upload_booster(
                booster, model_key(model_name, "poisson", models_prefix), log_to_mlflow=True
            )

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
                model_io.stage_and_upload_booster(
                    q_booster, model_key(model_name, f"q{int(alpha * 100)}", models_prefix), log_to_mlflow=True
                )
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
            correction_payload = {"correction": correction, "target_coverage": config.CONFORMAL_TARGET_COVERAGE}
            s3_io.write_json(model_json_key(model_name, "conformal_correction", models_prefix), correction_payload)
            mlflow.log_dict(correction_payload, "conformal_correction.json")

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
            profile_payload = {"profile_name": common_config.PROFILE_NAME, **common_config.PROFILE}
            s3_io.write_json(model_json_key(model_name, "profile", models_prefix), profile_payload)
            mlflow.log_dict(profile_payload, "profile.json")
            # metrics의 "model_name"은 문자열이라 mlflow.log_metrics(숫자만 허용)에서 뺀다.
            mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, int | float)})

    _append_progress_log(f"[{model_name}] 학습 완료, peak_rss={_peak_rss_mb():.0f}MB")
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
