"""대여/반납 공통 학습 로직: Poisson(+exposure offset) 1개 + quantile(P10/50/90) 3개.

대여 모델은 품절 시간대 censoring을 exposure offset(init_score=log(exposure))으로
보정한다. 반납은 거치대 상태와 무관하게 항상 성공하므로 exposure=1인 순수 Poisson과
동일 — exposure_col=None으로 호출하면 offset 없이 학습한다.

offset 트릭 주의: LightGBM은 init_score를 모델에 저장하지 않는다. 학습 시
label에 대해 eta = init_score + tree(x) 로 적합되지만, predict()가 반환하는 값은
tree(x)의 objective 역변환(Poisson이면 exp(tree(x)))일 뿐 init_score를 포함하지
않는다. 따라서 실제 예측값은 항상 `exposure * booster.predict(X)`로 복원해야 한다.

**데이터 로딩(2026-08 전면 개편)**: train/valid/test 전체를 pandas DataFrame으로
한 번에 읽지 않는다 — `lazy_train_dataset.py`가 날짜 파티션 단위로 S3를 지연
조회한다(왜 필요한지는 그 모듈 docstring 참고, 요약하면 8억 행짜리 DataFrame을
통째로 읽으면 로컬 RAM에서 OOM이 났고 앵커/horizon을 줄이지 않기로 확정했으므로
이 방법뿐이었다). 이 파일은 그 위에서 poisson/quantile 학습·평가·저장만 담당한다.
"""

import contextlib
import json
import resource
import sys
import time
from datetime import UTC, date, datetime

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from core import s3 as s3_io
from ml_core import common_config, mlflow_tracking, model_io
from ml_core.metrics import pinball_loss as _pinball_loss
from ml_core.metrics import poisson_deviance as _poisson_deviance
from ml_core.model_contract import (
    RENTAL_FEATURE_COLUMNS,
    RETURN_FEATURE_COLUMNS,
    load_station_dtype,
    station_categories_path,
)
from ml_core.paths import archive_models_prefix, model_json_key, model_key

from . import config, lazy_train_dataset

__all__ = [
    "RENTAL_FEATURE_COLUMNS",
    "RETURN_FEATURE_COLUMNS",
    "load_station_dtype",
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

    **TRAIN_DAY_DIVISOR가 1(기본값, 날짜 다운샘플링 없음 — 1년 전체)이면 이
    검증을 건너뛴다** — 모든 정수가 1의 배수라 이 규칙 그대로 적용하면 항상
    "충돌"로 판정되지만, `_dates_for_split()`가 valid/test를 먼저 확정하고 그
    나머지 중에서만 train 배수 조건을 보므로(아래 참고) divisor=1에서도 애초에
    겹칠 수가 없다 — 이 함수가 막으려는 사고 자체가 구조적으로 불가능해졌다.

    divisor>=2일 때는: train이 "TRAIN_DAY_DIVISOR의 배수인 날 중 valid/test가
    아닌 날"이라, VALID/TEST_DAYS_OF_MONTH에 그 배수가 섞여 있으면 그 날짜가
    온전히 valid/test로만 가야 할 의도와 다르게 뒤섞일 수 있어 미리 막는다.
    """
    if config.TRAIN_DAY_DIVISOR == 1:
        return
    conflicting = {
        d for d in (*config.VALID_DAYS_OF_MONTH, *config.TEST_DAYS_OF_MONTH) if d % config.TRAIN_DAY_DIVISOR == 0
    }
    if conflicting:
        raise ValueError(
            f"VALID_DAYS_OF_MONTH/TEST_DAYS_OF_MONTH에 TRAIN_DAY_DIVISOR({config.TRAIN_DAY_DIVISOR})의 "
            f"배수가 섞여 있음(train과 겹쳐 누출): {sorted(conflicting)}"
        )


def _dates_for_split(start: date, end: date) -> tuple[list[str], list[str], list[str]]:
    """(train_dates, valid_dates, test_dates) — 전부 캘린더 연산만, I/O 없음.

    예전 `_split()`은 전체 df를 로드한 뒤 `day` 컬럼을 역산해 day-of-month를 구해
    나눴다. Spark가 이미 `date=YYYY-MM-DD/` Hive 파티션으로 저장해두었으므로
    **날짜 문자열 자체에서 day-of-month를 바로 뽑을 수 있어 데이터를 읽을 필요가
    전혀 없다** — `lazy_train_dataset`이 이 함수가 정한 날짜만 골라 S3를 조회한다.

    valid/test를 먼저 확정하고 그 나머지 중에서만 `TRAIN_DAY_DIVISOR` 배수 조건을
    보므로(`elif`), divisor=1(모든 날이 "배수")이어도 valid/test 날짜가 train으로
    새지 않는다.
    """
    train, valid, test = [], [], []
    for d in pd.date_range(start, end, freq="D"):
        date_str = d.strftime("%Y-%m-%d")
        if d.day in config.VALID_DAYS_OF_MONTH:
            valid.append(date_str)
        elif d.day in config.TEST_DAYS_OF_MONTH:
            test.append(date_str)
        elif d.day % config.TRAIN_DAY_DIVISOR == 0:
            train.append(date_str)
    return train, valid, test


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


def _chunk_progress(model_name: str, label: str):
    """`lazy_train_dataset`의 `on_chunk_loaded` 콜백 — 날짜 청크 하나가 로드될 때마다 남긴다.

    날짜 단위라 파일 단위인 `_progress_on_complete()`보다 호출 빈도가 낮아 스로틀
    없이 매번 남겨도 로그가 과하지 않다(1년 기준 최대 366줄/label).
    """

    def _on_chunk_loaded(date_str: str, row_count: int) -> None:
        _append_progress_log(f"[{model_name}][{label}] {date_str} 적재 완료({row_count:,}행), peak_rss={_peak_rss_mb():.0f}MB")

    return _on_chunk_loaded


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
    target_col: str,
    model_name: str,
    models_prefix: str,
    exposure_col: str | None = None,
) -> dict:
    """대여 또는 반납 하나를 학습한다 — Poisson(+exposure offset) 1개 + quantile(P10/50/90) 3개.

    학습된 booster 4개, station_id 카테고리 목록, conformal correction을 S3의
    models_prefix 아래 저장하고, 테스트셋 기준 평가 지표를 반환한다.

    데이터는 이 함수가 직접 S3에서 읽는다(예전엔 호출부가 `load_training_table()`로
    미리 읽어 df를 넘겼다) — `lazy_train_dataset`이 날짜 파티션 단위로 지연 조회하므로
    전체를 한 번에 들고 있을 실체(df/X_train 등)가 애초에 없다.

    args:
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
    raises:
        NotImplementedError: LGB_NUM_MACHINES>1(분산 학습) — lazy_train_dataset은
            아직 station_no 샤딩과 연동되지 않았다(2026-08 스트리밍 전환 시 범위 밖으로
            남김 — 필요해지면 `_DatePartitionSequence`가 청크를 읽은 직후 이 머신
            몫만 남기게 확장하면 된다, 구조상 막혀있지 않음).
        ValueError: train/valid/test 구간에 날짜 또는 데이터가 하나도 없을 때
    """
    if config.LGB_NUM_MACHINES > 1:
        raise NotImplementedError(
            "lazy_train_dataset(날짜 파티션 단위 지연 로딩)은 아직 분산 학습"
            "(LGB_NUM_MACHINES>1)과 연동되지 않았다 — station_no 샤딩을 날짜별 로더 "
            "안에서 다시 구현해야 한다. 지금은 LGB_TREE_LEARNER=serial(단일 머신)만 지원."
        )
    is_primary = config.LGB_MACHINE_RANK == 0

    feature_columns = _FEATURE_COLUMNS_BY_MODEL[model_name]
    table_path = _TRAINING_TABLE_BY_MODEL[model_name]
    filters = [("horizon", "<=", config.MAX_TRAIN_HORIZON)]

    _validate_valid_test_days_dont_overlap_train()
    train_dates, valid_dates, test_dates = _dates_for_split(config.TRAIN_WINDOW_START, config.TRAIN_WINDOW_END)
    if not train_dates or not valid_dates or not test_dates:
        raise ValueError(
            f"학습 구간에 날짜가 없음 — train {len(train_dates)}개, valid {len(valid_dates)}개, "
            f"test {len(test_dates)}개 ({config.TRAIN_WINDOW_START.isoformat()}~"
            f"{config.TRAIN_WINDOW_END.isoformat()}) — "
            "VALID_DAYS_OF_MONTH/TEST_DAYS_OF_MONTH/TRAIN_DAY_DIVISOR 설정을 확인하세요"
        )
    _append_progress_log(
        f"[{model_name}] 날짜 확정 — train {len(train_dates)}개, valid {len(valid_dates)}개, "
        f"test {len(test_dates)}개, max_horizon={config.MAX_TRAIN_HORIZON}"
    )

    # station_categories는 train/valid/test 전체 날짜에서 한 번만 뽑는다 — 세 split
    # 전체에서 station_no -> 코드 매핑이 같아야 LightGBM이 같은 station을 같은 코드로
    # 본다(예전 _prepare_xy()와 같은 이유). int()로 감싸는 이유: station_no 컬럼은
    # model_contract.NATIVE_COLUMN_DTYPES에 따라 int16이라 .unique()가 numpy.int16
    # 배열을 반환한다 — 그대로 s3_io.write_json()에 넘기면 json.dumps가 numpy 정수
    # 타입을 직렬화하지 못해 죽는다(lazy_train_dataset.station_categories_for_dates()가
    # 이미 int()로 변환해 반환).
    all_dates = sorted({*train_dates, *valid_dates, *test_dates})
    station_categories = lazy_train_dataset.station_categories_for_dates(
        table_path, all_dates, filters, on_complete=_progress_on_complete(f"{model_name}-station-categories")
    )
    station_dtype = pd.CategoricalDtype(categories=station_categories)

    if is_primary:
        mlflow_tracking.configure(config.MLFLOW_EXPERIMENT_NAME)
    # run_name은 archive_prefix(날짜+프로필)가 아니라 실제로 실험마다 바뀌는
    # divisor/horizon을 담는다 — 같은 날 같은 프로필로 여러 조합을 재시도한 이력
    # (2026-08 OOM 대응 폴백 체인)이 전부 archive_prefix는 같아서(S3 모델 파일이
    # 서로 덮어써짐) 그것만으로는 MLflow UI 목록에서 어느 시도가 어느 조합이었는지
    # 구분이 안 됐다.
    run_name = f"{model_name}_{common_config.PROFILE_NAME}_d{config.TRAIN_DAY_DIVISOR}_h{config.MAX_TRAIN_HORIZON}"
    # `with mlflow.start_run(...)`로 여는 이유: 예전엔 성공 경로 끝에서만
    # mlflow.end_run()을 불렀다 — lgb.train()/S3 업로드/평가 도중 예외가 나면 run이
    # RUNNING 상태로 영구 방치되고, 같은 프로세스가 다음 모델(반납 등)을 학습할 때
    # 활성 run과 충돌할 수 있었다. `with` 블록은 예외가 나도 자동으로 FAILED 처리하고
    # 종료한다. 분산 학습 워커(rank>0)는 위에서 이미 막았지만, 그 경우 run을 안 여는
    # 구조는 남겨둔다(nullcontext) — 나중에 분산 학습을 다시 연결할 때 그대로 쓸 수 있게.
    mlflow_run = mlflow.start_run(run_name=run_name) if is_primary else contextlib.nullcontext()

    with mlflow_run:
        if is_primary:
            mlflow.log_params({
                "model_name": model_name,
                "train_window_start": config.TRAIN_WINDOW_START.isoformat(),
                "train_window_end": config.TRAIN_WINDOW_END.isoformat(),
                "train_day_divisor": config.TRAIN_DAY_DIVISOR,
                "max_train_horizon": config.MAX_TRAIN_HORIZON,
                "valid_days_of_month": sorted(config.VALID_DAYS_OF_MONTH),
                "test_days_of_month": sorted(config.TEST_DAYS_OF_MONTH),
                "profile_name": common_config.PROFILE_NAME,
                "models_prefix": models_prefix,
                "train_dates": len(train_dates),
                "valid_dates": len(valid_dates),
                "test_dates": len(test_dates),
                "feature_columns": ",".join(feature_columns),
                "lgb_num_boost_round": config.LGB_NUM_BOOST_ROUND,
                "lgb_early_stopping_rounds": config.LGB_EARLY_STOPPING_ROUNDS,
                **config.LGB_PARAMS_COMMON,
            })
            s3_io.write_json(station_categories_path(model_name, models_prefix), station_categories)
            mlflow.log_dict(station_categories, "station_categories.json")

        metrics: dict = {"model_name": model_name}
        cache = lazy_train_dataset.ChunkCache()

        # 1) Poisson (+ exposure offset) — train/valid Dataset은 Sequence 기반(날짜별
        # 청크를 필요할 때만 S3에서 읽고 바이닝 후 버림, lazy_train_dataset.py 참고).
        train_set, y_train, exposure_train = lazy_train_dataset.build_lazy_dataset(
            table_path, train_dates, feature_columns, station_dtype, filters, target_col, exposure_col, cache,
            on_chunk_loaded=_chunk_progress(model_name, "train"),
        )
        valid_set, y_valid, exposure_valid = lazy_train_dataset.build_lazy_dataset(
            table_path, valid_dates, feature_columns, station_dtype, filters, target_col, exposure_col, cache,
            reference=train_set, on_chunk_loaded=_chunk_progress(model_name, "valid"),
        )
        _append_progress_log(
            f"[{model_name}] train/valid Dataset 구성 완료 — train {len(y_train):,}행, "
            f"valid {len(y_valid):,}행, peak_rss={_peak_rss_mb():.0f}MB"
        )
        print(f"[{model_name}] train={len(y_train):,} valid={len(y_valid):,}")
        del exposure_train, exposure_valid  # init_score로 이미 train_set/valid_set 구성 시점에 반영됨, 더 안 씀

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
        if is_primary:
            model_io.stage_and_upload_booster(
                booster, model_key(model_name, "poisson", models_prefix), log_to_mlflow=True
            )

        # test는 lgb.Dataset으로 안 쓴다(학습 없이 predict()/지표 계산에만 쓰임) — 날짜별로
        # 그 청크만 읽어 즉시 predict한 뒤 큰 feature 행렬은 버리고 작은 결과만 이어붙인다
        # (lazy_train_dataset.predict_over_dates(), X_test라는 실체를 아예 안 만듦).
        test_poisson = lazy_train_dataset.predict_over_dates(
            table_path, test_dates, feature_columns, station_dtype, filters, target_col, exposure_col,
            {"poisson": booster}, on_chunk_loaded=_chunk_progress(model_name, "test-poisson"),
        )
        y_test = test_poisson["y"]
        exposure_test = test_poisson["exposure"] if exposure_col else np.ones(len(y_test))
        mu_test = exposure_test * test_poisson["poisson"]
        metrics["poisson_deviance_test"] = _poisson_deviance(y_test, mu_test)
        metrics["rmse_test"] = float(np.sqrt(np.mean((y_test - mu_test) ** 2)))
        metrics["best_iteration"] = booster.best_iteration
        print(
            f"  [poisson] best_iter={booster.best_iteration} "
            f"test_deviance={metrics['poisson_deviance_test']:.4f} test_rmse={metrics['rmse_test']:.4f}"
        )

        # 2) Quantile P10/P50/P90 (exposure offset 미적용 — quantile loss는 offset 해석이 표준적이지 않음)
        #
        # exposure_col이 없는 모델(반납)은 train_set/valid_set에 애초에 init_score가
        # 없으므로 그대로 재사용한다 — LightGBM Dataset은 objective와 무관한 순수
        # 데이터 컨테이너라 lgb.train()을 다른 params로 여러 번 호출해도(quantile
        # alpha별로 3번 포함) 안전하다(별도 Dataset과 예측값이 byte-identical함을
        # 직접 검증, Sequence 기반이어도 이미 construct()된 뒤라 마찬가지). exposure_col이
        # 있는 모델(대여)은 poisson용 init_score(log(exposure))가 이미 박혀 있고
        # `Dataset.set_init_score(None)`으로도 지워지지 않아(직접 확인, 재사용 시 quantile
        # 예측이 전부 0으로 붕괴) 재사용을 포기하고 offset 없는 별도 Dataset을 다시
        # 만든다(같은 날짜를 한 번 더 읽음 — 메모리 대신 I/O를 쓰는 트레이드오프).
        if exposure_col is None:
            train_set_q, valid_set_q = train_set, valid_set
        else:
            train_set_q, _, _ = lazy_train_dataset.build_lazy_dataset(
                table_path, train_dates, feature_columns, station_dtype, filters, target_col, None, cache,
                on_chunk_loaded=_chunk_progress(model_name, "train-quantile"),
            )
            valid_set_q, _, _ = lazy_train_dataset.build_lazy_dataset(
                table_path, valid_dates, feature_columns, station_dtype, filters, target_col, None, cache,
                reference=train_set_q, on_chunk_loaded=_chunk_progress(model_name, "valid-quantile"),
            )

        quantile_boosters: dict[float, lgb.Booster] = {}
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
            quantile_boosters[alpha] = q_booster
            print(f"  [q{int(alpha * 100)}] best_iter={q_booster.best_iteration}")

        # valid/test 청크를 quantile booster 3개 전부로 한 번에 predict한다(alpha별로
        # 따로 부르면 같은 날짜를 3번씩 다시 읽게 되므로 I/O가 3배 든다).
        q_named = {f"q{int(alpha * 100)}": b for alpha, b in quantile_boosters.items()}
        valid_q = lazy_train_dataset.predict_over_dates(
            table_path, valid_dates, feature_columns, station_dtype, filters, target_col, exposure_col, q_named,
            on_chunk_loaded=_chunk_progress(model_name, "valid-quantile-predict"),
        )
        test_q = lazy_train_dataset.predict_over_dates(
            table_path, test_dates, feature_columns, station_dtype, filters, target_col, exposure_col, q_named,
            on_chunk_loaded=_chunk_progress(model_name, "test-quantile-predict"),
        )
        quantile_preds_valid = {alpha: valid_q[f"q{int(alpha * 100)}"] for alpha in config.QUANTILE_ALPHAS}
        quantile_preds_test = {alpha: test_q[f"q{int(alpha * 100)}"] for alpha in config.QUANTILE_ALPHAS}
        for alpha in config.QUANTILE_ALPHAS:
            metrics[f"pinball_test_q{int(alpha * 100)}"] = _pinball_loss(y_test, quantile_preds_test[alpha], alpha)
            print(f"  [q{int(alpha * 100)}] pinball={metrics[f'pinball_test_q{int(alpha * 100)}']:.4f}")

        p10_test, p90_test = quantile_preds_test[0.1], quantile_preds_test[0.9]
        raw_coverage = float(np.mean((y_test >= p10_test) & (y_test <= p90_test)))
        metrics["p10_p90_coverage_raw_test"] = raw_coverage
        print(f"  [calibration] 보정 전 P10~P90 커버리지 = {raw_coverage:.3f} (이론값 {config.CONFORMAL_TARGET_COVERAGE})")

        # split-conformal 보정: 검증셋(build_lazy_dataset이 이미 구해둔 y_valid — 위
        # valid_q["y"]와 같은 날짜/필터라 동일한 값, 다시 안 뽑음)에서 목표 커버리지에
        # 맞는 correction을 구해 저장, 테스트셋에 적용해 재평가.
        correction = _conformal_correction(
            y_valid, quantile_preds_valid[0.1], quantile_preds_valid[0.9], config.CONFORMAL_TARGET_COVERAGE
        )
        if is_primary:
            correction_payload = {"correction": correction, "target_coverage": config.CONFORMAL_TARGET_COVERAGE}
            s3_io.write_json(model_json_key(model_name, "conformal_correction", models_prefix), correction_payload)
            mlflow.log_dict(correction_payload, "conformal_correction.json")

        p10_calibrated = np.clip(p10_test - correction, 0, None)  # count는 음수가 될 수 없음
        p90_calibrated = p90_test + correction
        calibrated_coverage = float(np.mean((y_test >= p10_calibrated) & (y_test <= p90_calibrated)))
        metrics["conformal_correction"] = correction
        metrics["p10_p90_coverage_calibrated_test"] = calibrated_coverage
        print(f"  [calibration] correction={correction:.4f} 적용 후 커버리지 = {calibrated_coverage:.3f}")

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
    # 대여/반납은 완전히 분리된 데이터셋이라 각자 train_target()을 호출한다 — 실제
    # 운영 엔트리포인트는 train_rental_model.py/train_return_model.py(모델 아카이브
    # 경로/프로필까지 처리)이고, 이 블록은 로컬 ad-hoc 실행용이다. models_prefix에
    # 기본값이 없으므로(챔피언 자리에 직접 못 씀) 여기서도 실제 엔트리포인트와
    # 똑같이 아카이브 경로를 명시적으로 계산해서 넘긴다 — 같은 날 두 번 돌려도
    # archive_prefix가 겹치면 안 되므로 config.unique_archive_date() 사용
    # (config.today_kst()만 쓰면 안 됨, unique_archive_date() 참고).
    _archive_prefix = archive_models_prefix(config.unique_archive_date(), common_config.PROFILE_NAME)

    rental_metrics = train_target(
        "rental_count", "rental", _archive_prefix, exposure_col="rental_exposure"
    )
    return_metrics = train_target("return_count", "return", _archive_prefix, exposure_col=None)

    print(json.dumps({"rental": rental_metrics, "return": return_metrics}, indent=2, ensure_ascii=False))
