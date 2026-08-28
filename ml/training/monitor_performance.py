"""매달 챔피언 모델의 실측 성능을 확인해서 재학습이 필요한지 판단한다.

**기준을 "절대 수치"가 아니라 "baseline 대비 상대 악화율"로 잡은 이유와 임계값
10%(deviance)/15%p(커버리지)의 근거는 [common_config.py](../../libs/ml_core/common_config.py)에
적어뒀다 — 계절성 때문에 절대 임계값은 계절과 뒤섞이고, 실측한 노이즈 바닥
(run-to-run 편차 0.3~0.5%, embargo 파라미터 스윕 편차 ~0.6%)보다 한참 위로 잡아야
순수 변동으로 오탐이 안 난다. 이 파일은 그 기준을 실제로 적용만 한다.

**baseline**은 "이 모델이 마지막으로 학습됐을 때 테스트셋에서 낸 성능"
(`{model_name}_metrics.json`, `train_common.train_target()`이 학습 시점에
챔피언의 archive_prefix 밑에 저장해둠 — `ml_core.paths.read_champion_prefix()`로
"지금 챔피언"이 가리키는 위치를 찾는다)이다. 매달 이 값과 "최근
`config.MONITOR_LOOKBACK_MONTHS`개월 실측"을 비교한다.

실행: `python -m training.monitor_performance` (ml/ 디렉토리에서, 대여/반납 둘 다 확인)
"""

import json
from datetime import date

import mlflow
import numpy as np
import pandas as pd
from core import s3 as s3_io
from ml_core import mlflow_tracking
from ml_core.metrics import poisson_deviance as _poisson_deviance
from ml_core.model_contract import RENTAL_FEATURE_COLUMNS, RETURN_FEATURE_COLUMNS
from ml_core.paths import model_json_key, read_champion_prefix
from ml_core.scoring import predict

from . import config

MODEL_SPECS = [
    ("rental", "rental_count", "rental_exposure"),
    ("return", "return_count", None),
]
_FEATURE_COLUMNS_BY_MODEL = {"rental": RENTAL_FEATURE_COLUMNS, "return": RETURN_FEATURE_COLUMNS}
_TRAINING_TABLE_BY_MODEL = {
    "rental": config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    "return": config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
}


def _load_baseline_metrics(model_name: str) -> dict:
    """train_target()이 학습 시점에 저장해둔 baseline 지표를 S3에서 읽는다.

    args:
        model_name: "rental" 또는 "return"
    returns:
        dict: train_target()이 반환했던 것과 같은 키의 metrics
    raises:
        FileNotFoundError: 아직 한 번도 승격된 적 없어 챔피언 포인터가 없거나
            (`read_champion_prefix()`), 포인터는 있는데 metrics.json이 없을 때
    """
    archive_prefix = read_champion_prefix(model_name)
    key = model_json_key(model_name, "metrics", archive_prefix)
    data = s3_io.read_json(key)
    if data is None:
        raise FileNotFoundError(f"baseline metrics 없음: {key}")
    return data


def _recent_month_range(lookback_months: int, as_of: date | None = None) -> tuple[str, str]:
    """최근 "완전히 확정된" lookback_months개월의 (시작일, 종료일) "YYYY-MM-DD" 문자열을 만든다.

    "이번 달 제외 = 완결"이 아니다 — 대여이력은 반납 완료 시에만 Silver에
    나타나므로(`feature_engine/spark/run_pipeline.py` 참고), `rental_count`는
    한동안 계속 사후 보정될 수 있다. 그래서 "지난달 말일"이 아니라 "그 달의
    마지막 날이 `as_of - config.TRAINING_SAFETY_MARGIN_DAYS`보다 확실히 이전인
    가장 최근 달"을 끝으로 잡는다 — 안 그러면(예: 8/1에 실행) 7/31까지를 확정된
    걸로 보고 baseline과 비교하는데, 그 구간은 며칠 뒤 증분 실행에서
    rental_count가 또 바뀔 수 있어 "재학습 필요" 판정이 아직 안정화되지 않은
    값으로 오염된다.

    마진은 `feature_engine`의 `INCREMENTAL_LOOKBACK_HOURS`(35일 — feature mart
    자체가 사후 보정을 계속 반영하는 폭이 넓은 안전 마진)가 아니라 `training/
    config.py`의 `TRAINING_SAFETY_MARGIN_DAYS`(7일 — "이 정도면 거의 다
    반납됐다"는 실용적 기준, 학습 구간 계산과 동일)를 그대로 재사용한다 —
    35일을 쓰면 모니터링이 항상 두세 달 전 데이터만 보게 돼 너무 뒤처진다.

    args:
        lookback_months: 몇 개월치를 볼지
        as_of: 기준 날짜(기본값 오늘) — 테스트에서 날짜를 고정하기 위한 override
    returns:
        tuple[str, str]: (start_date, end_date)
    """
    as_of_ts = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now().normalize()
    safe_cutoff = as_of_ts - pd.Timedelta(days=config.TRAINING_SAFETY_MARGIN_DAYS)

    end = as_of_ts.replace(day=1) - pd.Timedelta(days=1)  # 지난달 마지막 날(후보)
    while end > safe_cutoff:  # 아직 사후 보정 대상이면 그 이전 달로 계속 밀어낸다
        end = end.replace(day=1) - pd.Timedelta(days=1)

    start = (end - pd.DateOffset(months=lookback_months - 1)).replace(day=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _split_date_range(start: str, end: str, num_shards: int) -> list[tuple[str, str] | None]:
    """[start, end](양끝 포함, "YYYY-MM-DD")를 날짜 수 기준으로 거의 균등한
    `num_shards`개의 연속 구간으로 나눈다.

    station_no가 아니라 날짜로 샤딩하는 이유: feature mart가 날짜별 Spark
    파티션 파일이라(`date=YYYY-MM-DD/*.parquet`), 날짜로 나누면 워커마다 서로
    다른 파일만 읽어 총 I/O가 그대로 유지된다. station_no로 나누면 워커 전원이
    같은 전체 파티션을 다 읽은 뒤 행만 버려야 해서 총 I/O가 워커 수만큼 늘어난다.

    args:
        start, end: 전체 구간(양끝 포함)
        num_shards: 나눌 조각 수(1 이상)
    returns:
        list[tuple[str, str] | None]: 길이 num_shards. 구간 안 날짜 수가
            num_shards보다 적으면 뒤쪽 일부 원소는 `None`(그 워커가 담당할
            날짜가 하나도 없다는 뜻 — `evaluate_recent_performance_shard()`를
            부르는 쪽이 S3를 읽지 않고 곧바로 n_rows=0 부분합을 반환해야 한다.
            barrier 자체는 여전히 num_shards개 등록을 기다리므로 에러가 아니다).
    """
    all_days = pd.date_range(start, end, freq="D")
    chunks = np.array_split(all_days, num_shards)
    return [(chunk[0].strftime("%Y-%m-%d"), chunk[-1].strftime("%Y-%m-%d")) if len(chunk) else None for chunk in chunks]


def evaluate_recent_performance_shards_by_day(
    model_name: str,
    target_col: str,
    exposure_col: str | None,
    date_range: tuple[str, str] | None,
    horizon: int = 1,
) -> dict[str, dict]:
    """날짜 부분구간에 대해 평가를 수행하고 일자별 부분합 딕셔너리를 반환한다.

    args:
        model_name: "rental" 또는 "return"
        target_col: "rental_count" 또는 "return_count"
        exposure_col: predict()에 전달할 exposure 컬럼명 (반납은 None)
        date_range: (start, end) — 대상 부분구간 (None이면 빈 딕셔너리 반환)
        horizon: 예측 타겟 horizon
    returns:
        dict[str, dict]: {"YYYY-MM-DD": {"n_rows": int, "sum_deviance_term": float, "sum_sq_err": float, "sum_coverage_hits": float}}
    raises:
        FileNotFoundError: 대상 테이블이 S3에 존재하지 않을 때
    """
    if date_range is None:
        return {}
    start, end = date_range
    table_path = _TRAINING_TABLE_BY_MODEL[model_name]
    feature_columns = _FEATURE_COLUMNS_BY_MODEL[model_name]
    needed = sorted(
        set(feature_columns) | {target_col, "horizon", "date", "hour"} | ({exposure_col} if exposure_col else set())
    )
    df = s3_io.read_parquet(table_path, columns=needed, date_range=(start, end), filters=[("horizon", "==", horizon)])
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {table_path}")
    if df.empty:
        return {}

    preds = predict(df, model_name, exposure_col=exposure_col)
    y = df[target_col].to_numpy()
    pred_mean = preds["pred_mean"].to_numpy()

    mu = np.clip(pred_mean, 1e-6, None)
    y_safe = np.where(y > 0, y, 1.0)
    deviance_term = np.where(y > 0, y * np.log(y_safe / mu) - (y - mu), mu)
    coverage_hit = ((y >= preds["pred_p10"].to_numpy()) & (y <= preds["pred_p90"].to_numpy())).astype(float)
    sq_err = (y - pred_mean) ** 2

    df_eval = pd.DataFrame({
        "date": df["date"].astype(str),
        "deviance_term": deviance_term,
        "sq_err": sq_err,
        "coverage_hit": coverage_hit,
    })

    result_by_day: dict[str, dict] = {}
    for d, g in df_eval.groupby("date"):
        result_by_day[str(d)] = {
            "n_rows": len(g),
            "sum_deviance_term": float(g["deviance_term"].sum()),
            "sum_sq_err": float(g["sq_err"].sum()),
            "sum_coverage_hits": float(g["coverage_hit"].sum()),
        }

    return result_by_day


def evaluate_recent_performance_shard(
    model_name: str,
    target_col: str,
    exposure_col: str | None,
    date_range: tuple[str, str] | None,
    horizon: int = 1,
) -> dict:
    """`evaluate_recent_performance()`의 핵심 계산을 날짜 부분구간에 대해 실행한다.

    각 워커는 자기가 맡은 날짜 부분구간만 읽어 `(n_rows, sum_deviance_term,
    sum_sq_err, sum_coverage_hits)` 네 가지 "부분합"만 계산해서 돌려준다.
    `combine_evaluation_shards()`는 이 조각들을 단순히 더하기만 해도(선형성)
    합쳐도 전체를 한 번에 계산한 것과 수학적으로 완전히 동일하다 — 근사가
    아니다.

    args:
        model_name: "rental" 또는 "return"
        target_col: "rental_count" 또는 "return_count"
        exposure_col: predict()에 전달할 exposure 컬럼명 (반납은 None)
        date_range: (start, end) — 이 워커가 담당할 부분구간(양끝 포함).
            `_split_date_range()`가 이 워커에 배정할 날짜가 없다고 판단하면
            `None` — 이때는 S3를 읽지 않고 곧바로 빈 부분합을 반환한다.
        horizon: `evaluate_recent_performance()` 참고
    returns:
        dict: n_rows, sum_deviance_term, sum_sq_err, sum_coverage_hits.
            n_rows=0이면 나머지 sum_*은 전부 0.0(이 조각에 해당 날짜가 없거나
            비어 있었다는 뜻 — 오류가 아니다).
    """
    by_day = evaluate_recent_performance_shards_by_day(
        model_name, target_col, exposure_col, date_range, horizon=horizon
    )
    if not by_day:
        return {"n_rows": 0, "sum_deviance_term": 0.0, "sum_sq_err": 0.0, "sum_coverage_hits": 0.0}
    return {
        "n_rows": sum(s["n_rows"] for s in by_day.values()),
        "sum_deviance_term": sum(s["sum_deviance_term"] for s in by_day.values()),
        "sum_sq_err": sum(s["sum_sq_err"] for s in by_day.values()),
        "sum_coverage_hits": sum(s["sum_coverage_hits"] for s in by_day.values()),
    }


def combine_evaluation_shards(
    model_name: str, period: tuple[str, str], shards: list[dict], baseline: dict
) -> dict:
    """`evaluate_recent_performance_shard()` 부분합들을 최종 지표로 합친다.

    args:
        model_name: "rental" 또는 "return"
        period: (start, end) — 전체 평가 구간(로그/응답용, 계산에는 안 쓰임)
        shards: evaluate_recent_performance_shard()가 반환한 dict들
        baseline: `_load_baseline_metrics()`가 반환한 dict
    returns:
        dict: evaluate_recent_performance()와 정확히 같은 키
    raises:
        ValueError: 모든 조각의 n_rows 합이 0(구간 전체에 데이터가 없음)
    """
    n_rows = sum(s["n_rows"] for s in shards)
    if n_rows == 0:
        raise ValueError(
            f"{period[0]}~{period[1]} 구간에 feature mart 데이터가 없음 — 최신 데이터가 반영됐는지 확인하세요"
        )

    current_deviance = 2 * sum(s["sum_deviance_term"] for s in shards) / n_rows
    current_rmse = float(np.sqrt(sum(s["sum_sq_err"] for s in shards) / n_rows))
    current_coverage = sum(s["sum_coverage_hits"] for s in shards) / n_rows

    baseline_deviance = baseline["poisson_deviance_test"]
    baseline_coverage = baseline["p10_p90_coverage_calibrated_test"]

    return {
        "model_name": model_name,
        "period": {"start": period[0], "end": period[1]},
        "n_rows": n_rows,
        "baseline_deviance": baseline_deviance,
        "current_deviance": current_deviance,
        "deviance_relative_change": (current_deviance - baseline_deviance) / baseline_deviance,
        "baseline_rmse": baseline["rmse_test"],
        "current_rmse": current_rmse,
        "baseline_coverage": baseline_coverage,
        "current_coverage": current_coverage,
        "coverage_drift": abs(current_coverage - baseline_coverage),
    }


def _eval_cache_key(model_name: str, archive_prefix: str, horizon: int, date_str: str) -> str:
    """일자별 평가 부분합 캐시의 S3 키를 반환한다."""
    prefix_clean = archive_prefix.strip("/").replace("/", "_")
    return f"models/eval_cache/{model_name}/{prefix_clean}/h{horizon}/{date_str}.json"


def _group_contiguous_dates(dates: list[str]) -> list[tuple[str, str]]:
    """'YYYY-MM-DD' 문자열 리스트를 연속된 (start, end) 구간들의 리스트로 묶는다.

    args:
        dates: "YYYY-MM-DD" 형태의 날짜 문자열 리스트
    returns:
        list[tuple[str, str]]: 연속된 구간 (시작일, 종료일) 튜플 리스트
    """
    if not dates:
        return []
    ts_list = sorted(pd.Timestamp(d) for d in dates)
    ranges: list[tuple[str, str]] = []
    curr_start = ts_list[0]
    curr_end = ts_list[0]
    for ts in ts_list[1:]:
        if ts == curr_end + pd.Timedelta(days=1):
            curr_end = ts
        else:
            ranges.append((curr_start.strftime("%Y-%m-%d"), curr_end.strftime("%Y-%m-%d")))
            curr_start = ts
            curr_end = ts
    ranges.append((curr_start.strftime("%Y-%m-%d"), curr_end.strftime("%Y-%m-%d")))
    return ranges


def evaluate_recent_performance_cached(
    model_name: str,
    target_col: str,
    exposure_col: str | None,
    start: str,
    end: str,
    horizon: int = 1,
) -> dict:
    """평가 대상 구간의 일자별 부분합 캐시를 활용해, 빠진 날짜만 증분 계산하고 최종 합산한다.

    args:
        model_name: "rental" 또는 "return"
        target_col: "rental_count" 또는 "return_count"
        exposure_col: exposure 컬럼명
        start: 시작일 ("YYYY-MM-DD")
        end: 종료일 ("YYYY-MM-DD")
        horizon: 예측 타겟 horizon
    returns:
        dict: combine_evaluation_shards() 결과
    """
    archive_prefix = read_champion_prefix(model_name)
    all_days = [d.strftime("%Y-%m-%d") for d in pd.date_range(start, end, freq="D")]

    shards: list[dict] = []
    missing_days: list[str] = []

    for d in all_days:
        cache_key = _eval_cache_key(model_name, archive_prefix, horizon, d)
        cached = s3_io.read_json(cache_key)
        if cached is not None and "n_rows" in cached:
            shards.append(cached)
        else:
            missing_days.append(d)

    # 캐시에 없는 빠진 날짜들만 연속된 구간별로 묶어 증분 계산하고 일자별 캐시 저장
    if missing_days:
        for r_start, r_end in _group_contiguous_dates(missing_days):
            day_shards = evaluate_recent_performance_shards_by_day(
                model_name, target_col, exposure_col, (r_start, r_end), horizon
            )
            for day_str, day_shard in day_shards.items():
                if day_shard["n_rows"] > 0:
                    shards.append(day_shard)
                    s3_io.write_json(
                        _eval_cache_key(model_name, archive_prefix, horizon, day_str),
                        day_shard,
                    )

    baseline = _load_baseline_metrics(model_name)
    return combine_evaluation_shards(model_name, (start, end), shards, baseline)

    baseline = _load_baseline_metrics(model_name)
    return combine_evaluation_shards(model_name, (start, end), shards, baseline)


def evaluate_recent_performance(
    model_name: str,
    target_col: str,
    exposure_col: str | None,
    lookback_months: int | None = None,
    as_of: date | None = None,
    horizon: int = 1,
) -> dict:
    """챔피언 모델을 최근 lookback_months개월 실측 데이터로 다시 평가해 baseline과 비교한다.

    args:
        model_name: "rental" 또는 "return"
        target_col: "rental_count" 또는 "return_count"
        exposure_col: predict()에 전달할 exposure 컬럼명 (반납은 None)
        lookback_months: None이면 config.MONITOR_LOOKBACK_MONTHS
        as_of: 기준 날짜(기본 오늘) — 테스트용 override
        horizon: 몇 시간 뒤 예측 기준으로 평가할지 (기본 1 — 기존 h=1 전용 챔피언과
            같은 조건으로 baseline 임계값(10%/15%p)의 연속성을 유지). multi-horizon
            테이블엔 horizon별 행이 섞여 있어 반드시 하나로 고정해서 걸러야 한다.
    returns:
        dict: model_name, period, n_rows, baseline_*/current_* (deviance, rmse,
            coverage), deviance_relative_change, coverage_drift
    raises:
        ValueError: 해당 기간·horizon에 feature mart 데이터가 전혀 없을 때
    """
    lookback_months = lookback_months or config.MONITOR_LOOKBACK_MONTHS
    start, end = _recent_month_range(lookback_months, as_of)

    return evaluate_recent_performance_cached(
        model_name, target_col, exposure_col, start, end, horizon=horizon
    )



def decide_retrain(evaluation: dict) -> dict:
    """evaluate_recent_performance() 결과에 common_config 임계값을 적용해 재학습 필요 여부를 정한다.

    args:
        evaluation: evaluate_recent_performance()의 결과
    returns:
        dict: evaluation 전체 + needs_retrain(bool) + reasons(list[str])
    """
    reasons = []
    if evaluation["deviance_relative_change"] > config.PERFORMANCE_DEGRADATION_THRESHOLD:
        reasons.append(
            f"poisson_deviance {evaluation['deviance_relative_change']:+.1%} 변화 "
            f"(baseline {evaluation['baseline_deviance']:.4f} -> 현재 {evaluation['current_deviance']:.4f}, "
            f"임계값 {config.PERFORMANCE_DEGRADATION_THRESHOLD:.0%})"
        )
    if evaluation["coverage_drift"] > config.COVERAGE_DRIFT_THRESHOLD:
        reasons.append(
            f"P10~P90 커버리지 {evaluation['coverage_drift']:.1%}p 드리프트 "
            f"(baseline {evaluation['baseline_coverage']:.3f} -> 현재 {evaluation['current_coverage']:.3f}, "
            f"임계값 {config.COVERAGE_DRIFT_THRESHOLD:.0%}p)"
        )
    return {**evaluation, "needs_retrain": len(reasons) > 0, "reasons": reasons}


def _log_to_mlflow(result: dict, horizon: int) -> None:
    """월별 성능 점검 결과를 MLflow에도 남긴다.

    학습(`train_common.train_target()`)과 같은 서버, 다른 experiment
    (`config.MLFLOW_MONITORING_EXPERIMENT_NAME`)에 점검마다 run 하나씩 쌓는다 —
    "언제부터 드리프트가 시작됐는지"를 매달 콘솔 로그를 뒤져서 찾는 대신 MLflow UI의
    지표 추이(deviance_relative_change/coverage_drift)로 바로 볼 수 있게 하려는
    용도다. 이 로깅 자체는 재학습 필요 여부 판단(`decide_retrain()`)의 정확성과
    무관한 부가 기능이라, MLflow 서버가 마침 안 떠 있어도(로컬 개발 등) 실제 점검
    결과 출력/재학습 판단을 막지 않도록 실패를 삼키고 경고만 남긴다.
    """
    try:
        mlflow_tracking.configure(config.MLFLOW_MONITORING_EXPERIMENT_NAME)
        with mlflow.start_run(run_name=f"{result['model_name']}_h{horizon}_{result['period']['end']}"):
            mlflow.log_params({
                "model_name": result["model_name"],
                "horizon": horizon,
                "period_start": result["period"]["start"],
                "period_end": result["period"]["end"],
            })
            mlflow.log_metrics({
                "n_rows": result["n_rows"],
                "baseline_deviance": result["baseline_deviance"],
                "current_deviance": result["current_deviance"],
                "deviance_relative_change": result["deviance_relative_change"],
                "baseline_rmse": result["baseline_rmse"],
                "current_rmse": result["current_rmse"],
                "baseline_coverage": result["baseline_coverage"],
                "current_coverage": result["current_coverage"],
                "coverage_drift": result["coverage_drift"],
                "needs_retrain": int(result["needs_retrain"]),
            })
            if result["reasons"]:
                mlflow.log_dict({"reasons": result["reasons"]}, "reasons.json")
    except Exception as exc:  # noqa: BLE001 — 부가 로깅이라 어떤 이유로 실패하든 점검 자체를 막으면 안 됨
        print(f"[monitor_performance] MLflow 로깅 실패(무시하고 진행): {exc}")


def check_all_models(
    as_of: date | None = None, horizon: int = 1, model_names: list[str] | None = None
) -> list[dict]:
    """대여/반납 챔피언 모델을 확인한다.

    args:
        as_of: 기준 날짜 (테스트용 override)
        horizon: `evaluate_recent_performance()` 참고 — 기본 1
        model_names: None(기본)이면 대여/반납 둘 다 확인한다. 지정하면 그 모델만
            확인한다 — `evaluate_recent_performance()` 한 번 호출마다 feature
            mart를 한 달치 통째로 읽어들이는데(m4.large 마스터/컨테이너의 좁은
            메모리 예산에서 실제로 exitCode 137 OOM으로 확인됨, 2026-08-26),
            호출부가 모델 하나만 필요로 할 때 나머지 모델까지 읽어서 메모리를
            두 배로 쓸 이유가 없다.
    returns:
        list[dict]: decide_retrain() 결과 (요청한 모델 순서, 기본은 rental, return 순)
    """
    specs = [spec for spec in MODEL_SPECS if spec[0] in model_names] if model_names else MODEL_SPECS
    results = []
    for model_name, target_col, exposure_col in specs:
        evaluation = evaluate_recent_performance(model_name, target_col, exposure_col, as_of=as_of, horizon=horizon)
        result = decide_retrain(evaluation)
        _log_to_mlflow(result, horizon)
        results.append(result)
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="월별 챔피언 모델 성능 점검 (baseline 대비 상대 악화율)")
    parser.add_argument(
        "--as-of", default=None,
        help="YYYY-MM-DD, 기준 날짜(기본값: 오늘) — 미지정 시 실제 배포 환경에서는 항상 실행 시점의 "
             "'지난달'을 본다. 로컬 검증처럼 시스템 시계와 보유 데이터 기간이 다른 경우(예: 이 저장소의 "
             "샘플 데이터는 2025년뿐인데 시스템 날짜는 그 이후) 반드시 지정할 것 — 안 그러면 데이터가 "
             "없는 기간을 조회해 ValueError가 난다.",
    )
    parser.add_argument("--horizon", type=int, default=1, help="점검할 horizon(1~HORIZON_COUNT), 기본값 1")
    args = parser.parse_args()
    as_of = date.fromisoformat(args.as_of) if args.as_of else None

    results = check_all_models(as_of=as_of, horizon=args.horizon)
    for r in results:
        status = "재학습 필요" if r["needs_retrain"] else "정상"
        print(f"[{r['model_name']}] {status} — {r['period']['start']}~{r['period']['end']} ({r['n_rows']:,}행)")
        print(f"    deviance: baseline={r['baseline_deviance']:.4f} 현재={r['current_deviance']:.4f} ({r['deviance_relative_change']:+.1%})")
        print(f"    coverage: baseline={r['baseline_coverage']:.3f} 현재={r['current_coverage']:.3f} (drift={r['coverage_drift']:.1%}p)")
        for reason in r["reasons"]:
            print(f"    - {reason}")
    print(json.dumps(results, indent=2, ensure_ascii=False))
