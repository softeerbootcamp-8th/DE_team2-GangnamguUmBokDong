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

import numpy as np
import pandas as pd
from core import s3 as s3_io
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

    # date_range로 이번에 볼 N개월 파티션만 받는다 — 그동안 쌓인 전체 히스토리를 매달
    # 실행할 때마다 다 받으면, 실행 비용이 "최근 N개월"이 아니라 "서비스 시작 이후
    # 전체 기간"에 비례해서 계속 커진다(s3_io.py 모듈 docstring 참고).
    table_path = _TRAINING_TABLE_BY_MODEL[model_name]
    feature_columns = _FEATURE_COLUMNS_BY_MODEL[model_name]
    needed = sorted(set(feature_columns) | {target_col, "date", "horizon"} | ({exposure_col} if exposure_col else set()))
    df = s3_io.read_parquet(table_path, columns=needed, date_range=(start, end))
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {table_path}")
    df = df[df["horizon"] == horizon].reset_index(drop=True)
    if df.empty:
        raise ValueError(
            f"{start}~{end} 구간·horizon={horizon}에 feature mart 데이터가 없음 — 최신 데이터가 반영됐는지 확인하세요"
        )

    preds = predict(df, model_name, exposure_col=exposure_col)
    y = df[target_col].to_numpy()
    baseline = _load_baseline_metrics(model_name)

    current_deviance = _poisson_deviance(y, preds["pred_mean"].to_numpy())
    current_rmse = float(np.sqrt(np.mean((y - preds["pred_mean"].to_numpy()) ** 2)))
    current_coverage = float(np.mean((y >= preds["pred_p10"].to_numpy()) & (y <= preds["pred_p90"].to_numpy())))

    baseline_deviance = baseline["poisson_deviance_test"]
    baseline_coverage = baseline["p10_p90_coverage_calibrated_test"]

    return {
        "model_name": model_name,
        "period": {"start": start, "end": end},
        "n_rows": len(df),
        "baseline_deviance": baseline_deviance,
        "current_deviance": current_deviance,
        "deviance_relative_change": (current_deviance - baseline_deviance) / baseline_deviance,
        "baseline_rmse": baseline["rmse_test"],
        "current_rmse": current_rmse,
        "baseline_coverage": baseline_coverage,
        "current_coverage": current_coverage,
        "coverage_drift": abs(current_coverage - baseline_coverage),
    }


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


def check_all_models(as_of: date | None = None, horizon: int = 1) -> list[dict]:
    """대여/반납 챔피언 모델을 모두 확인한다.

    args:
        as_of: 기준 날짜 (테스트용 override)
        horizon: `evaluate_recent_performance()` 참고 — 기본 1
    returns:
        list[dict]: decide_retrain() 결과 (rental, return 순)
    """
    results = []
    for model_name, target_col, exposure_col in MODEL_SPECS:
        evaluation = evaluate_recent_performance(model_name, target_col, exposure_col, as_of=as_of, horizon=horizon)
        results.append(decide_retrain(evaluation))
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
