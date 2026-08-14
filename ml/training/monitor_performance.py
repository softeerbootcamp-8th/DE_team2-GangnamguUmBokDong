"""매달 챔피언 모델의 실측 성능을 확인해서 재학습이 필요한지 판단한다.

**기준을 "절대 수치"가 아니라 "baseline 대비 상대 악화율"로 잡은 이유와 임계값
10%(deviance)/15%p(커버리지)의 근거는 [common_config.py](../../libs/ml_common/common_config.py)에
적어뒀다 — 계절성 때문에 절대 임계값은 계절과 뒤섞이고, 실측한 노이즈 바닥
(run-to-run 편차 0.3~0.5%, embargo 파라미터 스윕 편차 ~0.6%)보다 한참 위로 잡아야
순수 변동으로 오탐이 안 난다. 이 파일은 그 기준을 실제로 적용만 한다.

**baseline**은 "이 모델이 마지막으로 학습됐을 때 테스트셋에서 낸 성능"
(`models/{model_name}_metrics.json`, `train_common.train_target()`이 학습 시점에
저장해둠)이다. 매달 이 값과 "최근 `config.MONITOR_LOOKBACK_MONTHS`개월 실측"을
비교한다.

실행: `python -m training.monitor_performance` (ml/ 디렉토리에서, 대여/반납 둘 다 확인)
"""

import json
from datetime import date

import numpy as np
import pandas as pd
from ml_common.metrics import poisson_deviance as _poisson_deviance
from ml_common.scoring import predict

from . import config

MODEL_SPECS = [
    ("rental", "rental_count", "rental_exposure"),
    ("return", "return_count", None),
]


def _load_baseline_metrics(model_name: str) -> dict:
    """train_target()이 학습 시점에 저장해둔 baseline 지표를 읽는다.

    args:
        model_name: "rental" 또는 "return"
    returns:
        dict: train_target()이 반환했던 것과 같은 키의 metrics
    raises:
        FileNotFoundError: 아직 한 번도 학습 안 해서 baseline이 없는 모델
    """
    path = config.MODELS_DIR / f"{model_name}_metrics.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _recent_month_range(lookback_months: int, as_of: date | None = None) -> tuple[str, str]:
    """최근 완결된 lookback_months개월의 (시작일, 종료일) "YYYY-MM-DD" 문자열을 만든다.

    "완결된" 개월만 본다 — 이번 달은 아직 안 끝났으니 제외(부분 데이터로 성능을
    오판하지 않기 위함).

    args:
        lookback_months: 몇 개월치를 볼지
        as_of: 기준 날짜(기본값 오늘) — 테스트에서 날짜를 고정하기 위한 override
    returns:
        tuple[str, str]: (start_date, end_date)
    """
    as_of_ts = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now().normalize()
    end = as_of_ts.replace(day=1) - pd.Timedelta(days=1)  # 지난달 마지막 날
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

    df = pd.read_parquet(config.MULTI_HORIZON_FEATURES_TABLE_PARQUET)
    df = df[(df["date"] >= start) & (df["date"] <= end) & (df["horizon"] == horizon)].reset_index(drop=True)
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
    results = check_all_models()
    for r in results:
        status = "재학습 필요" if r["needs_retrain"] else "정상"
        print(f"[{r['model_name']}] {status} — {r['period']['start']}~{r['period']['end']} ({r['n_rows']:,}행)")
        print(f"    deviance: baseline={r['baseline_deviance']:.4f} 현재={r['current_deviance']:.4f} ({r['deviance_relative_change']:+.1%})")
        print(f"    coverage: baseline={r['baseline_coverage']:.3f} 현재={r['current_coverage']:.3f} (drift={r['coverage_drift']:.1%}p)")
        for reason in r["reasons"]:
            print(f"    - {reason}")
    print(json.dumps(results, indent=2, ensure_ascii=False))
