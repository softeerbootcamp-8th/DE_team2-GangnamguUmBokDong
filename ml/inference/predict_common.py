"""배치 조회 CLI(`predict_rental_demand.py`/`predict_return_demand.py`)가 공유하는 실행기.

실제 채점 로직(`predict()`, booster 로드 등)은 `ml_common/scoring.py`(training의
`monitor_performance.py`/`compare_baselines.py`도 같이 씀)에 있다 — 이 모듈은 그 위에
"station_id/기간을 골라 조회 -> 저장 -> 요약 출력"하는 배치 CLI 경험만 얹는다.

이 프로젝트엔 실시간 서빙 API가 없으므로, CLI(`run_predict_cli`)는 이미 구축된
`station_hour_features_multihorizon_2025.parquet`(feature_engineering이 만든 multi-horizon
학습 테이블 — horizon=1..HORIZON_COUNT가 섞여 있음)에서 station_id/기간/horizon을 골라
예측을 뽑아보는 용도다. 그 범위를 벗어난 날짜나 날씨·인구 데이터가 없는 미래 시점은
예측할 수 없다 — 그러려면 해당 시점의 날씨·인구·최근 실적 데이터를 먼저
`feature_engineering`의 피처마트 생성 파이프라인(`feature_engineering/spark/`)으로
넣어줘야 한다(그런 임의 시점 예측은 `predict_single.py`가 담당).
"""

import argparse

import pandas as pd
from ml_common import s3_io
from ml_common.scoring import predict, print_metrics

from . import config


def run_predict_cli(model_name: str, target_col: str, exposure_col: str | None, default_output: str) -> pd.DataFrame:
    """station_id/기간/horizon을 골라 예측하는 CLI 진입점. 두 predict_*.py가 공유한다.

    args:
        model_name: "rental" 또는 "return"
        target_col: "rental_count" 또는 "return_count" (actual 비교용)
        exposure_col: predict()에 그대로 전달할 exposure 컬럼명
        default_output: --out 미지정시 저장할 parquet S3 키
    returns:
        pd.DataFrame: predict() 결과에 "actual" 컬럼을 추가한 DataFrame
    """
    parser = argparse.ArgumentParser(description=f"{model_name} 수요 예측 (feature mart 범위 내에서 조회)")
    parser.add_argument("--station-id", default=None, help="정류소 ID 1개 (예: ST-1234). --station-ids와 동시 사용 불가")
    parser.add_argument(
        "--station-ids", default=None,
        help="정류소 ID 여러 개, 쉼표로 구분 (예: ST-1234,ST-5678) — 이 목록에 속한 대여소만 한 번에 배치 예측. "
        "--station-id와 동시 사용 불가, 둘 다 미지정시 전체 정류소",
    )
    parser.add_argument("--start-date", default=config.TEST_START, help="YYYY-MM-DD, 기본값: 테스트 기간 시작")
    parser.add_argument("--end-date", default=config.TEST_END, help="YYYY-MM-DD, 기본값: 테스트 기간 끝")
    parser.add_argument("--hour", type=int, default=None, help="특정 시(0~23)만 조회하고 싶을 때")
    parser.add_argument(
        "--horizon", type=int, default=1,
        help="몇 시간 뒤 예측을 조회할지 (1~HORIZON_COUNT, 기본값 1 — 기존 단일 horizon "
        "챔피언과 동일한 조회 범위). multi-horizon 테이블엔 horizon별 행이 섞여 있어 "
        "반드시 하나로 골라야 한다.",
    )
    parser.add_argument("--out", default=None, help="결과 저장 S3 키(parquet). 미지정시 기본 경로")
    args = parser.parse_args()

    if args.station_id and args.station_ids:
        raise SystemExit("--station-id와 --station-ids는 동시에 지정할 수 없습니다.")
    if not (1 <= args.horizon <= config.HORIZON_COUNT):
        raise SystemExit(f"--horizon은 1~{config.HORIZON_COUNT} 사이여야 합니다: {args.horizon}")

    df = s3_io.read_parquet(config.MULTI_HORIZON_FEATURES_TABLE_PARQUET)
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {config.MULTI_HORIZON_FEATURES_TABLE_PARQUET}")
    df = df[(df["date"] >= args.start_date) & (df["date"] <= args.end_date) & (df["horizon"] == args.horizon)]
    if args.station_id:
        df = df[df["station_id"] == args.station_id]
        if df.empty:
            raise SystemExit(
                f"station_id '{args.station_id}' 데이터가 없습니다 — "
                "2025년에 실제 트립이 있던 정류소인지 확인하세요 (station_master.parquet 참고)."
            )
    elif args.station_ids:
        requested = [s.strip() for s in args.station_ids.split(",") if s.strip()]
        df = df[df["station_id"].isin(requested)]
        found = set(df["station_id"].unique())
        missing = [s for s in requested if s not in found]
        if missing:
            raise SystemExit(
                f"station_id {missing} 데이터가 없습니다 — "
                "2025년에 실제 트립이 있던 정류소인지 확인하세요 (station_master.parquet 참고)."
            )
    if args.hour is not None:
        df = df[df["hour"] == args.hour]
    df = df.reset_index(drop=True)

    preds = predict(df, model_name, exposure_col=exposure_col)
    preds["actual"] = df[target_col].to_numpy()

    out_path = args.out if args.out else default_output
    s3_io.write_parquet(preds, out_path)
    print(f"예측 {len(preds):,}행 -> {out_path}")

    if len(preds) <= 48:
        pd.set_option("display.width", 160)
        print(preds.to_string(index=False))
    else:
        print_metrics(preds)

    return preds
