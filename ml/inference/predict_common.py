"""배치 조회 CLI(`predict_rental_demand.py`/`predict_return_demand.py`)가 공유하는 실행기.

실제 채점 로직(`predict()`, booster 로드 등)은 `ml_core/scoring.py`(training의
`monitor_performance.py`/`compare_baselines.py`도 같이 씀)에 있다 — 이 모듈은 그 위에
"station_id/기간을 골라 조회 -> 저장 -> 요약 출력"하는 배치 CLI 경험만 얹는다.

이 프로젝트엔 실시간 서빙 API가 없으므로, CLI(`run_predict_cli`)는 이미 구축된
`station_hour_features_multihorizon_{rental,return}_2025.parquet`(feature_engine이
만든 multi-horizon 학습 테이블 — horizon=1..HORIZON_COUNT가 섞여 있음, model_name에
맞는 쪽을 읽음)에서 station_id/기간/horizon을 골라
예측을 뽑아보는 용도다. 그 범위를 벗어난 날짜나 날씨·인구 데이터가 없는 미래 시점은
예측할 수 없다 — 그러려면 해당 시점의 날씨·인구·최근 실적 데이터를 먼저
`feature_engine`의 피처마트 생성 파이프라인(`feature_engine/spark/`)으로
넣어줘야 한다(그런 임의 시점 예측은 `predict_single.py`가 담당).

**station_id(텍스트)는 multi-horizon 테이블 자체엔 없다** — horizon self-join으로
최대 HORIZON_COUNT배까지 불어나는 큰 테이블이라 station_no(정수)만 담는다
(`feature_engine/spark/build_multi_horizon_features.py` 모듈 docstring 참고).
이 CLI는 사람이 station_id로 조회/확인하는 용도라, 작은 station_master만 따로 읽어
`--station-id`/`--station-ids`를 station_no로 바꿔 큰 테이블을 필터링하고, 결과를
출력하기 직전에 station_id를 다시 붙인다.
"""

import argparse

import pandas as pd
from core import s3 as s3_io
from ml_core.paths import STATION_MASTER_PARQUET
from ml_core.scoring import predict, print_metrics

from . import config

_TRAINING_TABLE_BY_MODEL = {
    "rental": config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    "return": config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
}


def _load_station_master() -> pd.DataFrame:
    """station_id<->station_no 크로스워크만 필요한 만큼만 읽는다 (작은 테이블).

    station_master.parquet의 station_no는 원본(Silver) 타입 그대로 저장돼 있어
    "00001"처럼 앞자리 0이 있는 문자열일 수 있다 — feature_engine이 multi-horizon
    테이블을 만들 때는 이걸 정수로 캐스팅해서 쓰므로(build_merged_table.py), 여기서도
    int로 통일해야 station_no 기준 join/필터가 실제로 매칭된다.
    """
    master = s3_io.read_parquet(STATION_MASTER_PARQUET, columns=["station_id", "station_no"])
    if master is None:
        raise FileNotFoundError(f"S3에 없음: {STATION_MASTER_PARQUET}")
    master["station_no"] = master["station_no"].astype(int)
    return master


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

    master = _load_station_master()

    table_path = _TRAINING_TABLE_BY_MODEL[model_name]
    # date_range 없이 부르면 "prefix 전체 나열" 경로를 타서 (1) date 파티션 컬럼이
    # 파일 내용에 없어 아예 복원이 안 되고(바로 아래 필터에서 KeyError, 리뷰 지적)
    # (2) 조회 범위 밖 파티션까지 전부 받아온다 — date_range로 필요한 날짜만 미리
    # 좁혀서 읽는다(core.s3._read_parquet_by_dates()가 date를 복원해줌).
    df = s3_io.read_parquet(table_path, date_range=(args.start_date, args.end_date))
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {table_path} ({args.start_date}~{args.end_date})")
    df = df[df["horizon"] == args.horizon]
    if args.station_id:
        match = master[master["station_id"] == args.station_id]
        if match.empty:
            raise SystemExit(f"station_id '{args.station_id}'를 station_master에서 찾을 수 없습니다.")
        df = df[df["station_no"].isin(match["station_no"])]
        if df.empty:
            raise SystemExit(
                f"station_id '{args.station_id}' 데이터가 없습니다 — "
                "2025년에 실제 트립이 있던 정류소인지 확인하세요 (station_master.parquet 참고)."
            )
    elif args.station_ids:
        requested = [s.strip() for s in args.station_ids.split(",") if s.strip()]
        matched_master = master[master["station_id"].isin(requested)]
        unknown = [s for s in requested if s not in set(matched_master["station_id"])]
        if unknown:
            raise SystemExit(f"station_id {unknown}를 station_master에서 찾을 수 없습니다.")
        df = df[df["station_no"].isin(matched_master["station_no"])]
        found_nos = set(df["station_no"].unique())
        missing = [
            s for s in requested
            if matched_master.loc[matched_master["station_id"] == s, "station_no"].iloc[0] not in found_nos
        ]
        if missing:
            raise SystemExit(
                f"station_id {missing} 데이터가 없습니다 — "
                "2025년에 실제 트립이 있던 정류소인지 확인하세요 (station_master.parquet 참고)."
            )
    if args.hour is not None:
        df = df[df["hour"] == args.hour]
    df = df.reset_index(drop=True)

    if df.empty:
        # station_id(s)를 지정 안 한 경로(전체 정류소)는 위 station_id/station_ids
        # 분기의 개별 빈-결과 체크를 안 거치므로, 여기서 한 번 더 걸러야 --hour나
        # 날짜 범위만으로 조용히 0행짜리 predict()/parquet 저장이 되는 걸 막는다.
        raise SystemExit(
            f"조회 조건에 맞는 데이터가 없습니다 (기간 {args.start_date}~{args.end_date}, "
            f"horizon={args.horizon}{f', hour={args.hour}' if args.hour is not None else ''}) — "
            "feature mart 범위/조건을 확인하세요."
        )

    preds = predict(df, model_name, exposure_col=exposure_col)
    preds["actual"] = df[target_col].to_numpy()
    # predict()는 station_no만 반환한다 — 사람이 보는 출력/저장 결과엔 station_id를
    # 다시 붙여준다(작은 station_master와의 join이라 비용은 무시할 만함).
    preds = preds.merge(master, on="station_no", how="left")
    preds = preds[["station_id", "station_no", *[c for c in preds.columns if c not in ("station_id", "station_no")]]]

    out_path = args.out if args.out else default_output
    s3_io.write_parquet(preds, out_path)
    print(f"예측 {len(preds):,}행 -> {out_path}")

    if len(preds) <= 48:
        pd.set_option("display.width", 160)
        print(preds.to_string(index=False))
    else:
        print_metrics(preds)

    return preds
