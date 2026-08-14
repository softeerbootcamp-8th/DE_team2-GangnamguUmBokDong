"""대여이력 원본(트립 단위) parquet -> station_id 매칭된 (start_dt, end_dt) 로딩.

**왜 공유하는가**: `feature_engineering/spark/build_targets.py`(타겟 생성)와
`feature_engineering/spark/build_rolling_rental_features.py`(배치 point-in-time 카운트)가 배치로
쓰는 것과, `inference/predict_single.py`가 실시간 서빙을 흉내내는 point-in-time
censoring 계산을 위해 최근 트립을 조회하는 것이 **같은 크로스워크 로직**(대여소번호
정규화 + station_master 매칭)을 써야 한다 — 트립 파일 재로딩/매칭 로직을 두 곳에서
따로 구현하면 한쪽만 고치고 잊어버려 조용히 갈라질 위험이 있다.
"""

import pandas as pd

from . import paths


def normalize_station_no(series: pd.Series) -> pd.Series:
    """대여소번호 컬럼을 5자리 zero-padding 문자열로 정규화한다 ('\\N'은 결측 처리).

    args:
        series: start_st 또는 end_st 컬럼 (문자열, 0-padding 불일치 + '\\N' 포함 가능)
    returns:
        pd.Series: 5자리로 zfill된 문자열, '\\N'이었던 자리는 NaN
    """
    numeric = series.where(series != "\\N")
    return numeric.astype("Int64").astype(str).str.zfill(5).where(numeric.notna())


def load_rental_trip_events(verbose: bool = True) -> pd.DataFrame:
    """대여이력에서 station_id 매칭된 (start_dt, end_dt, station_id)만 추린다.

    args:
        verbose: True면 월별 매칭 건수를 출력한다 (배치 스크립트용 기본값).
            inference/predict_single.py처럼 대화형/반복 호출되는 곳에서는 False로
            호출해 출력을 줄인다.
    returns:
        pd.DataFrame: station_id, start_dt, end_dt (station_master와 매칭 안 되는
            약 5~7%의 트립은 제외됨)
    """
    master = pd.read_parquet(paths.STATION_MASTER_PARQUET)
    no_to_id = dict(zip(master["station_no"], master["station_id"]))

    frames = []
    for ym in paths.TRAIN_MONTHS:
        path = paths.RENTAL_PARQUET_DIR / f"서울특별시 공공자전거 대여이력 정보_{ym}.parquet"
        df = pd.read_parquet(path, columns=["start_dt", "start_st", "end_dt"])
        station_id = normalize_station_no(df["start_st"]).map(no_to_id)
        matched = pd.DataFrame({"station_id": station_id, "start_dt": df["start_dt"], "end_dt": df["end_dt"]})
        matched = matched.dropna(subset=["station_id"])
        frames.append(matched)
        if verbose:
            print(f"  {ym}: {len(df):,}건 중 {len(matched):,}건 매칭")

    return pd.concat(frames, ignore_index=True)
