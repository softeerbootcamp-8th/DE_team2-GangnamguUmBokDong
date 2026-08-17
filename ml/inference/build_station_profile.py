"""정류소 x 시간대 x 요일 x 월별 대여/반납 실적 프로필 ("평소 패턴") 생성.

predict_single.py에서 실시간 실적 히스토리가 끊기거나 지연됐을 때, lag/rolling
feature를 결측(NaN) 대신 "그 정류소가 이 달 이 요일 이 시간에 보통 어느 정도
였는지"로 채우기 위한 fallback 테이블이다.

**월(month)을 반드시 그룹 키에 포함해야 한다** — 계절에 따라 대여량 자체가
크게 달라지기 때문이다 (실측 1월 2,291건/일 vs 6월 5,589건/일,
약 2.44배 차이). station x hour x dow로만 묶으면 1월의 결측치와 6월의 결측치가
똑같은 "연간 평균"으로 채워져 계절성이 통째로 사라지는 문제가 생긴다. 대신
station x hour x dow x month로 묶으면 표본이 그 달에 그 요일이 나온 횟수
(보통 4~5회)로 줄어드는 트레이드오프가 있지만, 계절성을 무시하는 것보다는
훨씬 낫다.
"""

import pandas as pd
from core import s3 as s3_io

from . import config


def build_station_profile() -> pd.DataFrame:
    """station x hour x dow x month별 대여/반납 평균·표준편차 프로필을 만든다.

    returns:
        pd.DataFrame: station_id, hour, dow, month, rental_mean, rental_std,
            return_mean, return_std, n_samples
    """
    df = s3_io.read_parquet(
        config.MERGED_TABLE_PARQUET, columns=["station_id", "date", "hour", "rental_count", "return_count"]
    )
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {config.MERGED_TABLE_PARQUET}")
    dt = pd.to_datetime(df["date"])
    # .dt.dayofweek/.dt.month는 기본 int32를 낸다 — ml_common/model_contract.NATIVE_COLUMN_DTYPES와
    # 맞춰 int8로(둘 다 0~11 범위라 여유 있음).
    df["dow"] = dt.dt.dayofweek.astype("int8")
    df["month"] = dt.dt.month.astype("int8")

    profile = df.groupby(["station_id", "hour", "dow", "month"], observed=True).agg(
        rental_mean=("rental_count", "mean"),
        rental_std=("rental_count", "std"),
        return_mean=("return_count", "mean"),
        return_std=("return_count", "std"),
        n_samples=("rental_count", "size"),
    ).reset_index()

    # 표본이 1개뿐이면 std가 NaN이 되므로 0으로 채움 (변동성 없다고 간주)
    profile[["rental_std", "return_std"]] = profile[["rental_std", "return_std"]].fillna(0.0)

    # groupby().mean()/std()는 정수 컬럼(rental_count 등 int16)을 집계해도
    # 항상 float64를 낸다 — ml_common/model_contract.FEATURE_COLUMN_DTYPES와 맞춰
    # float32로 다운캐스트(predict_single.py가 이 값을 그대로 lag/rolling
    # feature 자리에 fallback으로 채우므로).
    mean_std_cols = ["rental_mean", "rental_std", "return_mean", "return_std"]
    profile[mean_std_cols] = profile[mean_std_cols].astype("float32")
    profile["n_samples"] = profile["n_samples"].astype("int32")

    s3_io.write_parquet(profile, config.STATION_HOURLY_PROFILE_PARQUET)
    print(
        f"station_hourly_profile: {profile.shape[0]:,}행 "
        f"({df['station_id'].nunique()}개 정류소 x 24시간 x 7요일 x 12개월), "
        f"그룹당 표본 수 min={profile['n_samples'].min()} max={profile['n_samples'].max()} "
        f"-> {config.STATION_HOURLY_PROFILE_PARQUET}"
    )
    return profile


if __name__ == "__main__":
    build_station_profile()
