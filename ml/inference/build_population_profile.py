"""격자 x 시간대 x 요일별 생활인구 평균 프로필 ("평소 인구") 생성.

predict_single.py에서 실시간(또는 최신) 인구 데이터가 없을 때 `population`
인자 없이도 예측할 수 있도록, "이 격자가 이 요일 이 시간에 보통 인구가 얼마나
있었는지"로 대체하기 위한 fallback 테이블이다.

station 프로필(build_station_profile.py)과 달리 **month는 그룹 키에 넣지
않는다** — 실측 기준 생활인구는 월별로는 최대/최소 비율이 1.05배에 불과해
거의 안 변하고, 시간대별로는 1.42배(출퇴근 패턴)로 크게 변한다. 즉 인구는
계절성보다 시간대 패턴이 압도적으로 지배적이라 month을 추가해도 얻는 게
적고, 오히려 표본 수만 station 프로필처럼 4~5개로 줄어드는 손해가 크다.
grid_id x hour x dow만으로도 격자당 표본이 연간 약 52개(그 요일이 1년에
나온 횟수)로 충분히 안정적이다.
"""

import pandas as pd
from ml_common import s3_io

from . import config


def build_population_profile() -> pd.DataFrame:
    """grid_id x hour x dow별 생활인구 평균 프로필을 만든다.

    returns:
        pd.DataFrame: grid_id, hour, dow, pop_resd_mean, pop_long_foreign_mean,
            pop_short_foreign_mean, pop_total_mean, n_samples
    """
    df = s3_io.read_parquet(config.POPULATION_PARQUET)
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {config.POPULATION_PARQUET}")
    # .dt.dayofweek/.dt.hour는 기본 int32를 낸다 — ml_common/model_contract.NATIVE_COLUMN_DTYPES와
    # 맞춰 int8로.
    df["dow"] = df["hour_ts"].dt.dayofweek.astype("int8")
    df["hour"] = df["hour_ts"].dt.hour.astype("int8")

    profile = df.groupby(["grid_id", "hour", "dow"], observed=True).agg(
        pop_resd_mean=("pop_resd", "mean"),
        pop_long_foreign_mean=("pop_long_foreign", "mean"),
        pop_short_foreign_mean=("pop_short_foreign", "mean"),
        pop_total_mean=("pop_total", "mean"),
        n_samples=("pop_total", "size"),
    ).reset_index()

    # groupby().mean()은 float32 원본(pop_resd 등)을 집계해도 항상 float64를
    # 낸다 — ml_common/model_contract.FEATURE_COLUMN_DTYPES와 맞춰 다운캐스트.
    mean_cols = ["pop_resd_mean", "pop_long_foreign_mean", "pop_short_foreign_mean", "pop_total_mean"]
    profile[mean_cols] = profile[mean_cols].astype("float32")
    profile["n_samples"] = profile["n_samples"].astype("int32")

    s3_io.write_parquet(profile, config.POPULATION_HOURLY_PROFILE_PARQUET)
    print(
        f"population_hourly_profile: {profile.shape[0]:,}행 "
        f"({df['grid_id'].nunique()}개 격자 x 24시간 x 7요일), "
        f"그룹당 표본 수 min={profile['n_samples'].min()} max={profile['n_samples'].max()} "
        f"-> {config.POPULATION_HOURLY_PROFILE_PARQUET}"
    )
    return profile


if __name__ == "__main__":
    build_population_profile()
