"""Collector Silver(S3)로부터 "1차 정제" 산출물(station_master/targets/station_status/
weather/population)을 직접 만든다 — `config.py`가 예전에 "이미 S3에 존재한다고 가정"
하던 입력을 이제 이 모듈이 Silver 원본에서 만들어낸다. 이 패키지는 이제
`ml/data/processed_v2/*` 같은 로컬 파생 데이터를 전혀 보지 않는다 — Silver만 본다.

컬럼명 매핑(`RENT_DT`->`start_dt` 등)은 `libs/ml_common/silver_schema.py`의
COLUMN_MAP들을 그대로 재사용한다 — `inference/predict_single.py`의 실시간 조회가
같은 Silver를 같은 이름으로 읽으므로, 매핑을 한 곳(`ml_common`)에만 두고 두 패키지가
같이 참조해야 스키마가 바뀔 때 한쪽만 고치고 잊어버리는 사고를 막는다.

Spark는 boto3로 파일 하나씩 긁는 `inference`와 달리 **glob 하나로 한 해 전체의
조각 파일 수천~수만 개를 한 번에** 읽는다(`spark.read.parquet("s3a://.../dt=2025-*/hh=*/*.parquet")`)
— 이게 이 패키지가 Spark를 쓰는 이유 그 자체다.

`bike_station_realtime`/`weather_ultra_short_term`은 파일 **내용에 시각 컬럼이
없다**(예시 데이터로 확인 — 시각은 S3 키 경로의 `dt=/hh=/HHMM`에만 있음). Spark의
`input_file_name()`으로 각 행이 어느 파일에서 왔는지 알아내 거기서 시각을
역추출한다(`_tick_from_path()`).
"""

from __future__ import annotations

from ml_common import silver_schema
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config


def _silver_glob(source_id: str) -> str:
    """한 해 전체의 Silver 조각 파일을 한 번에 잡는 glob 패턴.

    `config.SILVER_ROOT` 아래에서 찾는다(테스트가 로컬 tmp_path로 monkeypatch하기
    쉽도록 이 상수 하나로 루트를 분리해뒀다). `config.TRAIN_YEAR`로 연도만 좁히고
    (그 외엔 전체 히스토리) `since` 필터는 각 read_*() 함수가 실제 timestamp 컬럼
    기준으로 읽은 뒤에 건다 — glob 자체는 날짜 기준 partition pruning을 안 하므로,
    연 단위보다 더 좁은 조회가 필요해지면 이 함수를 연/월까지 받도록 넓혀야 한다.
    """
    return f"{config.SILVER_ROOT}/{source_id}/dt={config.TRAIN_YEAR}-*/hh=*/*.parquet"


def _rename(df: DataFrame, column_map: dict[str, str]) -> DataFrame:
    """ml_common.silver_schema의 COLUMN_MAP({원본: 우리이름})을 그대로 적용한다."""
    for src, dst in column_map.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)
    return df


def _tick_from_path() -> Column:
    """`input_file_name()`에서 `dt=YYYY-MM-DD/hh=HH/HHMM.parquet` 부분을 파싱해
    그 파일이 대표하는 시각(timestamp_ntz)을 만든다.

    `bike_station_realtime`/`weather_ultra_short_term`처럼 시각이 파일 내용이
    아니라 S3 키 경로에만 있는 소스에 쓴다.
    """
    path = F.input_file_name()
    dt_str = F.regexp_extract(path, r"dt=(\d{4}-\d{2}-\d{2})", 1)
    hhmm_str = F.regexp_extract(path, r"/(\d{4})\.parquet$", 1)
    ts_str = F.concat(
        dt_str, F.lit(" "), F.substring(hhmm_str, 1, 2), F.lit(":"), F.substring(hhmm_str, 3, 2), F.lit(":00")
    )
    return F.to_timestamp(ts_str, "yyyy-MM-dd HH:mm:ss").cast("timestamp_ntz")


def read_station_master(spark: SparkSession) -> DataFrame:
    """Silver `station/station_master.parquet`(정적 파일 1개)를 우리 컬럼명으로 읽는다.

    returns:
        DataFrame: station_id, station_no, station_name, capacity, lat, lon, grid_id
    """
    station_master_suffix = silver_schema.STATION_MASTER_KEY.removeprefix("silver/")
    df = spark.read.parquet(f"{config.SILVER_ROOT}/{station_master_suffix}")
    df = _rename(df, silver_schema.STATION_COLUMN_MAP)
    return df.select("station_id", "station_no", "station_name", "capacity", "lat", "lon", "grid_id")


def read_rental_trips(spark: SparkSession, since: str | None = None) -> DataFrame:
    """Silver `bike_rental_history`(5분 tick)를 station_id 매칭까지 끝낸 트립 단위로 읽는다.

    `RENT_STATION_ID`/`RETURN_STATION_ID`가 이미 `station_id`와 같은 형식이라
    (`ml-integration-requests.md` 1번) 예전처럼 station_no 크로스워크를 안 하고,
    `station_master`에 실제로 있는 station_id인지만 확인한다 — 대여 쪽
    (`station_id`)이 안 걸리면 그 트립 자체를 버리고, 반납 쪽(`end_station_id`)만
    안 걸리면 그 방향만 null로 남긴다(`inference/predict_single.py`의
    `_resolve_rental_stations()`와 동일한 원칙).

    tick 파일들이 델타(그 5분간 새 트립)인지 누적(그날 트립 전체 재수록)인지
    실제 예시 데이터만으로 확정 못 했다(`ml-integration-requests.md` 10번) — 안전하게
    `(bike_id, start_dt)` 기준 중복 제거를 항상 적용한다.

    args:
        spark: SparkSession
        since: 지정하면 start_dt >= since인 트립만 남긴다(증분 재계산용)
    returns:
        DataFrame: station_id(대여), end_station_id(반납, 없으면 null), start_dt, end_dt
    """
    master_ids = read_station_master(spark).select("station_id")

    df = spark.read.parquet(_silver_glob(silver_schema.RENTAL_SOURCE_ID))
    df = _rename(df, silver_schema.RENTAL_COLUMN_MAP)
    df = df.select(
        F.to_timestamp("start_dt", "yyyy-MM-dd HH:mm:ss").cast("timestamp_ntz").alias("start_dt"),
        F.to_timestamp("end_dt", "yyyy-MM-dd HH:mm:ss").cast("timestamp_ntz").alias("end_dt"),
        F.col("start_st").alias("station_id"),
        F.col("end_st").alias("end_station_id"),
        "bike_id",
    )
    if since is not None:
        df = df.filter(F.col("start_dt") >= F.lit(since))
    df = df.dropDuplicates(["bike_id", "start_dt"]).drop("bike_id")

    df = df.join(master_ids, on="station_id", how="inner")

    # end_station_id는 "매칭 안 되면 null로 남긴다"가 목표라 단순 LEFT JOIN으론 안 된다
    # — 조인 키 하나짜리 우변과 조인하면 매칭 실패해도 원래 값이 그대로 남는다(매칭
    # 성공 여부를 구분할 별도 컬럼이 없어서). 매칭 여부를 나타내는 표시 컬럼을 하나
    # 만들어 조인한 뒤, 그 표시가 없는 행만 null로 덮어쓴다.
    known_end = master_ids.withColumnRenamed("station_id", "end_station_id").withColumn("_end_matched", F.lit(True))
    df = df.join(known_end, on="end_station_id", how="left")
    df = df.withColumn("end_station_id", F.when(F.col("_end_matched"), F.col("end_station_id")))
    return df.select("station_id", "end_station_id", "start_dt", "end_dt")


def _pick_first_per_hour(df: DataFrame, tick_col: str, partition_cols: list[str]) -> DataFrame:
    """(partition_cols, hour) 그룹마다 tick_col이 가장 이른 행 하나만 남긴다.

    `bike_station_realtime`/`weather_ultra_short_term`은 5~10분 tick이지만, 지금
    다운스트림(`build_merged_table.py`)이 기대하는 station_status/weather 입력은
    시간(hour) 단위 스냅샷 하나다 — 그 시간의 첫 tick을 대표값으로 쓴다(그 시간
    동안 값이 유지된다고 forward-fill하는 것과 동치, `build_merged_table.py`의
    `_expand_hourly_to_ticks()`가 그 forward-fill을 이미 담당).
    """
    window = Window.partitionBy(*partition_cols, "hour_ts").orderBy(tick_col)
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", tick_col)
    )


def read_station_status(spark: SparkSession, since: str | None = None) -> DataFrame:
    """Silver `bike_station_realtime`(5분 tick)에서 정류소별 시간당 재고 스냅샷을 만든다.

    args:
        spark: SparkSession
        since: 지정하면 hour_ts >= since인 시간대만 남긴다(증분 재계산용)
    returns:
        DataFrame: station_id, hour_ts, bike_count, stockout_flag
    """
    df = spark.read.parquet(_silver_glob(silver_schema.BIKE_REALTIME_SOURCE_ID))
    df = _rename(df, silver_schema.BIKE_REALTIME_COLUMN_MAP)
    df = df.withColumn("_tick", _tick_from_path())
    df = df.withColumn("hour_ts", F.date_trunc("hour", F.col("_tick")))
    if since is not None:
        df = df.filter(F.col("hour_ts") >= F.lit(since))
    df = df.select("station_id", "hour_ts", "bike_count", "_tick")
    df = _pick_first_per_hour(df, "_tick", ["station_id"])
    df = df.withColumn("stockout_flag", (F.col("bike_count") <= 0).cast("byte"))
    return df.select("station_id", "hour_ts", "bike_count", "stockout_flag")


def read_weather(spark: SparkSession, since: str | None = None) -> DataFrame:
    """Silver `weather_ultra_short_term`(10분 tick, 서울 전체 공통)에서 시간당 관측값을 만든다.

    `weather_short_term_forecast`(예보, 3시간)는 강수량이 아니라 강수확률만 있어
    아직 안 쓴다(`ml-integration-requests.md` 6번) — `inference/predict_single.py`의
    `_get_recent_weather()`와 같은 소스 선택.

    args:
        spark: SparkSession
        since: 지정하면 hour_ts >= since인 시간대만 남긴다(증분 재계산용)
    returns:
        DataFrame: hour_ts, temp, precip, wind, humidity
    """
    df = spark.read.parquet(_silver_glob(silver_schema.WEATHER_SOURCE_ID))
    df = _rename(df, silver_schema.WEATHER_COLUMN_MAP)
    df = df.withColumn("_tick", _tick_from_path())
    df = df.withColumn("hour_ts", F.date_trunc("hour", F.col("_tick")))
    if since is not None:
        df = df.filter(F.col("hour_ts") >= F.lit(since))
    df = df.select("hour_ts", "temp", "precip", "wind", "humidity", "_tick")
    df = _pick_first_per_hour(df, "_tick", [])
    return df.withColumn("humidity", F.round(F.col("humidity")).cast("int")).select(
        "hour_ts", "temp", "precip", "wind", "humidity"
    )


def read_population(spark: SparkSession, since: str | None = None) -> DataFrame:
    """Silver `living_population_grid`(하루 1개 파일, YMD+TT로 24시간 내장)에서 격자x시간 인구를 만든다.

    실제 예시 데이터에서 두 가지를 확인했다(`ml-integration-requests.md` 8번):
    (1) 나이대x성별(`M00`~`F70`) breakdown만 있고 pop_resd/pop_long_foreign/
    pop_short_foreign 구분이 없다 — `SPOP`(총 인구)만 `pop_total`로 쓰고, `pop_resd`는
    `pop_total`과 같다고 근사(전부 내국인으로 간주)한 뒤 나머지 둘은 0으로 둔다.
    (2) 파일의 수집일(`dt=`)과 내용의 `YMD`가 다를 수 있다(공표 지연, 예시 기준
    4일 정도) — 같은 (grid_id, hour_ts)가 서로 다른 수집일 파일에 걸쳐 중복
    등장할 수 있어, **가장 최근에 수집된 값**을 우선하도록 수집일 기준으로
    중복을 제거한다.

    args:
        spark: SparkSession
        since: 지정하면 hour_ts >= since인 시간대만 남긴다(증분 재계산용)
    returns:
        DataFrame: grid_id, hour_ts, pop_resd, pop_long_foreign, pop_short_foreign, pop_total
    """
    df = spark.read.parquet(_silver_glob(silver_schema.POPULATION_SOURCE_ID))
    df = df.withColumn(
        "_collected_dt",
        F.to_date(F.regexp_extract(F.input_file_name(), r"dt=(\d{4}-\d{2}-\d{2})", 1)),
    )
    df = _rename(df, silver_schema.POPULATION_COLUMN_MAP)
    df = df.withColumn(
        "hour_ts",
        F.to_timestamp(F.concat_ws(" ", F.col("YMD"), F.col("TT")), "yyyyMMdd HH").cast("timestamp_ntz"),
    )
    if since is not None:
        df = df.filter(F.col("hour_ts") >= F.lit(since))
    df = df.select("grid_id", "hour_ts", "pop_total", "_collected_dt")

    window = Window.partitionBy("grid_id", "hour_ts").orderBy(F.col("_collected_dt").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_collected_dt")
    )
    df = df.withColumn("pop_resd", F.col("pop_total"))
    df = df.withColumn("pop_long_foreign", F.lit(0.0))
    df = df.withColumn("pop_short_foreign", F.lit(0.0))
    return df.select("grid_id", "hour_ts", "pop_resd", "pop_long_foreign", "pop_short_foreign", "pop_total")
