"""모든 소스를 station x tick(5분 간격) 기준으로 병합해 최종 feature 테이블의 입력을 만든다 (PySpark 포팅).

**그리드가 시간(hour) 단위에서 5분 tick 단위로 바뀌었다** — `src/build_merged_table.py`
(pandas)와 동일한 이유: "3시 40분 기준으로 앞으로 1시간"처럼 임의의 5분 단위 기준
시각에서 예측하려면 그리드 자체가 그 해상도여야 한다(타겟은 이미
`build_targets.py`에서 5분 tick sparse step function으로 바뀜). `hour_ts` 컬럼명은
그대로 두지만(다른 파일들과의 접점이 많아 이름을 바꾸지 않음) 이제 5분 단위로도
값을 가진다.

`src/build_merged_table.py`(pandas, 로컬 검증용)와 **의도적으로 다른 부분이 하나
있다** — station 활성 구간(그리드) 정의:

- **기존(pandas, src/)**: "2025년에 트립이 1건이라도 있는 station"이면 1월 1일~
  12월 31일 전체를 그리드에 넣고, 빈 시간은 rental/return_count=0으로 채운다.
- **여기(수정됨)**: `station_status`(재고 스냅샷)에 **실제로 관측 기록이 있는
  (station_id, hour_ts)만** tick으로 펼쳐 그리드로 쓴다.

이렇게 바꾼 이유: 실측 결과 활성 station 중 일부가 연중에 폐쇄/임시휴업으로
재고 스냅샷이 통째로 몇 주~몇 달씩 끊긴다. 기존 방식대로 그 구간까지 그리드에
넣고 rental_count=0으로 채우면, "서비스가 없었던 기간"을 "서비스는 있었는데 수요가
0이었던 기간"으로 잘못 학습시키게 된다. station을 "폐쇄됐다"고 분류하거나 목록에서
빼지는 않는다(재개될 수 있으므로) — 그저 **기록이 없는 시간은 그리드에 아예 안
넣어서** 학습에서 자연스럽게 제외한다.

**타겟 조회가 sparse step function이라는 점이 pandas와 동일하게 중요하다** — 타겟
parquet(`TARGETS_PARQUET`/`RETURN_TARGETS_PARQUET`)은 이제 (station_id, hour_ts,
rental/return_count) dense 테이블이 아니라 (station_id, tick, count) sparse
step function이라, 그리드의 각 (station, tick)에 대해 `lookup_count_at_ticks()`로
"그 tick 이하 중 가장 최근 delta 이후의 값"을 조회해야 한다. **증분(`since`) 실행
시에도 이 타겟 parquet 자체는 절대 `since`로 필터링하면 안 된다** — 필터링하면
`since` 이전의 마지막 delta를 잃어버려, `since` 직후 tick들의 조회값이 깨진다
(그리드/날씨/인구처럼 "그 시점 이후 데이터만 있으면 되는" 소스와 다름).

날씨는 실제 수집 tick별 서울 평균을 보존한 뒤, 각 관측을 다음 관측 직전 또는
최대 3시간까지만 5분 grid로 과거 방향 forward-fill해 exact join한다. 따라서 같은
시간 안의 08:55 관측이 08:00~08:50 행에 역전파되지 않는다. 생활인구만 원본의
시간 단위 값을 정시로 내려서 그 시간의 tick들과 join한다.

**메모리(dtype 최적화)**: `src/build_merged_table.py`의 `NATIVE_COLUMN_DTYPES`
(값 범위 실측 기반 다운캐스트)와 동일한 Spark 타입 매핑을 적용한다 — 값 범위는
그대로, 자료형만 줄여서 EMR에서의 셔플/저장 비용을 낮춘다. 같은 이유로 이후
단계에서 안 쓰는 wind/humidity/pop 세부분류(pop_resd/pop_long_foreign/
pop_short_foreign)는 원본 join 직후 즉시 버린다(뒤로 전파시키지 않음).
"""

from datetime import datetime, timedelta

from ml_core import silver_schema
from ml_core.day_index import DAY_INDEX_EPOCH
from ml_core.holidays_kr import korean_holidays
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ByteType, FloatType, ShortType

from . import config
from .rolling_window_features import (
    _seconds_to_ntz,
    _unix_seconds_ntz,
    lookup_count_at_ticks,
)

# src/build_merged_table.py의 NATIVE_COLUMN_DTYPES와 동일한 근거(값 범위 실측)로
# 매핑한 Spark 타입 — bike_count 0~478, rental/return_count 0~245 등은 전부
# ShortType/ByteType 범위 안. day(2000-01-01 기준 경과일수, ml_core.day_index)/
# station_no도 ShortType(부호 있는 16비트, 지금 값 범위엔 충분) — Spark엔 unsigned
# 타입이 없어(uint 계열 자체가 없음) 이 값 그대로가 최종 저장 타입이고, pandas/학습
# 쪽 model_contract.NATIVE_COLUMN_DTYPES도 같은 int16으로 맞춘다(uint16으로 선언해도
# 여기서 이미 int16 범위로 잘려 있어 의미가 없다).
NATIVE_COLUMN_DTYPES = {
    "bike_count": ShortType(),
    "stockout_flag": ByteType(),
    "rental_count": ShortType(),
    "return_count": ShortType(),
    # capacity(거치대 수)는 원래 "LEFT JOIN이라 결측 가능"이라는 이유로 float32였는데,
    # 실제로는 이 join이 결측을 낼 수 없다 — active_station_ids(rental_targets/
    # return_targets의 station_id 합집합)는 이미 silver_source.read_rental_trips()의
    # INNER JOIN(대여 쪽)과 build_targets.py의 end_station_id null 필터(반납 쪽)를
    # 거쳐서 항상 station_master의 부분집합이다. status도 그 active_station_ids로
    # 한 번 더 걸러지므로, 여기 capacity를 채우는 LEFT JOIN 시점엔 좌변의 station_id
    # 집합이 이미 master의 부분집합이라 이 join이 실제로 결측을 낼 수 없다(2026-08
    # 확인, 코드 경로 추적 — station_no와 결측 이유가 아예 없다는 점이 다름).
    # 항상 정수(거치대 개수)라 ShortType으로 줄인다 — 최대 대여소 규모는 실측 데이터
    # 없이 확인 못 했지만 int16(32,767) 여유는 충분히 안전하다고 판단.
    "capacity": ShortType(),
    "lat": FloatType(),
    "lon": FloatType(),
    "temp": FloatType(),
    "precip": FloatType(),
    "pop_total": FloatType(),
    "hour": ByteType(),  # 더 이상 모델 feature가 아니다(minute이 대체) — 출력/CLI 식별용
    "minute": ShortType(),  # 자정 기준 경과분(0~1439, ml_core.minute_of_day) — 실제 모델 feature
    "dow": ByteType(),
    "is_holiday": ByteType(),
    "day": ShortType(),
    # station_id(텍스트, "ST-2565")는 모델 feature에서 station_no(정수 일련번호)로
    # 대체됐다 — Parquet dictionary encoding이 to_pandas()에서 안 살아남아 매 학습
    # 읽기마다 object dtype 문자열 배열을 통째로 materialize하는 비용이 있었는데,
    # station_no는 처음부터 정수라 그 비용 자체가 없다.
    "station_no": ShortType(),
}


def _holidays_for_train_year() -> set[str]:
    """`config.WINDOW_START`~`WINDOW_END`(롤링 학습기간) 전후(±1년) 대한민국 공휴일을 계산한다.

    `holidays` 패키지(ml_core.holidays_kr)로 오프라인 계산하므로 analysis_summary.json
    같은 사전 준비 파일이 필요 없다. 앞뒤로 1년씩 여유를 두는 이유: 증분(`since`)
    재계산이나 연말/연초 경계에 걸치는 구간이 윈도우 밖 날짜를 포함할 수 있어서다.
    윈도우 자체가 연도를 걸칠 수 있으므로(2026-08부터, 고정 TRAIN_YEAR 폐지) 범위
    전체를 `range()`로 커버한다.
    """
    return korean_holidays(range(config.WINDOW_START.year - 1, config.WINDOW_END.year + 2))


def _expand_hourly_to_ticks(status: DataFrame, tick_minutes: int, spark: SparkSession) -> DataFrame:
    """station_status(시간 단위)를 tick_minutes 간격으로 펼친다 — 그리드 정의 + forward-fill을 동시에 한다.

    station_status는 원본이 시간 단위(0~23시간대)뿐이라, 5분 tick 그리드를 만들려면
    각 시간 관측을 그 시간에 속한 모든 tick으로 복제해야 한다(값은 그 시간 동안
    유지된다고 forward-fill). station_status에 실제 관측이 있는 시간만 이렇게
    펼쳐지므로, 이 결과 자체가 곧 "station 활성 구간" 그리드가 된다.

    args:
        status: station_id, hour_ts, bike_count, stockout_flag (시간 단위, 정시)
        tick_minutes: 그리드 간격(분) — 60으로 나누어 떨어져야 함(예: 5, 15, 30)
        spark: SparkSession (offset 조회용 소형 DataFrame 생성에 필요)
    returns:
        DataFrame: station_id, tick, bike_count, stockout_flag (tick_minutes 간격)
    """
    n_ticks_per_hour = 60 // tick_minutes
    offsets = spark.createDataFrame(
        [(i * tick_minutes * 60,) for i in range(n_ticks_per_hour)], ["_offset_seconds"]
    )
    expanded = status.crossJoin(offsets)
    # F.unix_timestamp()/F.timestamp_seconds()를 직접 쓰지 않는다 — 세션 타임존이
    # UTC가 아니면(우리는 KST로 씀) 왕복이 어긋난다. rolling_window_features.py의
    # 타임존 무관 헬퍼로 대체(모듈 docstring 참고).
    expanded = expanded.withColumn("_hour_ts_unix", _unix_seconds_ntz(F.col("hour_ts")) + F.col("_offset_seconds"))
    expanded = expanded.withColumn("tick", _seconds_to_ntz("_hour_ts_unix"))
    return expanded.drop("hour_ts", "_offset_seconds", "_hour_ts_unix")


def _forward_fill_weather_to_ticks(
    weather: DataFrame,
    tick_minutes: int,
    max_staleness_hours: int,
) -> DataFrame:
    """수집 tick별 날씨를 미래 관측을 쓰지 않고 feature grid tick으로 펼친다.

    각 관측은 그 시각부터 다음 관측 직전까지만 유효하다. 다음 관측이 늦거나 없으면
    inference의 lookback 계약과 같은 ``max_staleness_hours``까지만 유지한다.
    station grid와 range join하면 station 수만큼 중간 행이 폭증하므로, 작은 도시
    공통 weather 테이블만 ``lead``/``sequence``로 먼저 확장한 뒤 exact join한다.

    args:
        weather: hour_ts(실제 수집 tick), temp, precip 컬럼을 가진 DataFrame
        tick_minutes: feature grid 간격(분)
        max_staleness_hours: 관측을 재사용할 수 있는 최대 시간(경계 포함)
    returns:
        DataFrame: hour_ts가 tick_minutes 간격으로 확장된 날씨 관측
    """
    if tick_minutes <= 0:
        raise ValueError("tick_minutes는 양수여야 합니다")
    if max_staleness_hours <= 0:
        raise ValueError("max_staleness_hours는 양수여야 합니다")

    tick_interval = F.expr(f"INTERVAL {tick_minutes} MINUTES")
    stale_interval = F.expr(f"INTERVAL {max_staleness_hours} HOURS")
    ordered = weather.withColumn(
        "_next_weather_ts",
        F.lead("hour_ts").over(Window.orderBy("hour_ts")),
    )
    expires_at = F.col("hour_ts") + stale_interval
    before_next = F.col("_next_weather_ts") - tick_interval
    fill_through = F.when(
        F.col("_next_weather_ts").isNull(),
        expires_at,
    ).otherwise(F.least(before_next, expires_at))
    expanded = ordered.withColumn("_fill_through", fill_through).filter(
        F.col("_fill_through") >= F.col("hour_ts")
    )
    expanded = expanded.withColumn(
        "_weather_tick",
        F.explode(F.sequence(F.col("hour_ts"), F.col("_fill_through"), tick_interval)),
    )
    return (
        expanded.drop("hour_ts", "_next_weather_ts", "_fill_through")
        .withColumnRenamed("_weather_tick", "hour_ts")
    )


def _weather_context_start(since: str) -> str:
    """target 시작점에서 최대 freshness만큼 앞선 weather source 시작점을 반환한다."""
    return (
        datetime.fromisoformat(since)
        - timedelta(hours=silver_schema.WEATHER_MAX_STALENESS_HOURS)
    ).strftime("%Y-%m-%d %H:%M:%S")


def build_merged_table(
    spark: SparkSession,
    since: str | None = None,
    until: str | None = None,
) -> DataFrame:
    """station master/타겟/재고/날씨/인구를 station x tick(config.GRID_TICK_MINUTES) 기준으로 병합한다.

    args:
        spark: SparkSession
        since: 지정하면 hour_ts >= since인 행만 남긴다 (증분 재계산용 — 호출부가
            "워터마크 - INCREMENTAL_LOOKBACK_HOURS"를 넘겨줘야 lag/rolling이 안전하게
            맞물린다). None이면(기본값) 전체 히스토리. **타겟 parquet(rental/return)은
            sparse step function이라 이 필터와 무관하게 항상 전체를 읽는다** — 위
            모듈 docstring 참고.
        until: 지정하면 시계열 입력과 결과의 hour_ts가 이 값보다 이른 행만 남긴다.
            학습 window 종료일 다음날 00:00을 쓰는 exclusive upper bound다.
    returns:
        DataFrame: 최종 병합 테이블 (station_no, rental_count, return_count, bike_count,
            stockout_flag, capacity, lat, lon, temp, precip, pop_total, hour_ts,
            date, day, hour, minute, dow, is_holiday) — station_id(텍스트)는 없다(아래
            station_no 관련 주석 참고).
    """
    master = spark.read.parquet(config.STATION_MASTER_PARQUET)
    rental_targets = spark.read.parquet(config.TARGETS_PARQUET)  # sparse step function
    return_targets = spark.read.parquet(config.RETURN_TARGETS_PARQUET)  # sparse step function
    status = spark.read.parquet(config.STATION_STATUS_PARQUET)
    # wind/humidity/pop_resd/pop_long_foreign/pop_short_foreign은 더 이상 모델
    # 피처가 아니다 — join 전에 select로 걸러서 뒤 단계로 전파되지 않게 한다.
    weather = spark.read.parquet(config.WEATHER_PARQUET).select("hour_ts", "temp", "precip")
    population = spark.read.parquet(config.POPULATION_PARQUET).select("grid_id", "hour_ts", "pop_total")
    holidays = _holidays_for_train_year()

    # "활성 station" 여부(트립이 한 번이라도 있었는지)는 반드시 전체 히스토리 기준이어야
    # 한다 — since 필터를 먼저 걸면 증분 실행 시 "최근엔 조용하지만 예전엔 활발했던"
    # station이 활성 목록에서 잘못 빠질 수 있다.
    active_station_ids = (
        rental_targets.select("station_id")
        .distinct()
        .unionByName(return_targets.select("station_id").distinct())
        .distinct()
    )

    if since is not None:
        status = status.filter(F.col("hour_ts") >= F.lit(since))
        # target 시작 tick도 inference와 같이 직전 최대 3시간 관측을 쓸 수 있어야
        # 한다. weather source만 context를 더 읽고, 최종 행 범위는 status grid가
        # 여전히 since 이상으로 제한한다.
        weather = weather.filter(F.col("hour_ts") >= F.lit(_weather_context_start(since)))
        population = population.filter(F.col("hour_ts") >= F.lit(since))
    if until is not None:
        status = status.filter(F.col("hour_ts") < F.lit(until))
        weather = weather.filter(F.col("hour_ts") < F.lit(until))
        population = population.filter(F.col("hour_ts") < F.lit(until))

    weather = _forward_fill_weather_to_ticks(
        weather,
        config.GRID_TICK_MINUTES,
        silver_schema.WEATHER_MAX_STALENESS_HOURS,
    )

    # 그리드 = station_status에 실제로 관측 기록이 있는 (station_id, hour_ts)를
    # GRID_TICK_MINUTES 간격으로 펼친 것 — 8,760시간 dense grid를 따로 만들지 않는다.
    status = status.join(active_station_ids, on="station_id", how="inner")
    df = _expand_hourly_to_ticks(status, config.GRID_TICK_MINUTES, spark)
    df = df.withColumnRenamed("tick", "hour_ts")

    query = df.select("station_id", F.col("hour_ts").alias("tick"))
    rental_lookup = lookup_count_at_ticks(
        rental_targets, query, station_col="station_id", tick_col="tick", query_tick_col="tick"
    ).withColumnRenamed("count", "rental_count").withColumnRenamed("tick", "hour_ts")
    return_lookup = lookup_count_at_ticks(
        return_targets, query, station_col="station_id", tick_col="tick", query_tick_col="tick"
    ).withColumnRenamed("count", "return_count").withColumnRenamed("tick", "hour_ts")

    df = df.join(rental_lookup, on=["station_id", "hour_ts"], how="left")
    df = df.join(return_lookup, on=["station_id", "hour_ts"], how="left")
    df = df.fillna(0, subset=["rental_count", "return_count"])

    df = df.join(
        master.select("station_id", "station_no", "capacity", "lat", "lon", "grid_id"), on="station_id", how="left"
    )

    df = df.join(weather, on="hour_ts", how="left")
    df = df.withColumn("_hour_floor", F.date_trunc("hour", F.col("hour_ts")))
    df = df.join(population.withColumnRenamed("hour_ts", "_hour_floor"), on=["grid_id", "_hour_floor"], how="left")
    df = df.drop("_hour_floor")
    df = df.fillna(0.0, subset=["pop_total"])

    df = df.withColumn("date", F.date_format("hour_ts", "yyyy-MM-dd"))
    df = df.withColumn("day", F.datediff(F.col("hour_ts"), F.lit(DAY_INDEX_EPOCH.isoformat())))
    df = df.withColumn("hour", F.hour("hour_ts"))  # 더 이상 모델 feature 아님 — 출력/CLI 식별용
    # minute = 자정 기준 경과분(hour*60+분, ml_core.minute_of_day와 동일 공식) — hour
    # 대신 쓰는 실제 모델 feature. 그리드가 5분 tick이라 hour만 쓰면 같은 시간
    # 안의 17:00/17:05/.../17:55가 모델에 전부 같은 값으로 보이는데, minute은 그
    # 구분을 그대로 담는다.
    df = df.withColumn("minute", F.hour("hour_ts") * 60 + F.minute("hour_ts"))
    df = df.withColumn("dow", F.weekday("hour_ts"))  # Monday=0 ... Sunday=6, pandas .dt.dayofweek와 동일
    # 주말 + 공휴일을 is_holiday 하나로 통합 — 과거의 is_weekend/is_next_day_off/
    # is_prev_day_off를 대체한다(다음날/전날 조회가 없어져 연도 경계 처리도 단순해짐).
    df = df.withColumn("is_holiday", (F.col("date").isin(list(holidays)) | (F.col("dow") >= 5)).cast("int"))

    # station_id(텍스트)는 위 join들의 키로만 쓰이고 결과엔 안 남긴다 — station_no(정수)로
    # 이미 매 station이 유일하게 식별되고(모듈 docstring 참고), 이 테이블은 tick x station
    # 전체 히스토리라 station_id를 담아두면 그만큼 저장/셔플 비용이 계속 붙는다. 사람이 보는
    # station_id가 필요한 소비처(inference/build_station_profile.py 등)는 작은
    # station_master로 따로 join해서 붙인다.
    df = df.drop("grid_id", "station_id")

    for col_name, dtype in NATIVE_COLUMN_DTYPES.items():
        df = df.withColumn(col_name, F.col(col_name).cast(dtype))

    if until is not None:
        # target은 `[T, T+TARGET_HORIZON)`이므로 source 종료 직전 tick은 완결된
        # 라벨이 아니다. 마지막 완결 기준시각까지만 남겨 12/31 23:55처럼 다음 해
        # outcome이 잘린 행을 정상 라벨로 학습하지 않는다.
        complete_through = (
            datetime.fromisoformat(until)
            - timedelta(minutes=config.TARGET_HORIZON_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")
        df = df.filter(F.col("hour_ts") <= F.lit(complete_through))
    return df


if __name__ == "__main__":
    from .spark_session import get_spark

    spark = get_spark()
    result = build_merged_table(spark)
    result.write.mode("overwrite").parquet(config.MERGED_TABLE_PARQUET)
    print(f"merged table -> {config.MERGED_TABLE_PARQUET}")
