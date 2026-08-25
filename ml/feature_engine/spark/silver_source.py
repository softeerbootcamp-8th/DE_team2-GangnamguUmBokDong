"""확정 Archive fact와 최신 Silver dimension에서 학습용 1차 정제 산출물을 만든다.

대여·재고·날씨·생활인구는 날짜별 flat parquet인
`archive/{source_id}/dt=YYYY-MM-DD.parquet`를 정확한 날짜 목록으로 읽는다. 요청 범위
중 일부 날짜만 없으면(수집 공백 등) 그 날짜만 건너뛰고 표준출력에 경고를 남긴
채 계속한다 — 월간 재학습이 하루치 결측 때문에 통째로 실패하면 안 되기
때문이다(2026-08). 요청 범위 **전체**가 다 없을 때만 실제로 실패한다. 정류소
마스터만 과거 snapshot이 보장되지 않는 current dimension이라 최신
`silver/station_master_enriched`를 유지한다.

컬럼명 매핑(`RENT_DT`->`start_dt` 등)은 `libs/ml_core/silver_schema.py`의
COLUMN_MAP들을 그대로 재사용한다 — `inference/predict_single.py`의 실시간 조회가
같은 Silver를 같은 이름으로 읽으므로, 매핑을 한 곳(`ml_core`)에만 두고 두 패키지가
같이 참조해야 스키마가 바뀔 때 한쪽만 고치고 잊어버리는 사고를 막는다.

Archive compaction/bootstrap은 `_window_start`·`_source_kind` 메타를 붙이지만,
nowcaster 생활인구와 일부 과거 적재물에는 그 메타가 없을 수 있다. 시각을 나타내는
물리 컬럼(`RENT_DT`, `baseDate+baseTime`, `YMD+TT`)이 있으면 처리한다. 재고는 물리
`stationDt`가 있으면 보조 fallback으로만 쓰고, 표준 archive의 `_window_start`를
우선한다.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from ml_core import silver_schema
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from . import config

_ARCHIVE_REQUIRED_COLUMNS: dict[str, tuple[tuple[str, ...], ...]] = {
    silver_schema.RENTAL_SOURCE_ID: (
        ("BIKE_ID",),
        ("RENT_DT",),
        ("RTN_DT",),
        ("RENT_STATION_ID",),
        ("RETURN_STATION_ID",),
    ),
    silver_schema.BIKE_REALTIME_SOURCE_ID: (
        ("stationId",),
        ("parkingBikeTotCnt",),
        ("_window_start", "stationDt"),
    ),
    silver_schema.WEATHER_SOURCE_ID: (
        ("baseDate",),
        ("baseTime",),
        ("nx",),
        ("ny",),
        ("T1H",),
        ("RN1",),
        ("WSD",),
        ("REH",),
    ),
    silver_schema.POPULATION_SOURCE_ID: (
        ("YMD",),
        ("TT",),
        ("H_DNG_CD",),
        ("CELL_ID",),
        ("SPOP",),
    ),
}


def _default_archive_bounds() -> tuple[str, str]:
    """설정의 inclusive 날짜 window를 Archive용 `[since, until)` 경계로 바꾼다."""
    since = config.WINDOW_START.strftime("%Y-%m-%d 00:00:00")
    until = (config.WINDOW_END + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    return since, until


def _parse_archive_bounds(since: str | None, until: str | None) -> tuple[datetime, datetime]:
    """reader의 선택적 경계를 완전한 `[since, until)` datetime 쌍으로 검증한다."""
    if since is None and until is None:
        since, until = _default_archive_bounds()
    elif since is None or until is None:
        raise ValueError("Archive reader는 since와 until을 함께 받아야 합니다")

    try:
        since_dt = datetime.fromisoformat(since)
        until_dt = datetime.fromisoformat(until)
    except ValueError as exc:
        raise ValueError(f"Archive reader 시각 경계 형식이 잘못됐습니다: since={since}, until={until}") from exc
    if (since_dt.tzinfo is None) != (until_dt.tzinfo is None):
        raise ValueError("Archive reader의 since/until timezone 표기 방식이 서로 다릅니다")
    if until_dt <= since_dt:
        raise ValueError(f"Archive reader는 until이 since보다 뒤여야 합니다: since={since}, until={until}")
    return since_dt, until_dt


def _archive_dates(since: str | None, until: str | None) -> tuple[list[date], str, str]:
    """`[since, until)`이 닿는 날짜를 빠짐없이 오름차순으로 반환한다."""
    since_dt, until_dt = _parse_archive_bounds(since, until)
    last_day = (until_dt - timedelta(microseconds=1)).date()
    days = []
    cursor = since_dt.date()
    while cursor <= last_day:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days, since_dt.isoformat(sep=" "), until_dt.isoformat(sep=" ")


def _archive_path(source_id: str, day: date) -> str:
    """source/date 하나의 flat Archive parquet 경로를 반환한다."""
    return f"{config.ARCHIVE_ROOT.rstrip('/')}/{source_id}/dt={day:%Y-%m-%d}.parquet"


def _path_exists(spark: SparkSession, path: str) -> bool:
    """Spark Hadoop 설정을 그대로 사용해 로컬/S3A 경로의 존재 여부를 확인한다."""
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(filesystem.exists(jpath))


def _latest_glob_path(spark: SparkSession, path_pattern: str) -> str:
    """Hadoop filesystem metadata만 조회해 glob과 일치하는 최신 경로를 반환한다."""
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path_pattern)
    filesystem = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    statuses = filesystem.globStatus(jpath)
    if not statuses:
        raise FileNotFoundError(f"경로와 일치하는 Parquet이 없습니다: {path_pattern}")
    return max(str(status.getPath()) for status in statuses)


def _validate_archive_schema(
    spark: SparkSession,
    source_id: str,
    paths: list[str],
) -> None:
    """각 날짜 파일이 source별 필수 물리 컬럼 대안을 만족하는지 검증한다."""
    required_groups = _ARCHIVE_REQUIRED_COLUMNS[source_id]
    for path in paths:
        columns = set(spark.read.parquet(path).columns)
        missing_groups = [group for group in required_groups if not any(name in columns for name in group)]
        if missing_groups:
            expected = ["|".join(group) for group in missing_groups]
            raise ValueError(
                f"Archive schema가 {source_id} 계약과 호환되지 않습니다: path={path}, "
                f"missing={expected}"
            )


def _read_archive_daily(
    spark: SparkSession,
    source_id: str,
    since: str | None,
    until: str | None,
    read_schema: StructType | None = None,
) -> tuple[DataFrame, str, str]:
    """정확한 일별 Archive 경로를 읽는다 — 일부 날짜가 없으면 경고만 남기고
    건너뛰며, 요청 범위 전체가 다 없을 때만 실패한다(fail-closed에서 완화,
    모듈 docstring 참고).

    **주의**: 대여/반납처럼 결측 날짜가 그대로 "수요 0"으로 학습에 들어갈 수
    있는 소스도 포함된다 — "그날 데이터가 없었다"와 "그날 수요가 0이었다"를
    구분하지 못한다는 트레이드오프를 감수하고, 학습이 절대 실패하지 않는 쪽을
    우선한 결정이다(2026-08).

    args:
        read_schema: 지정하면 `mergeSchema=true` 대신 이 스키마로 강제 읽는다 —
            날짜 파일마다 물리 타입이 다른 컬럼(예: 날씨 archive의 `PTY`, 실측
            2026-08-25에서 BIGINT/DOUBLE이 섞여 있어 `CANNOT_MERGE_SCHEMAS`로
            전체 읽기가 실패했다)이 있지만 그 컬럼을 실제로 안 쓸 때 쓴다 —
            스키마에 없는 컬럼은 그냥 안 읽으므로 타입 충돌 자체가 생기지 않는다.
    """
    days, since_bound, until_bound = _archive_dates(since, until)
    paths = [_archive_path(source_id, day) for day in days]
    exists_flags = [_path_exists(spark, path) for path in paths]
    missing_days = [day.isoformat() for day, exists in zip(days, exists_flags, strict=True) if not exists]
    existing_paths = [path for path, exists in zip(paths, exists_flags, strict=True) if exists]

    if missing_days:
        print(
            f"[archive] 경고: source={source_id} 일부 날짜 partition이 없어 건너뜀: {missing_days}",
            flush=True,
        )
    if not existing_paths:
        raise FileNotFoundError(
            f"Archive에 요청 범위 전체가 없습니다: source={source_id}, "
            f"dates={[day.isoformat() for day in days]}"
        )

    print(
        f"[archive] source={source_id} daily partitions {len(existing_paths)}/{len(days)}개 사용 "
        f"({days[0]:%Y-%m-%d}..{days[-1]:%Y-%m-%d} 범위)",
        flush=True,
    )
    _validate_archive_schema(spark, source_id, existing_paths)
    if read_schema is not None:
        df = spark.read.schema(read_schema).parquet(*existing_paths)
    else:
        df = spark.read.option("mergeSchema", "true").parquet(*existing_paths)
    archive_day = F.regexp_extract(F.input_file_name(), r"dt=(\d{4}-\d{2}-\d{2})\.parquet$", 1)
    return df.withColumn("_archive_dt", F.to_date(archive_day)), since_bound, until_bound


def _rename(df: DataFrame, column_map: dict[str, str]) -> DataFrame:
    """ml_core.silver_schema의 COLUMN_MAP({원본: 우리이름})을 그대로 적용한다."""
    for src, dst in column_map.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)
    return df


def _optional_column(df: DataFrame, name: str, dtype: str) -> Column:
    """컬럼이 있으면 지정 타입으로 반환하고 없으면 같은 타입의 null을 반환한다."""
    if name in df.columns:
        return F.col(name).cast(dtype)
    return F.lit(None).cast(dtype)


def _archive_window_start(df: DataFrame) -> Column:
    """Archive 표준 `_window_start`를 timestamp_ntz로 파싱한다."""
    return F.to_timestamp(_optional_column(df, "_window_start", "string")).cast("timestamp_ntz")


def _population_hour_ts() -> Column:
    """숫자형/숫자 문자열 `YMD`·`TT`를 엄격한 정시각으로 변환한다.

    과거 CSV에서 생성된 Parquet의 `TT` 문자열은 `"0 "`처럼 한 자리이거나
    뒤 공백이 붙을 수 있다. parser에 넘기기 전에 trim하고 시각을 2자리로
    zero-padding하되, 날짜는 정확히 8자리 정수이고 시각은 0~23의
    1~2자리 정수인 경우만 받는다. 잘못된 행은 학습 범위 필터에서
    조용히 사라지지 않고 Spark job을 실패시킨다.
    """
    ymd_text = F.trim(F.col("YMD").cast("string"))
    hour_text = F.trim(F.col("TT").cast("string"))
    ymd_is_integer = ymd_text.rlike(r"^[0-9]{8}$")
    hour_is_integer = hour_text.rlike(r"^[0-9]{1,2}$")
    hour_number = hour_text.cast("int")
    normalized = F.concat_ws(" ", ymd_text, F.lpad(hour_number.cast("string"), 2, "0"))
    parsed = F.try_to_timestamp(normalized, F.lit("yyyyMMdd HH")).cast("timestamp_ntz")
    valid = ymd_is_integer & hour_is_integer & hour_number.between(0, 23) & parsed.isNotNull()
    error_message = F.concat(
        F.lit("Archive living_population_grid YMD/TT가 잘못됐습니다: YMD="),
        F.coalesce(ymd_text, F.lit("<null>")),
        F.lit(", TT="),
        F.coalesce(hour_text, F.lit("<null>")),
        F.lit(" (YMD=yyyyMMdd, TT=0..23 정수 필수)"),
    )
    return F.when(valid, parsed).otherwise(F.raise_error(error_message)).cast("timestamp_ntz")


def _population_h_dng_cd() -> Column:
    """행정동 코드를 trim하고 필수 숫자 문자열 계약을 엄격히 검증한다.

    서울 API의 `H_DNG_CD`는 뒤 공백을 붙여 내려보내므로 논리 키로 사용하기
    전에 반드시 trim해야 한다. 이 컬럼은 source 설정상 required이고 행정동
    코드이므로 null·빈 문자열·숫자가 아닌 값은 서로 같은 빈 키로 합치지 않고
    Spark job을 실패시킨다. 코드 길이는 과거 적재물 간 차이를 허용한다.
    """
    raw_text = F.col("H_DNG_CD").cast("string")
    normalized = F.trim(raw_text)
    valid = normalized.isNotNull() & (F.length(normalized) > 0) & normalized.rlike(r"^[0-9]+$")
    error_message = F.concat(
        F.lit("Archive living_population_grid H_DNG_CD가 잘못됐습니다: H_DNG_CD="),
        F.coalesce(raw_text, F.lit("<null>")),
        F.lit(" (공백이 아닌 숫자 문자열 필수)"),
    )
    return F.when(valid, normalized).otherwise(F.raise_error(error_message)).cast("string")


def read_station_master(spark: SparkSession) -> DataFrame:
    """Silver `station_master_enriched`의 최신 snapshot을 우리 컬럼명으로 읽는다.

    normalizer가 `dt=.../hh=.../HHMM.parquet`로 새 snapshot을 계속 쌓으므로 Hadoop
    filesystem listing에서 최신 파일 하나를 먼저 고른 뒤 그것만 읽는다. 날짜·시각
    segment가 zero-padding된 키 계약이라 경로의 사전순 최대값이 최신 snapshot과 같다.

    returns:
        DataFrame: station_id, station_no, station_name, capacity, lat, lon, grid_id
    """
    station_master_suffix = silver_schema.STATION_MASTER_ENRICHED_PREFIX.removeprefix("silver/")
    path_pattern = f"{config.SILVER_ROOT}/{station_master_suffix}dt=*/hh=*/*.parquet"
    df = spark.read.parquet(_latest_glob_path(spark, path_pattern))
    df = _rename(df, silver_schema.STATION_COLUMN_MAP)
    return df.select("station_id", "station_no", "station_name", "capacity", "lat", "lon", "grid_id")


def read_rental_trips(
    spark: SparkSession,
    since: str | None = None,
    until: str | None = None,
    *,
    partition_since: str | None = None,
    partition_until: str | None = None,
) -> DataFrame:
    """Archive `bike_rental_history`를 station_id 매칭까지 끝낸 트립 단위로 읽는다.

    `RENT_STATION_ID`/`RETURN_STATION_ID`가 이미 `station_id`와 같은 형식이라
    (`ml-integration-requests.md` 1번) 예전처럼 station_no 크로스워크를 안 하고,
    `station_master`에 실제로 있는 station_id인지만 확인한다 — 대여 쪽
    (`station_id`)이 안 걸리면 그 트립 자체를 버리고, 반납 쪽(`end_station_id`)만
    안 걸리면 그 방향만 null로 남긴다(`inference/predict_single.py`의
    `_resolve_rental_stations()`와 동일한 원칙).

    compaction과 bootstrap이 같은 날짜 파일에 들어갈 수 있고, 수집 재시도로 같은
    트립이 다시 나타날 수 있으므로 `(bike_id, start_dt)` 기준 중복 제거를 항상
    적용한다. 파일 선택은 `[since, until)`이 닿는 정확한 날짜 목록이며, 선택한
    파일 안에서도 `start_dt`로 논리 경계를 다시 적용한다. target 생성처럼
    "발생 시각 범위"와 "나중에 수집돼 archive에 들어온 날짜 범위"가 다른 호출자는
    `partition_since`/`partition_until`을 별도로 넘긴다.

    args:
        spark: SparkSession
        since: 지정하면 start_dt >= since인 트립만 남긴다(증분 재계산용)
        until: 지정하면 start_dt < until인 트립만 남긴다(exclusive upper bound)
        partition_since: archive 파일 날짜 선택 시작. None이면 since와 같다.
        partition_until: archive 파일 날짜 선택 종료(exclusive). None이면 until과 같다.
    returns:
        DataFrame: station_id(대여), end_station_id(반납, 없으면 null), start_dt, end_dt
    """
    master_ids = read_station_master(spark).select("station_id")

    if (partition_since is None) != (partition_until is None):
        raise ValueError("partition_since와 partition_until은 반드시 함께 지정해야 합니다")
    archive_since = partition_since if partition_since is not None else since
    archive_until = partition_until if partition_until is not None else until
    df, _, _ = _read_archive_daily(
        spark,
        silver_schema.RENTAL_SOURCE_ID,
        archive_since,
        archive_until,
    )
    _, since_bound, until_bound = _archive_dates(since, until)
    df = _rename(df, silver_schema.RENTAL_COLUMN_MAP)
    df = df.select(
        F.to_timestamp("start_dt", "yyyy-MM-dd HH:mm:ss").cast("timestamp_ntz").alias("start_dt"),
        F.to_timestamp("end_dt", "yyyy-MM-dd HH:mm:ss").cast("timestamp_ntz").alias("end_dt"),
        F.col("start_st").alias("station_id"),
        F.col("end_st").alias("end_station_id"),
        "bike_id",
    )
    df = df.filter(
        (F.col("start_dt") >= F.lit(since_bound))
        & (F.col("start_dt") < F.lit(until_bound))
    )
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

    `bike_station_realtime`은 5분 tick이지만 학습 station grid는 시간별 첫 재고
    스냅샷을 대표값으로 쓴다. weather에는 이 helper를 쓰지 않는다. 날씨는 실제
    수집 tick을 모두 보존한 뒤 별도의 과거 방향 as-of 확장을 해야 미래 누수를
    막을 수 있다.
    """
    window = Window.partitionBy(*partition_cols, "hour_ts").orderBy(tick_col)
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", tick_col)
    )


def read_station_status(
    spark: SparkSession,
    since: str | None = None,
    until: str | None = None,
) -> DataFrame:
    """Archive `bike_station_realtime`에서 정류소별 시간당 재고 스냅샷을 만든다.

    표준 archive에서는 `_window_start`가 유일한 관측 시각이다. 메타 컬럼이 없는
    호환 과거 파일은 물리 `stationDt`가 있을 때만 처리하며, 둘 다 없으면 파일별
    schema 검증에서 실패한다. 선택한 파일 안의 시각도 `[since, until)`로 다시
    잘라 파티션 경계를 벗어난 행을 차단한다.

    args:
        spark: SparkSession
        since: 지정하면 hour_ts >= since인 시간대만 남긴다(증분 재계산용)
        until: 지정하면 hour_ts < until인 시간대만 남긴다(exclusive upper bound)
    returns:
        DataFrame: station_id, hour_ts, bike_count, stockout_flag
    """
    df, since_bound, until_bound = _read_archive_daily(
        spark,
        silver_schema.BIKE_REALTIME_SOURCE_ID,
        since,
        until,
    )
    physical_tick = _optional_column(df, "stationDt", "string")
    physical_tick = F.coalesce(
        F.to_timestamp(physical_tick, "yyyyMMddHH"),
        F.to_timestamp(physical_tick),
    ).cast("timestamp_ntz")
    df = df.withColumn("_tick", F.coalesce(_archive_window_start(df), physical_tick))
    df = _rename(df, silver_schema.BIKE_REALTIME_COLUMN_MAP)
    df = df.withColumn("hour_ts", F.date_trunc("hour", F.col("_tick")))
    df = df.filter(
        (F.col("_tick") >= F.lit(since_bound))
        & (F.col("_tick") < F.lit(until_bound))
    )
    df = df.select("station_id", "hour_ts", "bike_count", "_tick")
    df = _pick_first_per_hour(df, "_tick", ["station_id"])
    df = df.withColumn("stockout_flag", (F.col("bike_count") <= 0).cast("byte"))
    return df.select("station_id", "hour_ts", "bike_count", "stockout_flag")


# `PTY`(강수형태 코드)가 날짜 파일마다 BIGINT/DOUBLE로 물리 타입이 갈려 있어
# (collector 실측, 2026-08-25) `mergeSchema=true`가 CANNOT_MERGE_SCHEMAS로 전체
# 읽기를 실패시킨다 — read_weather()가 실제로 쓰는 컬럼(PTY 제외)만 명시해서
# 그 컬럼 자체를 안 읽게 한다. `_ARCHIVE_REQUIRED_COLUMNS[WEATHER_SOURCE_ID]`와
# 이름은 반드시 같이 맞출 것.
_WEATHER_READ_SCHEMA = StructType(
    [
        StructField("nx", LongType(), True),
        StructField("ny", LongType(), True),
        StructField("baseDate", StringType(), True),
        StructField("baseTime", StringType(), True),
        StructField("T1H", DoubleType(), True),
        StructField("REH", DoubleType(), True),
        StructField("WSD", DoubleType(), True),
        StructField("RN1", DoubleType(), True),
        StructField("_window_start", StringType(), True),
        StructField("_source_kind", StringType(), True),
    ]
)


def read_weather(
    spark: SparkSession,
    since: str | None = None,
    until: str | None = None,
) -> DataFrame:
    """Archive `weather_ultra_short_live`에서 availability 시각별 서울 평균을 만든다.

    collector compaction의 `_window_start`는 모델이 그 값을 처음 알 수 있었던 수집
    시각이라 availability로 쓴다. 반면 현재 ASOS bootstrap mapping은 날짜 버킷용
    `baseDate`만 window 원천으로 써 `_window_start`가 전부 자정이므로,
    `_source_kind=bootstrap`은 `baseDate+baseTime` 실제 관측시각을 쓴다. 메타 없는 legacy는
    `_window_start`가 있으면 우선하고 없으면 물리 관측시각으로 fallback한다. 같은
    baseTime을 여러 collection tick에서 수정한 행을 observation time 하나로 collapse
    하면 최신값이 과거로 역전파되므로, collector의 각 availability tick을 보존한다.
    설정된 model grid로의 과거 방향 forward-fill은 `build_merged_table.py`가 inference와
    같은 최대 3시간 freshness 계약으로 담당한다.

    args:
        spark: SparkSession
        since: 지정하면 availability 시각 >= since인 행만 남긴다(증분 재계산용)
        until: 지정하면 availability 시각 < until인 행만 남긴다(exclusive upper bound)
    returns:
        DataFrame: hour_ts, temp, precip, wind, humidity
    """
    df, since_bound, until_bound = _read_archive_daily(
        spark,
        silver_schema.WEATHER_SOURCE_ID,
        since,
        until,
        read_schema=_WEATHER_READ_SCHEMA,
    )
    base_date = F.regexp_replace(F.col("baseDate").cast("string"), r"\D", "")
    base_time = F.lpad(F.regexp_replace(F.col("baseTime").cast("string"), r"\D", ""), 4, "0")
    df = df.withColumn(
        "_observation_ts",
        F.to_timestamp(F.concat_ws(" ", base_date, base_time), "yyyyMMdd HHmm").cast("timestamp_ntz"),
    )
    source_kind = F.lower(_optional_column(df, "_source_kind", "string"))
    window_start = _archive_window_start(df)
    df = df.withColumn(
        "_tick",
        F.when(source_kind == F.lit("collector"), window_start)
        .when(source_kind == F.lit("bootstrap"), F.col("_observation_ts"))
        .otherwise(F.coalesce(window_start, F.col("_observation_ts"))),
    )
    df = _rename(df, silver_schema.WEATHER_COLUMN_MAP)
    df = df.filter(
        (F.col("_tick") >= F.lit(since_bound))
        & (F.col("_tick") < F.lit(until_bound))
    )
    weather_revision = Window.partitionBy("_tick", "nx", "ny").orderBy(
        F.col("_observation_ts").desc_nulls_last(),
        F.col("_archive_dt").desc_nulls_last(),
    )
    df = (
        df.withColumn("_revision", F.row_number().over(weather_revision))
        .filter(F.col("_revision") == 1)
        .select("_tick", "temp", "precip", "wind", "humidity")
    )
    for column in ("temp", "precip", "wind", "humidity"):
        df = df.withColumn(column, F.col(column).cast("double"))
    valid = (
        F.col("temp").isNotNull()
        & ~F.isnan("temp")
        & F.col("temp").between(silver_schema.WEATHER_TEMP_MIN, silver_schema.WEATHER_TEMP_MAX)
        & F.col("precip").isNotNull()
        & ~F.isnan("precip")
        & F.col("precip").between(
            silver_schema.WEATHER_PRECIP_MIN, silver_schema.WEATHER_PRECIP_MAX
        )
    )
    # tick별 전체 격자 평균만 만든다. 유효 행이 하나도 없는 tick은 빠지고,
    # build_merged_table의 과거 방향 forward-fill이 직전 유효 tick을 선택하므로
    # inference가 깨진 최신 파일을 건너뛰는 동작과 같아진다.
    df = df.filter(valid).groupBy("_tick").agg(
        F.avg("temp").alias("temp"),
        F.avg("precip").alias("precip"),
        F.avg("wind").alias("wind"),
        F.avg("humidity").alias("humidity"),
    )
    df = df.withColumnRenamed("_tick", "hour_ts")
    return df.withColumn("humidity", F.round(F.col("humidity")).cast("int")).select(
        "hour_ts", "temp", "precip", "wind", "humidity"
    )


def read_population(
    spark: SparkSession,
    since: str | None = None,
    until: str | None = None,
) -> DataFrame:
    """Archive `living_population_grid`에서 격자x시간 생활인구를 만든다.

    실제 nowcaster archive 계약에서 두 가지를 확인했다:
    (1) 나이대x성별(`M00`~`F70`) breakdown만 있고 pop_resd/pop_long_foreign/
    pop_short_foreign 구분이 없다 — `SPOP`(총 인구)만 `pop_total`로 쓰고, `pop_resd`는
    `pop_total`과 같다고 근사(전부 내국인으로 간주)한 뒤 나머지 둘은 0으로 둔다.
    (2) nowcaster는 `is_estimated`/`estimation_method`를 붙이고 실측 도착 시 실제
    `YMD` 날짜 archive를 갱신한다. `H_DNG_CD` 뒤 공백을 제거한 뒤 같은
    `(H_DNG_CD, grid_id, hour_ts)` 안에서 명시적 actual, 메타 없는 호환 실측,
    estimated 순으로 한 revision만 고르고, 같은 등급에서는 최신
    `_window_start`/archive 날짜를 우선한다. 그 다음 서로 다른 행정동 component의
    `SPOP`을 격자·시간별로 합산한다. 전부 null인 합계는 0으로 바꾸지 않고 null로
    유지한다. 메타 컬럼 자체가 없어도 필수 물리 컬럼이 있으면 호환 실측으로 처리한다.

    args:
        spark: SparkSession
        since: 지정하면 hour_ts >= since인 시간대만 남긴다(증분 재계산용)
        until: 지정하면 hour_ts < until인 시간대만 남긴다(exclusive upper bound)
    returns:
        DataFrame: grid_id, hour_ts, pop_resd, pop_long_foreign, pop_short_foreign, pop_total
    """
    df, since_bound, until_bound = _read_archive_daily(
        spark,
        silver_schema.POPULATION_SOURCE_ID,
        since,
        until,
    )
    df = df.withColumn("_collected_ts", _archive_window_start(df))
    df = _rename(df, silver_schema.POPULATION_COLUMN_MAP)
    df = df.withColumn("hour_ts", _population_hour_ts())
    df = df.withColumn("_h_dng_cd", _population_h_dng_cd())
    df = df.filter(
        (F.col("hour_ts") >= F.lit(since_bound))
        & (F.col("hour_ts") < F.lit(until_bound))
    )
    is_estimated = _optional_column(df, "is_estimated", "boolean")
    estimation_method = F.lower(_optional_column(df, "estimation_method", "string"))
    # 2=명시적 actual, 1=메타 없는 호환 실측, 0=estimated. 상충 메타는
    # is_estimated=true를 우선해 추정치를 actual로 오인하지 않는다.
    actual_priority = (
        F.when(is_estimated.isNotNull() & is_estimated, F.lit(0))
        .when((is_estimated == F.lit(False)) | (estimation_method == F.lit("actual")), F.lit(2))
        .otherwise(F.lit(1))
    )
    df = df.select(
        "_h_dng_cd",
        "grid_id",
        "hour_ts",
        F.col("pop_total").cast("double").alias("pop_total"),
        "_archive_dt",
        "_collected_ts",
        actual_priority.alias("_actual_priority"),
    )

    # 동일 우선순위·수집시각·archive 날짜까지 겹친 비정상 중복은 non-null/큰
    # SPOP을 안정적인 최종 tie-breaker로 써 Spark 실행 순서에 좌우되지 않게 한다.
    window = Window.partitionBy("_h_dng_cd", "grid_id", "hour_ts").orderBy(
        F.col("_actual_priority").desc(),
        F.col("_collected_ts").desc_nulls_last(),
        F.col("_archive_dt").desc_nulls_last(),
        F.col("pop_total").desc_nulls_last(),
    )
    df = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_actual_priority", "_collected_ts", "_archive_dt")
    )
    # Spark sum은 non-null component만 더하고 모든 component가 null일 때 null을
    # 반환한다. 따라서 masking된 전체 그룹을 인구 0으로 오해하지 않는다.
    df = df.groupBy("grid_id", "hour_ts").agg(F.sum("pop_total").alias("pop_total"))
    df = df.withColumn("pop_resd", F.col("pop_total"))
    df = df.withColumn("pop_long_foreign", F.lit(0.0))
    df = df.withColumn("pop_short_foreign", F.lit(0.0))
    return df.select("grid_id", "hour_ts", "pop_resd", "pop_long_foreign", "pop_short_foreign", "pop_total")
