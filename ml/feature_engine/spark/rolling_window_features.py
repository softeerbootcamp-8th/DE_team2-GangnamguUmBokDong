"""point-in-time(관측 시점 기준) 정합 대여 rolling window 카운트 — PySpark 포팅.

`src/rolling_window_features.py`(pandas, 로컬/검증용)와 **정확히 같은 핵심 규칙**을
구현한다 — 그 파일의 모듈 docstring에 설계 배경(우측 절단/right-censoring 문제,
"5분 단위"가 윈도우 폭이 아니라 갱신 주기라는 점 등)이 자세히 있으니 여기서는
반복하지 않는다.

핵심 규칙(양쪽 구현이 반드시 동일하게 지켜야 함):

    윈도우 = [T - embargo - width, T - embargo)
    포함 조건: start_ts가 이 구간 안에 있고, end_ts가 결측이 아니며 end_ts <= T

pandas 버전은 `pd.merge_asof`/`groupby().cumsum()`을 쓰지만 Spark엔 merge_asof가
없어서, 여기서는 (1) 차분 배열(difference array)은 Window 함수의 `sum().over()`로,
(2) "as-of backward" 조회는 Window의 `last(값, ignorenulls=True)`(정렬 후 직전
non-null 값 전파, forward-fill과 동일 원리)로 대체했다. `tests/test_feature_engine_rolling_parity.py`가
이 포팅이 pandas 버전과 정확히 같은 값을 내는지 합성 데이터로 대조 검증한다 —
이 대조 없이는 두 구현이 조용히 갈라져도 알아챌 방법이 없으므로, 이 파일의 로직을
바꿀 때는 반드시 그 테스트를 같이 갱신해야 한다.

**타임존 관련 주의사항(직접 겪은 버그)**: 초 단위 정수로 변환해 버킷 연산을 하고
다시 타임스탬프로 되돌리는 과정에서 `F.unix_timestamp()`/`F.timestamp_seconds()`를
그냥 쓰면 안 된다 — `F.unix_timestamp()`는 `timestamp_ntz`(parquet에서 읽은 값,
실제 배치 경로) 입력에 한해 **세션 타임존을 무시하고 항상 UTC로 해석**하는 반면,
`F.timestamp_seconds()`는 되돌릴 때 **세션 타임존으로 표시값을 만든다** — 둘이
비대칭이라 세션 타임존이 UTC가 아니면(우리는 KST로 쓰기로 함) 왕복 변환이 세션
타임존만큼 조용히 어긋난다. 아래 `_unix_seconds_ntz()`/`_seconds_to_ntz()`가 이
비대칭을 없앤 안전한 대체 함수다 — **이 파일과 `build_merged_table.py`에서 초
단위 정수 <-> 타임스탬프 왕복이 필요하면 반드시 이 둘을 쓸 것.**
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

_NTZ_EPOCH_LITERAL = "TIMESTAMP_NTZ '1970-01-01 00:00:00'"


def _unix_seconds_ntz(ts_col: Column) -> Column:
    """타임스탬프 컬럼을 세션 타임존과 무관하게 항상 UTC 기준 epoch 초로 바꾼다.

    `F.unix_timestamp()`는 입력이 `timestamp_ntz`일 때 세션 타임존을 무시하고 항상
    UTC로 취급하는 것으로 확인됨(Spark 3.5 기준 실측) — 반대로 입력이 tz-aware
    `timestamp`(예: `spark.createDataFrame(pandas_df)`로 만든 컬럼)면 그 순간의
    JVM 기본 타임존으로 해석된 절대 시각을 담고 있다. 여기서 먼저 `timestamp_ntz`로
    캐스트해두면(JVM 기본 타임존과 세션 타임존이 같다는 전제 하에 — `spark_session.py`
    참고) 두 입력 경로가 **항상 같은 epoch 값**을 내도록 통일된다.

    args:
        ts_col: 타임스탬프 컬럼(timestamp 또는 timestamp_ntz) — null이면 null 전파
    returns:
        Column: UTC 기준 epoch 초(LongType), 세션 타임존과 무관하게 결정적
    """
    return F.unix_timestamp(ts_col.cast("timestamp_ntz"))


def _seconds_to_ntz(seconds_col_name: str) -> Column:
    """epoch 초(위 함수의 역연산)를 세션 타임존과 무관하게 timestamp_ntz로 되돌린다.

    `F.timestamp_seconds()`는 내부적으로는 항상 같은 절대 시각(instant)을 담지만,
    이후 `timestamp_ntz`로 캐스트하거나 다른 `timestamp_ntz` 컬럼과 비교/조인하는
    순간 **세션 타임존으로 변환된 표시값**이 굳어버려 세션 타임존이 UTC가 아니면
    어긋난다. 대신 `timestampadd()`(순수 wall-clock 구간 연산, 타임존 변환을 전혀
    거치지 않음)로 1970-01-01 00:00:00(ntz 리터럴)에 초를 더해서 이 문제를 원천
    차단한다 — 세션 타임존이 무엇이든(KST든 UTC든) 항상 정확한 왕복을 보장한다.

    `timestampadd()`는 SQL 표현식이라 컬럼을 이름으로 참조해야 한다 — 그래서 Column
    객체가 아니라 **이미 DataFrame에 존재하는(먼저 `.select()`/`.withColumn()`으로
    만들어둔) epoch 초 컬럼의 이름**을 받는다.

    args:
        seconds_col_name: `_unix_seconds_ntz()`로 만든 epoch 초 컬럼의 이름
    returns:
        Column: timestamp_ntz, 세션 타임존과 무관하게 결정적
    """
    return F.expr(f"timestampadd(SECOND, {seconds_col_name}, {_NTZ_EPOCH_LITERAL})")


def _ceil_to_seconds(unix_col: Column, bucket_seconds: int) -> Column:
    """unix timestamp(초)를 bucket_seconds 단위로 올림한다 (pandas Timestamp.ceil(freq)와 동일).

    args:
        unix_col: 정수 초 단위 컬럼(`_unix_seconds_ntz()` 결과) — null이면 null 전파
        bucket_seconds: 버킷 크기(초)
    returns:
        Column: bucket_seconds의 배수로 올림된 정수 초
    """
    return (F.ceil(unix_col / F.lit(bucket_seconds)).cast(LongType()) * F.lit(bucket_seconds)).cast(LongType())


def _floor_to_seconds(unix_col: Column, bucket_seconds: int) -> Column:
    """unix timestamp(초)를 bucket_seconds 단위로 내림한다 (pandas Timestamp.floor(freq)와 동일)."""
    return (F.floor(unix_col / F.lit(bucket_seconds)).cast(LongType()) * F.lit(bucket_seconds)).cast(LongType())


def censored_rolling_counts(
    trips: DataFrame,
    window_minutes: int,
    embargo_minutes: int,
    tick_minutes: int = 5,
    station_col: str = "station_id",
    start_col: str = "start_dt",
    end_col: str = "end_dt",
) -> DataFrame:
    """배치용: station별로 [T-embargo-window, T-embargo) point-in-time 카운트를 모든 tick T에 대해 계산한다.

    `src/rolling_window_features.py`의 동명 함수와 알고리즘·결과가 동일해야 한다
    (차분 배열 기법 — "카운트가 +1 되는 시작 tick"과 "-1 되는 종료+1 tick"만
    기록한 뒤 station별로 시간순 Window 누적합).

    args:
        trips: station_col/start_col/end_col을 포함한 트립 단위 DataFrame
            (반납 미완료 트립은 end_col이 null)
        window_minutes: 윈도우 폭(분)
        embargo_minutes: as_of에서 윈도우까지의 간격(분)
        tick_minutes: 서빙 갱신 주기(분), 기본 5분
        station_col: 정류소 ID 컬럼명
        start_col: 대여 시작 시각 컬럼명
        end_col: 반납 완료 시각 컬럼명 (null 허용)
    returns:
        DataFrame: station_col, tick(timestamp), count — station별 tick 오름차순
            정렬은 보장하지 않음(호출부에서 필요시 정렬) — sparse step function이라
            특정 tick의 값은 `lookup_count_at_ticks()`로 조회해야 한다.
    """
    tick_seconds = tick_minutes * 60
    embargo_seconds = embargo_minutes * 60
    width_seconds = window_minutes * 60

    with_unix = trips.select(
        F.col(station_col).alias(station_col),
        F.col(end_col).alias("_end"),
        _unix_seconds_ntz(F.col(start_col)).alias("_start_unix"),
        _unix_seconds_ntz(F.col(end_col)).alias("_end_unix"),  # null이면 null 전파
    )

    lo_bound_unix = F.col("_start_unix") + F.lit(embargo_seconds)
    hi_bound_unix = lo_bound_unix + F.lit(width_seconds)

    # 포함 조건은 "start_ts < T-embargo"(엄격한 부등호) — T > lo_bound인 가장 빠른
    # tick이 필요하다. lo_bound가 정확히 tick 위에 있으면 _ceil_to_seconds()는 그
    # 값 자체를 돌려줘 "엄격히 큼" 조건을 깨뜨린다 — floor+tick_seconds는 정렬 여부와
    # 무관하게 항상 다음 tick을 반환한다 (src/rolling_window_features.py와 동일한 수정).
    lo_t_unix = _floor_to_seconds(lo_bound_unix, tick_seconds) + F.lit(tick_seconds)
    hi_t_unix = _floor_to_seconds(hi_bound_unix, tick_seconds)  # 이쪽은 "<=" 비-엄격 조건이라 floor가 맞음
    vis_t_unix = _ceil_to_seconds(F.col("_end_unix"), tick_seconds)  # _end_unix가 null이면 null 그대로 전파

    with_bounds = with_unix.select(
        F.col(station_col),
        F.col("_end"),
        lo_t_unix.alias("_lo_t_unix"),
        hi_t_unix.alias("_hi_t_unix"),
        vis_t_unix.alias("_vis_t_unix"),
    )

    effective_lo_t_unix = F.when(
        F.col("_vis_t_unix").isNull(), F.col("_lo_t_unix")
    ).otherwise(F.greatest(F.col("_lo_t_unix"), F.col("_vis_t_unix")))

    valid = with_bounds.withColumn("_effective_lo_t_unix", effective_lo_t_unix).filter(
        F.col("_end").isNotNull() & (F.col("_effective_lo_t_unix") <= F.col("_hi_t_unix"))
    )
    valid = valid.withColumn("_end_tick_unix", F.col("_hi_t_unix") + F.lit(tick_seconds))

    starts = valid.select(
        F.col(station_col),
        _seconds_to_ntz("_effective_lo_t_unix").alias("tick"),
        F.lit(1).alias("delta"),
    )
    ends = valid.select(
        F.col(station_col),
        _seconds_to_ntz("_end_tick_unix").alias("tick"),
        F.lit(-1).alias("delta"),
    )

    deltas = starts.unionByName(ends)
    agg = deltas.groupBy(station_col, "tick").agg(F.sum("delta").alias("delta"))

    w = (
        Window.partitionBy(station_col)
        .orderBy("tick")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    result = agg.withColumn("count", F.sum("delta").over(w).cast("int"))
    return result.select(station_col, "tick", "count")


def future_rolling_counts(
    trips: DataFrame,
    width_minutes: int,
    tick_minutes: int = 5,
    station_col: str = "station_id",
    event_col: str = "start_dt",
) -> DataFrame:
    """타겟 생성용: station별로 "[T, T+width) 구간에 이벤트가 있었던 건수"를 모든 tick T에 대해 계산한다.

    `src/rolling_window_features.py`의 동명 함수와 알고리즘·결과가 동일해야 한다 —
    `censored_rolling_counts()`가 "그 시점에 관측 가능했던 과거"(입력 피처)를 보는
    것과 정반대로, 이 함수는 이미 몇 달~몇 년 지나 전부 확정된 과거 데이터에서
    "T를 기준으로 앞으로 width분 동안 실제로 일어난 진짜 값"을 계산하므로 관측
    가능성(censoring/embargo)을 따질 필요가 없다 — 인자도 이벤트 시각(event_col)
    하나뿐이다(대여 타겟="start_dt", 반납 타겟="end_dt").

    알고리즘은 `censored_rolling_counts()`와 동일한 차분 배열(difference array)
    기법이다 — 이벤트 하나가 카운트에 잡히는 tick T의 조건은 "T <= event <
    event+width"(동치: `event-width < T <= event`)이므로, 그 구간의 시작/끝(+1/-1)
    델타만 기록한 뒤 station별 Window 누적합으로 계산한다.

    args:
        trips: station_col/event_col을 포함하는 트립 단위 DataFrame
        width_minutes: 타겟 윈도우 폭(분) — "앞으로 몇 분간"(기본 설계: 60분=1시간)
        tick_minutes: grid 간격(분)
        station_col: 정류소 ID 컬럼명
        event_col: 이벤트 시각 컬럼명 (대여 타겟="start_dt", 반납 타겟="end_dt")
    returns:
        DataFrame: station_col, tick(timestamp), count — sparse step function.
            `lookup_count_at_ticks()`로 조회한다.
    """
    tick_seconds = tick_minutes * 60
    width_seconds = width_minutes * 60

    with_unix = trips.select(
        F.col(station_col),
        _unix_seconds_ntz(F.col(event_col)).alias("_event_unix"),
    )

    hi_t_unix = _floor_to_seconds(F.col("_event_unix"), tick_seconds)  # T <= event를 만족하는 가장 늦은 tick
    # T > event-width(엄격한 부등호)를 만족하는 가장 빠른 tick. floor+tick_seconds를 쓰는
    # 이유는 censored_rolling_counts()와 동일(src/rolling_window_features.py 참고) —
    # event-width가 정확히 tick 위에 있을 때 ceil은 "엄격히 큼" 조건을 깨뜨린다.
    lo_t_unix = _floor_to_seconds(F.col("_event_unix") - F.lit(width_seconds), tick_seconds) + F.lit(tick_seconds)

    with_bounds = with_unix.select(
        F.col(station_col),
        lo_t_unix.alias("_lo_t_unix"),
        (hi_t_unix + F.lit(tick_seconds)).alias("_end_tick_unix"),
    )

    starts = with_bounds.select(
        F.col(station_col),
        _seconds_to_ntz("_lo_t_unix").alias("tick"),
        F.lit(1).alias("delta"),
    )
    ends = with_bounds.select(
        F.col(station_col),
        _seconds_to_ntz("_end_tick_unix").alias("tick"),
        F.lit(-1).alias("delta"),
    )

    deltas = starts.unionByName(ends)
    agg = deltas.groupBy(station_col, "tick").agg(F.sum("delta").alias("delta"))

    w = (
        Window.partitionBy(station_col)
        .orderBy("tick")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    result = agg.withColumn("count", F.sum("delta").over(w).cast("int"))
    return result.select(station_col, "tick", "count")


def lookup_count_at_ticks(
    cumulative: DataFrame,
    query_ticks: DataFrame,
    station_col: str = "station_id",
    tick_col: str = "tick",
    query_tick_col: str = "tick",
) -> DataFrame:
    """censored_rolling_counts()의 sparse step function을 원하는 tick 목록에서 조회한다.

    pandas 버전은 `pd.merge_asof(direction="backward")`를 쓰지만, Spark엔 이게 없어서
    "값 있는 행(source)"과 "조회하고 싶은 행(query)"을 한 타임라인에 합친 뒤,
    station별로 tick 순 정렬해서 **직전 non-null 값을 전파**(forward-fill,
    `F.last(ignorenulls=True)`)하는 방식으로 같은 효과를 낸다. 동일 tick에 source와
    query가 같이 있으면 source가 먼저 적용되도록(=`direction="backward"`가 동일
    시각의 값도 포함하는 것과 동일하게) 정렬 순서에 보조키를 둔다.

    args:
        cumulative: censored_rolling_counts()의 결과 (station_col, tick_col, "count")
        query_ticks: station_col과 query_tick_col을 포함한, 값을 조회하고 싶은
            (station, tick) 목록
        station_col: 정류소 ID 컬럼명
        tick_col: cumulative 쪽 tick 컬럼명
        query_tick_col: query_ticks 쪽 tick 컬럼명
    returns:
        DataFrame: query_ticks와 같은 (station_col, query_tick_col) 조합에 "count"
            컬럼이 붙어서 반환 (해당 station에 그 시점 이전 delta가 전혀 없으면 0)
    """
    # tick 컬럼을 양쪽 다 timestamp_ntz로 통일해둔다 — 호출부가 하나는 timestamp_ntz
    # (parquet에서 읽은 값), 하나는 tz-aware timestamp(예: 테스트의 createDataFrame)로
    # 서로 다른 타입을 넘겨도 union 시 묵시적 타입 승격에 세션 타임존이 개입해
    # 조용히 어긋나는 걸 막는다(모듈 docstring의 타임존 주의사항과 동일한 이유).
    source = cumulative.select(
        F.col(station_col).alias("_station"),
        F.col(tick_col).cast("timestamp_ntz").alias("_tick"),
        F.col("count").alias("_raw_count"),
        F.lit(0).alias("_is_query"),  # source가 query보다 먼저 적용되도록 0
    )
    query = query_ticks.select(
        F.col(station_col).alias("_station"),
        F.col(query_tick_col).cast("timestamp_ntz").alias("_tick"),
        F.lit(None).cast(source.schema["_raw_count"].dataType).alias("_raw_count"),
        F.lit(1).alias("_is_query"),
    )

    combined = source.unionByName(query)
    w = (
        Window.partitionBy("_station")
        .orderBy("_tick", "_is_query")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    filled = combined.withColumn("_filled_count", F.last("_raw_count", ignorenulls=True).over(w))

    return (
        filled.filter(F.col("_is_query") == 1)
        .select(
            F.col("_station").alias(station_col),
            F.col("_tick").alias(query_tick_col),
            F.coalesce(F.col("_filled_count"), F.lit(0)).cast("long").alias("count"),
        )
    )
