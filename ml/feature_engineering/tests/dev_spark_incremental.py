"""증분(watermark 기반 날짜 파티션 overwrite) 계산이 전체 재빌드와 정확히 같은 값을
내는지, 그리고 뒤늦게 나타난 대여 트립이 이미 발행된 과거 날짜를 실제로 보정하는지
검증한다.

`run_pipeline._run_incremental()`은 "워터마크 - INCREMENTAL_LOOKBACK_HOURS"를 자정
경계로 내린 시각부터 다시 계산해서, 그 구간에 걸리는 `date` 파티션을 통째로
덮어쓴다(append가 아님 — `run_pipeline.py` 모듈 docstring의 "왜 append가 아니라
overwrite인가" 참고). lookback 마진이 충분하면(lag_168h 등 7일 참조보다 넉넉하면)
이 방식이 "처음부터 전체를 다시 계산"한 것과 동일한 값을 내야 한다 —
`test_incremental_append_matches_full_rebuild`가 그 핵심 주장을 실측으로 확인한다:
없으면 증분 로직이 경계 근처에서 조용히 다른 값(또는 결측)을 내는 회귀를 잡을
방법이 없다.

`test_incremental_corrects_rental_count_for_late_arriving_trip`은 그 overwrite가
실제로 필요한 이유(대여이력은 반납 완료 시에만 Silver에 나타나므로, 뒤늦게 반납된
트립이 이미 발행된 과거 시간대의 rental_count를 늘려야 함)를 검증한다 — 예전
append 구현이었다면 이 보정이 조용히 영구 누락됐을 회귀다.

Silver만 읽도록 바뀐 뒤로(collector Silver 예시 데이터 기준, `docs/collector/
ml-integration-requests.md` 참고) 이 fixture는 "1차 정제 산출물"을 직접 만드는 대신
Silver 조각 파일(`bike_station_realtime`/`bike_rental_history`/
`weather_ultra_short_term`/`living_population_grid`) 자체를 로컬 tmp_path에
만들어서 `fe_config.SILVER_ROOT`를 그리로 돌린다 — `silver_source.py`의 실제 파싱
로직(경로에서 시각 역추출, station_id 직접 매칭 등)까지 이 테스트가 같이 검증한다.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import functions as F

from feature_engineering.spark import config as fe_config
from feature_engineering.spark.build_features import build_features
from feature_engineering.spark.build_merged_table import build_merged_table
from feature_engineering.spark.build_rolling_rental_features import (
    build_rolling_rental_features,
)
from feature_engineering.spark.run_pipeline import (
    _refresh_primary_tables,
    _run_incremental,
)
from feature_engineering.spark.watermark import read_watermark, write_watermark

N_HOURS = 600  # 25일
WATERMARK_OFFSET_HOURS = 432  # 18일차 -> 신규 구간 168시간(7일)
LOOKBACK_HOURS = 240  # 10일 (lag_168h의 7일보다 넉넉한 마진)
TICKS_PER_HOUR = 60 // fe_config.GRID_TICK_MINUTES  # 그리드가 이제 시간이 아니라 5분 tick 단위


def _write_parquet(path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


@pytest.fixture
def synthetic_environment(spark, tmp_path, monkeypatch):
    """station 1개, 25일(600시간)치 Silver 조각 파일을 로컬 tmp_path에 만들고 config를 그쪽으로 돌린다."""
    start = pd.Timestamp("2025-01-01 00:00")
    hours = pd.date_range(start, periods=N_HOURS, freq="h")

    silver_root = tmp_path / "silver"

    # station master — Silver 실제 컬럼명(sta_id 등, STATION_COLUMN_MAP 참고).
    master_pdf = pd.DataFrame([{
        "sta_id": "A", "sta_no": "00001", "sta_nm": "test",
        "hold_cnt": 10, "lat": 37.5, "lon": 127.0, "grid_id": "다사00000000",
    }])
    _write_parquet(silver_root / "station" / "station_master.parquet", master_pdf)

    # bike_station_realtime — 시각은 파일 내용이 아니라 경로(dt=/hh=/HHMM)에만 있다.
    # station_status는 시간 단위 대표값 하나면 충분하므로(_pick_first_per_hour), 시간마다
    # 파일 1개(정시)만 만든다.
    for h in hours:
        status_pdf = pd.DataFrame([{
            "stationId": "A", "stationName": "test", "rackTotCnt": 10,
            "parkingBikeTotCnt": 5, "shared": 50, "stationLatitude": 37.5, "stationLongitude": 127.0,
        }])
        _write_parquet(
            silver_root / "bike_station_realtime" / f"dt={h:%Y-%m-%d}" / f"hh={h:%H}" / f"{h:%H}00.parquet",
            status_pdf,
        )
        weather_pdf = pd.DataFrame([{"T1H": 20.0, "REH": 50.0, "WSD": 2.0, "RN1": 0.0, "PTY": 0}])
        _write_parquet(
            silver_root / "weather_ultra_short_term" / f"dt={h:%Y-%m-%d}" / f"hh={h:%H}" / f"{h:%H}00.parquet",
            weather_pdf,
        )

    # living_population_grid — 하루 1개 파일(YMD/TT로 24시간 내장)이 실제 구조지만,
    # 이 테스트는 그 daily-file 특성 자체가 아니라 증분/전체 재빌드 일치를 검증하는
    # 것이 목적이라 한 파일에 전체 기간의 (YMD, TT)를 다 넣어 단순화한다.
    pop_rows = [{
        "YMD": f"{h:%Y%m%d}", "TT": f"{h:%H}", "H_DNG_CD": "", "CELL_ID": "다사00000000",
        "SPOP": 102.0,
    } for h in hours]
    _write_parquet(
        silver_root / "living_population_grid" / f"dt={start:%Y-%m-%d}" / "hh=09" / "0900.parquet",
        pd.DataFrame(pop_rows),
    )

    # bike_rental_history — 시간마다 1건, 대여 소요시간을 5~40분 사이에서 순환시켜
    # censoring 신호를 다양하게 만든다. start/end 둘 다 station "A"에서 일어난다고
    # 두고 rental/return 타겟을 동시에 만든다. 실제 예시 데이터도 파일의 dt=
    # 파티션과 RENT_DT 내용이 어긋났으므로(수집 지연), 여기서도 트립 전부를 파일
    # 하나에 담아 파티션 경로와 무관하게 만든다 — read_rental_trips()는 RENT_DT/
    # RTN_DT 컬럼만 보고 파티션 경로의 시각은 안 쓴다.
    trip_rows = []
    for i, h in enumerate(hours):
        duration_min = 5 + (i % 8) * 5
        trip_start = h + pd.Timedelta(minutes=(i % 3) * 10)
        trip_rows.append({
            "BIKE_ID": f"BIKE-{i}",
            "RENT_DT": trip_start.strftime("%Y-%m-%d %H:%M:%S"),
            "RTN_DT": (trip_start + pd.Timedelta(minutes=duration_min)).strftime("%Y-%m-%d %H:%M:%S"),
            "RENT_STATION_ID": "A",
            "RETURN_STATION_ID": "A",
            "USE_MIN": str(duration_min),
            "USE_DST": "100.0",
        })
    _write_parquet(
        silver_root / "bike_rental_history" / f"dt={start:%Y-%m-%d}" / "hh=00" / "0000.parquet",
        pd.DataFrame(trip_rows),
    )

    # 2025-01-15(수요일 — 주말이 아님)를 공휴일로 넣어서, is_next_day_off/is_prev_day_off의
    # "휴일" 분기가 "주말" 분기와 뒤섞이지 않고 독립적으로 검증되게 한다(아래
    # test_next_and_prev_day_off_match_pandas_reference 참고).
    summary_path = tmp_path / "analysis_summary.json"
    summary_path.write_text(json.dumps({"holidays_2025": ["2025-01-15"]}), encoding="utf-8")

    output_root = str(tmp_path / "output")

    monkeypatch.setattr(fe_config, "SILVER_ROOT", str(silver_root))
    monkeypatch.setattr(fe_config, "TRAIN_YEAR", 2025)
    monkeypatch.setattr(fe_config, "STATION_MASTER_PARQUET", str(tmp_path / "station_master.parquet"))
    monkeypatch.setattr(fe_config, "TARGETS_PARQUET", str(tmp_path / "targets.parquet"))
    monkeypatch.setattr(fe_config, "RETURN_TARGETS_PARQUET", str(tmp_path / "return_targets.parquet"))
    monkeypatch.setattr(fe_config, "STATION_STATUS_PARQUET", str(tmp_path / "status.parquet"))
    monkeypatch.setattr(fe_config, "WEATHER_PARQUET", str(tmp_path / "weather.parquet"))
    monkeypatch.setattr(fe_config, "POPULATION_PARQUET", str(tmp_path / "population.parquet"))
    monkeypatch.setattr(fe_config, "ANALYSIS_SUMMARY_JSON", str(summary_path))
    monkeypatch.setattr(fe_config, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(fe_config, "ROLLING_RENTAL_FEATURES_PARQUET", output_root + "/rolling.parquet")
    monkeypatch.setattr(fe_config, "MERGED_TABLE_PARQUET", output_root + "/merged.parquet")
    monkeypatch.setattr(fe_config, "FEATURES_TABLE_PARQUET", output_root + "/features.parquet")
    monkeypatch.setattr(fe_config, "WATERMARK_PATH", output_root + "/_watermark.json")
    monkeypatch.setattr(fe_config, "INCREMENTAL_LOOKBACK_HOURS", LOOKBACK_HOURS)

    # Silver로부터 1차 정제 산출물(station_master/targets/station_status/weather/
    # population)을 한 번 만들어둔다 — run_pipeline._run_incremental()도 내부에서
    # 이 함수를 다시 부르지만(매번 전체 재계산, run_pipeline.py 참고), 여기서
    # 미리 한 번 만들어야 아래 테스트들이 build_merged_table() 등을 fixture 밖에서
    # 직접 부를 때도 그 산출물이 이미 있다.
    _refresh_primary_tables(spark)

    watermark_cutoff = start + pd.Timedelta(hours=WATERMARK_OFFSET_HOURS)
    return {"watermark_cutoff": watermark_cutoff}


COMPARE_COLS = [
    "hour_ts",
    "rental_lag_1h", "rental_lag_24h", "rental_lag_168h",
    "rental_roll_mean_3h", "rental_roll_std_3h", "rental_roll_mean_24h", "rental_roll_std_24h",
    "return_lag_1h", "return_lag_24h", "return_lag_168h",
    "return_roll_mean_3h", "return_roll_std_3h", "return_roll_mean_24h", "return_roll_std_24h",
]


def test_incremental_append_matches_full_rebuild(spark, synthetic_environment, tmp_path):
    watermark_cutoff = synthetic_environment["watermark_cutoff"]

    # (A) 기준값: 전체 600시간으로 한 번에 계산한 뒤, 워터마크 이후 구간만 잘라낸다.
    full_rolling_path = str(tmp_path / "full_rolling.parquet")
    build_rolling_rental_features(spark, output_path=full_rolling_path)
    full_merged = build_merged_table(spark)
    full_features = build_features(spark, full_merged, rolling_parquet_path=full_rolling_path)

    expected_new = (
        full_features.filter(F.col("hour_ts") > F.lit(watermark_cutoff))
        .select(*COMPARE_COLS)
        .toPandas()
        .sort_values("hour_ts")
        .reset_index(drop=True)
    )
    # watermark_cutoff 자체는 "이후"가 아니므로 제외. 그리드가 이제 시간이 아니라
    # 5분 tick 단위라 시간당 TICKS_PER_HOUR개 행이 있다.
    assert len(expected_new) == (N_HOURS - WATERMARK_OFFSET_HOURS) * TICKS_PER_HOUR - 1

    # 기존 피처마트(챔피언 산출물) 역할 — 워터마크까지의 부분만 미리 저장해둔다.
    # 실제 파이프라인은 전체/증분 빌드 모두 date 파티션으로 쓰므로 여기서도 동일한
    # 레이아웃으로 미리 채워둬야 한다(안 그러면 이후 dynamic partition overwrite가
    # 파티션 없는 파일과 뒤섞여 깨진다).
    existing = full_features.filter(F.col("hour_ts") <= F.lit(watermark_cutoff))
    existing.write.mode("overwrite").partitionBy("date").parquet(fe_config.FEATURES_TABLE_PARQUET)
    write_watermark(fe_config.WATERMARK_PATH, watermark_cutoff.isoformat(), {})

    # (B) 증분 실행 — lookback 구간부터 다시 계산해서 해당 날짜 파티션을 덮어쓴다.
    # 워터마크 이후 구간은 이번이 처음 계산되는 것이므로 "새 행"과 다름없다.
    _run_incremental(spark, {"max_hour_ts": watermark_cutoff.isoformat()})

    appended = spark.read.parquet(fe_config.FEATURES_TABLE_PARQUET)
    got_new = (
        appended.filter(F.col("hour_ts") > F.lit(watermark_cutoff))
        .select(*COMPARE_COLS)
        .toPandas()
        .sort_values("hour_ts")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(got_new, expected_new, check_dtype=False, check_exact=False, rtol=1e-9)


def test_incremental_corrects_rental_count_for_late_arriving_trip(spark, synthetic_environment, tmp_path):
    """반납이 뒤늦게 완료돼 트립이 나중에야 Silver에 나타나도, 이미 발행된 과거 날짜의
    rental_count가 다음 증분 실행에서 사후 보정되는지 확인한다.

    `bike_rental_history`는 반납 완료 시에만 한 행으로 잡힌다(`silver_source.
    read_rental_trips()`) — 대여 시작 시각(`start_dt`)이 이미 피처마트에 발행되고
    한참 지난 뒤에야 그 트립이 카운트에 반영될 수 있다는 뜻이다. append 방식이었던
    이전 구현은 워터마크 이하 행을 전부 버렸으므로 이 보정이 영구 누락됐다 — 이
    테스트가 그 회귀를 잡는다(`run_pipeline.py` 모듈 docstring 참고).
    """
    watermark_cutoff = synthetic_environment["watermark_cutoff"]
    start = pd.Timestamp("2025-01-01 00:00")

    # (A) 1차 증분 — late trip이 아직 Silver에 없는 상태로 워터마크까지 발행해둔다.
    full_rolling_path = str(tmp_path / "pre_late_rolling.parquet")
    build_rolling_rental_features(spark, output_path=full_rolling_path)
    pre_late_merged = build_merged_table(spark)
    pre_late_features = build_features(spark, pre_late_merged, rolling_parquet_path=full_rolling_path)
    existing = pre_late_features.filter(F.col("hour_ts") <= F.lit(watermark_cutoff))
    existing.write.mode("overwrite").partitionBy("date").parquet(fe_config.FEATURES_TABLE_PARQUET)
    write_watermark(fe_config.WATERMARK_PATH, watermark_cutoff.isoformat(), {})

    _run_incremental(spark, {"max_hour_ts": watermark_cutoff.isoformat()})

    # 이미 발행된 과거(1차 증분의 lookback 안쪽) 정시 하나를 고른다 — WATERMARK_OFFSET_HOURS(432)
    # 보다 한참 전이지만, 2차 증분의 lookback(LOOKBACK_HOURS=240)에는 걸리도록
    # watermark_cutoff에서 240시간 이내로 잡는다.
    late_start = start + pd.Timedelta(hours=WATERMARK_OFFSET_HOURS - 32)
    before = (
        spark.read.parquet(fe_config.FEATURES_TABLE_PARQUET)
        .filter((F.col("station_id") == "A") & (F.col("hour_ts") == F.lit(late_start)))
        .select("rental_count")
        .collect()[0][0]
    )

    # (B) 이제서야 반납이 완료돼 Silver에 나타난 트립 — start_dt는 이미 발행된 과거 시각.
    late_trip = pd.DataFrame([{
        "BIKE_ID": "BIKE-LATE",
        "RENT_DT": late_start.strftime("%Y-%m-%d %H:%M:%S"),
        "RTN_DT": (late_start + pd.Timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"),
        "RENT_STATION_ID": "A",
        "RETURN_STATION_ID": "A",
        "USE_MIN": "10",
        "USE_DST": "100.0",
    }])
    _write_parquet(
        Path(fe_config.SILVER_ROOT) / "bike_rental_history" / "dt=2025-02-15" / "hh=00" / "0000_late.parquet",
        late_trip,
    )

    # (C) 2차 증분 — 방금 반영된 워터마크를 읽어 다시 실행한다.
    _run_incremental(spark, read_watermark(fe_config.WATERMARK_PATH))

    after = (
        spark.read.parquet(fe_config.FEATURES_TABLE_PARQUET)
        .filter((F.col("station_id") == "A") & (F.col("hour_ts") == F.lit(late_start)))
        .select("rental_count")
        .collect()[0][0]
    )

    assert after == before + 1


def test_next_and_prev_day_off_match_pandas_reference(spark, synthetic_environment):
    """build_merged_table()의 is_next_day_off/is_prev_day_off(Spark)가 pandas로 손계산한
    기대값과 정확히 같은지 확인한다 — inference/predict_single.py의
    `_build_target_time_fields()`가 쓰는 것과 정확히 같은 공식
    (`(dow+1)%7>=5`/`(dow+6)%7>=5` OR 휴일 멤버십)을 pandas로 재현해서 대조한다.
    이 컬럼은 이번에 신규 추가됐는데 기존 회귀 테스트(COMPARE_COLS)엔 lag/rolling만
    있어서 값 자체를 검증하는 테스트가 따로 없었다 — 이 테스트가 그 공백을 메운다.

    fixture의 공휴일(2025-01-15, 수요일)이 주말이 아니므로, "휴일 분기"가 "주말
    분기"와 뒤섞이지 않고 독립적으로 검증된다(아래 마지막 assert).
    """
    merged = build_merged_table(spark)
    got = merged.select("hour_ts", "is_next_day_off", "is_prev_day_off").toPandas()
    got = got.sort_values("hour_ts").reset_index(drop=True)

    holidays = {"2025-01-15"}
    hour_ts = pd.to_datetime(got["hour_ts"])
    dow = hour_ts.dt.dayofweek
    next_date = (hour_ts + pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d")
    prev_date = (hour_ts - pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d")
    expected_next = (next_date.isin(holidays) | (((dow + 1) % 7) >= 5)).astype(int)
    expected_prev = (prev_date.isin(holidays) | (((dow + 6) % 7) >= 5)).astype(int)

    assert (got["is_next_day_off"].to_numpy() == expected_next.to_numpy()).all()
    assert (got["is_prev_day_off"].to_numpy() == expected_prev.to_numpy()).all()

    # 2025-01-14(화, 평일)의 다음날은 공휴일(01-15, 수)이지만 주말은 아니다 —
    # is_next_day_off가 "주말이 아닌데도" 1이어야 휴일 분기가 실제로 동작한 것.
    jan14 = got[hour_ts.dt.strftime("%Y-%m-%d") == "2025-01-14"]
    assert (jan14["is_next_day_off"] == 1).all() and len(jan14) > 0


def test_incremental_is_noop_when_no_new_data(spark, synthetic_environment, tmp_path):
    """워터마크가 데이터의 최신 시각과 같으면(새 데이터 없음) append 없이 조용히 끝나야 한다."""
    full_rolling_path = str(tmp_path / "full_rolling2.parquet")
    build_rolling_rental_features(spark, output_path=full_rolling_path)
    full_merged = build_merged_table(spark)
    full_features = build_features(spark, full_merged, rolling_parquet_path=full_rolling_path)
    full_features.write.mode("overwrite").partitionBy("date").parquet(fe_config.FEATURES_TABLE_PARQUET)

    # 그리드가 5분 tick 단위라 마지막 시각은 "N_HOURS-1시간째" 정각이 아니라 그 시간의
    # 마지막 tick이다 — 손으로 계산하지 않고 실제 데이터의 max(hour_ts)를 그대로 쓴다.
    last_tick = full_features.agg(F.max("hour_ts")).collect()[0][0]
    write_watermark(fe_config.WATERMARK_PATH, last_tick.isoformat(), {})

    before_count = spark.read.parquet(fe_config.FEATURES_TABLE_PARQUET).count()
    _run_incremental(spark, {"max_hour_ts": last_tick.isoformat()})
    after_count = spark.read.parquet(fe_config.FEATURES_TABLE_PARQUET).count()

    assert before_count == after_count == N_HOURS * TICKS_PER_HOUR
