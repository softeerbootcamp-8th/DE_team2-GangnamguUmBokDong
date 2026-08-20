"""_build_station_profile_arrays()/_profile_stat()의 dense 배열 기반 구현을 검증한다
(PR 리뷰 지적 — 예전 dict[tuple, dict[str,float]] 캐시가 station x minute x dow x month
전체 조합(수천 정류소 기준 약 1,800만 항목)을 그대로 담아 프로세스당 수 GB를 먹었다,
predict_single.py의 `_build_station_profile_arrays()` 모듈 docstring 참고).

이 테스트는 그 결과가 예전 dict 기반 구현과 정확히 같은 값을 돌려주는지(값 자체는
바뀌면 안 됨 — 순수 내부 표현 교체이므로) 확인한다.
"""

import numpy as np
import pandas as pd
import pytest

from inference import config
from inference import predict_single as ps


@pytest.fixture(autouse=True)
def _reset_module_caches():
    names = ["_station_profile_station_index", "_station_profile_values"]
    saved = {n: getattr(ps, n) for n in names}
    yield
    for n, v in saved.items():
        setattr(ps, n, v)


def _profile_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_build_station_profile_arrays_round_trips_exact_values():
    ts_a = pd.Timestamp("2026-01-05 00:00")  # 월요일(dow=0)
    ts_b = pd.Timestamp("2026-06-03 00:00") + pd.Timedelta(minutes=config.GRID_TICK_MINUTES)  # 수요일(dow=2)
    df = _profile_df(
        [
            {"station_no": 5, "minute": ts_a.hour * 60 + ts_a.minute, "dow": ts_a.dayofweek, "month": ts_a.month,
             "rental_mean": 1.0, "rental_std": 0.5, "return_mean": 2.0, "return_std": 0.25},
            {"station_no": 5, "minute": ts_b.hour * 60 + ts_b.minute, "dow": ts_b.dayofweek, "month": ts_b.month,
             "rental_mean": 9.0, "rental_std": 3.0, "return_mean": 4.0, "return_std": 1.5},
            {"station_no": 9, "minute": ts_a.hour * 60 + ts_a.minute, "dow": ts_a.dayofweek, "month": ts_a.month,
             "rental_mean": 100.0, "rental_std": 10.0, "return_mean": 50.0, "return_std": 5.0},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert ps._profile_stat(5, ts_a, "rental_mean") == pytest.approx(1.0)
    assert ps._profile_stat(5, ts_a, "return_std") == pytest.approx(0.25)
    assert ps._profile_stat(5, ts_b, "rental_mean") == pytest.approx(9.0)
    assert ps._profile_stat(9, ts_a, "return_mean") == pytest.approx(50.0)


def test_profile_stat_returns_nan_for_missing_combination():
    df = _profile_df(
        [{"station_no": 5, "minute": 0, "dow": 0, "month": 1,
          "rental_mean": 1.0, "rental_std": 0.5, "return_mean": 2.0, "return_std": 0.25}]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    # 같은 station이지만 등록되지 않은 (minute, dow, month) 조합
    assert np.isnan(ps._profile_stat(5, pd.Timestamp("2026-02-01 00:00"), "rental_mean"))


def test_profile_stat_returns_nan_for_unknown_station():
    df = _profile_df(
        [{"station_no": 5, "minute": 0, "dow": 0, "month": 1,
          "rental_mean": 1.0, "rental_std": 0.5, "return_mean": 2.0, "return_std": 0.25}]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert np.isnan(ps._profile_stat(999, pd.Timestamp("2026-01-01 00:00"), "rental_mean"))


@pytest.mark.parametrize("requested_minute", [5, 10, 15])
def test_profile_stat_floors_serving_tick_to_previous_twenty_minute_anchor(
    monkeypatch,
    requested_minute,
):
    """20분 profile 사이의 5분 요청은 미래값이 아닌 직전 학습 anchor를 사용한다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 20)
    monkeypatch.setattr(config, "TRAIN_ANCHOR_TICK_MINUTES", 20)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 17 * 60, "dow": 0, "month": 1,
             "rental_mean": 100.0, "rental_std": 1.0, "return_mean": 50.0, "return_std": 0.5},
            {"station_no": 5, "minute": 17 * 60 + 20, "dow": 0, "month": 1,
             "rental_mean": 200.0, "rental_std": 2.0, "return_mean": 80.0, "return_std": 0.8},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    ts = pd.Timestamp("2026-01-05 17:00") + pd.Timedelta(minutes=requested_minute)
    assert ps._profile_stat(5, ts, "rental_mean") == pytest.approx(100.0)


def test_profile_stat_keeps_exact_twenty_minute_anchor(monkeypatch):
    """20분 경계 요청은 해당 profile을 그대로 사용한다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 20)
    monkeypatch.setattr(config, "TRAIN_ANCHOR_TICK_MINUTES", 20)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 17 * 60, "dow": 0, "month": 1,
             "rental_mean": 100.0, "rental_std": 1.0, "return_mean": 50.0, "return_std": 0.5},
            {"station_no": 5, "minute": 17 * 60 + 20, "dow": 0, "month": 1,
             "rental_mean": 200.0, "rental_std": 2.0, "return_mean": 80.0, "return_std": 0.8},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert ps._profile_stat(5, pd.Timestamp("2026-01-05 17:20"), "rental_mean") == pytest.approx(200.0)


def test_profile_stat_floor_stays_on_requested_calendar_day(monkeypatch):
    """00:05 요청은 전날 23:40이 아니라 같은 날 00:00의 요일·월 profile을 쓴다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 20)
    monkeypatch.setattr(config, "TRAIN_ANCHOR_TICK_MINUTES", 20)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 23 * 60 + 40, "dow": 5, "month": 1,
             "rental_mean": 999.0, "rental_std": 1.0, "return_mean": 999.0, "return_std": 1.0},
            {"station_no": 5, "minute": 0, "dow": 6, "month": 2,
             "rental_mean": 100.0, "rental_std": 1.0, "return_mean": 50.0, "return_std": 0.5},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert ps._profile_stat(5, pd.Timestamp("2026-02-01 00:05"), "rental_mean") == pytest.approx(100.0)


@pytest.mark.parametrize(
    ("grid_tick", "expected_minute"),
    [(5, 55), (10, 50), (15, 45), (20, 40), (30, 30), (60, 0)],
)
def test_profile_stat_floors_1755_for_every_supported_model_grid(
    monkeypatch,
    grid_tick,
    expected_minute,
):
    """지원하는 모든 model grid에서 17:55가 미래 anchor를 참조하지 않는다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", grid_tick)
    monkeypatch.setattr(config, "TRAIN_ANCHOR_TICK_MINUTES", grid_tick)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 17 * 60 + expected_minute, "dow": 0, "month": 1,
             "rental_mean": 100.0, "rental_std": 1.0, "return_mean": 50.0, "return_std": 0.5},
            {"station_no": 5, "minute": 18 * 60, "dow": 0, "month": 1,
             "rental_mean": 200.0, "rental_std": 2.0, "return_mean": 80.0, "return_std": 0.8},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert ps._profile_stat(5, pd.Timestamp("2026-01-05 17:55"), "rental_mean") == pytest.approx(100.0)


def test_profile_stat_five_minute_model_uses_exact_anchor(monkeypatch):
    """비교용 5분 프로필을 선택하면 17:05 값을 내림 없이 그대로 사용한다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 5)
    monkeypatch.setattr(config, "TRAIN_ANCHOR_TICK_MINUTES", 5)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 17 * 60, "dow": 0, "month": 1,
             "rental_mean": 100.0, "rental_std": 1.0, "return_mean": 50.0, "return_std": 0.5},
            {"station_no": 5, "minute": 17 * 60 + 5, "dow": 0, "month": 1,
             "rental_mean": 105.0, "rental_std": 1.5, "return_mean": 55.0, "return_std": 0.6},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert ps._profile_stat(5, pd.Timestamp("2026-01-05 17:05"), "rental_mean") == pytest.approx(105.0)


def test_profile_stat_hybrid_five_grid_twenty_anchor_uses_training_minute(monkeypatch):
    """g5/a20 fallback은 base profile의 17:15가 아니라 실제 학습 anchor 17:00을 쓴다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 5)
    monkeypatch.setattr(config, "TRAIN_ANCHOR_TICK_MINUTES", 20)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 17 * 60, "dow": 0, "month": 1,
             "rental_mean": 100.0, "rental_std": 1.0, "return_mean": 50.0, "return_std": 0.5},
            {"station_no": 5, "minute": 17 * 60 + 15, "dow": 0, "month": 1,
             "rental_mean": 115.0, "rental_std": 1.5, "return_mean": 55.0, "return_std": 0.6},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert ps._profile_stat(5, pd.Timestamp("2026-01-05 17:15"), "rental_mean") == pytest.approx(100.0)


def test_profile_stat_twenty_grid_hourly_anchor_uses_previous_hour(monkeypatch):
    """g20/a60 fallback은 base profile을 20분 index로 읽되 직전 정각 anchor를 쓴다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 20)
    monkeypatch.setattr(config, "TRAIN_ANCHOR_TICK_MINUTES", 60)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 17 * 60, "dow": 0, "month": 1,
             "rental_mean": 100.0, "rental_std": 1.0, "return_mean": 50.0, "return_std": 0.5},
            {"station_no": 5, "minute": 17 * 60 + 40, "dow": 0, "month": 1,
             "rental_mean": 140.0, "rental_std": 1.5, "return_mean": 55.0, "return_std": 0.6},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert ps._profile_stat(5, pd.Timestamp("2026-01-05 17:55"), "rental_mean") == pytest.approx(100.0)


def test_build_station_profile_arrays_rejects_profile_from_different_model_grid(monkeypatch):
    """20분 모델이 5분 profile을 잘못 읽으면 배열 충돌 전에 즉시 실패해야 한다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 20)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 17 * 60 + 5, "dow": 0, "month": 1,
             "rental_mean": 105.0, "rental_std": 1.5, "return_mean": 55.0, "return_std": 0.6},
        ]
    )

    with pytest.raises(ValueError, match="활성 모델 grid"):
        ps._build_station_profile_arrays(df)


@pytest.mark.parametrize("minute", [None, "17:05"])
def test_build_station_profile_arrays_rejects_malformed_minute(monkeypatch, minute):
    """파싱할 수 없는 minute은 배열 인덱싱 전에 명시적인 계약 오류로 실패한다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 20)
    df = _profile_df(
        [
            {"station_no": 5, "minute": minute, "dow": 0, "month": 1,
             "rental_mean": 105.0, "rental_std": 1.5, "return_mean": 55.0, "return_std": 0.6},
        ]
    )

    with pytest.raises(ValueError, match="활성 모델 grid"):
        ps._build_station_profile_arrays(df)


@pytest.mark.parametrize(("dow", "month"), [(-1, 1), (7, 1), (0, 0), (0, 13), ("Mon", 1)])
def test_build_station_profile_arrays_rejects_invalid_calendar_key(monkeypatch, dow, month):
    """음수 numpy 인덱스로 조용히 다른 profile 칸을 덮는 잘못된 키를 거부한다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 20)
    df = _profile_df(
        [
            {"station_no": 5, "minute": 17 * 60, "dow": dow, "month": month,
             "rental_mean": 105.0, "rental_std": 1.5, "return_mean": 55.0, "return_std": 0.6},
        ]
    )

    with pytest.raises(ValueError, match="dow/month 범위"):
        ps._build_station_profile_arrays(df)


def test_build_station_profile_arrays_rejects_duplicate_logical_key(monkeypatch):
    """동일 key의 두 행이 dense 배열 한 칸을 순서 의존적으로 덮어쓰면 안 된다."""
    monkeypatch.setattr(config, "GRID_TICK_MINUTES", 20)
    row = {
        "station_no": 5,
        "minute": 17 * 60,
        "dow": 0,
        "month": 1,
        "rental_mean": 100.0,
        "rental_std": 1.0,
        "return_mean": 50.0,
        "return_std": 0.5,
    }
    df = _profile_df([row, {**row, "rental_mean": 999.0}])

    with pytest.raises(ValueError, match="logical key가 중복"):
        ps._build_station_profile_arrays(df)


def test_build_station_profile_arrays_uses_compact_dense_shape_not_per_combo_dict():
    """station 축은 실제로 등장한 station_no 개수만큼만 잡아야 한다(예: 정류소가
    2개뿐인데 station_no 값 자체는 1과 9000처럼 멀리 떨어져 있어도 배열 크기가
    9000이 되면 안 된다) — dense 배열 압축의 핵심 전제."""
    df = _profile_df(
        [
            {"station_no": 1, "minute": 0, "dow": 0, "month": 1,
             "rental_mean": 1.0, "rental_std": 0.0, "return_mean": 0.0, "return_std": 0.0},
            {"station_no": 9000, "minute": 0, "dow": 0, "month": 1,
             "rental_mean": 2.0, "rental_std": 0.0, "return_mean": 0.0, "return_std": 0.0},
        ]
    )
    station_index, values = ps._build_station_profile_arrays(df)

    assert values.shape[0] == 2  # station_no 값 범위(1~9000)가 아니라 실제 등장 개수
    assert set(station_index) == {1, 9000}
