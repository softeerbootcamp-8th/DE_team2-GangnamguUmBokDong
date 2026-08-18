"""train_common._split()이 빈 구간을 조용히 통과시키지 않고 바로 에러를 내는지 검증한다.

`config.py`의 TRAIN/VALID/TEST 구간이 이제 "오늘 - 안전마진" 기준으로 매번 동적으로
계산되므로(고정 캘린더 달이 아님), feature mart가 그 구간까지 아직 안 쌓였으면
학습이 빈 데이터로 진행되다가 lgb.train() 안에서 알아보기 힘든 에러로 죽을 수 있다 —
이 테스트가 그 경우 `_split()` 단계에서 먼저 걸러지는지 확인한다.
"""

import pandas as pd
import pytest

from training import config
from training.train_common import _split


def _make_df(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"date": dates, "value": range(len(dates))})


def test_split_raises_when_test_window_has_no_rows(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_START", "2026-01-01")
    monkeypatch.setattr(config, "TRAIN_END", "2026-01-05")
    monkeypatch.setattr(config, "VALID_START", "2026-01-06")
    monkeypatch.setattr(config, "VALID_END", "2026-01-06")
    monkeypatch.setattr(config, "TEST_START", "2026-01-07")
    monkeypatch.setattr(config, "TEST_END", "2026-01-07")

    # feature mart가 2026-01-06까지만 쌓여있는 상황(test 구간 데이터 없음)을 흉내낸다.
    df = _make_df(["2026-01-01", "2026-01-02", "2026-01-06"])

    with pytest.raises(ValueError, match="학습 구간에 데이터가 없음"):
        _split(df)


def test_split_succeeds_when_all_windows_have_rows(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_START", "2026-01-01")
    monkeypatch.setattr(config, "TRAIN_END", "2026-01-02")
    monkeypatch.setattr(config, "VALID_START", "2026-01-03")
    monkeypatch.setattr(config, "VALID_END", "2026-01-03")
    monkeypatch.setattr(config, "TEST_START", "2026-01-04")
    monkeypatch.setattr(config, "TEST_END", "2026-01-04")

    df = _make_df(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])

    train, valid, test = _split(df)

    assert len(train) == 2 and len(valid) == 1 and len(test) == 1
