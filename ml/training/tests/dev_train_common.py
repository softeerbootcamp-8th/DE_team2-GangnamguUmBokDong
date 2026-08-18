"""train_common._split()이 day-of-month 기준으로 train/valid/test를 정확히 나누고,
빈 구간을 조용히 통과시키지 않고 바로 에러를 내는지 검증한다.

매달 `config.VALID_DAYS_OF_MONTH`/`TEST_DAYS_OF_MONTH`에 속하는 날짜는 valid/test로,
나머지는 train으로 분류한다 — `config.TRAIN_YEAR` 밖 날짜나
`config.safety_cutoff_date()`를 넘는(아직 라벨이 확정 안 됐을 수 있는) 날짜는 셋
다에서 제외된다. feature mart가 특정 구간까지 아직 안 쌓였으면 학습이 빈 데이터로
진행되다가 lgb.train() 안에서 알아보기 힘든 에러로 죽을 수 있다 — 이 테스트가 그
경우 `_split()` 단계에서 먼저 걸러지는지 확인한다.

`_split()`은 이제 `date`(문자열) 대신 `day`(2000-01-01 기준 경과일수)만 본다 —
실제 multi-horizon 테이블에 `date` 컬럼 자체가 없어졌기 때문(Spark 파티션 컬럼이라
파일 내용엔 없음, train_common.load_training_table() 참고). 테스트는 가독성을 위해
날짜 문자열로 입력을 쓰되 `_day()`로 day 정수로 변환해서 넣는다.
"""

from datetime import date

import pandas as pd
import pytest
from ml_core.day_index import day_index

from training import config
from training.train_common import _split


def _day(date_str: str) -> int:
    return day_index(date.fromisoformat(date_str))


def _make_df(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"day": [_day(d) for d in dates], "value": range(len(dates))})


def test_split_raises_when_test_days_of_month_have_no_rows(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({20}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({24}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 1, 31))

    # 2026-01-24(test)에 해당하는 행이 전혀 없다.
    df = _make_df(["2026-01-01", "2026-01-20"])

    with pytest.raises(ValueError, match="학습 구간에 데이터가 없음"):
        _split(df)


def test_split_buckets_rows_by_day_of_month(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({20}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({24}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 12, 31))

    df = _make_df(["2026-01-01", "2026-01-20", "2026-01-24", "2026-02-01", "2026-02-20", "2026-02-24"])

    train, valid, test = _split(df)

    assert sorted(train["day"]) == sorted(_day(d) for d in ["2026-01-01", "2026-02-01"])
    assert sorted(valid["day"]) == sorted(_day(d) for d in ["2026-01-20", "2026-02-20"])
    assert sorted(test["day"]) == sorted(_day(d) for d in ["2026-01-24", "2026-02-24"])


def test_split_excludes_rows_past_safety_cutoff(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({20}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({24}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 1, 21))

    # 01-24(test)는 안전 마진을 넘어서 통째로 제외돼야 하므로 test 구간에 데이터가
    # 없다는 에러가 나야 한다.
    df = _make_df(["2026-01-01", "2026-01-20", "2026-01-24"])

    with pytest.raises(ValueError, match="학습 구간에 데이터가 없음"):
        _split(df)


def test_split_excludes_rows_outside_train_year(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({20}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({24}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 12, 31))

    df = _make_df(["2025-01-01", "2025-01-20", "2025-01-24", "2026-01-01", "2026-01-20", "2026-01-24"])

    train, valid, test = _split(df)

    assert list(train["day"]) == [_day("2026-01-01")]
    assert list(valid["day"]) == [_day("2026-01-20")]
    assert list(test["day"]) == [_day("2026-01-24")]
