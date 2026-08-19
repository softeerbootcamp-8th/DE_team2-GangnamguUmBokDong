"""train_common._split()이 day-of-month 배수 기준으로 train/valid/test를 정확히
나누고, 빈 구간이나 TRAIN_DAY_DIVISOR와 겹치는 VALID/TEST 설정을 조용히 통과시키지
않고 바로 에러를 내는지 검증한다.

train은 `config.TRAIN_DAY_DIVISOR`의 배수인 날 전부(기본 2=짝수날), valid/test는
`config.VALID_DAYS_OF_MONTH`/`TEST_DAYS_OF_MONTH`(TRAIN_DAY_DIVISOR의 배수가 아닌
날짜만)만 쓴다 — 2026-08 기준 2025년 전체 multi-horizon 테이블이 8억 행이라 행
단위 표본 추출(`TRAIN_SAMPLE_FRAC`)과 별개로 애초에 읽어들이는 총 행 수 자체를
줄이려고 도입한 방식이다(짝수날만으로도 부족하면 TRAIN_DAY_DIVISOR를 3, 5로
올려서 더 줄인다 — `training/config.py` 참고). `config.TRAIN_YEAR` 밖 날짜나
`config.safety_cutoff_date()`를 넘는(아직 라벨이 확정 안 됐을 수 있는) 날짜는 셋
다에서 제외된다. feature mart가 특정 구간까지 아직 안 쌓였으면 학습이 빈 데이터로
진행되다가 lgb.train() 안에서 알아보기 힘든 에러로 죽을 수 있다 — 이 테스트가 그
경우 `_split()` 단계에서 먼저 걸러지는지 확인한다.

`_split()`은 `date`(문자열) 대신 `day`(2000-01-01 기준 경과일수)만 본다 — 실제
multi-horizon 테이블에 `date` 컬럼 자체가 없어졌기 때문(Spark 파티션 컬럼이라 파일
내용엔 없음, train_common.load_training_table() 참고). 테스트는 가독성을 위해 날짜
문자열로 입력을 쓰되 `_day()`로 day 정수로 변환해서 넣는다.
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


def test_split_rejects_valid_or_test_days_overlapping_train_divisor(monkeypatch):
    """VALID/TEST에 TRAIN_DAY_DIVISOR의 배수가 섞이면 train과 겹쳐 누출이 생기므로 바로 에러여야 한다."""
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 2)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({20}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({7}))
    df = _make_df(["2026-01-01"])

    with pytest.raises(ValueError, match="TRAIN_DAY_DIVISOR"):
        _split(df)


def test_split_raises_when_test_days_of_month_have_no_rows(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 2)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 1, 31))

    # 2026-01-23(test)에 해당하는 행이 전혀 없다.
    df = _make_df(["2026-01-02", "2026-01-19"])

    with pytest.raises(ValueError, match="학습 구간에 데이터가 없음"):
        _split(df)


def test_split_buckets_rows_by_train_day_divisor(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 2)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 12, 31))

    df = _make_df([
        "2026-01-02", "2026-01-19", "2026-01-23", "2026-01-05",  # train / valid / test / 버려짐(미지정)
        "2026-02-04", "2026-02-19", "2026-02-23",
    ])

    train, valid, test = _split(df)

    assert sorted(train["day"]) == sorted(_day(d) for d in ["2026-01-02", "2026-02-04"])
    assert sorted(valid["day"]) == sorted(_day(d) for d in ["2026-01-19", "2026-02-19"])
    assert sorted(test["day"]) == sorted(_day(d) for d in ["2026-01-23", "2026-02-23"])

    # divisor의 배수도 아니고 valid/test로 지정도 안 된 날짜(01-05)는 셋 어디에도 없어야 한다.
    all_days = set(train["day"]) | set(valid["day"]) | set(test["day"])
    assert _day("2026-01-05") not in all_days


def test_split_with_train_day_divisor_three(monkeypatch):
    """TRAIN_DAY_DIVISOR를 3으로 올리면(짝수날만으로 부족할 때의 폴백) 3의 배수인
    날만 train이 되고, 짝수지만 3의 배수가 아닌 날(01-02)은 버려져야 한다."""
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 3)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 12, 31))

    df = _make_df(["2026-01-02", "2026-01-03", "2026-01-06", "2026-01-19", "2026-01-23"])

    train, _valid, _test = _split(df)

    assert sorted(train["day"]) == sorted(_day(d) for d in ["2026-01-03", "2026-01-06"])
    assert _day("2026-01-02") not in set(train["day"])


def test_split_excludes_rows_past_safety_cutoff(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 2)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 1, 20))

    # 01-23(test)는 안전 마진을 넘어서 통째로 제외돼야 하므로 test 구간에 데이터가
    # 없다는 에러가 나야 한다.
    df = _make_df(["2026-01-02", "2026-01-19", "2026-01-23"])

    with pytest.raises(ValueError, match="학습 구간에 데이터가 없음"):
        _split(df)


def test_split_excludes_rows_outside_train_year(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_YEAR", 2026)
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 2)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2026, 12, 31))

    df = _make_df(["2025-01-02", "2025-01-19", "2025-01-23", "2026-01-02", "2026-01-19", "2026-01-23"])

    train, valid, test = _split(df)

    assert list(train["day"]) == [_day("2026-01-02")]
    assert list(valid["day"]) == [_day("2026-01-19")]
    assert list(test["day"]) == [_day("2026-01-23")]
