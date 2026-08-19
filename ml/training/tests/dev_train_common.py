"""train_common._dates_for_split()이 day-of-month 배수 기준으로 train/valid/test 날짜를
정확히 나누고, 빈 구간이나 TRAIN_DAY_DIVISOR와 겹치는 VALID/TEST 설정을 조용히 통과시키지
않고 바로 에러를 내는지 검증한다.

**2026-08 전면 개편**: 예전엔 이 함수(`_split()`)가 이미 로드된 DataFrame을 받아
`day` 컬럼을 역산해 day-of-month를 구했다. 지금은 Spark의 `date=YYYY-MM-DD/` 파티션
이름 자체에서 day-of-month를 바로 뽑을 수 있어(`lazy_train_dataset.py`가 이 함수가
정한 날짜만 S3에서 읽음), 데이터를 전혀 읽지 않는 순수 캘린더 연산이 됐다 —
`_dates_for_split(start, end) -> (train_dates, valid_dates, test_dates)`.

train은 `config.TRAIN_DAY_DIVISOR`의 배수인 날 중 valid/test가 아닌 날(기본값
1 — 사실상 전체 날짜, 다운샘플링 없음), valid/test는 `config.VALID_DAYS_OF_MONTH`/
`TEST_DAYS_OF_MONTH`만 쓴다 — divisor는 로컬 RAM이 부족해 날짜 자체를 임시로
줄여야 할 때만 2, 3, 5로 올리는 dial이다(`training/config.py` 참고, 기본
정책은 20분 tick 밀도를 유지한 채 1년 전체 사용).
"""

from datetime import date

import pytest

from training import config
from training.train_common import _dates_for_split


def test_dates_for_split_rejects_valid_or_test_days_overlapping_train_divisor(monkeypatch):
    """VALID/TEST에 TRAIN_DAY_DIVISOR의 배수가 섞이면 train과 겹쳐 누출이 생기므로 바로 에러여야 한다."""
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 2)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({20}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({7}))

    from training.train_common import _validate_valid_test_days_dont_overlap_train

    with pytest.raises(ValueError, match="TRAIN_DAY_DIVISOR"):
        _validate_valid_test_days_dont_overlap_train()


def test_dates_for_split_buckets_dates_by_train_day_divisor(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 2)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))

    train, valid, test = _dates_for_split(date(2026, 1, 1), date(2026, 2, 28))

    assert "2026-01-02" in train  # 짝수날
    assert "2026-01-05" not in train  # 홀수날, valid/test도 아님 -> 어디에도 없음
    assert "2026-01-05" not in valid
    assert "2026-01-05" not in test
    assert valid == ["2026-01-19", "2026-02-19"]
    assert test == ["2026-01-23", "2026-02-23"]
    # divisor의 배수인 19/23일이 우연히 있어도 valid/test가 먼저 확정되므로 train엔 없다
    assert "2026-01-19" not in train
    assert "2026-01-23" not in train


def test_dates_for_split_with_default_divisor_uses_full_year_excluding_valid_test(monkeypatch):
    """기본값(TRAIN_DAY_DIVISOR=1)에서는 모든 날짜가 "배수"라 valid/test를 먼저
    확정하고 나머지 중에서만 train 조건을 보지 않으면 전부 train에도 겹쳐 들어간다."""
    assert config.TRAIN_DAY_DIVISOR == 1  # 이 테스트가 실제 기본값을 검증하고 있는지 확인
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))

    train, valid, test = _dates_for_split(date(2026, 1, 1), date(2026, 1, 31))

    # 19일(valid)/23일(test)을 제외한 나머지 전부가 train — divisor=2였다면
    # 버려졌을 01-05(홀수)도 이제 train에 포함된다.
    assert "2026-01-02" in train
    assert "2026-01-05" in train
    assert valid == ["2026-01-19"]
    assert test == ["2026-01-23"]
    assert set(train) & set(valid) == set()
    assert set(train) & set(test) == set()


def test_dates_for_split_with_train_day_divisor_three(monkeypatch):
    """TRAIN_DAY_DIVISOR를 3으로 올리면(짝수날만으로 부족할 때의 폴백) 3의 배수인
    날만 train이 되고, 짝수지만 3의 배수가 아닌 날(01-02)은 버려져야 한다."""
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 3)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))

    train, _valid, _test = _dates_for_split(date(2026, 1, 1), date(2026, 1, 6))

    assert train == ["2026-01-03", "2026-01-06"]
    assert "2026-01-02" not in train


def test_dates_for_split_empty_range_returns_empty_lists(monkeypatch):
    monkeypatch.setattr(config, "TRAIN_DAY_DIVISOR", 2)
    monkeypatch.setattr(config, "VALID_DAYS_OF_MONTH", frozenset({19}))
    monkeypatch.setattr(config, "TEST_DAYS_OF_MONTH", frozenset({23}))

    # end < start — safety_cutoff_date가 TRAIN_YEAR 1월 1일보다 이른 극단적인 경우를 흉내
    train, valid, test = _dates_for_split(date(2026, 2, 1), date(2026, 1, 1))

    assert train == []
    assert valid == []
    assert test == []
