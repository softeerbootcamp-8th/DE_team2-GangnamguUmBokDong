"""dev-only 계약 테스트: rebalance가 자체 재구현한 S3 키 컨벤션이
libs/ml_core/silver_schema.py의 규칙과 갈라지지 않는지 확인한다.

rebalance 런타임(main.py/reader.py/urgency.py)은 ml_core을 import하지 않는다 —
이 파일만 예외적으로 dev 의존성(pyproject.toml dependency-groups.dev)의 ml_core을
테스트 목적으로만 가져온다. loader/tests/test_predictions_key_contract.py와 같은
패턴이다.
"""

from datetime import UTC, datetime

import pandas as pd
from ml_core import silver_schema

from reader import _predictions_key, _silver_key


def test_silver_key_matches_ml_core_convention():
    samples = [
        datetime(2026, 8, 16, 14, 5, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 12, 31, 23, 55, tzinfo=UTC),
    ]
    for window_start in samples:
        expected = silver_schema.silver_key("bike_station_realtime", pd.Timestamp(window_start))
        assert _silver_key("bike_station_realtime", window_start) == expected


def test_predictions_key_matches_ml_core_convention():
    samples = [
        datetime(2026, 8, 16, 14, 5, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 12, 31, 23, 55, tzinfo=UTC),
    ]
    for window_start in samples:
        expected = silver_schema.predictions_key(pd.Timestamp(window_start))
        assert _predictions_key(window_start) == expected
