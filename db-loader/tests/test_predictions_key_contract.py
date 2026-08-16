"""dev-only 계약 테스트: db-loader가 자체 재구현한 predictions 키 컨벤션이
libs/ml_common/silver_schema.py:predictions_key()와 갈라지지 않는지 확인한다.

db-loader 런타임(main.py/s3_reader.py)은 ml_common을 import하지 않는다 — 이 파일만
예외적으로 dev 의존성(pyproject.toml dependency-groups.dev)의 ml_common을 테스트
목적으로만 가져온다.
"""

from datetime import UTC, datetime

import pandas as pd
from ml_common import silver_schema

from s3_reader import _predictions_key


def test_predictions_key_matches_ml_common_convention():
    samples = [
        datetime(2026, 8, 16, 14, 5, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 12, 31, 23, 55, tzinfo=UTC),
    ]

    for window_start in samples:
        expected = silver_schema.predictions_key(pd.Timestamp(window_start))
        assert _predictions_key(window_start) == expected
