"""S3에서 실버 계층 및 ML 추론 결과 Parquet 데이터를 읽어온다."""

from __future__ import annotations

import io
from datetime import datetime

# pyrefly: ignore [missing-import]
import pyarrow.parquet as pq

# pyrefly: ignore [missing-import]
from core.s3 import get_object_bytes


def _silver_key(source_id: str, window_start: datetime) -> str:
    """지정된 소스와 윈도우 시각에 대응하는 Silver Parquet S3 키를 생성한다."""
    return (
        f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.parquet"
    )


def _predictions_key(window_start: datetime) -> str:
    """지정된 윈도우 시각에 대응하는 ML 추론 결과 Parquet S3 키를 생성한다."""
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}.parquet"


def _urgency_key(window_start: datetime) -> str:
    """지정된 윈도우 시각에 대응하는 urgency 배치 결과 Parquet S3 키를 생성한다.
    rebalance/main.py의 _urgency_key와 같은 포맷을 여기서도 자체 복제한다 —
    loader가 다른 배치 모듈을 import하지 않는 기존 원칙(read_predictions와
    동일한 이유). 두 파일의 포맷이 갈라지면 loader가 rebalance의 결과물을
    못 찾게 되니, 한쪽을 바꾸면 반드시 다른 쪽도 같이 바꿔야 한다."""
    return f"urgency/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/urgency_{window_start:%H%M}.parquet"


def _routes_key(window_start: datetime) -> str:
    """지정된 윈도우 시각에 대응하는 라우트 배치 결과(헤더) Parquet S3 키를 생성한다.
    rebalance/routes_main.py의 _routes_key와 같은 포맷을 여기서도 자체 복제한다
    (_urgency_key와 동일한 이유)."""
    return f"routes/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/routes_{window_start:%H%M}.parquet"


def _route_stops_key(window_start: datetime) -> str:
    """지정된 윈도우 시각에 대응하는 라우트 배치 결과(스톱) Parquet S3 키를 생성한다.
    rebalance/routes_main.py의 _route_stops_key와 같은 포맷을 여기서도 자체 복제한다."""
    return f"route_stops/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/route_stops_{window_start:%H%M}.parquet"


