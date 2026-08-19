"""데이터 레이크 계층의 객체 키 규칙.

여러 모듈이 같은 계층을 읽고 쓰므로 경로 규칙을 한 곳에 둔다. `s3.py`는 버킷과
직렬화만 다루는 제네릭 입출력이고, "어떤 키에 무엇이 있는가"는 이 모듈이 정한다.

지금 여기 있는 것은 archive 계층뿐이다. silver·bronze는 `collector/storage.py`,
`nowcaster/storage.py`, `loader/s3_reader.py`, `libs/ml_core/silver_schema.py`에
네 벌로 흩어져 있는데, 옮기려면 네 모듈을 동시에 건드려야 해서 별도 작업으로 둔다.
"""

from __future__ import annotations

from datetime import date


def archive_key(source_id: str, day: date) -> str:
    """archive 객체 키를 만든다.

    silver와 달리 `hh=` 파티션이 없고 날짜당 파일 하나다 — archive는 "하루치"가 단위다.

    args:
        source_id: 소스 id
        day: 대상 날짜
    returns:
        `archive/{source_id}/dt=YYYY-MM-DD.parquet`
    """
    return f"archive/{source_id}/dt={day:%Y-%m-%d}.parquet"


def archive_prefix(source_id: str) -> str:
    """해당 소스의 archive 객체들이 모이는 prefix."""
    return f"archive/{source_id}/"
