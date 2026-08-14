"""구조화 로그 설정 — 고정 필드(source_id·window·attempt) 주입.


## 구현할 것

- `source_id` · `window` · `attempt`를 **모든 로그 레코드에 자동으로 붙인다.**
  `logging.LoggerAdapter`나 `logging.Filter` 중 하나로 구현하고, 호출부가 매번 같은
  필드를 다시 적지 않게 한다.
- 출력은 컨테이너 stdout으로 보낸다. 형식은 `key=value` 평문에서 시작한다
  (JSON으로 바꿀지는 수집기를 붙이는 시점에 결정한다).
- 레벨 규칙 — 정상 단계는 INFO, `PARTIAL`은 WARN, `FAILED`는 ERROR.

## 출력량 규칙: 배치당 몇 줄, 행당 0줄

2,765행 × 288회/일이면 행 단위 로그는 로그를 터뜨린다. **행 상세는 quarantine 파일이
담당한다.** 조각마다, 그리고 라운드마다 로그를 남기지도 않는다. 아래 3줄이 한 배치의
정상 출력 전부다.

    INFO  source_id=bike_station_realtime window=2026-08-12T14:10Z
          stage=bronze_written parts=3/3 rounds=1 rows=2765 bytes=482113 ms=1203
    WARN  source_id=… stage=validated status=PARTIAL kept=2740 repaired=31
          dropped=25 drop_ratio=0.009 completeness=0.991
    INFO  source_id=… stage=completed revision=1 key=s3://…/1410.parquet

`stage` 값은 manifest의 `Stage`와 같은 어휘를 쓴다. `fetched`는 없다 — 조각을 도착 즉시
저장하므로 fetch 완료와 bronze 완료가 같은 시점이고, 위 첫 줄이 그 둘을 한꺼번에 알린다.
`parts`는 **받은 조각 / 계획한 조각**이고 `rounds`는 라운드 수다. 라운드별 로그 대신
이 두 값으로 재시도가 있었는지 알 수 있다.

누락이 발생하면 첫 줄이 WARN이 되고 무엇이 빠졌는지 붙는다.

    WARN  source_id=… stage=bronze_written parts=2/3 rounds=3
          missing=page-02001-02765 missing_rows=765 completeness=0.717

실패 시에는 `failure_reason`을 함께 남긴다. 수집 게이트와 폐기 게이트는 사유가 다르므로
로그만 봐도 재시도할 실패인지 config를 고칠 실패인지 구분된다.

    ERROR source_id=… stage=validated status=FAILED failure_reason=quality_gate
          dropped=412 drop_ratio=0.149
    ERROR source_id=… stage=bronze_written status=FAILED failure_reason=fetch_error
          missing_ratio=0.638 reason=budget_exceeded

백필 실행은 `revision` 변화를 남긴다. 이 한 줄이 "silver 내용이 언제 바뀌었는지"의
유일한 기록이므로 하류 재처리를 추적할 때 쓰인다.

    INFO  source_id=… mode=backfill parts=1 filled=page-02001-02765
          revision=1→2 completeness=0.717→1.0

## 구현 방향

- `configure_logging(source_id, window_start, attempt)`은 **root 로거**에 핸들러를
  붙인다. `pipeline.py`는 그냥 `logging.getLogger(__name__)`으로 자기 이름의 로거를
  얻어 쓰면 되고, 전파(`propagate`)를 타고 올라와 root의 핸들러에서 고정 필드가
  주입된다 — 모듈마다 로거를 따로 설정할 필요가 없다.
- 고정 필드 주입은 `logging.Filter`가 담당하고, 나머지 필드는 호출부가 `extra=`로
  넘긴 값을 포매터가 `key=value`로 이어붙인다.
- 인증키 마스킹도 여기 한 곳에서 한다 — 최종 로그 줄 문자열에서 `key=`류 쿼리
  파라미터 값을 정규식으로 가려, 예외 메시지에 URL이 실려도 키가 새지 않게 막는다.
  (어댑터마다 마스킹을 반복하지 않기 위해 이 모듈에 모았다.)

## 주의

- boto3 · httpx의 기본 로거가 시끄러우면 레벨을 따로 낮춘다.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import datetime

_FIXED_FIELDS = ("source_id", "window", "attempt")
_STANDARD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {"message", "asctime"}

# 기상청 API는 인증키를 쿼리 파라미터로 받고, 서울 API는 경로 세그먼트로 받는다.
# 1. 쿼리 파라미터 형태 (?key=..., &ServiceKey=...)
_SECRET_QUERY_RE = re.compile(r"(?i)([?&](?:service|auth)?key=)[^&\s'\"]+")
# 2. 서울 API 경로 파라미터 형태 (openapi.seoul.go.kr:8088/{key}/...)
_SECRET_PATH_RE = re.compile(r"(?i)(openapi\.seoul\.go\.kr:8088/)[^/\s'\"]+")


def _redact(text: str) -> str:
    """URL 쿼리 파라미터와 경로에 실린 인증키 값을 가린다."""
    text = _SECRET_QUERY_RE.sub(r"\1***", text)
    text = _SECRET_PATH_RE.sub(r"\1***", text)
    return text


class _ContextFilter(logging.Filter):
    """모든 레코드에 source_id·window·attempt를 주입한다."""

    def __init__(self, source_id: str, window_start: datetime, attempt: int):
        super().__init__()
        self._source_id = source_id
        self._window = window_start.isoformat()
        self._attempt = attempt

    def filter(self, record: logging.LogRecord) -> bool:
        """레코드에 고정 필드 3개를 얹고 항상 통과시킨다(걸러내는 필터가 아니다)."""
        record.source_id = self._source_id
        record.window = self._window
        record.attempt = self._attempt
        return True


class _KeyValueFormatter(logging.Formatter):
    """`LEVEL message key=value ...` 형식으로 렌더링한다.

    고정 필드(source_id·window·attempt)를 먼저 쓰고, 호출부가 `extra=`로 넘긴
    필드를 그 뒤에 넘긴 순서대로 이어붙인다.
    """

    def format(self, record: logging.LogRecord) -> str:
        """레코드 하나를 한 줄로 렌더링한다."""
        parts = [record.levelname]
        message = record.getMessage()
        if message:
            parts.append(message)
        for field in _FIXED_FIELDS:
            if hasattr(record, field):
                parts.append(f"{field}={getattr(record, field)}")
        # 표준 LogRecord 속성(pathname·lineno 등)을 뺀 나머지가 `extra=`로 넘어온
        # 호출부 필드다. dict 순서가 삽입 순서를 보존하므로 넘긴 순서 그대로 붙는다.
        for key, value in vars(record).items():
            if key in _FIXED_FIELDS or key in _STANDARD_ATTRS:
                continue
            parts.append(f"{key}={value}")
        return _redact(" ".join(parts))


def configure_logging(
    source_id: str,
    window_start: datetime,
    attempt: int = 1,
    *,
    level: int = logging.INFO,
    stream=None,
) -> logging.Logger:
    """source_id·window·attempt를 모든 로그에 자동으로 붙이는 root 로거를 설정한다.

    `main.py`가 pipeline을 부르기 **전에** 호출해야 pipeline이 남기는 로그에도
    고정 필드가 붙는다.

    args:
        source_id: 이번 실행의 소스 id.
        window_start: 이번 실행이 처리하는 window의 시작 시각.
        attempt: 이번 실행이 몇 번째 시도인지. manifest의 attempt와 같은 값을 쓴다.
        level: root 로거의 최소 레벨.
        stream: 로그를 보낼 스트림. 생략하면 컨테이너 stdout(`sys.stdout`).
    returns:
        설정이 끝난 root 로거.
    """
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(_KeyValueFormatter())
    handler.addFilter(_ContextFilter(source_id, window_start, attempt))
    root.addHandler(handler)

    return root
