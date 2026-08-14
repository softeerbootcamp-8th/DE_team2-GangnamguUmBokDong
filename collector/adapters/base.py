"""Adapter 프로토콜(fetch, normalize), 어댑터 레지스트리 및 공통 재시도 로직을 정의한다.

어댑터가 지켜야 할 계약과 `adapter` 문자열을 실제 객체로 매핑하는 레지스트리를 제공하며,
조각(Chunk) 실패 시 대응하기 위한 공통 재시도(라운드) 구조를 포함합니다.
어댑터는 데이터 소스가 아닌 API 제공처 단위로 작성되므로, 여러 소스가 하나의 어댑터를 공유할 수 있습니다.

## 조각 키
파일명이 되는 값이므로 **어댑터가 요청 파라미터에서 만든다.** 실행 간에 같은 요청이면
같은 키가 나와야 백필이 조각을 지목할 수 있다.

    page-00001-01000     서울 — 페이지 인덱스 범위 (제로 패딩)
    grid-060x127         기상청 — 격자 좌표

## 실패는 예외가 아니라 값이다
`fetch`는 실패를 **`error`가 채워진 `FetchResult`로 흘려보낸다.** 예외로 올리면 첫
실패에서 이터레이터가 끊겨 나머지 조각을 시도할 수 없다. 라운드 루프가 어댑터 밖에
있으므로 어댑터는 실패를 보고만 하고 판단하지 않는다.

| 범주 | 해당 | 라운드 재투입 |
| --- | --- | --- |
| `TRANSIENT` | 타임아웃 · 429 · 5xx · 서울 `ERROR-5xx` | O |
| `PERMANENT` | 400 · 404 | X — 그 조각만 누락 확정 |
| `FATAL` | 401 · 403 · 서울 `INFO-100`(인증키 오류) | X — **fetch 전체 즉시 중단** |

httpx 클라이언트 구성(타임아웃 기본값 등)도 여기서 만들어 어댑터가 주입받게 한다.
라운드 수 · 대기는 config가 아니라 이 모듈의 상수다. 소스별로 다를 이유가 없다.

## 계약 규칙
- **어댑터는 저장소를 알지 못한다.** `fetch`는 조각을 `yield`할 뿐이고 S3에 쓰는 것은
  pipeline이다. 어댑터가 storage를 직접 부르면 어댑터 단위 테스트에 S3 목이 필요해지고
  "어댑터는 네트워크만 안다"는 경계가 무너진다.
- **어댑터는 라운드를 알지 못한다.** 실패를 값으로 보고할 뿐 몇 번 다시 시도할지는
  밖에서 정한다. `skip`을 받아 이미 있는 조각을 건너뛰는 것이 어댑터가 하는 전부다.
- `fetch`는 응답을 **가공하지 않는다.** 필드를 고르거나 이름을 바꾸면 bronze 무손실이
  깨지고, 정책을 바꿔 재처리할 때 원본이 없다.
- `expected_total`은 **알 수 있는 소스만** 채운다. 서울은 첫 페이지의
  `list_total_count`, 기상청은 `None`이다. pipeline이 이 값을 기억했다가 다음 라운드와
  백필에 되돌려주므로, 첫 페이지를 skip해도 계획을 세울 수 있다.
- `normalize`는 **네트워크를 타지 않는 순수 함수**여야 한다. 재개 시 bronze를 읽어
  항상 다시 호출되기 때문이다(계획서 7절).
- `normalize`는 조각 하나가 아니라 **조각 목록 전체**를 받는다. 페이지 이어붙이기와 격자
  pivot이 조각을 가로질러야 한다. 반환은 항상 "행 = 레코드"인 `list[dict]`다.
- 소스별 차이는 전부 `config.adapter_params`로 받는다. 어댑터 안에
  `if source_id == ...`를 쓰면 "소스가 늘어도 공통 코드는 바뀌지 않는다"는 목표가
  깨진다.

## 확장 여지
동시 호출이 필요해지면(격자가 수십 개로 늘어나는 등) 여기서 async 클라이언트를 제공하고
`asyncio.run`을 어댑터 `fetch` 안에 가둔다. 프로토콜을 동기로 유지하므로 pipeline ·
manifest · 재개 분기는 바뀌지 않는다.
"""


from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import httpx

    from config.schema import SourceConfig


@dataclass(frozen=True, slots=True)
class Window:
    """수집 대상 시간 창."""

    window_start: datetime
    window_end: datetime


class FetchErrorKind(Enum):
    """조각 실패 범주."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True)
class FetchResult:
    """조각 하나의 결과. 성공이든 실패든 이 타입으로 나온다.
    
    key: 조각 식별 키
    payload: 성공 시 원본 응답
    error: 실패 시 범주 (TRANSIENT, PERMANENT, FATAL)
    expected_total: 전체 행 수 (알 수 있는 소스의 첫 조각에만)

    에러 시 예외를 던지지 않고 종류를 적어서 반환한다.
    파이프라인이 멈추지 않고 계속 진행하여 재시도 전략을 짤 수 있도록 하기 위함.
    """

    key: str
    payload: bytes | None
    error: FetchErrorKind | None   
    expected_total: int | None


class Adapter(Protocol):
    """API 제공처 하나에 대응하는 fetch · normalize 계약.
    서울시 API든. 기상청 API든, 새로운 외부 데이터 소스를 추가할 때
    fetch와 normalize 규격만 맞춰서 구현하면, 엔진이 알아서 파이프라인을 돌려준다.
    """

    # fetch: 네트워크를 열어 원본 데이터를 가져오는 역할만 담당.
    @staticmethod
    def fetch(
        config: SourceConfig,
        window: Window,
        *,
        client: httpx.Client,
        skip: frozenset[str] = frozenset(),
        expected_total: int | None = None,
    ) -> Iterator[FetchResult]: ...

    # normalize: bytes 조각들을 파이썬 딕셔너리 리스트로 파싱하는 역할만 담당
    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]: ...


class UnknownAdapterError(ValueError):
    """등록되지 않은 어댑터 이름을 조회했다."""


class DuplicateAdapterError(ValueError):
    """같은 이름을 두 번 등록했다. 조용히 덮어쓰면 어느 어댑터가 돌았는지 알 수 없다."""


_ADAPTERS: dict[str, type] = {}


def adapter(name: str) -> Callable[[type], type]:
    """어댑터를 이름으로 등록한다. 원본 클래스를 그대로 반환한다."""

    def register(cls: type) -> type:
        if name in _ADAPTERS:
            raise DuplicateAdapterError(
                f"어댑터 '{name}'이 이미 등록되어 있습니다: {_ADAPTERS[name].__qualname__}"
            )
        _ADAPTERS[name] = cls
        return cls

    return register


def get_adapter(name: str) -> type:
    """등록된 어댑터를 이름으로 조회한다."""
    try:
        return _ADAPTERS[name]
    except KeyError:
        listed = ", ".join(adapter_names()) or "(없음)"
        raise UnknownAdapterError(
            f"어댑터 '{name}'이 등록되어 있지 않습니다. 등록된 이름: {listed}"
        ) from None


def is_adapter_registered(name: str) -> bool:
    """어댑터 이름이 등록돼 있는지 본다. 인스턴스를 만들지 않는다."""
    return name in _ADAPTERS


def adapter_names() -> tuple[str, ...]:
    """등록된 어댑터 이름을 정렬된 튜플로 반환한다."""
    return tuple(sorted(_ADAPTERS))


def classify_http_status(status_code: int) -> FetchErrorKind | None:
    """HTTP 상태 코드를 실패 범주로 매핑한다. `None`은 성공(2xx)이다.
    HTTP 상태만으로 성공/실패를 가르는 제공처가 공유하는 규칙이다.
    """
    if 200 <= status_code < 300:
        return None
    if status_code in (401, 403):
        return FetchErrorKind.FATAL
    if status_code == 429 or status_code >= 500:
        return FetchErrorKind.TRANSIENT
    return FetchErrorKind.PERMANENT


@dataclass(frozen=True, slots=True)
class FetchRoundResult:
    """라운드 오케스트레이션의 최종 결과."""

    chunks: dict[str, bytes]
    missing: dict[str, FetchErrorKind]
    expected_total: int | None


ROUND_WAITS_SECONDS = (15, 30)
"""라운드 0→1 대기, 라운드 1→2 대기."""

MAX_ROUNDS = len(ROUND_WAITS_SECONDS) + 1


def fetch_with_rounds(
    fetch_fn,
    config,
    window,
    *,
    client,
    skip=frozenset(),
    expected_total=None,
    sleep_fn=time.sleep,
    now_fn=time.monotonic,
    on_chunk=None,
):
    """실패분을 모아 재순회하는 공통 라운드 루프이다.

    어댑터의 fetch 함수를 호출하여 데이터를 수집하며, 
    TRANSIENT가 발생한 조각들을 모아 지정된 대기 시간 후 최대 지정된 라운드 횟수까지 재시도합니다.
    어댑터는 이 라운드 로직을 알지 못하며 오직 파이프라인 엔진에서만 이 함수를 호출합니다.

    args:
        fetch_fn: 어댑터의 fetch 제너레이터 함수
        config: 소스별 설정 객체
        window: 수집 대상 시간 윈도우
        client: 통신에 사용할 httpx 클라이언트
        skip: 이미 확보되어 이번 수집에서 건너뛸 조각 키들의 집합
        expected_total: 예상되는 전체 데이터 건수 (이전 라운드나 백필에서 넘겨줌)
        sleep_fn: 대기 함수 (테스트 시 모킹용)
        now_fn: 현재 시간 측정 함수 (테스트 시 모킹용)
        on_chunk: 조각 수집 성공 시 즉시 실행할 콜백 함수 (보통 S3 스트리밍 저장용)
    returns:
        성공분(chunks), 누락분(missing), 전체 행 수(expected_total)를 담은 FetchRoundResult 객체
    """
    collected: dict[str, bytes] = {}
    permanent: dict[str, FetchErrorKind] = {}
    transient: dict[str, FetchErrorKind] = {}

    # (1) 마감 시한 방어
    # fetch_budget: window 하나의 fetch 전체에 걸리는 시간 제한
    deadline = now_fn() + config.effective_fetch_budget().total_seconds()

    for round_index in range(MAX_ROUNDS):
        # 라운드 1·2는 재시도할 TRANSIENT가 없으면 돌지 않는다.
        if round_index > 0 and not transient:
            break
        # 이미 시작된 라운드는 끝까지 진행한다.
        if now_fn() >= deadline:
            break

        # (2) 재시도 타겟팅
        # skip: 저번 배치에서 이미 성공했던 것들
        # collected.keys(): 방금 전 라운드에서 방금 성공한 것들
        # permanent.keys(): 아예 포기하기로 확정된 것들

        round_skip = frozenset(skip) | collected.keys() | permanent.keys()
        transient = {}
        aborted = False  # FATAL로 이번 fetch 전체를 접었는지

        iterator = fetch_fn(config, window, client=client, skip=round_skip, expected_total=expected_total)
        
        # 새 호출을 시작하기 직전에만 budget을 체크한다.
        while True:
            if now_fn() >= deadline:
                aborted = True
                break
            try:
                result = next(iterator)
            except StopIteration:
                break

            # expected_total은 첫 조각에만 실려 온다. 
            # 한 번 채워지면 이후 라운드·백필에 그대로 되돌려주므로 여기서 덮어쓰지 않는다.
            if expected_total is None and result.expected_total is not None:
                expected_total = result.expected_total

            if result.error is None:
                collected[result.key] = result.payload

                # (3) 스트리밍 저장
                # on_chunk 콜백 함수(pipeline이 즉시 bronze에 쓴다)
                if on_chunk is not None:
                    on_chunk(result.key, result.payload)
            elif result.error is FetchErrorKind.FATAL:
                # 모든 조각이 같은 인증키를 쓰므로 하나가 FATAL이면 나머지도 잘못되었을 것이므로,
                # 라운드를 더 돌 이유가 없어 즉시 전체를 접는다.
                permanent[result.key] = result.error
                aborted = True
                break
            elif result.error is FetchErrorKind.PERMANENT:
                permanent[result.key] = result.error  # 이 조각만 누락 확정, 재시도 안 함
            else:  # TRANSIENT
                transient[result.key] = result.error  # 다음 라운드가 다시 시도한다

        if aborted:
            break

        # 마지막 라운드 뒤에는 대기하지 않는다.
        # transient가 비었을 때도 대기하지 않는다.
        if round_index < len(ROUND_WAITS_SECONDS) and transient:
            sleep_fn(ROUND_WAITS_SECONDS[round_index])

    # 여기까지 남은 transient는 라운드를 다 써버려서 더 재시도하지 못하는 것들이다.
    # permanent와 합쳐 이번 실행에서 못 받은 조각으로 pipeline에 돌려준다.
    missing = {**permanent, **transient}
    return FetchRoundResult(chunks=collected, missing=missing, expected_total=expected_total)
