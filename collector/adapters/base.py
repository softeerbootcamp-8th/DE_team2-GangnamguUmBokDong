"""API에서 데이터를 가져오는(fetch) · 가공하는(normalize) 어댑터의 공통 규칙.

어댑터 하나는 소스 하나가 아니라 API 제공처 하나를 맡는다. 예를 들어 서울시 API를
쓰는 소스가 5개여도 어댑터는 `seoul_openapi` 하나면 충분하다.

`fetch`는 실패해도 예외를 던지지 않고 "실패했다"는 정보를 담은 값을 그냥 반환한다.
예외를 던지면 그 순간 나머지 조각을 아예 못 받아오기 때문이다. 실패는 세 가지로 나뉜다.

| 범주 | 해당 | 처리 |
| --- | --- | --- |
| `TRANSIENT` | 타임아웃 · 429 · 5xx | 다시 시도하면 될 수도 있다 → 재시도 |
| `PERMANENT` | 400 · 404 | 다시 시도해도 안 된다 → 그 조각만 포기 |
| `FATAL` | 401 · 403 (인증키 오류) | 키 자체가 문제다 → fetch 전체 중단 |

어댑터가 지켜야 할 것:
- S3 저장은 모른다. 가져온 데이터만 넘기면 pipeline이 저장한다.
- 몇 번 재시도할지도 모른다. 그건 바깥의 라운드 로직이 정한다.
- 받아온 응답을 임의로 바꾸지 않는다. 원본 그대로 넘긴다.
- `normalize`는 네트워크를 쓰지 않는 순수 변환 함수다.
- 소스마다 다른 부분은 config로 받는다. 코드 안에서 "이 소스면 이렇게" 식으로 분기하지 않는다.
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
