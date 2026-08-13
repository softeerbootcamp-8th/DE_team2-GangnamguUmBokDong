"""Adapter 프로토콜(fetch / normalize), 어댑터 레지스트리, 라운드 재시도.

구현 예정: docs/collector/implementation-issues.md #6
설계 근거: docs/collector/implementation-plan.md 3절 (어댑터 계약)
          docs/collector/implementation-plan.md 8절 (부분 실패와 백필)
          docs/adr/0003-bronze-streaming-and-scaling-boundaries.md
          docs/adr/0004-partial-fetch-and-backfill.md

## 이 모듈의 역할

어댑터가 지켜야 할 계약, config의 `adapter` 문자열을 실제 어댑터로 바꾸는 레지스트리,
그리고 **조각 실패를 다루는 공통 재시도 구조**를 둔다. **어댑터는 API 제공처 단위**라
소스 7개를 어댑터 2개로 수용한다.

## 정의할 타입

- `Window` — `window_start` · `window_end`(UTC aware). 멱등 키의 절반이고
  `window_end`는 config의 `schedule.interval`로 계산된 값이 들어온다.
- `RawChunk` — **호출 한 번의 응답 원본**. 이 값 하나가 bronze 조각 하나
  (`part={chunk_key}.json.gz`)가 된다. 가공되지 않은 그대로여야 한다.
- `FetchErrorKind` — `TRANSIENT` · `PERMANENT` · `FATAL` (아래 참고)
- `FetchResult` — 조각 하나의 결과. 성공이든 실패든 이 타입으로 나온다.

      @dataclass(frozen=True)
      class FetchResult:
          key: str                        # 조각 식별 키
          payload: RawChunk | None        # 성공 시 원본 응답
          error: FetchErrorKind | None    # 실패 시 범주
          expected_total: int | None      # 전체 행 수 (알 수 있는 소스의 첫 조각에만)

- `Adapter` 프로토콜
  - `fetch(config, window, *, skip=frozenset(), expected_total=None)
     -> Iterator[FetchResult]`
  - `normalize(chunks) -> list[dict]` — 조각 목록을 행 = 레코드 형태로 변환한다.

## 조각 키

파일명이 되는 값이므로 **어댑터가 요청 파라미터에서 만든다.** 실행 간에 같은 요청이면
같은 키가 나와야 백필이 조각을 지목할 수 있다.

    page-00001-01000     서울 — 페이지 인덱스 범위 (제로 패딩)
    grid-060x127         기상청 — 격자 좌표

순번(`part={NNN}`)을 쓰지 않는 이유는 `list_total_count`가 변하면 같은 번호가 다른
요청을 가리키기 때문이다. 읽는 순서는 manifest의 `artifacts.bronze.parts` 목록이
정하므로 파일명이 순서를 표현할 필요가 없다.

## 실패는 예외가 아니라 값이다

`fetch`는 실패를 **`error`가 채워진 `FetchResult`로 흘려보낸다.** 예외로 올리면 첫
실패에서 이터레이터가 끊겨 나머지 조각을 시도할 수 없다. 라운드 루프가 어댑터 밖에
있으므로 어댑터는 실패를 보고만 하고 판단하지 않는다.

| 범주 | 해당 | 라운드 재투입 |
| --- | --- | --- |
| `TRANSIENT` | 타임아웃 · 429 · 5xx · 서울 `ERROR-5xx` | O |
| `PERMANENT` | 400 · 404 | X — 그 조각만 누락 확정 |
| `FATAL` | 401 · 403 · 서울 `INFO-100`(인증키 오류) | X — **fetch 전체 즉시 중단** |

`FATAL`을 분리하는 이유는 **모든 조각이 같은 인증키를 쓰기 때문**이다. 하나가 401이면
나머지도 401이라 20개 × 6회를 헛되이 부르게 된다. 원인이 확정적인데 서킷브레이커
발동을 기다릴 이유가 없다.

## 구현할 것

- 어댑터 레지스트리 — 등록 데코레이터와 `get_adapter(name)`. config의 `adapter` 키가
  실제 등록된 어댑터인지 기동 시점에 확인할 수 있게 존재 확인 함수를 노출한다.
- **라운드 오케스트레이션** — 실패분을 모아 재순회하는 공통 루프. pipeline이 호출하고
  어댑터는 알지 못한다. 반환은 `(성공 조각 dict, 누락 정보)`다.

      라운드 0 : 전체 순회 → 성공분은 콜백으로 즉시 저장, 실패분 수집
        ↓ 15s
      라운드 1 : TRANSIENT 실패분만 재순회 (skip = 이미 확보한 키)
        ↓ 30s
      라운드 2 : 남은 TRANSIENT 실패분만
        ↓
      남은 것은 누락 확정 → pipeline이 게이트 판정

- **안전장치 셋** — 전부 여기서만 정하고 어댑터마다 다시 구현하지 않는다.
  - `fetch_budget` — window 하나의 fetch 전체 예산. **새 호출을 시작하기 직전에만**
    판정해 진행 중인 호출은 끝까지 둔다.
  - 서킷브레이커 — 연속 실패 5회면 라운드를 더 돌지 않고 종료. 비율이 아니라 연속
    횟수인 이유는 조각 3개짜리 소스에서 1개 실패가 33%라 비율 기준이 정상 상황에
    발동하기 때문이다.
  - 라운드 간 대기 — 15s → 30s. 조각이 3개뿐이면 라운드 0이 1~2초에 끝나 간격을 벌지
    못한다.
- 호출 단위 백오프 — **2회로 줄인다.** 라운드가 얹히므로 그대로 두면 총 시도가
  곱해지고, 429는 짧은 재시도가 오히려 rate limit 창을 연장시킨다. `Retry-After`
  헤더가 오면 항상 존중하고, 남은 예산을 넘는 값이면 그 조각을 이번 실행에서 포기한다.
- httpx 클라이언트 구성(타임아웃 기본값 등)도 여기서 만들어 어댑터가 주입받게 한다.
  테스트에서 `httpx.MockTransport`를 끼울 수 있어야 한다.

라운드 수 · 대기 · 브레이커 임계는 **config가 아니라 이 모듈의 상수**다. 소스별로 다를
이유가 없고, 설정 키를 늘리면 소스 YAML 7개에 그대로 곱해진다.

## 계약 규칙 (어겨지면 설계가 무너지는 지점)

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
manifest · 재개 분기는 바뀌지 않는다. 근거는 ADR 0003 마지막 항목.

검증(계획서 12절): `httpx.MockTransport`로 3범주 분류, 라운드 재투입(429 → 성공),
서킷브레이커(연속 5회), `fetch_budget` 초과 시 새 호출 중단을 확인한다.
"""
