"""Adapter 프로토콜(fetch / normalize)과 어댑터 레지스트리.

구현 예정: docs/collector/implementation-issues.md #6
설계 근거: docs/collector/implementation-plan.md 3절 (어댑터 계약)
          docs/adr/0003-bronze-streaming-and-scaling-boundaries.md

## 이 모듈의 역할

어댑터가 지켜야 할 계약과, config의 `adapter` 문자열을 실제 어댑터로 바꾸는 레지스트리를
둔다. **어댑터는 API 제공처 단위**라 소스 7개를 어댑터 2개로 수용한다.

## 정의할 타입

- `Window` — `window_start` · `window_end`(UTC aware). 멱등 키의 절반이고
  `window_end`는 config의 `schedule.interval`로 계산된 값이 들어온다.
- `RawChunk` — **호출 한 번의 응답 원본**. 이 값 하나가 bronze 조각 하나
  (`part={NNN}.json.gz`)가 된다. 가공되지 않은 그대로여야 한다.
- `Adapter` 프로토콜
  - `fetch(config, window) -> Iterator[RawChunk]` — 호출할 때마다 응답을 `yield`한다.
  - `normalize(chunks) -> list[dict]` — 조각 목록을 행 = 레코드 형태로 변환한다.

## 구현할 것

- 어댑터 레지스트리 — 등록 데코레이터와 `get_adapter(name)`. config의 `adapter` 키가
  실제 등록된 어댑터인지도 기동 시점에 확인할 수 있게 존재 확인 함수를 노출한다.
- 공통 재시도 유틸 — 타임아웃 · 429 · 5xx는 지수 백오프로 재시도한다. 429를 제외한
  4xx는 재시도하지 않는다(인증키 오류를 반복해도 결과가 같다). 재시도 횟수와 대기는
  어댑터마다 다시 구현하지 않고 여기서만 정한다.
- httpx 클라이언트 구성(타임아웃 기본값 등)도 여기서 만들어 어댑터가 주입받게 한다.
  테스트에서 `httpx.MockTransport`를 끼울 수 있어야 한다.

## 계약 규칙 (어겨지면 설계가 무너지는 지점)

- **어댑터는 저장소를 알지 못한다.** `fetch`는 조각을 `yield`할 뿐이고 S3에 쓰는 것은
  pipeline이다. 어댑터가 storage를 직접 부르면 어댑터 단위 테스트에 S3 목이 필요해지고
  "어댑터는 네트워크만 안다"는 경계가 무너진다.
- `fetch`는 응답을 **가공하지 않는다.** 필드를 고르거나 이름을 바꾸면 bronze 무손실이
  깨지고, 정책을 바꿔 재처리할 때 원본이 없다.
- **`yield` 순서가 곧 조각 인덱스**(`part={NNN}`)다. pipeline이 이 순서로 번호를
  붙이므로 같은 bronze를 재처리하면 언제나 같은 silver가 나온다.
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

검증(계획서 11절): `httpx.MockTransport`로 재시도 분기(타임아웃 · 429 · 5xx · 4xx)를
확인한다.
"""
