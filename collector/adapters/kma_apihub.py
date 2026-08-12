"""기상청 API 허브 어댑터 — 소스 2종이 공유한다.

구현 예정: docs/collector/implementation-issues.md #9
설계 근거: docs/collector/implementation-plan.md 3절 (어댑터 계약)

## 공유하는 소스 2종

기상청 초단기 실황·예보(10분) · 기상청 단기예보(3시간).

## adapter_params

- `endpoint` — 호출할 엔드포인트(예: `getUltraSrtNcst`)
- `root_key` — 행 배열 경로(예: `response.body.items.item`)
- `pivot` — long → wide 변환 기준(예: `{key: category, value: obsrValue}`)
- `grids` — 서울을 커버할 격자 목록(예: `[[60, 127], [61, 127]]`)

## fetch 구현할 것 — 격자마다 yield

- **격자 반복** — 기상청은 격자(`nx`, `ny`)마다 별도 호출이 필요하다. `grids`를 순서대로
  돌며 각 격자의 응답 원본을 그대로 `yield`한다. 조각 저장은 pipeline이 맡고,
  `yield` 순서가 곧 `part={NNN}`이 된다.
- `base_date` · `base_time`은 window로 계산한다. 발표 주기와 window 경계가 어긋날 때
  어느 발표분을 집을지는 #9에서 확정한다.
- 격자 수만큼 호출이 선형으로 늘어난다. 격자 목록을 코드에 박지 않고 config에 두는
  이유가 이것이다. 동시 호출이 필요해지면 base의 async 클라이언트를 쓰되 `asyncio.run`은
  이 `fetch` 안에 가둔다(ADR 0003).

## normalize 구현할 것 — long → wide pivot

기상청 응답은 기온 · 습도 · 풍속이 **각각 별도 행으로 쌓인 long format**이다.

    # 원본 (bronze 조각에 저장되는 형태)
    {"category": "T1H", "obsrValue": "31.6", "nx": 60, "ny": 127, ...}
    {"category": "REH", "obsrValue": "42",   ...}

    # normalize 결과 (검증 엔진이 받는 형태)
    {"baseDate": ..., "baseTime": ..., "nx": 60, "ny": 127,
     "T1H": "31.6", "REH": "42", "WSD": "3.2", "PTY": "0", ...}

- **조각 목록 전체를 가로질러** 처리한다. 그룹 키는 `(baseDate, baseTime, nx, ny)`이고,
  같은 키의 행들을 한 행으로 합쳐 `pivot.key`의 값을 컬럼명으로, `pivot.value`의 값을
  값으로 올린다.
- 예보 엔드포인트는 값 필드가 `fcstValue`이고 예보 시각(`fcstDate` · `fcstTime`)이
  그룹 키에 추가된다. **코드 분기가 아니라 `pivot` 설정으로 흡수한다.**

이 정규화 덕분에 config가 `T1H` · `REH`처럼 컬럼별로 서로 다른 정상 범위를 선언할 수
있고, 검증 엔진에는 조건부 range 같은 개념이 필요 없어진다.

## 주의

- 인증키는 `KMA_APIHUB_KEY`에서 읽고 로그 · bronze에 남지 않게 마스킹한다.
- 캐스팅은 검증 엔진의 `types`가 담당한다. pivot은 구조만 바꾸고 값은 문자열 그대로
  둔다.

검증(계획서 11절): long → wide pivot이 관측 항목을 컬럼으로 올바르게 펴는지 확인한다.
"""
