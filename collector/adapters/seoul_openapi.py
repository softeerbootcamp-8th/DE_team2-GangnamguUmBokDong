"""서울 열린데이터광장 어댑터 — 소스 5종이 공유한다.

구현 예정: docs/collector/implementation-issues.md #6
설계 근거: docs/collector/implementation-plan.md 3절 (어댑터 계약)

## 공유하는 소스 5종

따릉이 실시간 대여정보 · 따릉이 대여이력 정보 · 서울 실시간 인구 데이터 ·
서울시 문화행사·공연행사 · 서울 생활인구(250m).

5종이 `{인증키}/{포맷}/{서비스명}/{시작}/{끝}/` 형태의 **동일한 페이지네이션 규약**을
쓰므로 어댑터 하나로 커버된다. 소스별 차이는 `adapter_params`로만 표현한다.

## adapter_params

- `service` — 서비스명(예: `bikeList`)
- `page_size` — 한 번에 가져올 건수. 서울 API 상한은 1,000건이다.
- `root_key` — 응답에서 행 배열을 꺼낼 경로(예: `rentBikeStatus.row`)

## fetch 구현할 것 — 페이지마다 yield

1. 첫 페이지를 호출해 `list_total_count`를 읽고, **그 응답을 바로 `yield`한다.**
2. `page_size` 단위로 `{시작}/{끝}` 인덱스를 만들어 남은 페이지를 순회하며 각 응답을
   차례로 `yield`한다. 인덱스는 **1부터 시작하고 끝 값을 포함**한다.
3. **`RESULT.CODE` 검사** — 서울 API는 HTTP 200으로 응답하면서 본문에 에러 코드를
   담는다. `rentBikeStatus.RESULT.CODE`가 `INFO-000`이 아니면 실패로 올린다.
   "해당 데이터 없음" 계열 코드를 빈 결과로 볼지 실패로 볼지는 #6에서 확정한다.

`yield`하는 값은 **가공되지 않은 응답 원본**이다. 여기서 행을 꺼내거나 페이지를 합치지
않는다. 조각을 S3에 저장하는 것은 pipeline이고, `yield` 순서가 곧 `part={NNN}`이 된다.

재시도 · HTTP 클라이언트는 base의 공통 유틸을 쓴다.

## normalize 구현할 것

- 조각 목록을 순서대로 돌며 `root_key`를 점 표기 경로로 따라가 행 배열을 꺼내고, 그
  결과를 이어 붙인다.
- 이 API는 이미 "행 = 레코드"이므로 pivot이 필요 없다. 구조 변환은 이것뿐이다.

## 주의

- 인증키는 `SEOUL_OPENAPI_KEY`에서 읽는다. **키가 URL 경로에 들어가므로** 로그 ·
  예외 메시지 · bronze에 남지 않게 마스킹한다.
- 전 필드가 문자열로 내려온다. 캐스팅은 검증 엔진의 `types`가 담당하고 어댑터는
  값에 손대지 않는다.

검증(계획서 11절): `httpx.MockTransport`로 페이지네이션 · 재시도 · `RESULT.CODE` 에러
처리를 확인한다.
"""
