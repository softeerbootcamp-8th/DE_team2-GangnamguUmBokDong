"""YAML 로드 + 스키마 검증 + 정책 이름 존재 검증 + SHA-256 해시.

구현 예정: docs/collector/implementation-issues.md #2
설계 근거: docs/collector/implementation-plan.md 4절 (config 스키마)

## 이 모듈의 역할

`sources/{source_id}.yaml` 한 개를 읽어 **검증이 끝난** `SourceConfig`로 바꾸는 단일
관문이다. pipeline이 가장 먼저 호출하며, 여기서 예외가 나면 네트워크 호출은 한 번도
나가지 않는다.

## 공개 인터페이스

- `load(source_id) -> SourceConfig`
  - 경로는 `sources/{source_id}.yaml`로 조립한다.
  - YAML 안의 `source_id` 값과 파일명이 일치하는지도 확인한다. 어긋나면 manifest와
    S3 경로가 파일명과 달라져 추적이 깨진다.
  - 캐시는 두지 않는다. 한 프로세스는 한 소스의 한 window만 처리한다.

## 검증 5단계 (순서가 의미를 가진다)

1. **pydantic 스키마 검증** — 구조 · 타입 · 필수 키를 본다. 여기서 실패하면 필드
   자체가 없으므로 뒤 단계를 볼 의미가 없다.
2. **정책 이름 존재 검증** — pydantic은 `on_outlier: clip_to_rnge` 같은 오타도 그냥
   `str`로 보고 통과시킨다. "YAML이 함수를 문자열로 가리킨다"는 설계의 대가를 기동
   시점에 갚는 단계다. 검사 대상은 6곳이다.
   - `policies`의 4분면 4개 — `required_missing` · `required_outlier` ·
     `optional_missing` · `optional_outlier`
   - `policies.row` — null이면 행 정책을 쓰지 않는다는 뜻이므로 검사에서 제외한다
   - 컬럼마다 선언된 `on_missing` · `on_outlier` (오버라이드가 있을 때만)

   컬럼 정책은 `@policy` 레지스트리에서, `row`는 `@row_policy` 레지스트리에서 찾는다.
   **두 레지스트리를 섞어 조회하면 안 된다** — 계약(인자 개수)이 달라 실행 중에 터진다.
3. **`row_params` 검증** — `policies.row`에 등록된 params 모델을 레지스트리에서 꺼내
   `row_params`를 그 모델로 파싱한다. 두 방향 모두 오류로 막는다.
   - params 모델이 있는 정책인데 `row_params`가 없거나 필드가 틀렸다
   - params 모델이 없는 정책인데 `row_params`가 들어왔다

   이 단계가 없으면 `max_issue: 3` 같은 오타가 런타임까지 살아남아 "config 오타는 수집이
   시작되기 전에 죽는다"는 원칙이 깨진다.
4. **`backfill` 조합 검증** — `enabled: true`인데 `max_age`가 없으면 오류다. 만료되지
   않는 백필은 `_retry_queue` 마커를 영원히 쌓는다. 기본값을 주지 않는 이유는 소스마다
   API 보관 기간이 다르고(기상청 6h, 서울 일 단위 7d), 잘못된 기본값은 "만료된 줄
   알았는데 계속 도는" 조용한 낭비가 되기 때문이다.
5. **SHA-256 계산** — 결과를 `config_version`(`sha256:...`)으로 모델에 담는다.
   파싱된 dict가 아니라 **파일 원문 바이트**를 해시한다. 키 순서나 pydantic 기본값
   채움에 값이 흔들리면 재처리 대상 선별에 쓸 수 없다.

## 왜 전부 로드 시점에 하는가

5분 주기 실시간 API는 실행 창이 짧다. 2,700행을 모두 받아놓고 검증 막판에 "정책
이름이 없다"로 죽으면 그 window의 데이터는 영구히 받을 수 없다. 그래서 fetch 전에
죽인다. **config 오타는 수집이 시작되기 전에 죽는다.**

## 주의

- 레지스트리는 데코레이터가 실행돼야 채워진다. 이 보장은 loader가 아니라
  `validation/__init__.py`가 맡는다 — 패키지를 import하면 `policies`가 함께 로드되므로
  레지스트리가 비어 있는 상태가 존재하지 않는다. loader는 `validation.registry`의 조회
  함수를 그냥 부르면 된다.
- 에러 메시지 품질이 이 모듈의 기능이다. 어느 파일 · 어느 컬럼 · 어떤 이름이
  틀렸는지와 **등록된 이름 목록**을 함께 보여준다. 고치는 주체는 사람이다.
- `config_version`은 manifest에 기록되어 "이 silver가 어떤 정책으로 만들어졌는지"를
  남긴다. 나중에 범위 기준을 바꾸면 이 해시로 재처리 대상을 골라낸다.
- 이 모듈은 `validation.registry`만 참조하면 되고 계약 타입은 필요 없다. `types.py`를
  import하지 않는 것이 정상이다.
"""
