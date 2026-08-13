"""S3/MinIO 입출력과 경로 규칙 생성.

구현 예정: docs/collector/implementation-issues.md #4
설계 근거: docs/collector/implementation-plan.md 6절 (경로 규칙)
          docs/collector/implementation-plan.md 8절 (부분 실패와 백필)
          docs/adr/0003-bronze-streaming-and-scaling-boundaries.md
          docs/adr/0004-partial-fetch-and-backfill.md

## 이 모듈의 역할

객체 저장소와 이야기하는 **유일한 창구**다. 경로 문자열을 만드는 곳도 여기 하나뿐이다.
pipeline · manifest는 경로를 조립하지 않고 이 모듈에 요청한다.

## 경로 규칙

    bronze             bronze/{source_id}/dt={date}/hh={hour}/{HHMM}/part={chunk_key}.json.gz
    silver·quarantine  {layer}/{source_id}/dt={date}/hh={hour}/{HHMM}.{ext}
    _manifest          _manifest/{source_id}/dt={date}/hh={hour}/{HHMM}.json
    _retry_queue       _retry_queue/{source_id}/{window_start}.json

- **bronze만** window마다 디렉토리를 하나 더 갖는다. 조각으로 저장되기 때문이다.
- `dt` · `hh` · `HHMM`은 모두 `window_start`에서 파생된다. 멱등 키가
  `(source_id, window_start)`이므로 **같은 키는 항상 같은 경로**로 떨어지고, 재실행이
  이전 결과를 덮어쓴다.
- `_retry_queue`만 날짜 파티션이 없다. 백필 잡이 소스별 prefix 하나만 LIST해서 대상을
  얻어야 하기 때문이다.

## 조각 키

`{chunk_key}`는 **요청을 식별하는 키**이고 어댑터가 만든다.

    part=page-00001-01000.json.gz     서울 — 페이지 인덱스 범위
    part=grid-060x127.json.gz         기상청 — 격자 좌표

순번(`part={NNN}`)을 쓰지 않는 이유는 실행 간에 안정적이지 않기 때문이다.
`list_total_count`가 변하면 같은 번호가 다른 페이지 범위를 가리켜 백필이 조각을 지목할
수 없다. **읽는 순서는 파일명이 아니라 manifest의 `artifacts.bronze.parts` 목록이
정한다** — 순번이 맡던 역할은 원래 그쪽에 있었다.

## 구현할 것

- `write_bronze_part(source_id, window_start, chunk_key, chunk)` — 응답 조각 하나를 원본
  그대로(무손실) + gzip으로 올린다. `fetch`가 조각을 흘려보낼 때마다 호출된다.
  정규화하거나 필드를 고르지 않는다.
- `read_bronze(artifacts.bronze)` — 조각을 **`parts` 목록 순서로** 읽어 목록으로
  돌려준다. 반환 형태가 `write_bronze_part`에 넘어온 조각과 **왕복 일치**해야
  `normalize`가 그대로 동작한다. **`parts`에 없는 객체는 읽지 않는다** — 이 규칙이 백필
  모드에서 `clear_bronze`를 생략해도 유령 조각이 섞이지 않게 막는다.
- `clear_bronze(source_id, window_start)` — 재개 전에 그 window의 bronze prefix를
  비운다. 조각 수가 실행마다 달라질 수 있어(5조각 → 3조각) 유령 조각이 남는다.
  **백필 모드에서는 호출하지 않는다.** 기존 조각을 살리는 것이 목적이다.
- `write_silver` — pyarrow로 Parquet **파일 하나**를 쓴다. `_row_status` 메타 컬럼을
  포함하고, 컬럼 타입은 검증 엔진이 캐스팅한 결과를 따른다.
  게이트(`max_missing_ratio` · `max_drop_ratio`)를 넘긴 배치에서는 **호출되지
  않는다**(pipeline이 판정을 먼저 한다). 그 경우 `artifacts.silver`는 null로 남는다.
  백필이 갱신할 때는 **같은 경로에 덮어쓴다** — 추가 파일을 만들지 않는다.
- `write_quarantine` — 폐기된 행을 JSONL로 쓴다. **폐기 행이 0건이면 호출하지 않고
  객체도 만들지 않는다.** 5분 주기 소스 3종이면 빈 객체가 하루 864개 쌓이고, 대부분의
  실행은 폐기 0건이다. `artifacts.quarantine`이 null인지로 폐기 여부를 알 수 있고
  `counts.dropped`와 교차 검증도 된다. 이 파일을 읽는 쪽은 부재를 정상으로 처리한다.
- **`_retry_queue` 마커 I/O** — `write_retry_marker` · `list_retry_markers(source_id)` ·
  `delete_retry_marker`. 마커는 백필 대상 인덱스다(계획서 8.5절).
- boto3 클라이언트 구성 — 엔드포인트 · 액세스 키 · 버킷은 환경변수에서 읽는다
  (`.env.example` 참고). MinIO(로컬)와 S3를 같은 코드로 다룬다.

## 마커는 인덱스, manifest는 진실

    _retry_queue/{source_id}/{window_start}.json
    { "source_id": ..., "window_start": ..., "missing_parts": ["page-02001-02765"],
      "first_failed_at": ..., "expires_at": ..., "attempts": 3 }

manifest 전수 스캔은 대상 소스 5종 기준 하루 442개, 보관 7일이면 3,094개를 매 실행마다
훑는다. 마커는 **스캔 비용이 실패 건수에 비례**하고 평상시에는 LIST 한 번에 빈 결과다.

이 모듈은 마커를 **쓰고 읽고 지울 뿐 판단하지 않는다.** 백필 잡은 마커로 후보를 얻은 뒤
반드시 manifest를 읽어 실제 상태를 확인하므로, 마커가 잔존하거나 유실돼도 오동작이
아니라 스킵 또는 백필 누락으로만 이어진다. 마커 쓰기 실패의 최악은 "백필을 안 하는
것"이지 잘못된 데이터가 아니다.

## 주의

- **조각이 존재하는 것과 bronze가 완결된 것은 다르다.** 이 모듈은 조각을 쓰고 읽을 뿐
  완결 여부를 판단하지 않는다. 실행 진행도는 manifest의 `stage`가, 조각이 다 모였는지는
  `completeness` · `missing`이 표현한다.
- manifest **직렬화**는 manifest.py, manifest **I/O**는 이 모듈이다. 역할을 섞지 않는다.
- 쓰기는 실패할 수 있는 지점이다. 예외를 삼키지 말고 올려 pipeline이 `stage`와
  `failure_reason=storage_error`를 남기게 한다. silver 쓰기 실패가 bronze 재사용 재개의
  주요 시나리오다.
- 조각 키가 파일명이 되므로 **인증키 같은 비밀이 섞이지 않게** 한다. 키를 만드는 것은
  어댑터지만 경로로 굳는 곳은 여기다.

검증(계획서 12절): 조각 왕복 일치(`parts` 순서 포함), `parts`에 없는 객체를 무시하는지,
`clear_bronze`(5조각 → 3조각 재실행에서 유령 조각이 남지 않는지), 마커 생성·삭제를
확인한다. `make up`으로 MinIO를 띄우면 bronze 조각 수가 API 호출 수와 일치하는지,
silver · manifest가 하나씩 생겼는지 콘솔에서 볼 수 있다.
"""
