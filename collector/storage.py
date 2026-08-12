"""S3/MinIO 입출력과 경로 규칙 생성.

구현 예정: docs/collector/implementation-issues.md #4
설계 근거: docs/collector/implementation-plan.md 6절 (경로 규칙)
          docs/adr/0003-bronze-streaming-and-scaling-boundaries.md

## 이 모듈의 역할

객체 저장소와 이야기하는 **유일한 창구**다. 경로 문자열을 만드는 곳도 여기 하나뿐이다.
pipeline · manifest는 경로를 조립하지 않고 이 모듈에 요청한다.

## 경로 규칙

    bronze             bronze/{source_id}/dt={date}/hh={hour}/{HHMM}/part={NNN}.json.gz
    silver·quarantine  {layer}/{source_id}/dt={date}/hh={hour}/{HHMM}.{ext}
    _manifest          _manifest/{source_id}/dt={date}/hh={hour}/{HHMM}.json

- **bronze만** window마다 디렉토리를 하나 더 갖는다. 조각으로 저장되기 때문이다.
- `dt` · `hh` · `HHMM`은 모두 `window_start`에서 파생된다. 멱등 키가
  `(source_id, window_start)`이므로 **같은 키는 항상 같은 경로**로 떨어지고, 재실행이
  이전 결과를 덮어쓴다.
- `part={NNN}`은 호출 순서다. 도착 순서가 아니라 이 인덱스가 읽는 순서를 결정한다.

## 구현할 것

- `write_bronze_part(source_id, window_start, index, chunk)` — 응답 조각 하나를 원본
  그대로(무손실) + gzip으로 올린다. `fetch`가 조각을 흘려보낼 때마다 호출된다.
  정규화하거나 필드를 고르지 않는다.
- `read_bronze(artifacts.bronze)` — 조각을 **`part` 인덱스 순서로** 읽어 목록으로
  돌려준다. 반환 형태가 `write_bronze_part`에 넘어온 조각과 **왕복 일치**해야
  `normalize`가 그대로 동작한다.
- `clear_bronze(source_id, window_start)` — 재개 전에 그 window의 bronze prefix를
  비운다. 조각 수가 실행마다 달라질 수 있어(5조각 → 3조각) 유령 조각이 남는다.
- `write_silver` — pyarrow로 Parquet **파일 하나**를 쓴다. `_row_status` 메타 컬럼을
  포함하고, 컬럼 타입은 검증 엔진이 캐스팅한 결과를 따른다.
  `drop_ratio`가 임계치를 넘은 배치에서는 **호출되지 않는다**(pipeline이 판정을 먼저
  한다). 그 경우 `artifacts.silver`는 null로 남는다.
- `write_quarantine` — 폐기된 행을 JSONL로 쓴다. **폐기 행이 0건이면 호출하지 않고
  객체도 만들지 않는다.** 5분 주기 소스 3종이면 빈 객체가 하루 864개 쌓이고, 대부분의
  실행은 폐기 0건이다. `artifacts.quarantine`이 null인지로 폐기 여부를 알 수 있고
  `counts.dropped`와 교차 검증도 된다. 이 파일을 읽는 쪽은 부재를 정상으로 처리한다.
- boto3 클라이언트 구성 — 엔드포인트 · 액세스 키 · 버킷은 환경변수에서 읽는다
  (`.env.example` 참고). MinIO(로컬)와 S3를 같은 코드로 다룬다.

## 주의

- **조각이 존재하는 것과 bronze가 완결된 것은 다르다.** 완결 판정은 manifest의 `stage`가
  단독으로 한다. 이 모듈은 조각을 쓰고 읽을 뿐 완결 여부를 판단하지 않는다.
- manifest **직렬화**는 manifest.py, manifest **I/O**는 이 모듈이다. 역할을 섞지 않는다.
- 쓰기는 실패할 수 있는 지점이다. 예외를 삼키지 말고 올려 pipeline이 `stage`와
  `failure_reason=storage_error`를 남기게 한다. silver 쓰기 실패가 bronze 재사용 재개의
  주요 시나리오다.

검증(계획서 11절): 조각 왕복 일치(순서 포함)와 `clear_bronze`(5조각 → 3조각 재실행에서
유령 조각이 남지 않는지)를 확인한다. `make up`으로 MinIO를 띄우면 bronze 조각 수가 API
호출 수와 일치하는지, silver · manifest가 하나씩 생겼는지 콘솔에서 볼 수 있다.
"""
