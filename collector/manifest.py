"""manifest 스키마와 읽기/쓰기, 상태 어휘(RunStatus / Stage / FailureReason).

구현 예정: docs/collector/implementation-issues.md #4
설계 근거: docs/collector/implementation-plan.md 6절 (상태 어휘와 manifest)
          docs/adr/0003-bronze-streaming-and-scaling-boundaries.md

## 이 모듈의 역할

한 번의 실행이 **무엇을 했고 어디까지 갔는지**를 남긴다. 이 기록이 재개 판단의 유일한
근거이고, 소스가 몇 개든 어휘는 완전히 동일하다.

## RunStatus — 실행 결과

| 상태 | 의미 |
| --- | --- |
| `RUNNING` | 실행 중(시작 시 기록) |
| `SUCCEEDED` | silver까지 완료, 폐기된 행 없음 |
| `PARTIAL` | silver까지 완료, 일부 행 quarantine (`drop_ratio <= max_drop_ratio`) |
| `FAILED` | 단계 실패, `max_drop_ratio` 초과, 또는 0건인데 `allow_empty=false` |
| `EMPTY` | 행 0건이지만 `allow_empty=true`라 정상 |
| `SKIPPED` | 같은 멱등 키가 이미 `completed` — 아무것도 하지 않고 종료 |

## Stage — 재개 근거

`bronze_written` → `validated` → `completed`

**`fetched`는 두지 않는다.** 조각을 도착 즉시 저장하므로 실행이
`fetch₁ → save₁ → fetch₂ → save₂ → …`로 흐르고, 마지막 조각을 저장한 순간
`fetched`와 `bronze_written`이 동시에 달성된다. 도달 불가능한 중간 단계를 어휘에
남기지 않는다.

pipeline이 `stage >= Stage.BRONZE_WRITTEN` 형태로 비교하므로 **순서 비교가 가능한
타입**(IntEnum 등)으로 만든다. 단순 문자열 Enum이면 재개 분기를 쓸 수 없다.

## FailureReason — 재시도가 의미 있는 실패인지 구분한다

| 값 | 의미 | 재시도가 도움이 되는가 |
| --- | --- | --- |
| `fetch_error` | API 호출 실패 | 예 |
| `storage_error` | bronze · silver · quarantine 쓰기 실패 | 예 |
| `quality_gate` | `max_drop_ratio` 초과, 0건인데 `allow_empty=false` | **아니오** |
| `config_error` | 스키마 · 정책 이름 · `row_params` 검증 실패 | 아니오 |

`quality_gate` 실패는 같은 bronze에 같은 config를 적용하므로 재개해도 결과가 같다.
config를 고쳐 재처리해야 한다. status를 늘리지 않고 사유만 부가 정보로 남기므로 재개
분기는 이 필드를 보지 않는다.

## 스키마

`source_id` · `window_start` · `window_end` · `status` · `stage` · `failure_reason` ·
`attempt` · `started_at` · `ended_at` · `duration_ms` · `artifacts` ·
`counts{fetched, kept, repaired, dropped}` · `drop_ratio` · `column_issues` ·
`policy_actions` · `config_version`. 필드 예시는 계획서 6절의 JSON을 그대로 따르고,
`counts` · `column_issues` · `policy_actions`는 검증 엔진의 집계를 변환 없이 받는다.

`artifacts`의 세 항목은 형태가 다르고 null 가능 여부도 다르다.

| 키 | 형태 | null이 되는 경우 |
| --- | --- | --- |
| `bronze` | `{prefix, parts}` | `bronze_written` 미도달 |
| `silver` | 단일 키 | `drop_ratio` 초과로 silver를 쓰지 않았을 때 |
| `quarantine` | 단일 키 | 폐기 행이 0건이라 객체를 만들지 않았을 때 |

`bronze`가 목록을 갖는 이유는 조각이 여러 개이므로 `read_bronze`가 무엇을 어떤 순서로
읽어야 하는지 알아야 하고, S3 LIST에 의존하지 않도록 명시적으로 기록하기 때문이다.

## 읽기 / 쓰기

- `load(source_id, window_start) -> Manifest | None` — 없으면 None.
  재개 분기의 입력이다.
- 시작 시 `RUNNING`을 기록하고, 단계를 넘을 때마다 `stage`를 갱신하고, 종료 시
  `status` · `ended_at` · `duration_ms` · 집계를 채운다. **중간에 죽어도 도달한
  `stage`가 남아 있어야** 다음 실행이 재개할 수 있다.
- **`stage`를 `bronze_written`으로 올리는 것은 마지막 조각까지 저장된 뒤다.** 그 전에
  죽으면 조각이 S3에 남아 있어도 미완결로 취급되고, 재실행은 fetch부터 다시 한다.
  즉 bronze 완결 판정의 권한은 S3가 아니라 이 필드에 있다.
- `attempt` — 기존 manifest가 있으면 그 값에 1을 더한다. 로그 고정 필드로도 쓰인다.

## 주의

- 저장소는 **S3 단독**이다. collector가 DB 커넥션 없이 동작하게 하려는 결정이며, 그래서
  트랜잭션이 없다. 갱신은 객체 덮어쓰기로 이뤄지고 부분 갱신이라는 개념이 없다.
- `config_version`은 이 silver를 만든 정책 버전이다. 나중에 범위 기준을 바꿨을 때 이
  해시로 재처리 대상을 골라내므로 반드시 기록한다.
- 실패 실행의 manifest도 남긴다. `FAILED`인데 기록이 없으면 재개가 fetch부터 다시 하게
  되고, 실시간 소스에서는 그 window 데이터를 잃는다.
"""
