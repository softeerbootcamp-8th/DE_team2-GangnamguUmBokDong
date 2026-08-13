"""manifest 스키마와 읽기/쓰기, 상태 어휘(RunStatus / Stage / FailureReason).

구현 예정: docs/collector/implementation-issues.md #4
설계 근거: docs/collector/implementation-plan.md 6절 (상태 어휘와 manifest)
          docs/collector/implementation-plan.md 8절 (부분 실패와 백필)
          docs/adr/0003-bronze-streaming-and-scaling-boundaries.md
          docs/adr/0004-partial-fetch-and-backfill.md

## 이 모듈의 역할

한 번의 실행이 **무엇을 했고 어디까지 갔는지**를 남긴다. 이 기록이 재개 판단의 유일한
근거이고, 소스가 몇 개든 어휘는 완전히 동일하다.

## RunStatus — 실행 결과

| 상태 | 의미 |
| --- | --- |
| `RUNNING` | 실행 중(시작 시 기록) |
| `SUCCEEDED` | silver까지 완료, 폐기된 행도 누락된 조각도 없음 |
| `PARTIAL` | silver까지 완료, 일부 행 quarantine **또는 일부 조각 누락** (두 게이트 이내) |
| `FAILED` | 단계 실패, 게이트 초과, 또는 0건인데 `allow_empty=false` |
| `EMPTY` | 행 0건이지만 `allow_empty=true`라 정상 |
| `SKIPPED` | 같은 멱등 키가 이미 `completed`이고 누락도 없음 — 아무것도 하지 않고 종료 |

**부분 수집을 위해 status를 늘리지 않았다.** `PARTIAL`의 의미를 "일부 행 폐기"에서
"완전하지 않지만 쓸 수 있음"으로 넓혔다. 무엇이 어떻게 불완전한지는 `missing` ·
`completeness` · `drop_ratio`가 표현하므로, 상태 어휘를 늘려 조합을 폭발시키지 않는다.

## Stage — 재개 근거

`bronze_written` → `validated` → `completed`

**`fetched`는 두지 않는다.** 조각을 도착 즉시 저장하므로 실행이
`fetch₁ → save₁ → fetch₂ → save₂ → …`로 흐르고, 마지막 조각을 저장한 순간
`fetched`와 `bronze_written`이 동시에 달성된다. 도달 불가능한 중간 단계를 어휘에
남기지 않는다.

**`stage`는 실행이 어디까지 갔는지만 뜻한다.** 조각이 다 모였는지는 `completeness` ·
`missing`이 따로 표현하므로 `stage=completed`이면서 불완전한 window가 존재한다.
`bronze_written`은 "계획한 조각을 전부 받았다"가 아니라 "이번 실행이 fetch 단계를
마쳤다"는 뜻이고, 라운드 소진 · 예산 초과 · 서킷브레이커가 모두 그 시점이 될 수 있다.

pipeline이 `stage >= Stage.BRONZE_WRITTEN` 형태로 비교하므로 **순서 비교가 가능한
타입**(IntEnum 등)으로 만든다. 단순 문자열 Enum이면 재개 분기를 쓸 수 없다.

## FailureReason — 재시도가 의미 있는 실패인지 구분한다

| 값 | 의미 | 재시도가 도움이 되는가 |
| --- | --- | --- |
| `fetch_error` | API 호출 실패, `max_missing_ratio` 초과, 인증 오류(`FATAL`) | 예 (`FATAL`은 키를 고친 뒤) |
| `storage_error` | bronze · silver · quarantine 쓰기 실패 | 예 |
| `quality_gate` | `max_drop_ratio` 초과, 0건인데 `allow_empty=false` | **아니오** |
| `config_error` | 스키마 · 정책 이름 · `row_params` 검증 실패 | 아니오 |

**게이트가 곧 `failure_reason`을 결정한다.** 수집 게이트에 걸리면 `fetch_error`(재시도 ·
백필로 회복), 폐기 게이트에 걸리면 `quality_gate`(config 수정). 두 게이트를 하나로
합치지 않은 이유가 이것이다 — 합치면 완결도 71%라는 결과만 보고 어느 쪽인지 알 수 없다.

`quality_gate` 실패는 같은 bronze에 같은 config를 적용하므로 재개해도 결과가 같다.
status를 늘리지 않고 사유만 부가 정보로 남기므로 재개 분기는 이 필드를 보지 않는다.

## 스키마

`source_id` · `window_start` · `window_end` · `status` · `stage` · `failure_reason` ·
`attempt` · `revision` · `started_at` · `ended_at` · `duration_ms` · `artifacts` ·
`counts{expected, fetched, kept, repaired, dropped}` · `missing{parts, rows, basis}` ·
`drop_ratio` · `completeness` · `backfill_status` · `column_issues` · `policy_actions` ·
`config_version`. 필드 예시는 계획서 6절의 JSON을 그대로 따르고, `counts` ·
`column_issues` · `policy_actions`는 검증 엔진의 집계를 변환 없이 받는다.

### 수집 완결도 필드

부분 성공을 허용하면서도 **침묵한 손실을 만들지 않기 위해** 존재한다.

| 필드 | 의미 |
| --- | --- |
| `counts.expected` | 소스가 알려준 전체 행 수. 서울은 `list_total_count`, 기상청은 `null` |
| `missing.parts` | 끝내 받지 못한 조각 키 목록. **백필이 이 목록을 지목해 채운다** |
| `missing.rows` | 누락 행 수(`expected - fetched`). `expected`가 `null`이면 `null` |
| `missing.basis` | `rows`(행 기준) 또는 `parts`(조각 기준). 기상청은 `parts` |
| `completeness` | `kept / expected`. **게이트가 아니라 정보다** |
| `backfill_status` | `null`(해당 없음) · `pending`(마커 존재) · `expired`(만료) |

`completeness`를 게이트로 쓰지 않는 이유는 두 게이트가 이미 각 단계를 막고 있기
때문이다. 세 번째 임계치를 두면 소스 7개마다 튜닝할 값이 하나 더 늘어난다. 대신 게이트
둘을 각각 통과했는데 손실이 곱해져 최종 68%가 되는 경우를 하류가 알 수 있게 값만 남긴다.

### revision과 attempt는 다르다

| 필드 | 세는 것 | 올라가는 시점 |
| --- | --- | --- |
| `attempt` | 실행 횟수 | 매 실행 |
| `revision` | **silver 내용이 바뀐 횟수** | silver를 실제로 쓸 때만 |

실패한 재실행은 `attempt`만 올리고 `revision`은 그대로다. **하류가 재처리 여부를 판단할
때 보는 값은 `revision`이다** — 백필로 silver가 교체되면 이 값이 올라간다.

### artifacts

`artifacts`의 세 항목은 형태가 다르고 null 가능 여부도 다르다.

| 키 | 형태 | null이 되는 경우 |
| --- | --- | --- |
| `bronze` | `{prefix, parts}` | `bronze_written` 미도달 |
| `silver` | 단일 키 | 게이트 초과로 silver를 쓰지 않았을 때 |
| `quarantine` | 단일 키 | 폐기 행이 0건이라 객체를 만들지 않았을 때 |

`bronze.parts`는 **조각 키 목록**이다(`["page-00001-01000", ...]`). 목록을 갖는 이유는
`read_bronze`가 무엇을 어떤 순서로 읽어야 하는지 알아야 하고, S3 LIST에 의존하지 않도록
명시적으로 기록하기 때문이다. **`parts`에 없는 조각은 읽지 않는다** — 이 규칙이 백필
모드에서 `clear_bronze`를 생략해도 유령 조각이 섞이지 않게 막는다.

## 읽기 / 쓰기

- `load(source_id, window_start) -> Manifest | None` — 없으면 None.
  재개 분기의 입력이다.
- 시작 시 `RUNNING`을 기록하고, 단계를 넘을 때마다 `stage`를 갱신하고, 종료 시
  `status` · `ended_at` · `duration_ms` · 집계를 채운다. **중간에 죽어도 도달한
  `stage`가 남아 있어야** 다음 실행이 재개할 수 있다.
- **`stage`를 `bronze_written`으로 올리는 것은 fetch 단계를 마친 뒤다.** 그 전에 죽으면
  조각이 S3에 남아 있어도 미완결로 취급되고, 재실행은 fetch부터 다시 한다.
- `attempt` — 기존 manifest가 있으면 그 값에 1을 더한다. 로그 고정 필드로도 쓰인다.
- 백필로 갱신할 때는 `revision`을 올리고 `counts` · `missing` · `completeness`를 다시
  계산한다. `attempt`도 함께 올라간다(백필도 하나의 실행이다).

## 주의

- 저장소는 **S3 단독**이다. collector가 DB 커넥션 없이 동작하게 하려는 결정이며, 그래서
  트랜잭션이 없다. 갱신은 객체 덮어쓰기로 이뤄지고 부분 갱신이라는 개념이 없다.
- `config_version`은 이 silver를 만든 정책 버전이다. 나중에 범위 기준을 바꿨을 때 이
  해시로 재처리 대상을 골라내므로 반드시 기록한다.
- 실패 실행의 manifest도 남긴다. `FAILED`인데 기록이 없으면 재개가 fetch부터 다시 하게
  되고, 실시간 소스에서는 그 window 데이터를 잃는다.
- **manifest가 진실이고 `_retry_queue` 마커는 인덱스일 뿐이다.** 백필 잡은 마커로 후보를
  얻은 뒤 반드시 이 모듈로 실제 상태를 확인한다. 그래야 마커가 잔존하거나 유실돼도
  오동작이 아니라 스킵 또는 백필 누락으로만 이어진다.
"""
