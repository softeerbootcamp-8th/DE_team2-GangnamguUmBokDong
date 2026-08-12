"""fetch→bronze→validate→silver 오케스트레이션과 재개 분기.

구현 예정: docs/collector/implementation-issues.md #7
설계 근거: docs/collector/implementation-plan.md 7절 (재개 로직)
          docs/adr/0003-bronze-streaming-and-scaling-boundaries.md

## 실행 순서

config 로드 → manifest 로드 → **재개 분기** → (조각마다 fetch → bronze 즉시 저장) →
normalize → 검증 · 정책 적용 → silver + quarantine → manifest 마감.

단계를 넘어갈 때마다 manifest의 `stage`를 갱신한다. 중간에 죽어도 어디까지 진행됐는지
남아 있어야 다음 실행이 재개할 수 있다.

## 재개 분기와 조각 저장 (계획서 7절 그대로)

    manifest = manifest.load(source_id, window_start)      # 없으면 None

    if manifest and manifest.stage == Stage.COMPLETED and not force:
        return SKIPPED                                     # 멱등 — 재실행해도 안전

    if manifest and manifest.stage >= Stage.BRONZE_WRITTEN and not force:
        chunks = storage.read_bronze(manifest.artifacts.bronze)   # 순서대로 읽는다
    else:
        storage.clear_bronze(source_id, window_start)      # 이전 실행의 조각을 비운다
        chunks = []
        for i, chunk in enumerate(adapter.fetch(config, window)):
            storage.write_bronze_part(source_id, window_start, i, chunk)   # 즉시
            chunks.append(chunk)                           # 메모리에도 쌓는다
        manifest.update(stage=Stage.BRONZE_WRITTEN, parts=[...])   # 전 조각 완료 후에만

    rows = adapter.normalize(chunks)                       # 항상 다시 수행

- **조각 저장은 pipeline의 책임이다.** 어댑터는 `yield`만 하고 저장소를 알지 못한다.
  `enumerate`의 인덱스가 그대로 `part={NNN}`이 되므로 순서가 데이터에 새겨진다.
- **`stage`는 마지막 조각까지 저장된 뒤에만 올린다.** 그 전에 죽으면 조각이 S3에 남아도
  미완결로 취급된다. 조각 단위 재개는 하지 않는다.
- **조각 하나라도 실패하면 fetch 전체가 실패다.** 받은 조각으로 진행하지 않는다. 부분
  성공을 허용하면 `counts.fetched`가 줄어든 채 성공으로 기록되어 손실이 침묵한다.
- `clear_bronze`가 필요한 이유는 조각 수가 실행마다 달라질 수 있기 때문이다. 1회차에
  5조각, 2회차에 3조각이 나오면 이전 조각 2개가 유령으로 남는다.
- `normalize`는 bronze 재사용 여부와 **무관하게 항상** 수행한다. 네트워크를 타지 않는
  순수 변환이라 비용이 없고, bronze가 정규화 전 원본을 담고 있으므로 이 편이 단순하다.
- `--force`는 위 분기를 모두 무시하고 fetch부터 다시 한다.
- 조각을 저장한 뒤에도 메모리에서 놓지 않는다. 검증은 window 전체가 모인 뒤 배치로
  수행하므로 피크 메모리는 줄지 않는다(ADR 0003).

**bronze 재사용이 재개의 핵심인 이유**: 실시간 API(5분 주기)는 몇 분만 지나도 그 시점
데이터를 영영 받을 수 없다. silver 저장에서 실패했을 때 fetch부터 다시 하면 지금 시점의
**다른 데이터로 덮어쓰게 된다.**

    1회차: bronze ✓ → 정제 ✓ → silver ✗   stage=validated, status=FAILED
    2회차: fetch ⤳ skip(bronze 재사용) → 정제 ✓ → silver ✓   stage=completed, PARTIAL

## status 결정 규칙

| 조건 | status | failure_reason |
| --- | --- | --- |
| 행 0건 + `allow_empty=true` | `EMPTY` | — |
| 행 0건 + `allow_empty=false` | `FAILED` | `quality_gate` |
| `dropped == 0` | `SUCCEEDED` | — |
| `drop_ratio <= max_drop_ratio` | `PARTIAL` | — |
| `drop_ratio > max_drop_ratio` | `FAILED` | `quality_gate` |
| 조각 실패 · API 오류 | `FAILED` | `fetch_error` |
| S3 쓰기 실패 | `FAILED` | `storage_error` |
| 정책이 `FAIL_BATCH` 반환 | `FAILED` | `quality_gate` |

**`drop_ratio` 초과 시 silver를 쓰지 않는다.** `artifacts.silver`는 null로 남는다.
검증을 배치로 하는 이유가 판정을 쓰기 전에 할 수 있다는 것이므로 그 이점을 쓴다. FAILED
window에 silver가 존재하면 하류가 manifest를 확인하지 않고 읽을 위험이 있다.

quarantine은 이 경우에도 쓴다. 왜 실패했는지 분석하려면 폐기된 행이 필요하고,
quarantine은 하류 소비 대상이 아니다. 반대로 **폐기 행이 0건이면 객체를 만들지 않는다.**

`quality_gate` 실패는 재시도해도 결과가 같다(같은 bronze + 같은 config). config를 고쳐
재처리해야 하므로 manifest의 `failure_reason`으로 구분해 남긴다.

## 주의

- 예외를 삼키지 않는다. manifest에 `FAILED` · 도달한 `stage` · `failure_reason`을 남긴
  뒤 호출자에게 올려 종료 코드로 이어지게 한다.
- `attempt` 증가는 manifest 모듈이 담당한다. pipeline은 상태 전이만 지시한다.
- 각 단계 경계에서 로그 한 줄씩 남긴다(계획서 8절). 조각마다 로그를 남기지 않는다.

검증(계획서 11절): manifest `stage`별 분기 4가지(없음 / `bronze_written` /
`completed` / `--force`)를 확인하고, silver 쓰기 직전에 예외를 주입한 뒤 재실행해서
로그에 `stage=bronze_written`이 **없이** 완료되는지 본다.
"""
