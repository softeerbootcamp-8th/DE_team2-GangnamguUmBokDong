"""fetch→bronze→validate→silver 오케스트레이션과 재개 분기.

구현 예정: docs/collector/implementation-issues.md #7
설계 근거: docs/collector/implementation-plan.md 7절 (재개 로직)
          docs/collector/implementation-plan.md 8절 (부분 실패와 백필)
          docs/adr/0003-bronze-streaming-and-scaling-boundaries.md
          docs/adr/0004-partial-fetch-and-backfill.md

## 실행 순서

config 로드 → manifest 로드 → **재개 분기** → (라운드를 돌며 조각마다 fetch → bronze
즉시 저장) → **완결도 게이트** → normalize → 검증 · 정책 적용 → silver + quarantine →
manifest 마감 → (불완전하면) 백필 마커.

단계를 넘어갈 때마다 manifest의 `stage`를 갱신한다. 중간에 죽어도 어디까지 진행됐는지
남아 있어야 다음 실행이 재개할 수 있다.

## 재개 분기 (계획서 7절 그대로)

    manifest = manifest.load(source_id, window_start)      # 없으면 None

    if manifest and manifest.stage == Stage.COMPLETED and not force:
        if backfill and manifest.missing.parts:            # ← 분기 4
            have   = set(manifest.artifacts.bronze.parts)  # clear_bronze 하지 않는다
            chunks = storage.read_bronze(manifest.artifacts.bronze)
            new, missing = fetch_with_rounds(
                adapter, config, window,
                skip=have, expected_total=manifest.counts.expected)
            chunks += new
        else:
            return SKIPPED                                 # 멱등 — 재실행해도 안전

    elif manifest and manifest.stage >= Stage.BRONZE_WRITTEN and not force:
        chunks = storage.read_bronze(manifest.artifacts.bronze)

    else:
        storage.clear_bronze(source_id, window_start)      # 이전 실행의 조각을 비운다
        chunks, missing = fetch_with_rounds(adapter, config, window)

    rows = adapter.normalize(chunks)                       # 항상 다시 수행

| # | 조건 | 동작 |
| --- | --- | --- |
| 1 | `stage=completed` & 누락 없음 & `!force` | SKIPPED |
| 2 | `stage>=bronze_written` & `!force` | bronze 재사용 |
| 3 | 그 외 (또는 `--force`) | `clear_bronze` + 전체 fetch |
| 4 | `stage=completed` & 누락 존재 & `--backfill` | 누락 조각만 fetch → 전체 재처리 → `revision` +1 |

- **조각 저장은 pipeline의 책임이다.** 어댑터는 `yield`만 하고 저장소를 알지 못한다.
  파일명이 되는 조각 키는 어댑터가 만들어 `FetchResult.key`로 넘긴다.
- **`stage`는 fetch 단계를 마친 뒤에 올린다.** 라운드를 소진했든 예산이 끝났든,
  더 이상 호출하지 않기로 결정한 시점이다. 그 전에 죽으면 조각이
  S3에 남아도 미완결로 취급된다.
- **`stage`는 실행 진행도만 뜻한다.** 조각이 다 모였는지는 `completeness` · `missing`이
  따로 표현하므로 `stage=completed`이면서 불완전한 window가 존재한다.
- `clear_bronze`가 필요한 이유는 조각 수가 실행마다 달라질 수 있기 때문이다(5조각 →
  3조각). **백필 모드는 예외다** — 기존 조각을 살리는 것이 목적이고, 유령 조각은
  "manifest `parts`에 없는 조각은 읽지 않는다"는 규칙이 막는다.
- `normalize`는 bronze 재사용 여부와 **무관하게 항상** 수행한다. 네트워크를 타지 않는
  순수 변환이라 비용이 없고, bronze가 정규화 전 원본을 담고 있으므로 이 편이 단순하다.
- `--force`는 위 분기를 모두 무시하고 fetch부터 다시 한다. `--backfill`과는 목적이
  반대이므로 함께 주면 오류로 막는다.
- 조각을 저장한 뒤에도 메모리에서 놓지 않는다. 검증은 window 전체가 모인 뒤 배치로
  수행하므로 피크 메모리는 줄지 않는다(ADR 0003).

**bronze 재사용이 재개의 핵심인 이유**: 실시간 API(5분 주기)는 몇 분만 지나도 그 시점
데이터를 영영 받을 수 없다. silver 저장에서 실패했을 때 fetch부터 다시 하면 지금 시점의
**다른 데이터로 덮어쓰게 된다.**

    1회차: bronze ✓ → 정제 ✓ → silver ✗   stage=validated, status=FAILED
    2회차: fetch ⤳ skip(bronze 재사용) → 정제 ✓ → silver ✓   stage=completed, PARTIAL

## 조각 실패 처리 (계획서 8절)

**조각 하나가 실패해도 window 전체를 버리지 않는다.** 라운드 루프 자체는 `adapters/base`
의 공통 유틸이 돌리고, pipeline은 성공 조각을 즉시 저장하는 콜백과 종료 후 판정을
담당한다.

    fetch 종료 (라운드 소진 · fetch_budget 초과)
       ↓
    성공 조각으로 missing_ratio 계산
       ↓
       ├─ max_missing_ratio 이내 → normalize → 검증 → max_drop_ratio 판정 → silver
       └─ 초과 → silver 쓰지 않음, FAILED, failure_reason=fetch_error

`FATAL`(인증 오류)만 예외다. 게이트도 마커도 타지 않고 `fetch_error`로 즉시 끝낸다 —
재시도도 백필도 무의미하고 키를 고쳐 `--force`로 재실행할 문제다.

## status 결정 규칙

| 조건 | status | failure_reason |
| --- | --- | --- |
| 행 0건 + `allow_empty=true` | `EMPTY` | — |
| 행 0건 + `allow_empty=false` | `FAILED` | `quality_gate` |
| `dropped == 0` && 누락 없음 | `SUCCEEDED` | — |
| `dropped > 0` 또는 누락 존재 (두 게이트 이내) | `PARTIAL` | — |
| `missing_ratio > max_missing_ratio` | `FAILED` | `fetch_error` |
| `drop_ratio > max_drop_ratio` | `FAILED` | `quality_gate` |
| `FATAL` · 전체 API 오류 | `FAILED` | `fetch_error` |
| S3 쓰기 실패 | `FAILED` | `storage_error` |
| 정책이 `FAIL_BATCH` 반환 | `FAILED` | `quality_gate` |

**게이트에 걸리면 silver를 쓰지 않는다.** `artifacts.silver`는 null로 남는다. 검증을
배치로 하는 이유가 판정을 쓰기 전에 할 수 있다는 것이므로 그 이점을 쓴다. FAILED
window에 silver가 존재하면 하류가 manifest를 확인하지 않고 읽을 위험이 있다.

quarantine은 이 경우에도 쓴다. 왜 실패했는지 분석하려면 폐기된 행이 필요하고,
quarantine은 하류 소비 대상이 아니다. 반대로 **폐기 행이 0건이면 객체를 만들지 않는다.**

**두 게이트는 독립이고 `drop_ratio`의 분모는 `fetched`다.** `expected`로 바꾸면 수집이
완전한 평상시에는 차이가 없고 장애 때만 폐기율이 튀는 지표가 된다. 부분 수집 시 폐기
게이트가 다소 엄격해지는 대가가 남지만 통과시킬 것을 막는 안전한 방향이다.

`quality_gate` 실패는 재시도해도 결과가 같다(같은 bronze + 같은 config). config를 고쳐
재처리해야 하므로 manifest의 `failure_reason`으로 구분해 남긴다. 반면 `fetch_error`는
백필로 회복 가능하다.

## 백필 마커

`backfill.enabled`이고 누락이 남았으면 `_retry_queue/{source_id}/{window_start}.json`을
쓴다. **게이트 초과로 FAILED가 된 window도 쓴다** — bronze 조각은 남아 있으므로 백필이
채우면 완결시킬 수 있고, FAILED를 제외하면 가장 많이 빠진 window가 대상에서 빠진다.

백필로 완결되면 마커를 지운다. 부분적으로만 채웠으면 `missing_parts`를 줄여 유지한다.
**백필은 한 번에 완결될 필요가 없다.**

## 주의

- 예외를 삼키지 않는다. manifest에 `FAILED` · 도달한 `stage` · `failure_reason`을 남긴
  뒤 호출자에게 올려 종료 코드로 이어지게 한다.
- **부분 성공의 종료 코드는 0이다.** `stage=completed`로 끝나 재실행하면 분기 1에서
  `SKIPPED`로 빠지므로 Airflow retry가 할 일이 없다. 채우는 일은 백필 잡이 맡는다.
- `attempt` 증가는 manifest 모듈이 담당한다. pipeline은 상태 전이만 지시한다.
  `revision`은 **silver를 실제로 쓴 경우에만** 올린다 — `attempt`(실행 횟수)와 다르다.
- 각 단계 경계에서 로그 한 줄씩 남긴다(계획서 9절). 조각마다, 라운드마다 남기지 않는다.

검증(계획서 12절): manifest `stage`별 분기 5가지(없음 / `bronze_written` / `completed` /
`--force` / `completed` + 누락 + `--backfill`)를 확인하고, silver 쓰기 직전에 예외를
주입한 뒤 재실행해서 로그에 `stage=bronze_written`이 **없이** 완료되는지 본다. 조각
하나를 강제 실패시켜 게이트 통과/초과 양쪽에서 마커가 생기는지도 확인한다.

## `stage`의 최종 정지점 — 실패 종류별로 다르다

- `fetch_error`(누락 게이트 초과) · `FATAL` — fetch 단계에서 끝났으므로 `BRONZE_WRITTEN`.
  다음 실행은 분기 2(bronze 재사용)로 들어가 게이트를 다시 계산한다.
- `storage_error`(silver 쓰기 예외) · `quality_gate`(폐기율 초과, 행 0건인데
  `allow_empty=false`, 행 정책 `FAIL_BATCH`) — `normalize`·검증까지는 끝났으므로
  `VALIDATED`. `quality_gate`는 같은 bronze + 같은 config면 재시도해도 결과가 같지만,
  일부러 `COMPLETED`로 올리지 않는다 — `COMPLETED`로 올리면 다음 실행이 분기 1에서
  `missing.parts`가 비어 있는 한 곧장 `SKIPPED`(종료 코드 0)로 빠져 **실패가 조용히
  성공처럼 보이는** 문제가 생긴다.
- `SUCCEEDED`·`PARTIAL`·`EMPTY` — 이번 실행에서 더 할 일이 없다는 뜻이므로
  `COMPLETED`. 재실행하면 분기 1이 즉시 `SKIPPED`로 받아 Airflow retry가 헛돌지 않는다.

## 예외를 다루는 경계

`write_silver`·`write_quarantine`·`write_bronze_part` 호출에서 던진 예외와
`validate_batch`의 `BatchValidationFailed`만 여기서 잡아 `FAILED` manifest로 바꾼다.
그 외 예외(예: `manifest.save` 자체의 실패)는 잡지 않고 그대로 올린다 — 실행 결과를
남길 방법이 없는 예외까지 삼키면 무엇이 잘못됐는지 추적할 수 없다.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pyarrow as pa

import manifest as manifest_module
import storage
from adapters.base import FetchErrorKind, Window, fetch_with_rounds, get_adapter
from manifest import (
    Artifacts,
    BronzeArtifacts,
    ColumnIssueCount,
    Counts,
    FailureReason,
    Manifest,
    Missing,
    RunStatus,
    Stage,
)
from validation.engine import BatchValidationFailed, validate_batch
from validation.types import RunContext

_HTTP_TIMEOUT_SECONDS = 10.0


class ForceAndBackfillError(ValueError):
    """`--force`와 `--backfill`은 목적이 반대라 동시에 줄 수 없다."""


def _sorted_chunks(chunks: dict[str, bytes]) -> list[bytes]:
    """조각 키의 사전순 정렬이 호출 순서와 일치한다는 설계를 그대로 이용한다."""
    return [chunks[key] for key in sorted(chunks)]


def _missing_ratio(missing_count: int, expected_total: int | None, fetched_rows: int, collected_count: int) -> float:
    """성공 조각만으로 누락 비율을 계산한다.

    `expected_total`을 아는 소스(행 기준)는 실제 받은 행 수를 `normalize`로 세어
    `1 - fetched_rows/expected_total`로 계산한다. 모르는 소스(조각 기준, 기상청)는
    `누락 조각 수 / 계획된 조각 수`로 계산한다 — 어댑터마다 다른 조각 키 형식을
    pipeline이 몰라도 되게 하기 위해서다.
    """
    if expected_total is not None:
        return max(0.0, 1 - (fetched_rows / expected_total)) if expected_total else 0.0
    planned = collected_count + missing_count
    return (missing_count / planned) if planned else 0.0


def _build_missing(missing_keys: dict, expected_total: int | None, fetched_rows: int) -> Missing:
    parts = tuple(sorted(missing_keys))
    if expected_total is not None:
        return Missing(parts=parts, rows=max(0, expected_total - fetched_rows), basis="rows")
    return Missing(parts=parts, rows=None, basis="parts")


def _now() -> datetime:
    return datetime.now(UTC)


def execute_window(
    config,
    window_start: datetime,
    *,
    client,
    force: bool = False,
    backfill: bool = False,
    sleep_fn=time.sleep,
) -> Manifest:
    """window 하나를 재개 분기에 따라 처리하고 최종 manifest를 반환한다."""
    if force and backfill:
        raise ForceAndBackfillError("--force와 --backfill은 함께 줄 수 없다")

    window_end = window_start + config.schedule.interval
    started_at = _now()
    existing = manifest_module.load(config.source_id, window_start)
    adapter_cls = get_adapter(config.adapter)
    window = Window(window_start=window_start, window_end=window_end)

    if existing and existing.stage == Stage.COMPLETED and not force:
        if not (backfill and existing.missing.parts):
            return existing.model_copy(update={"status": RunStatus.SKIPPED})
        # 분기 4: 백필 — 기존 조각은 살리고 누락분만 받는다.
        have_parts = existing.artifacts.bronze.parts
        prior_chunks = dict(zip(have_parts, storage.read_bronze(config.source_id, window_start, have_parts)))
        round_result = fetch_with_rounds(
            adapter_cls.fetch, config, window, client=client,
            skip=frozenset(have_parts), expected_total=existing.counts.expected,
            sleep_fn=sleep_fn,
            on_chunk=lambda key, payload: storage.write_bronze_part(config.source_id, window_start, key, payload),
        )
        chunks = {**prior_chunks, **round_result.chunks}
        missing_keys = round_result.missing
        expected_total = round_result.expected_total if round_result.expected_total is not None else existing.counts.expected
        attempt = existing.attempt + 1
        revision_base = existing.revision
    elif existing and existing.stage.value >= Stage.BRONZE_WRITTEN.value and not force:
        # 분기 2: bronze 재사용 — fetch는 다시 하지 않는다.
        parts = existing.artifacts.bronze.parts
        chunks = dict(zip(parts, storage.read_bronze(config.source_id, window_start, parts)))
        missing_keys = {key: FetchErrorKind.PERMANENT for key in existing.missing.parts}
        expected_total = existing.counts.expected
        attempt = existing.attempt + 1
        revision_base = existing.revision
    else:
        # 분기 3(또는 --force): 처음부터 전체 fetch.
        storage.clear_bronze(config.source_id, window_start)
        round_result = fetch_with_rounds(
            adapter_cls.fetch, config, window, client=client,
            sleep_fn=sleep_fn,
            on_chunk=lambda key, payload: storage.write_bronze_part(config.source_id, window_start, key, payload),
        )
        chunks = round_result.chunks
        missing_keys = round_result.missing
        expected_total = round_result.expected_total
        attempt = (existing.attempt + 1) if existing else 1
        revision_base = existing.revision if existing else 0

    bronze_parts = tuple(sorted(chunks))
    artifacts = Artifacts(bronze=BronzeArtifacts(prefix=config.source_id, parts=bronze_parts))

    def _base_manifest(**over) -> Manifest:
        fields = {
            "source_id": config.source_id,
            "window_start": window_start,
            "window_end": window_end,
            "status": RunStatus.FAILED,
            "stage": Stage.BRONZE_WRITTEN,
            "attempt": attempt,
            "revision": revision_base,
            "started_at": started_at,
            "ended_at": _now(),
            "artifacts": artifacts,
            "config_version": config.config_version,
        }
        fields.update(over)
        return Manifest(**fields)

    def _finish(**over) -> Manifest:
        result = _base_manifest(**over)
        manifest_module.save(result)
        return result

    if FetchErrorKind.FATAL in missing_keys.values():
        return _finish(failure_reason=FailureReason.FETCH_ERROR)

    rows = adapter_cls.normalize(_sorted_chunks(chunks), config)
    fetched_rows = len(rows)
    ratio = _missing_ratio(len(missing_keys), expected_total, fetched_rows, len(chunks))
    missing = _build_missing(missing_keys, expected_total, fetched_rows)

    if ratio > config.quality.max_missing_ratio:
        return _finish(
            failure_reason=FailureReason.FETCH_ERROR,
            missing=missing, counts=Counts(expected=expected_total, fetched=fetched_rows),
        )

    if fetched_rows == 0:
        if config.quality.allow_empty:
            return _finish(
                status=RunStatus.EMPTY, stage=Stage.COMPLETED, failure_reason=None,
                missing=missing, counts=Counts(expected=expected_total),
            )
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.QUALITY_GATE,
            missing=missing, counts=Counts(expected=expected_total),
        )

    ctx = RunContext(source_id=config.source_id, window_start=window_start, window_end=window_end, attempt=attempt)
    try:
        outcome = validate_batch(rows, config, ctx)
    except BatchValidationFailed:
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.QUALITY_GATE,
            missing=missing, counts=Counts(expected=expected_total, fetched=fetched_rows),
        )

    counts = Counts(expected=expected_total, **outcome.counts)
    column_issues = {col: ColumnIssueCount(**v) for col, v in outcome.column_issues.items()}
    common = {
        "missing": missing, "counts": counts, "column_issues": column_issues,
        "policy_actions": outcome.policy_actions, "drop_ratio": outcome.drop_ratio,
    }

    try:
        quarantine_key = (
            storage.write_quarantine(config.source_id, window_start, outcome.quarantine_records)
            if outcome.quarantine_records else None
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        return _finish(stage=Stage.VALIDATED, failure_reason=FailureReason.STORAGE_ERROR, **common)

    if outcome.drop_ratio > config.quality.max_drop_ratio:
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.QUALITY_GATE, **common,
            artifacts=Artifacts(bronze=artifacts.bronze, quarantine=quarantine_key),
        )

    try:
        silver_key = storage.write_silver(config.source_id, window_start, pa.Table.from_pylist(outcome.silver_rows))
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.STORAGE_ERROR, **common,
            artifacts=Artifacts(bronze=artifacts.bronze, quarantine=quarantine_key),
        )

    status = RunStatus.SUCCEEDED if outcome.counts["dropped"] == 0 and ratio == 0.0 else RunStatus.PARTIAL
    return _finish(
        status=status, stage=Stage.COMPLETED, failure_reason=None,
        revision=revision_base + 1, completeness=1.0 - ratio, **common,
        artifacts=Artifacts(bronze=artifacts.bronze, silver=silver_key, quarantine=quarantine_key),
    )
