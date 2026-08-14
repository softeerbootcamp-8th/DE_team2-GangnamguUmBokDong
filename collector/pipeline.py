
"""데이터 수집 파이프라인.

## 실행 순서
config 로드 → manifest 로드 → 재개 분기 → (라운드를 돌며 조각마다 fetch → bronze 즉시 저장) 
→ 완결도 게이트 → normalize → 검증 · 정책 적용 → silver · quarantine → manifest 마감 → (불완전하면) 백필 마커.
단계를 넘어갈 때마다 manifest의 `stage`를 갱신한다. 중간에 죽어도 어디까지 진행됐는지 남아 있어야 다음 실행이 재개할 수 있다.

| # | 조건 | 동작 |
| --- | --- | --- |
| 1 | `stage=completed` & 누락 없음 | `SKIPPED` 반환 (백필 모드 포함) |
| 4 | `stage >= bronze_written` & 누락 존재 & `--backfill` | 기존 bronze 유지 + 누락 조각만 fetch → 전체 재처리 → `revision` +1 |
| 2 | `stage >= bronze_written` (일반 실행) | 기존 bronze 로드 (fetch 건너뜀) |
| 3 | 그 외 (또는 `--force`) | `clear_bronze` + 전체 fetch |

- 조각 저장은 pipeline의 책임이다. 어댑터는 `yield`만 하고 저장소를 알지 못한다.
  파일명이 되는 조각 키는 어댑터가 만들어 `FetchResult.key`로 넘긴다.
- `stage`는 fetch 단계를 마친 뒤에 올린다. 라운드를 소진했든 예산이 끝났든, 더 이상 호출하지 않기로 결정한 시점이다. 
  그 전에 죽으면 조각이 S3에 남아도 미완결로 취급된다.
- `stage`는 실행 진행도만 뜻한다. 조각이 다 모였는지는 `completeness` · `missing`이 따로 표현하므로 
  `stage=completed`이면서 불완전한 window가 존재한다.
- `clear_bronze`가 필요한 이유는 조각 수가 실행마다 달라질 수 있기 때문이다
   **백필 모드는 예외다** — 기존 조각을 살리는 것이 목적이고, 유령 조각은 "manifest `parts`에 없는 조각은 읽지 않는다"는 규칙이 막는다.
- `normalize`는 bronze 재사용 여부와 **무관하게 항상** 수행한다. 네트워크를 타지 않는 순수 변환이라 비용이 없고, 
   bronze가 정규화 전 원본을 담고 있으므로 이 편이 단순하다.
- `--force`는 위 분기를 모두 무시하고 fetch부터 다시 한다. `--backfill`과는 목적이 반대이므로 함께 주면 오류로 막는다.
- 조각을 저장한 뒤에도 메모리에서 놓지 않는다. 검증은 window 전체가 모인 뒤 배치로 수행하므로 피크 메모리는 줄지 않는다

bronze 재사용이 재개의 핵심인 이유: 실시간 API(5분 주기)는 몇 분만 지나도 그 시점 데이터를 영영 받을 수 없다. 
저장에서 실패했을 때 fetch부터 다시 하면 지금 시점의 다른 데이터로 덮어쓰게 된다.

    1회차: bronze ✓ → 정제 ✓ → silver ✗   stage=validated, status=FAILED
    2회차: fetch ⤳ skip(bronze 재사용) → 정제 ✓ → silver ✓   stage=completed, PARTIAL

## 조각 실패 처리

조각 하나가 실패해도 window 전체를 버리지 않는다. 라운드 루프 자체는 `adapters/base`의 공통 유틸이 돌리고, 
pipeline은 성공 조각을 즉시 저장하는 콜백과 종료 후 판정을 담당한다.

    fetch 종료 (라운드 소진 · fetch_budget 초과)
       ↓
    성공 조각으로 missing_ratio 계산
       ↓
       ├─ max_missing_ratio 이내 → normalize → 검증 → max_drop_ratio 판정 → silver
       └─ 초과 → silver 쓰지 않음, FAILED, failure_reason=fetch_error

`FATAL`(인증 오류)만 예외다. 게이트도 마커도 타지 않고 `fetch_error`로 즉시 끝낸다 
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

**게이트에 걸리면 silver를 쓰지 않는다.** `artifacts.silver`는 null로 남는다. 
검증을 배치로 하는 이유가 판정을 쓰기 전에 할 수 있다는 것이므로 그 이점을 쓴다.
FAILED window에 silver가 존재하면 하류가 manifest를 확인하지 않고 읽을 위험이 있다.

quarantine은 이 경우에도 쓴다. 왜 실패했는지 분석하려면 폐기된 행이 필요하고,
quarantine은 하류 소비 대상이 아니다. 반대로 **폐기 행이 0건이면 객체를 만들지 않는다.**

**두 게이트는 독립이고 `drop_ratio`의 분모는 `fetched`다.**  

`quality_gate` 실패는 재시도해도 결과가 같다(같은 bronze, 같은 config). 
config를 고쳐 재처리해야 하므로 manifest의 `failure_reason`으로 구분해 남긴다. 
반면 `fetch_error`는 백필로 회복 가능하다.

## 백필 마커

`backfill.enabled`이고 누락이 남았으면 `_retry_queue/{source_id}/{window_start}.json`을 쓴다. 
**게이트 초과로 FAILED가 된 window도 쓴다**
백필로 완결되면 마커를 지운다. 부분적으로만 채웠으면 `missing_parts`를 줄여 유지한다.
**백필은 한 번에 완결될 필요가 없다.**

## 주의

- 예외를 삼키지 않는다. manifest에 `FAILED` · 도달한 `stage` · `failure_reason`을 남긴 뒤 호출자에게 올려 종료 코드로 이어지게 한다.
- **부분 성공의 종료 코드는 0이다.** `stage=completed`로 끝나 재실행하면 분기 1에서 `SKIPPED`로 빠지므로 Airflow retry가 할 일이 없다. 채우는 일은 백필 잡이 맡는다.
- `attempt` 증가는 manifest 모듈이 담당한다. pipeline은 상태 전이만 지시한다.
  `revision`은 **silver를 실제로 쓴 경우에만** 올린다 `attempt`(실행 횟수)와 다르다.
- 각 단계 경계에서 로그 한 줄씩 남긴다. 조각마다, 라운드마다 남기지 않는다.


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

import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

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
    RetryMarker,
    RunStatus,
    Stage,
)
from validation.engine import BatchValidationFailed, validate_batch
from validation.types import RunContext

_HTTP_TIMEOUT_SECONDS = 10.0
_KST = ZoneInfo("Asia/Seoul")

logger = logging.getLogger(__name__)


class ForceAndBackfillError(ValueError):
    """`--force`와 `--backfill`은 목적이 반대라 동시에 줄 수 없다."""


def _sorted_chunks(chunks: dict[str, bytes]) -> list[bytes]:
    """조각을 키의 사전순으로 정렬해 값 목록만 반환한다."""
    return [chunks[key] for key in sorted(chunks)]


def _missing_ratio(missing_count: int, expected_total: int | None, fetched_rows: int, collected_count: int) -> float:
    """성공 조각만으로 누락 비율을 계산한다.

    `expected_total`을 아는 소스(서울, 행 기준)는 실제 받은 행 수를 `normalize`로 세어 `1 - fetched_rows/expected_total`로 계산한다. 
    모르는 소스(기상청, 조각 기준)는 `누락 조각 수 / 계획된 조각 수`로 계산한다. 
    행을 세는 방식을 쓰는 이유는 어댑터마다 다른 조각 키 형식을 pipeline이 몰라도 되게하기 위해서다.

    args:
        missing_count: 라운드를 다 써도 못 받은 조각 수.
        expected_total: 소스가 알려준 전체 행 수. 모르면 None.
        fetched_rows: 성공한 조각을 normalize했을 때 나온 행 수.
        collected_count: 성공한 조각 수.
    returns:
        0.0(누락 없음) ~ 1.0(전부 누락) 사이의 비율.
    """
    if expected_total is not None:
        return max(0.0, 1 - (fetched_rows / expected_total)) if expected_total else 0.0
    planned = collected_count + missing_count
    return (missing_count / planned) if planned else 0.0


def _build_missing(missing_keys: dict, expected_total: int | None, fetched_rows: int) -> Missing:
    """manifest에 남길 Missing 필드를 만든다."""
    parts = tuple(sorted(missing_keys))
    if expected_total is not None:
        return Missing(parts=parts, rows=max(0, expected_total - fetched_rows), basis="rows")
    return Missing(parts=parts, rows=None, basis="parts")


def _sync_retry_marker(config, window_start: datetime, missing: Missing, first_failed_at: datetime) -> str | None:
    """이번 실행이 남긴 누락에 맞춰 `_retry_queue` 마커를 쓰거나·갱신하거나·지운다.

    누락 조각이 남아 있으면 마커를 쓴다. 이미 마커가 있으면 `first_failed_at`은
    그대로 두고(나이는 처음 실패한 시점부터 잰다) `missing_parts`만 최신값으로
    바꾸고 `attempts`를 늘린다. 누락이 없어졌으면(이번에 다 채웠거나 애초에
    없었으면) 기존 마커를 지운다. `backfill.enabled`가 아닌 소스는 채울 방법이
    없는 마커를 쌓지 않도록 아예 건드리지 않는다 — 마커 만료 판정은 Airflow
    백필 DAG의 책임이라 여기서는 `expires_at`만 계산해 남긴다.

    args:
        config: `backfill` 설정을 담은 소스 설정.
        window_start: 이번 실행이 처리한 window의 시작 시각.
        missing: 이번 실행이 최종적으로 도달한 누락 정보.
        first_failed_at: 이번 실행을 첫 실패로 볼 때 쓸 시각. 기존 마커가 있으면
            무시되고 그 마커의 값이 유지된다.
    returns:
        마커가 존재하게 됐으면 `"pending"`, 지워졌거나 애초에 필요 없었으면 `None`.
        manifest의 `backfill_status` 필드에 그대로 쓰인다.
    """
    if config.backfill is None or not config.backfill.enabled:
        return None

    existing = next(
        (m for m in manifest_module.load_retry_markers(config.source_id) if m.window_start == window_start),
        None,
    )

    if not missing.parts:
        if existing is not None:
            manifest_module.clear_retry_marker(config.source_id, window_start)
        return None

    first_failed = existing.first_failed_at if existing is not None else first_failed_at
    manifest_module.save_retry_marker(
        RetryMarker(
            source_id=config.source_id,
            window_start=window_start,
            missing_parts=missing.parts,
            first_failed_at=first_failed,
            expires_at=first_failed + config.backfill.max_age,
            attempts=(existing.attempts + 1) if existing is not None else 1,
        )
    )
    return "pending"


def _now() -> datetime:
    """KST aware 현재 시각. manifest의 started_at·ended_at에 쓴다."""
    return datetime.now(_KST)


def execute_window(
    config,
    window_start: datetime,
    *,
    client,
    force: bool = False,
    backfill: bool = False,
    sleep_fn=time.sleep,
) -> Manifest:
    """window 하나를 재개 분기에 따라 처리하고 최종 manifest를 반환한다.

    fetch(또는 bronze 재사용) → 완결도 게이트 → normalize → 검증 → 폐기 게이트 →
    silver·quarantine 저장 → manifest 저장 순으로 진행한다. 
    실패하더라도 예외를 올리지 않고 FAILED manifest를 만들어 반환한다 
    호출자(CLI)가 `status`만 보고 종료 코드를 정할 수 있게 하기 위해서다.

    args:
        config: 소스 설정. 
        window_start: 수집 대상 window의 시작 시각. 
        client: 어댑터에 주입할 httpx 클라이언트.
        force: 재개 분기를 모두 무시하고 처음부터 다시 수집한다.
        backfill: 완결된 window의 누락 조각만 채운다. `force`와 동시에 줄 수 없다.
        sleep_fn: 라운드 간 대기 함수. 
    returns:
        이번 실행이 도달한 최종 manifest.
    raises:
        ForceAndBackfillError: `force`와 `backfill`을 동시에 줄 때.
    """
    if force and backfill:
        raise ForceAndBackfillError("--force와 --backfill은 함께 줄 수 없다")

    window_end = window_start + config.schedule.interval
    started_at = _now()
    existing = manifest_module.load(config.source_id, window_start)
    adapter_cls = get_adapter(config.adapter)
    window = Window(window_start=window_start, window_end=window_end)

    if existing and existing.stage == Stage.COMPLETED and not force and not (backfill and existing.missing.parts):
        # 분기 1: 이미 완결됐고 채울 누락도 없으면(또는 backfill이 아니면) 아무것도
        # 하지 않는다. 재실행해도 안전해야 하므로(Airflow retry) 어댑터를 다시
        # 부르지 않고, 저장된 manifest도 건드리지 않은 채 SKIPPED로만 표시해 돌려준다.
        logger.info("stage=completed status=skipped")
        return existing.model_copy(update={"status": RunStatus.SKIPPED})

    if existing and backfill and existing.missing.parts and existing.stage.value >= Stage.BRONZE_WRITTEN.value and not force:
        # 분기 4: 백필 — 기존 조각은 그대로 두고(clear_bronze 없이) 누락분만 받는다.
        # COMPLETED에서 누락이 있든(PARTIAL), FETCH_ERROR로 BRONZE_WRITTEN에서 멈췄든
        # 둘 다 이 분기를 타서 남은 조각을 마저 채운다.
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
        # 이번에 새로 알아낸 값이 있으면 그걸 쓰고, 없으면 이전 실행이 남긴 값을 이어받는다.
        expected_total = round_result.expected_total if round_result.expected_total is not None else existing.counts.expected
        attempt = existing.attempt + 1
        revision_base = existing.revision  # silver를 실제로 다시 쓸 때만 +1한다(아래에서).
    elif existing and existing.stage.value >= Stage.BRONZE_WRITTEN.value and not force:
        # 분기 2: bronze 재사용 — 이전 실행이 fetch까지는 끝냈지만(예: silver 쓰기
        # 실패로 VALIDATED에서 멈췄음) 그 뒤에서 죽은 경우다. 지금 다시 fetch하면
        # 실시간 API에서는 그 시점의 다른 데이터를 받게 되므로, 반드시 이전에 저장된
        # bronze 조각을 그대로 재사용한다.
        parts = existing.artifacts.bronze.parts
        chunks = dict(zip(parts, storage.read_bronze(config.source_id, window_start, parts)))
        # 조각 자체는 재시도하지 않으므로 실제 실패 종류는 중요하지 않다 — 아래
        # 게이트 계산이 "재시도 불가능한 누락"으로만 취급하면 되므로 PERMANENT로 채운다.
        missing_keys = {key: FetchErrorKind.PERMANENT for key in existing.missing.parts}
        expected_total = existing.counts.expected
        attempt = existing.attempt + 1
        revision_base = existing.revision
    else:
        # 분기 3(또는 --force): 처음부터 전체 fetch. 조각 수가 실행마다 달라질 수
        # 있으므로(예: 5조각 → 3조각) 이전 실행의 유령 조각이 남지 않도록 먼저 지운다.
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
        # 실패 케이스가 대부분 같은 필드(source_id·window·attempt 등)를 반복하므로,
        # 기본값은 "가장 이른 실패 지점"(FAILED, BRONZE_WRITTEN)으로 깔고 각
        # 호출부가 필요한 값만 덮어쓴다.
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
        """manifest를 만들고 마커를 동기화해 저장한 뒤 그대로 반환한다.

        아래 모든 종료 지점이 공유한다. `missing`을 넘기지 않은 호출(FATAL 등)은
        누락 없음으로 보고, 그 경우 마커도 건드리지 않는다.
        """
        missing = over.get("missing", Missing())
        backfill_status = _sync_retry_marker(config, window_start, missing, started_at)
        result = _base_manifest(backfill_status=backfill_status, **over)
        manifest_module.save(result)
        return result

    if FetchErrorKind.FATAL in missing_keys.values():
        # FATAL(인증 오류 등)은 나머지 조각도 같은 이유로 실패할 게 뻔하므로, 게이트
        # 계산도 검증도 건너뛰고 즉시 끝낸다. stage는 기본값 BRONZE_WRITTEN 그대로
        # 둔다 — 다음 실행이 분기 2로 들어가 재계산하게 하기 위해서다.
        logger.error("stage=bronze_written status=failed failure_reason=fetch_error reason=fatal")
        return _finish(failure_reason=FailureReason.FETCH_ERROR)

    # normalize는 bronze 재사용 여부와 무관하게 항상 다시 수행한다(네트워크를
    # 타지 않는 순수 변환이라 비용이 없다). 그 결과로 나온 행 수를 "성공 조각으로
    # 계산하는 missing_ratio"에도 그대로 재사용한다.
    rows = adapter_cls.normalize(_sorted_chunks(chunks), config)
    fetched_rows = len(rows)
    ratio = _missing_ratio(len(missing_keys), expected_total, fetched_rows, len(chunks))
    missing = _build_missing(missing_keys, expected_total, fetched_rows)

    parts_summary = f"{len(chunks)}/{len(chunks) + len(missing_keys)}"

    if ratio > config.quality.max_missing_ratio:
        # 완결도 게이트 초과 — silver를 쓰지 않고 fetch_error로 끝낸다. stage는
        # BRONZE_WRITTEN 그대로라 재실행(또는 백필)하면 분기 2/4로 들어간다.
        logger.error(
            "stage=bronze_written status=failed failure_reason=fetch_error "
            f"parts={parts_summary} missing_ratio={ratio:.3f}"
        )
        return _finish(
            failure_reason=FailureReason.FETCH_ERROR,
            missing=missing, counts=Counts(expected=expected_total, fetched=fetched_rows),
        )

    # 여기 도달했다는 것은 완결도 게이트를 통과했다는 뜻이다 — fetch 단계 경계의
    # 로그 한 줄을 여기서 남긴다. 누락이 남아 있으면 WARNING, 없으면 INFO.
    bronze_bytes = sum(len(v) for v in chunks.values())
    if missing_keys:
        logger.warning(
            f"stage=bronze_written parts={parts_summary} rows={fetched_rows} bytes={bronze_bytes} "
            f"missing={','.join(sorted(missing_keys))} completeness={1.0 - ratio:.3f}"
        )
    else:
        logger.info(f"stage=bronze_written parts={parts_summary} rows={fetched_rows} bytes={bronze_bytes}")

    if fetched_rows == 0:
        # 행이 0건이면 검증을 돌릴 대상이 없으므로 여기서 바로 갈린다. allow_empty인
        # 소스(행사 등)만 정상 종료로 인정하고, 그 외에는 quality_gate로 묶는다.
        if config.quality.allow_empty:
            logger.info("stage=completed status=empty")
            return _finish(
                status=RunStatus.EMPTY, stage=Stage.COMPLETED, failure_reason=None,
                missing=missing, counts=Counts(expected=expected_total),
            )
        logger.error("stage=validated status=failed failure_reason=quality_gate rows=0")
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.QUALITY_GATE,
            missing=missing, counts=Counts(expected=expected_total),
        )

    ctx = RunContext(source_id=config.source_id, window_start=window_start, window_end=window_end, attempt=attempt)
    try:
        outcome = validate_batch(rows, config, ctx)
    except BatchValidationFailed:
        # 컬럼 정책이 FAIL_BATCH를 반환했다 — 그 시점 이후 행은 처리되지 않았으므로
        # silver·quarantine 어느 쪽도 쓸 수 없다. 같은 bronze + 같은 config면
        # 재시도해도 결과가 같으므로 config를 고쳐야 하는 quality_gate로 남긴다.
        logger.error("stage=validated status=failed failure_reason=quality_gate reason=fail_batch")
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.QUALITY_GATE,
            missing=missing, counts=Counts(expected=expected_total, fetched=fetched_rows),
        )

    counts = Counts(expected=expected_total, **outcome.counts)
    column_issues = {col: ColumnIssueCount(**v) for col, v in outcome.column_issues.items()}
    # 아래 세 종료 지점(quarantine 실패·drop_ratio 초과·silver 실패)이 공통으로 남길
    # 필드. artifacts만 지점마다 달라(quarantine 키가 언제 붙는지) 따로 넘긴다.
    common = {
        "missing": missing, "counts": counts, "column_issues": column_issues,
        "policy_actions": outcome.policy_actions, "drop_ratio": outcome.drop_ratio,
    }

    # 데이터 품질 관점의 validated 단계 경계 로그. 폐기 게이트 초과 여부는 이 시점에
    # 이미 알 수 있으므로 여기서 ERROR로 확정해 남긴다 — 뒤에 storage_error가 나면
    # 그건 별개 사건이라 각자 자기 ERROR 줄을 남긴다(둘이 겹쳐도 서로 다른 사유다).
    validated_summary = (
        f"stage=validated kept={outcome.counts['kept']} repaired={outcome.counts['repaired']} "
        f"dropped={outcome.counts['dropped']} drop_ratio={outcome.drop_ratio:.3f} completeness={1.0 - ratio:.3f}"
    )
    if outcome.drop_ratio > config.quality.max_drop_ratio:
        logger.error(f"{validated_summary} status=failed failure_reason=quality_gate")
    elif outcome.counts["dropped"] or missing.parts:
        logger.warning(f"{validated_summary} status=partial")
    else:
        logger.info(f"{validated_summary} status=succeeded")

    try:
        # 폐기 행이 없으면 quarantine 객체 자체를 만들지 않는다(빈 객체를 굳이 남기지 않는다).
        quarantine_key = (
            storage.write_quarantine(config.source_id, window_start, outcome.quarantine_records)
            if outcome.quarantine_records else None
        )
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        logger.error("stage=validated status=failed failure_reason=storage_error reason=quarantine_write")
        return _finish(stage=Stage.VALIDATED, failure_reason=FailureReason.STORAGE_ERROR, **common)

    if outcome.drop_ratio > config.quality.max_drop_ratio:
        # 폐기 게이트 초과 — silver는 쓰지 않지만 quarantine은 남긴다(왜 실패했는지
        # 분석하려면 폐기된 행이 필요하다).
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.QUALITY_GATE, **common,
            artifacts=Artifacts(bronze=artifacts.bronze, quarantine=quarantine_key),
        )

    try:
        silver_key = storage.write_silver(config.source_id, window_start, pa.Table.from_pylist(outcome.silver_rows))
    except Exception:  # noqa: BLE001 — 저장소 예외 종류를 가리지 않고 storage_error로 묶는다
        logger.error("stage=validated status=failed failure_reason=storage_error reason=silver_write")
        return _finish(
            stage=Stage.VALIDATED, failure_reason=FailureReason.STORAGE_ERROR, **common,
            artifacts=Artifacts(bronze=artifacts.bronze, quarantine=quarantine_key),
        )

    # 여기까지 왔다는 것은 silver를 실제로 (다시) 썼다는 뜻이므로 revision을 올린다.
    # 폐기·누락이 전혀 없을 때만 SUCCEEDED이고, 그 외(둘 중 하나라도 게이트 이내로
    # 존재)에는 PARTIAL이다.
    status = RunStatus.SUCCEEDED if outcome.counts["dropped"] == 0 and ratio == 0.0 else RunStatus.PARTIAL
    new_revision = revision_base + 1
    logger.info(f"stage=completed status={status.value} revision={new_revision} key={silver_key}")
    return _finish(
        status=status, stage=Stage.COMPLETED, failure_reason=None,
        revision=new_revision, completeness=1.0 - ratio, **common,
        artifacts=Artifacts(bronze=artifacts.bronze, silver=silver_key, quarantine=quarantine_key),
    )
