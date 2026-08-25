"""CLI 진입점 — 인자 파싱 후 pipeline을 호출한다.

    cd collector
    uv run python main.py --source bike_station_realtime \
        --window-start 2026-08-12T23:10:00+09:00 [--force] [--backfill]

Airflow는 소스별 태스크에서 논리 시각을 KST로 변환해 `--window-start`로 넘긴다.
일반 fetch 실패는 같은 태스크의 retry에서 소스별 `fetch.retry_mode`로 복구한다.
현재 범용 Backfill DAG는 없으며, `--backfill`과 대상 조회 옵션은 legacy 코드
호환용으로만 유지한다. 운영 설정은 예전 retry marker를 발견하지 않는다.
`window_end`는 config의 `schedule.interval`로 계산하며, collector 자체는 스케줄을
모른다.

`--force`와 `--backfill`은 목적이 반대라 함께 줄 수 없다(오류로 막는다).

| 플래그 | 의미 | bronze |
| --- | --- | --- |
| `--force` | 재개 분기를 무시하고 처음부터 다시 | `clear_bronze` 후 전체 재수집 |
| `--backfill` | 완결된 window의 누락 조각만 채움 | 기존 조각 유지, 빠진 것만 호출 |

종료 코드는 `SUCCEEDED`·`PARTIAL`·`EMPTY`·`SKIPPED`가 0, `FAILED`가 non-zero다.
누락이 있어도 게이트를 통과했으면 `PARTIAL`(0)이다. 완전한 일일 snapshot이 필요한
운영 소스는 `max_missing_ratio=0`으로 두어 누락을 `FAILED`로 만들고 Airflow retry를
실행한다.

주의:
- 스택 트레이스를 그대로 뱉지 않는다. 실패는 manifest에 남기고 정리된 메시지와
  종료 코드로 전달한다.
- 인증키를 인자로 받지 않는다. 키는 환경변수(`SEOUL_OPENAPI_KEY`, `KMA_APIHUB_KEY`)
  에서만 읽는다.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

import adapters  # noqa: F401 (어댑터 레지스트리 로드용)
import config.loader as config_loader
import httpx
import manifest as manifest_module
import pipeline
import storage
from logging_setup import configure_logging
from manifest import RunStatus

_OK_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.PARTIAL, RunStatus.EMPTY, RunStatus.SKIPPED})

# httpx 기본값은 모든 단계에 5초다. 서울 열린데이터광장의 1000행 페이지 응답 시간을
# 실측하면 시점에 따라 0.6~7.2초로 흔들려서, 기본값이면 느린 시점에 페이지마다
# ReadTimeout으로 5초를 버리고 라운드 재시도(15s·30s 대기)로 넘어간다. 데이터를
# 잃지는 않지만 fetch 예산(`effective_fetch_budget`)을 헛되게 태운다.
#
# read를 30초로 두는 근거는 실측 최댓값(7.2초)의 4배 여유다. connect는 짧게 둔다 —
# 연결이 안 되는 상황은 기다려서 나아지지 않고, TRANSIENT로 라운드가 재시도한다.
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=30.0)


def _latest_source_uses_config(
    source_id: str, logical_dttm: datetime, config_version: str
) -> bool:
    """가장 최근 authority가 현재 배포 source 설정으로 수집됐는지 확인한다."""

    snapshots = manifest_module.load_source_snapshots(source_id, logical_dttm)
    return bool(
        snapshots and snapshots[-1].manifest.config_version == config_version
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.
    `--force`와 `--backfill`은 목적이 반대라 함께 주면 `argparse`가 사용법 메시지를 찍고 `SystemExit`을 일으키게 막는다.

    args:
        argv: 파싱할 인자 목록. 생략하면 `sys.argv`를 그대로 쓴다.
    returns:
        `source` · `window_start` · `force` · `backfill` · `list_backfill_targets` 필드를 담은 네임스페이스.
    raises:
        SystemExit: 필수 인자가 없거나 `--force`와 `--backfill`을 함께 줬을 때.
    """
    parser = argparse.ArgumentParser(prog="main.py")
    parser.add_argument("--source", required=True, help="소스 id (sources/{source_id}.yaml)")
    parser.add_argument("--window-start", help="ISO8601, KST 오프셋(+09:00) 포함")
    parser.add_argument("--force", action="store_true", help="재개 분기를 무시하고 clear_bronze 후 전체 재수집")
    parser.add_argument("--backfill", action="store_true", help="완결된 window의 누락 조각만 채운다")
    parser.add_argument("--list-backfill-targets", action="store_true", help="백필 대상을 JSON으로 출력하고 종료")
    parser.add_argument(
        "--check-due-after-seconds",
        type=int,
        default=None,
        help="마지막 성공 수집 이후 이 초만큼 안 지났으면 수집을 건너뛰어도 되는지 JSON으로 출력하고 종료",
    )
    args = parser.parse_args(argv)

    if (
        not args.list_backfill_targets
        and args.check_due_after_seconds is None
        and not args.window_start
    ):
        parser.error(
            "the following arguments are required: --window-start "
            "(unless --list-backfill-targets/--check-due-after-seconds is used)"
        )

    if args.force and args.backfill:
        parser.error("--force와 --backfill은 함께 줄 수 없다")

    return args


def exit_code_for(status: RunStatus) -> int:
    """manifest의 최종 status를 프로세스 종료 코드로 바꾼다.

    `SUCCEEDED` · `PARTIAL` · `EMPTY` · `SKIPPED`는 0, `FAILED`는 non-zero다.
    누락을 허용하지 않는 소스는 pipeline 완결도 게이트에서 `FAILED`가 되므로
    이 매핑을 바꾸지 않아도 Airflow retry가 동작한다.

    args:
        status: pipeline이 반환한 manifest의 최종 status.
    returns:
        Airflow 태스크 성공/실패를 가르는 프로세스 종료 코드.
    """
    return 0 if status in _OK_STATUSES else 1


def main(argv: list[str] | None = None) -> int:
    """인자를 파싱해 window 하나를 수집하고 그 결과를 종료 코드로 반환한다.

    처리 순서는 인자 파싱 → 로깅 초기화 → config 로드 → pipeline 실행 → 종료
    코드 반환이다. 로깅 초기화를 config 로드보다 먼저 하는 이유는, 
    이후 pipeline이 남기는 모든 로그에 고정 필드(source_id·window·attempt)가
    붙게 하기 위해서다 — 순서를 바꾸면 pipeline이 로드 직후에 남기는 초반 로그에는 고정 필드가 빠진다.

    args:
        argv: `parse_args`에 그대로 전달할 인자 목록.
    returns:
        Airflow가 태스크 성공/실패를 판단할 프로세스 종료 코드.
    """
    args = parse_args(argv)

    if args.list_backfill_targets:
        import json
        config = config_loader.load(args.source)
        targets = pipeline.get_backfill_targets(config)
        print(json.dumps(targets))
        return 0

    if args.check_due_after_seconds is not None:
        import json
        from zoneinfo import ZoneInfo

        config = config_loader.load(args.source)
        now = datetime.now(ZoneInfo("Asia/Seoul"))
        last = storage.latest_source_snapshot_logical_dttm(args.source, as_of=now)
        elapsed_seconds = None if last is None else (now - last).total_seconds()
        due = (
            elapsed_seconds is None
            or elapsed_seconds >= args.check_due_after_seconds
            or not _latest_source_uses_config(
                args.source, last, config.config_version
            )
        )
        print(json.dumps({
            "source_id": args.source,
            "due": due,
            "last_logical_dttm": None if last is None else last.isoformat(),
            "elapsed_seconds": elapsed_seconds,
        }))
        return 0

    window_start = datetime.fromisoformat(args.window_start)

    configure_logging(args.source, window_start, attempt=1)
    config = config_loader.load(args.source)

    # httpx.Client를 여기서 만들어 실행 하나에 재사용한다
    # 어댑터는 연결을 모르고 `client` 인자로 주입받기만 한다
    with httpx.Client(timeout=_HTTP_TIMEOUT) as client:
        result = pipeline.execute_window(
            config, window_start, client=client, force=args.force, backfill=args.backfill,
        )

    return exit_code_for(result.status)


if __name__ == "__main__":
    sys.exit(main())
