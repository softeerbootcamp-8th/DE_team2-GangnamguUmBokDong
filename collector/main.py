"""CLI 진입점 — 인자 파싱 후 pipeline을 호출한다.

## 실행 방법

wheel 빌드 없이 실행한다. 하위 디렉토리는 `sys.path[0] == collector/`라 그대로
import된다.

    cd collector
    uv run python main.py --source bike_station_realtime \
        --window-start 2026-08-12T23:10:00+09:00 [--force] [--backfill]

Airflow는 소스별 태스크에서 `data_interval_start`를 KST로 변환해 `--window-start`로
넘긴다. 백필 DAG는 `_retry_queue/`에서 얻은 대상에 `--backfill`을 붙여 호출한다.

## 구현할 것

- 인자 — `--source`(필수) · `--window-start`(필수, ISO8601, KST 오프셋(`+09:00`) 포함)
  · `--force` · `--backfill`
- `window_end`는 config의 `schedule.interval`로 계산한다. **collector 자체는 스케줄을
  모른다.**
- 처리 순서 — 인자 파싱 → 로깅 초기화 → config 로드 → pipeline 실행 → 종료 코드 반환.
  로깅 초기화가 pipeline보다 **먼저**여야 고정 필드가 모든 로그에 붙는다.

## --force와 --backfill

목적이 반대다.

| 플래그 | 의미 | bronze |
| --- | --- | --- |
| `--force` | 재개 분기를 무시하고 처음부터 다시 | `clear_bronze` 후 전체 재수집 |
| `--backfill` | 완결된 window의 **누락 조각만** 채움 | 기존 조각 유지, 빠진 것만 호출 |

**둘을 함께 주면 오류로 막는다.** 함께 주면 `--force`와 같아지는데, 백필 DAG가 실수로
둘을 넘겼을 때 조용히 전체 재수집이 도는 것은 실시간 소스에서 위험하다.

## 종료 코드

`SUCCEEDED` · `PARTIAL` · `EMPTY` · `SKIPPED`는 0, `FAILED`는 non-zero. Airflow 태스크
실패로 이어져야 한다.

**누락이 있어도 게이트를 통과했으면 `PARTIAL`이므로 0이다.** 부분 성공은
`stage=completed`로 끝나 재실행하면 재개 분기가 `SKIPPED`로 빠지므로 Airflow retry가 할
일이 없다 — 재시도가 무의미한데 태스크만 실패로 뜨는 셈이 된다. 채워 넣는 일은 백필 잡이
맡고, 가시성은 WARN 로그 · manifest · `_retry_queue` 마커로 확보한다.

`PARTIAL`을 0으로 두는 것은 두 게이트(`max_drop_ratio` · `max_missing_ratio`) 이내를
정상으로 본다는 설계 결정이다.

## 미결정

- `--window-start`가 주기 경계에 맞는지 검사할지(5분 주기에 `14:12`가 들어온 경우).
  경계 정렬 검증을 넣을지는 #8에서 정한다.

## 주의

- 스택 트레이스를 그대로 뱉지 않는다. 실패는 manifest에 남기고 정리된 메시지와 종료
  코드로 전달한다.
- 인증키를 인자로 받지 않는다. 키는 환경변수(`SEOUL_OPENAPI_KEY` ·
  `KMA_APIHUB_KEY`)에서만 읽는다.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

import httpx

import config.loader as config_loader
import pipeline
from logging_setup import configure_logging
from manifest import RunStatus

_OK_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.PARTIAL, RunStatus.EMPTY, RunStatus.SKIPPED})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다.
    `--force`와 `--backfill`은 목적이 반대라 함께 주면 `argparse`가 사용법 메시지를 찍고 `SystemExit`을 일으키게 막는다.

    args:
        argv: 파싱할 인자 목록. 생략하면 `sys.argv`를 그대로 쓴다.
    returns:
        `source` · `window_start` · `force` · `backfill` 네 필드를 담은 네임스페이스.
    raises:
        SystemExit: 필수 인자가 없거나 `--force`와 `--backfill`을 함께 줬을 때.
    """
    parser = argparse.ArgumentParser(prog="main.py")
    parser.add_argument("--source", required=True, help="소스 id (sources/{source_id}.yaml)")
    parser.add_argument("--window-start", required=True, help="ISO8601, KST 오프셋(+09:00) 포함")
    parser.add_argument("--force", action="store_true", help="재개 분기를 무시하고 clear_bronze 후 전체 재수집")
    parser.add_argument("--backfill", action="store_true", help="완결된 window의 누락 조각만 채운다")
    args = parser.parse_args(argv)

    if args.force and args.backfill:
        parser.error("--force와 --backfill은 함께 줄 수 없다")

    return args


def exit_code_for(status: RunStatus) -> int:
    """manifest의 최종 status를 프로세스 종료 코드로 바꾼다.

    `SUCCEEDED` · `PARTIAL` · `EMPTY` · `SKIPPED`는 0, `FAILED`는 non-zero다.
    누락이 있어도 게이트를 통과했으면 `PARTIAL`이므로 0이다 — 부분 성공을 실패로 표시하면 Airflow retry가 할 일 없이 태스크만 실패로 뜬다.

    args:
        status: pipeline이 반환한 manifest의 최종 status.
    returns:
        Airflow 태스크 성공/실패를 가르는 프로세스 종료 코드.
    """
    return 0 if status in _OK_STATUSES else 1


def main(argv: list[str] | None = None) -> int:
    """인자를 파싱해 window 하나를 수집하고 그 결과를 종료 코드로 반환한다.

    처리 순서는 인자 파싱 → 로깅 초기화 → config 로드 → pipeline 실행 → 종료
    코드 반환이다. 로깅 초기화를 config 로드보다 먼저 하는 이유는, 이후
    pipeline이 남기는 모든 로그에 고정 필드(source_id·window·attempt)가
    붙게 하기 위해서다 — 순서를 바꾸면 pipeline이 로드 직후에 남기는 초반 로그에는 고정 필드가 빠진다.

    args:
        argv: `parse_args`에 그대로 전달할 인자 목록.
    returns:
        Airflow가 태스크 성공/실패를 판단할 프로세스 종료 코드.
    """
    args = parse_args(argv)
    window_start = datetime.fromisoformat(args.window_start)

    configure_logging(args.source, window_start, attempt=1)
    config = config_loader.load(args.source)

    # httpx.Client를 여기서 만들어 실행 하나에 재사용한다 — 어댑터는 연결을 모르고
    # `client` 인자로 주입받기만 한다(테스트에서 MockTransport로 바꿔치기하기 쉽다).
    with httpx.Client() as client:
        result = pipeline.execute_window(
            config, window_start, client=client, force=args.force, backfill=args.backfill,
        )

    return exit_code_for(result.status)


if __name__ == "__main__":
    sys.exit(main())
