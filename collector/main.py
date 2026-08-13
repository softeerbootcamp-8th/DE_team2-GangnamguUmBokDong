"""Collector command-line entry point."""

import argparse
import sys

from sources.bike import fetch_bike_page

SUPPORTED_SOURCES = ("bike",)


def parse_args() -> argparse.Namespace:
    """Parse collector command-line arguments.

    Returns:
        argparse.Namespace: Parsed collector execution arguments.
    """
    parser = argparse.ArgumentParser(description="Run a data collector.")

    parser.add_argument(
        "--source",
        required=True,
        choices=SUPPORTED_SOURCES,
        help="Data source to collect.",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Logical collection run identifier.",
    )

    return parser.parse_args()


def main() -> int:
    """Run the collector and return an exit code.

    Returns:
        int: Process exit code for Airflow.
    """
    args = parse_args()

    print(f"source={args.source}")
    print(f"run_id={args.run_id}")
    print("collector started")

    payload = fetch_bike_page(1)

    service = payload["rentBikeStatus"]

    print(f"total_count={service['list_total_count']}")
    print(f"row_count={len(service.get('row', []))}")

    print("collector finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())


"""CLI 진입점 — 인자 파싱 후 pipeline을 호출한다.

구현 예정: docs/collector/implementation-issues.md #8
설계 근거: docs/collector/implementation-plan.md 10절 (실행 인터페이스)

## 실행 방법

wheel 빌드 없이 실행한다. 하위 디렉토리는 `sys.path[0] == collector/`라 그대로
import된다.

    cd collector
    uv run python main.py --source bike_station_realtime \
        --window-start 2026-08-12T14:10:00Z [--force] [--backfill]

Airflow는 소스별 태스크에서 `data_interval_start`를 `--window-start`로 넘긴다.
백필 DAG는 `_retry_queue/`에서 얻은 대상에 `--backfill`을 붙여 호출한다.

## 구현할 것

- 인자 — `--source`(필수) · `--window-start`(필수, ISO8601 Z) · `--force` · `--backfill`
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
