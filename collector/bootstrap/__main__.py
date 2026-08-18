"""CLI 진입점으로 과거 데이터를 archive에 한 번 적재한다.

## 실행 방법

    cd collector
    uv run --frozen python -m bootstrap --source bike_rental_history \
        --from 2025-01-01 --to 2025-12-31 --csv-dir ../data
    uv run --frozen python -m bootstrap --source bike_station_realtime \
        --from 2025-01-01 --to 2025-12-31 --concurrency 4

Airflow가 부르지 않는 수동 작업이라 태스크 빌더를 만들지 않는다.

## 재개

이미 archive가 있는 날짜는 건너뛴다. 상태 파일을 두지 않는다 — 날짜 단위로
원자적이라(한 날짜를 다 만든 뒤 쓴다) 중단 시 그 날짜는 아예 안 써지고 다음 실행이
다시 만든다. `--force`로 무시한다.

## 종료 코드

실패한 날짜가 하나라도 있으면 non-zero다. 재개가 archive 존재 기반이라 다시 돌리면
실패한 날짜만 재시도된다.

## 연속 실패 중단

`kind=history_api`에서 날짜가 연속으로 `_MAX_CONSECUTIVE_FAILURES`번 실패하면 남은
날짜는 시도하지 않고 중단한다. 인증키 오류·쿼터 소진 상황에서 나머지 범위 전체에
확정적으로 실패할 호출을 계속 날리는 것을 막기 위해서다. 중단 시 요약에
`aborted=true`가 붙는다. 중간에 성공이 한 번이라도 있으면 카운터가 리셋된다.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import httpx

import config.loader as config_loader
from bootstrap import api_source, csv_source, runner
from bootstrap import config as bootstrap_config
from logging_setup import configure_batch_logging

logger = logging.getLogger(__name__)

# 연속으로 이만큼 날짜가 실패하면 나머지는 시도하지 않고 중단한다. 인증키 오류나
# 일일 쿼터 소진처럼 "설정 자체가 잘못됐다"는 신호와, 일시적 네트워크 장애로 몇
# 개 날짜만 실패하는 것을 가르는 값이다. 3년 범위(약 1,095일)를 통째로 도는 작업이
# 있으므로 너무 크면 확정적 실패에 수만 번 헛호출하게 되고, 너무 작으면 흔한 일시적
# 장애에도 작업 전체가 중단된다 — 5 정도가 그 사이의 합리적인 선이다.
_MAX_CONSECUTIVE_FAILURES = 5


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(prog="python -m bootstrap")
    parser.add_argument("--source", required=True, help="소스 id")
    parser.add_argument("--from", dest="from", required=True, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--to", required=True, help="끝 날짜, 포함 (YYYY-MM-DD)")
    parser.add_argument("--csv-dir", help="kind=csv일 때 CSV들이 있는 디렉터리")
    parser.add_argument("--concurrency", type=int, default=4, help="kind=history_api의 동시 조회 수")
    parser.add_argument("--force", action="store_true", help="archive가 있어도 다시 쓴다")
    return parser.parse_args(argv)


def resolve_dates(args: argparse.Namespace) -> list[date]:
    """처리할 날짜를 오름차순으로 만든다.

    raises:
        SystemExit: `--from`이 `--to`보다 뒤일 때.
    """
    first, last = date.fromisoformat(getattr(args, "from")), date.fromisoformat(args.to)
    if first > last:
        raise SystemExit(f"--from({first})이 --to({last})보다 뒤다")
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def exit_code_for(results: list[runner.DateResult]) -> int:
    """실패한 날짜가 있으면 non-zero."""
    return 1 if any(r.status == "failed" for r in results) else 0


def main(argv: list[str] | None = None) -> int:
    """인자를 파싱해 대상 날짜를 적재하고 결과를 종료 코드로 반환한다."""
    args = parse_args(argv)
    configure_batch_logging(args.source)

    scfg = config_loader.load(args.source)
    bcfg = bootstrap_config.load(args.source)
    days = resolve_dates(args)

    results: list[runner.DateResult] = []
    aborted = False
    if bcfg.kind == "csv":
        if not args.csv_dir:
            raise SystemExit("kind=csv 소스에는 --csv-dir가 필요하다")
        csv_dir = Path(args.csv_dir)
        if not csv_dir.exists() or not csv_dir.is_dir():
            raise SystemExit(f"--csv-dir 경로가 없거나 디렉터리가 아니다: {csv_dir}")
        if not any(csv_dir.glob("*.csv")):
            logger.warning(
                f"stage=bootstrap source={args.source} csv_dir={csv_dir} "
                "csv 파일을 하나도 찾지 못했다"
            )
        by_date = csv_source.read_by_date(bcfg, csv_dir, set(days))
        for day in days:
            table = by_date.get(day)
            rows = table.to_pylist() if table is not None else []
            results.append(runner.load_date(scfg, bcfg, day, rows, force=args.force))
    else:
        consecutive_failures = 0
        with httpx.Client(timeout=60.0) as client:
            for day in days:
                try:
                    rows = api_source.fetch_by_date(
                        bcfg, day, client=client, concurrency=args.concurrency
                    )
                except api_source.FetchFailed as exc:
                    results.append(runner.DateResult(day=day, status="failed", error=str(exc)))
                    consecutive_failures += 1
                    if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                        remaining = len(days) - len(results)
                        logger.error(
                            f"stage=bootstrap source={args.source} "
                            f"연속 {consecutive_failures}회 실패해 중단한다. "
                            f"남은 {remaining}개 날짜는 시도하지 않았다"
                        )
                        aborted = True
                        break
                    continue
                consecutive_failures = 0
                results.append(runner.load_date(scfg, bcfg, day, rows, force=args.force))

    tally: dict[str, int] = {}
    for result in results:
        tally[result.status] = tally.get(result.status, 0) + 1
    summary = " ".join(f"{status}={count}" for status, count in sorted(tally.items()))
    overlapped = sum(1 for r in results if r.silver_present)
    dropped_total = sum(r.dropped or 0 for r in results)
    abort_note = " aborted=true" if aborted else ""
    print(
        f"source={args.source} dates={len(days)} {summary} "
        f"silver_overlap={overlapped} dropped={dropped_total}{abort_note}"
    )

    return exit_code_for(results)


if __name__ == "__main__":
    sys.exit(main())
