"""로컬 E2E 명령에서 사용할 이식 가능한 시각 계산을 제공한다."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta


def station_source_dttm(raw: str) -> str:
    """Logical time의 5분 전 시각을 같은 offset의 ISO 8601 문자열로 반환한다."""
    try:
        logical = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("logical time은 ISO 8601 시각이어야 합니다.") from exc
    if logical.tzinfo is None or logical.utcoffset() is None:
        raise ValueError("logical time에는 timezone offset이 필요합니다.")
    if logical.second or logical.microsecond or logical.minute % 5:
        raise ValueError("logical time은 초가 0인 5분 경계여야 합니다.")
    return (logical - timedelta(minutes=5)).isoformat(timespec="seconds")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """E2E 시각 계산 명령과 인자를 파싱한다."""
    parser = argparse.ArgumentParser(description="로컬 E2E 시각을 계산한다.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    station_source = subparsers.add_parser(
        "station-source",
        help="logical time보다 5분 앞선 station source 시각을 출력한다.",
    )
    station_source.add_argument("logical_dttm")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """계산 결과를 출력하고 잘못된 입력을 nonzero 종료로 변환한다."""
    args = parse_args(argv)
    try:
        if args.command == "station-source":
            print(station_source_dttm(args.logical_dttm))
            return 0
    except ValueError as exc:
        print(f"[e2e] ERROR: {exc}", file=sys.stderr)
        return 2
    raise RuntimeError(f"지원하지 않는 E2E 시각 명령입니다: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
