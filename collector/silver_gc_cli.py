"""Non-authority Silver 날짜 정리 CLI다."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

import config.loader as config_loader
import silver_gc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Source와 대상 날짜를 파싱한다."""
    parser = argparse.ArgumentParser(prog="silver_gc_cli.py")
    parser.add_argument("--source", required=True)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--require-archive", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """안전 조건을 만족하는 non-authority Silver를 정리한다."""
    args = parse_args(argv)
    result = silver_gc.collect_date(
        config_loader.load(args.source),
        args.date,
        require_archive=args.require_archive,
    )
    print(json.dumps({"status": result.status, "deleted": result.deleted, "retained": result.retained, "reason": result.reason}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
