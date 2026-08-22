"""2025 held-out 17일 여러 날짜를 같은 계약으로 실행하고 한 번에 집계한다."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from .aggregate_policy_results import aggregate_results, write_aggregate
from .run_policy_backtest import run_policy_backtest, write_result

DEFAULT_DATES = tuple(date(2025, month, 17) for month in range(3, 13))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """held-out 날짜 묶음과 공통 운영 조건을 파싱한다."""
    parser = argparse.ArgumentParser(description="2025 held-out 재배치 정책 suite")
    parser.add_argument("--dates", nargs="+", type=date.fromisoformat, default=DEFAULT_DATES)
    parser.add_argument("--center", default="hangnyeoul")
    parser.add_argument("--start-hour", type=int, default=6)
    parser.add_argument("--fleet-size", type=int, default=3)
    parser.add_argument("--max-stops", nargs="+", type=int, default=[5, 8])
    parser.add_argument(
        "--bootstrap-dir",
        type=Path,
        default=Path("../data/issue163-full-year/bootstrap"),
    )
    parser.add_argument(
        "--weather-csv",
        type=Path,
        default=Path("../data/issue163-full-year/bootstrap/weather_realtime_2025.csv"),
    )
    parser.add_argument(
        "--population-dir",
        type=Path,
        default=Path("../data/issue163-full-year/population"),
    )
    parser.add_argument(
        "--model-bundle",
        type=Path,
        default=Path("../models/aws-temporary-model-2025-d20-h12-r20"),
    )
    parser.add_argument(
        "--center-seed",
        type=Path,
        default=Path("../docs/gold/dispatch-center-seed.yaml"),
    )
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"))
    parser.add_argument("--s3-bucket", default="issue163-full-year")
    parser.add_argument("--access-key", default=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
    parser.add_argument("--secret-key", default=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--output-dir", type=Path, default=Path("../data/backtest-results"))
    args = parser.parse_args(argv)
    if any(target.year != 2025 or target.day != 17 for target in args.dates):
        parser.error("모든 --dates는 고정 모델의 2025년 held-out 17일이어야 합니다.")
    if len(args.dates) != len(set(args.dates)):
        parser.error("--dates에 중복 날짜가 있습니다.")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """날짜별 평가를 순차 실행하고 계약 검증된 suite 결과를 저장한다."""
    args = parse_args(argv)
    documents = []
    for index, target in enumerate(args.dates, start=1):
        month = f"25{target.month:02d}"
        print(f"[{index}/{len(args.dates)}] {target.isoformat()} 시작", flush=True)
        result = run_policy_backtest(
            target_date=target,
            center_id=args.center,
            start_hour=args.start_hour,
            evaluation_minutes=(60, 120, 180),
            fleet_size=args.fleet_size,
            max_stops_variants=tuple(dict.fromkeys(args.max_stops)),
            rental_csv=args.bootstrap_dir
            / f"서울특별시 공공자전거 대여이력 정보_{month}.csv",
            stock_csv=args.bootstrap_dir
            / f"대여소별 공공자전거 대여가능 수량_{month}.csv",
            weather_csv=args.weather_csv,
            population_dir=args.population_dir,
            model_bundle_root=args.model_bundle,
            center_seed=args.center_seed,
            endpoint_url=args.s3_endpoint,
            bucket=args.s3_bucket,
            access_key=args.access_key,
            secret_key=args.secret_key,
        )
        json_path, _ = write_result(result, args.output_dir)
        documents.append(json.loads(json_path.read_text(encoding="utf-8")))
        print(f"[{index}/{len(args.dates)}] {target.isoformat()} 완료", flush=True)
    aggregate = aggregate_results(documents)
    stem = (
        f"{min(args.dates).isoformat()}_{max(args.dates).isoformat()}-"
        f"{args.center}-{args.start_hour:02d}h-suite"
    )
    json_path = args.output_dir / f"{stem}.json"
    markdown_path = args.output_dir / f"{stem}.md"
    write_aggregate(
        aggregate,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    print(f"Suite JSON: {json_path}")
    print(f"Suite Markdown: {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
