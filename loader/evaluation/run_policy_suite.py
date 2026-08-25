"""2025 held-out 17일 여러 날짜를 같은 계약으로 실행하고 한 번에 집계한다."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY
from gold.rebalance_route import MAX_STOPS_PER_ROUTE

from .aggregate_policy_results import aggregate_results, write_aggregate
from .production_policy_contract import (
    PRODUCTION_CENTER_ID,
    PRODUCTION_EVALUATION_MINUTES,
    PRODUCTION_FLEET_SIZE,
    PRODUCTION_POLICY_NAME,
    PRODUCTION_START_HOUR,
    PRODUCTION_TARGET_DATES,
)
from .run_policy_backtest import PolicyVariant, run_policy_backtest, write_result

DEFAULT_DATES = PRODUCTION_TARGET_DATES


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """held-out 날짜 묶음과 공통 운영 조건을 파싱한다."""
    parser = argparse.ArgumentParser(description="2025 held-out 재배치 정책 suite")
    parser.add_argument("--dates", nargs="+", type=date.fromisoformat, default=DEFAULT_DATES)
    parser.add_argument("--center", default=PRODUCTION_CENTER_ID)
    parser.add_argument("--start-hour", type=int, default=PRODUCTION_START_HOUR)
    parser.add_argument("--fleet-size", type=int, default=PRODUCTION_FLEET_SIZE)
    parser.add_argument(
        "--max-stops",
        nargs="+",
        type=int,
        default=[MAX_STOPS_PER_ROUTE],
        help="production release 계약상 5만 허용한다.",
    )
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
    if tuple(args.dates) != PRODUCTION_TARGET_DATES:
        parser.error("production suite의 --dates는 2025-03..12 각 17일이어야 합니다.")
    if args.center != PRODUCTION_CENTER_ID:
        parser.error(f"production suite의 --center는 {PRODUCTION_CENTER_ID}여야 합니다.")
    if args.start_hour != PRODUCTION_START_HOUR:
        parser.error(
            f"production suite의 --start-hour는 {PRODUCTION_START_HOUR}여야 합니다."
        )
    if args.fleet_size != PRODUCTION_FLEET_SIZE:
        parser.error(
            f"production suite의 --fleet-size는 {PRODUCTION_FLEET_SIZE}이어야 합니다."
        )
    if args.max_stops != [MAX_STOPS_PER_ROUTE]:
        parser.error(
            "production suite의 --max-stops는 정확히 "
            f"{MAX_STOPS_PER_ROUTE} 하나여야 합니다."
        )
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
            evaluation_minutes=PRODUCTION_EVALUATION_MINUTES,
            fleet_size=args.fleet_size,
            max_stops_variants=(MAX_STOPS_PER_ROUTE,),
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
            policy_variants=(
                PolicyVariant(
                    name=PRODUCTION_POLICY_NAME,
                    max_stops_per_route=MAX_STOPS_PER_ROUTE,
                    policy_config=DEFAULT_REBALANCE_POLICY,
                ),
            ),
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
    if not aggregate["acceptance_gate"]["passed"]:
        print("Acceptance gate: FAILED")
        return 1
    print(
        "Acceptance gate: PASSED "
        f"({', '.join(aggregate['acceptance_gate']['passing_policies'])})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
