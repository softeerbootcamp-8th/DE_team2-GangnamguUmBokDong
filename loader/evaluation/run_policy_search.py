"""위험 구간 재고 정책 후보를 같은 point-in-time 계약으로 반복 평가한다."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from datetime import date
from itertools import product
from pathlib import Path

from gold.rebalance_policy import LEGACY_REBALANCE_POLICY, risk_band_policy

from .aggregate_policy_results import aggregate_results, write_aggregate
from .run_policy_backtest import PolicyVariant, run_policy_backtest, write_result


def build_policy_variants(
    *,
    protection_hours: tuple[int, ...],
    minimum_stock_ratios: tuple[float, ...],
    uncertainty_values: tuple[float, ...],
    max_pickup_stock_fractions: tuple[float, ...],
    pickup_cooldown_minutes: tuple[int, ...],
    max_stops: tuple[int, ...],
    include_legacy: bool,
) -> tuple[PolicyVariant, ...]:
    """제한된 Cartesian 후보군을 결정적인 이름과 순서로 만든다."""
    variants = []
    if include_legacy:
        variants.extend(
            PolicyVariant(
                name=f"legacy_s{stop_limit}",
                max_stops_per_route=stop_limit,
                policy_config=LEGACY_REBALANCE_POLICY,
            )
            for stop_limit in max_stops
        )
    for horizon, ratio, uncertainty, fraction, cooldown, stop_limit in product(
        protection_hours,
        minimum_stock_ratios,
        uncertainty_values,
        max_pickup_stock_fractions,
        pickup_cooldown_minutes,
        max_stops,
    ):
        config = risk_band_policy(
            protection_horizon_hours=horizon,
            minimum_stock_ratio=ratio,
            uncertainty_z=uncertainty,
            max_pickup_stock_fraction=fraction,
            pickup_cooldown_minutes=cooldown,
        )
        variants.append(
            PolicyVariant(
                name=(
                    f"risk_h{horizon}_r{round(ratio * 100):02d}_"
                    f"z{round(uncertainty * 1000):04d}_"
                    f"f{round(fraction * 100):03d}_"
                    f"cd{cooldown:03d}_s{stop_limit}"
                ),
                max_stops_per_route=stop_limit,
                policy_config=config,
            )
        )
    result = tuple(variants)
    if len(result) > 64:
        raise ValueError(
            "한 search의 정책 후보는 계산 안전을 위해 64개 이하여야 합니다."
        )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """정책 grid와 held-out 날짜·운영 조건을 파싱한다."""
    parser = argparse.ArgumentParser(description="재배치 위험 구간 정책 search")
    parser.add_argument("--dates", nargs="+", type=date.fromisoformat, required=True)
    parser.add_argument("--center", default="hangnyeoul")
    parser.add_argument("--start-hour", type=int, default=6)
    parser.add_argument("--evaluation-minutes", nargs="+", type=int, default=[180])
    parser.add_argument("--fleet-size", type=int, default=3)
    parser.add_argument("--protection-hours", nargs="+", type=int, default=[2, 3])
    parser.add_argument(
        "--minimum-stock-ratios",
        nargs="+",
        type=float,
        default=[0.2, 0.3],
    )
    parser.add_argument(
        "--uncertainty-z",
        nargs="+",
        type=float,
        default=[0.0, 1.282],
    )
    parser.add_argument("--max-stops", nargs="+", type=int, default=[5])
    parser.add_argument(
        "--max-pickup-stock-fractions",
        nargs="+",
        type=float,
        default=[1.0],
    )
    parser.add_argument(
        "--pickup-cooldown-minutes",
        nargs="+",
        type=int,
        default=[0],
    )
    parser.add_argument("--include-legacy", action="store_true")
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
    parser.add_argument(
        "--s3-endpoint",
        default=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
    )
    parser.add_argument("--s3-bucket", default="issue163-full-year")
    parser.add_argument(
        "--access-key",
        default=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
    )
    parser.add_argument(
        "--secret-key",
        default=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("../data/policy-search"),
    )
    args = parser.parse_args(argv)
    if any(target.year != 2025 or target.day != 17 for target in args.dates):
        parser.error("모든 날짜는 고정 모델의 2025년 held-out 17일이어야 합니다.")
    if len(args.dates) != len(set(args.dates)):
        parser.error("--dates에 중복 날짜가 있습니다.")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """날짜별 후보를 실행하고 같은 모델·계약 결과만 검증해 집계한다."""
    args = parse_args(argv)
    variants = build_policy_variants(
        protection_hours=tuple(dict.fromkeys(args.protection_hours)),
        minimum_stock_ratios=tuple(dict.fromkeys(args.minimum_stock_ratios)),
        uncertainty_values=tuple(dict.fromkeys(args.uncertainty_z)),
        max_pickup_stock_fractions=tuple(
            dict.fromkeys(args.max_pickup_stock_fractions)
        ),
        pickup_cooldown_minutes=tuple(dict.fromkeys(args.pickup_cooldown_minutes)),
        max_stops=tuple(dict.fromkeys(args.max_stops)),
        include_legacy=args.include_legacy,
    )
    print(f"정책 후보 {len(variants)}개", flush=True)
    documents = []
    for index, target in enumerate(args.dates, start=1):
        month = f"25{target.month:02d}"
        print(f"[{index}/{len(args.dates)}] {target.isoformat()} 시작", flush=True)
        result = run_policy_backtest(
            target_date=target,
            center_id=args.center,
            start_hour=args.start_hour,
            evaluation_minutes=tuple(args.evaluation_minutes),
            fleet_size=args.fleet_size,
            max_stops_variants=tuple(args.max_stops),
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
            policy_variants=variants,
        )
        json_path, _ = write_result(result, args.output_dir)
        documents.append(json.loads(json_path.read_text(encoding="utf-8")))
        print(f"[{index}/{len(args.dates)}] {target.isoformat()} 완료", flush=True)
    aggregate = aggregate_results(documents)
    date_stem = f"{min(args.dates)}_{max(args.dates)}"
    json_path = args.output_dir / f"{date_stem}-risk-search.json"
    markdown_path = args.output_dir / f"{date_stem}-risk-search.md"
    write_aggregate(
        aggregate,
        json_path=json_path,
        markdown_path=markdown_path,
    )
    print(f"Search JSON: {json_path}")
    print(f"Search Markdown: {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
