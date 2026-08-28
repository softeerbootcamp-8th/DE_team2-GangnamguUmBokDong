"""Calibration·confirmatory·production을 한 profile 기반 CLI로 실행한다."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY

from .evaluation_profiles import (
    PROFILE_NAMES,
    EvaluationProfile,
    get_evaluation_profile,
)
from .profile_evaluation import (
    evaluate_profile,
    load_backtest_result,
    write_evaluation_result,
)
from .run_policy_backtest import PolicyVariant, run_policy_backtest


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """단일 평가 CLI의 profile·입력·출력 인자를 파싱한다."""
    parser = argparse.ArgumentParser(
        description="재배치 정책의 profile 기반 단일 평가 실행기"
    )
    parser.add_argument("--profile", choices=PROFILE_NAMES, required=True)
    parser.add_argument(
        "--input-results",
        nargs="+",
        type=Path,
        help="기존 raw 결과를 재집계할 때만 지정한다. 과거 envelope도 읽을 수 있다.",
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
    parser.add_argument(
        "--s3-endpoint",
        default=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5433/app",
        ),
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
        required=True,
        type=Path,
    )
    return parser.parse_args(argv)


def run_profile_backtests(
    profile: EvaluationProfile,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    """Profile의 exact 셀을 공통 point-in-time 엔진으로 순차 실행한다."""
    documents = []
    variant = PolicyVariant(
        name=profile.policy_name,
        policy_config=DEFAULT_REBALANCE_POLICY,
    )
    for index, cell in enumerate(profile.cells, start=1):
        month = f"{cell.target_date.year % 100:02d}{cell.target_date.month:02d}"
        print(
            f"[{index}/{len(profile.cells)}] {cell.key} 시작",
            flush=True,
        )
        result = run_policy_backtest(
            target_date=cell.target_date,
            center_id=cell.center_id,
            start_hour=cell.start_hour,
            evaluation_minutes=profile.evaluation_minutes,
            fleet_size=profile.fleet_size,
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
            database_url=args.database_url,
            policy_variants=(variant,),
        )
        documents.append(asdict(result))
        print(
            f"[{index}/{len(profile.cells)}] {cell.key} 완료",
            flush=True,
        )
    return documents


def evaluate_documents(
    profile: EvaluationProfile,
    documents: Sequence[Mapping[str, Any]],
    output_dir: Path,
) -> tuple[dict[str, Any], Path, Path]:
    """Raw 결과를 공통 schema로 판정하고 JSON·Markdown을 저장한다."""
    result = evaluate_profile(profile, documents)
    json_path, markdown_path = write_evaluation_result(result, output_dir)
    return result, json_path, markdown_path


def main(argv: Sequence[str] | None = None) -> int:
    """선택 profile을 실행하거나 기존 raw를 읽어 같은 gate로 판정한다."""
    args = parse_args(argv)
    profile = get_evaluation_profile(args.profile)
    documents = (
        [load_backtest_result(path) for path in args.input_results]
        if args.input_results
        else run_profile_backtests(profile, args)
    )
    result, json_path, markdown_path = evaluate_documents(
        profile,
        documents,
        args.output_dir,
    )
    print(f"Evaluation JSON: {json_path}")
    print(f"Evaluation Markdown: {markdown_path}")
    if result["acceptance_gate"]["passed"]:
        print(f"Evaluation gate: PASSED ({profile.name})")
        return 0
    print(f"Evaluation gate: FAILED ({profile.name})")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
