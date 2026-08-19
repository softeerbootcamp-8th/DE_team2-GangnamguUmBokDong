"""CLI 진입점: anchor 시점 기준 전체 대여소 urgency_score를 계산해 S3에 저장한다.

Airflow(compute_urgency 태스크)가 run_inference 뒤에 `--date/--hour/--minute`
(KST 벽시계 시각, ml/inference/predict_single.py의 --all-stations 호출과 동일한
값)으로 실행한다. 이후 loader가 이 결과를 읽어 station_urgency 테이블에 적재한다.
"""

from __future__ import annotations

import argparse
import sys

from core.s3 import write_parquet

import urgency
from reader import _urgency_key, anchor_timestamp


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="대여소별 재배치 긴급도(urgency_score) 배치 계산")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (KST)")
    parser.add_argument("--hour", type=int, required=True)
    parser.add_argument("--minute", type=int, required=True)
    args = parser.parse_args(argv)

    if args.minute % 5:
        parser.error("--minute must be aligned to a 5-minute tick")

    anchor = anchor_timestamp(args.date, args.hour, args.minute)
    result = urgency.compute_all(anchor)

    out_path = _urgency_key(anchor)
    write_parquet(result, out_path)
    print(f"urgency 결과 저장: {out_path} ({len(result)}개 대여소)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
