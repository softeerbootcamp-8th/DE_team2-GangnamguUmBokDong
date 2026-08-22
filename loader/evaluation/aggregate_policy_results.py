"""여러 held-out 날짜의 정책 백테스트를 같은 계약으로 검증하고 집계한다."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def aggregate_results(documents: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """계약과 모델이 같은 날짜별 결과를 micro-average 지표로 집계한다."""
    if not documents:
        raise ValueError("집계할 결과가 없습니다.")
    model_hashes = {document["model_bundle_sha256"] for document in documents}
    if len(model_hashes) != 1:
        raise ValueError("서로 다른 모델 bundle 결과를 섞을 수 없습니다.")
    dates = [document["target_date"] for document in documents]
    if len(dates) != len(set(dates)):
        raise ValueError("집계 결과에 중복 날짜가 있습니다.")
    for document in documents:
        gate = document["evidence_gate"]
        if (
            not gate["point_in_time_feature_inputs"]
            or not gate["operation_contract_passed"]
            or not gate["legacy_endpoint_reconciliation_passed"]
            or not gate["heldout_day_of_month"]
        ):
            raise ValueError(f"point-in-time held-out gate를 통과하지 못했습니다: {document['target_date']}")
    _validate_contract_parity(documents)

    accumulators: dict[tuple[int, str], dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    legacy_accumulators: dict[int, dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    per_date: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for document in documents:
        for duration in document["durations"]:
            minutes = int(duration["evaluation_minutes"])
            legacy = duration["legacy_movement"]
            legacy_timing = duration["legacy_timing"]
            legacy_values = [row["empty_station_minutes"] for row in legacy_timing]
            legacy_negative = [row["negative_station_minutes"] for row in legacy_timing]
            legacy_accumulators[minutes]["movement_budget"] += legacy[
                "balanced_movement_budget"
            ]
            legacy_accumulators[minutes]["added_bikes"] += legacy["added_bikes"]
            legacy_accumulators[minutes]["removed_bikes"] += legacy["removed_bikes"]
            legacy_accumulators[minutes]["empty_min"] += min(legacy_values)
            legacy_accumulators[minutes]["empty_max"] += max(legacy_values)
            legacy_accumulators[minutes]["negative_min"] += min(legacy_negative)
            legacy_accumulators[minutes]["negative_max"] += max(legacy_negative)
            legacy_accumulators[minutes]["endpoint_max_error"] = max(
                legacy_accumulators[minutes]["endpoint_max_error"],
                max(row["endpoint_max_absolute_error"] for row in legacy_timing),
            )
            policies = [duration["no_rebalance"], *duration["model_policies"]]
            baseline = duration["no_rebalance"]
            for policy in policies:
                key = (minutes, policy["policy"])
                accumulator = accumulators[key]
                for name in (
                    "observed_requests",
                    "fulfilled_requests",
                    "unfulfilled_requests",
                    "empty_station_minutes",
                    "moved_bikes",
                    "dispatched_routes",
                    "vehicle_busy_minutes",
                    "planned_bikes",
                    "movement_budget_used",
                ):
                    accumulator[name] += float(policy[name])
                per_date[key].append(
                    {
                        "date": document["target_date"],
                        "fulfillment_delta": (
                            policy["observed_demand_fulfillment_rate"]
                            - baseline["observed_demand_fulfillment_rate"]
                        ),
                        "unfulfilled_delta": (
                            policy["unfulfilled_requests"]
                            - baseline["unfulfilled_requests"]
                        ),
                        "empty_station_minutes_delta": (
                            policy["empty_station_minutes"]
                            - baseline["empty_station_minutes"]
                        ),
                    }
                )
    legacy_summaries = [
        {
            "evaluation_minutes": minutes,
            "balanced_movement_budget": int(values["movement_budget"]),
            "added_bikes": int(values["added_bikes"]),
            "removed_bikes": int(values["removed_bikes"]),
            "empty_station_minutes_min": round(values["empty_min"], 3),
            "empty_station_minutes_max": round(values["empty_max"], 3),
            "negative_station_minutes_min": round(values["negative_min"], 3),
            "negative_station_minutes_max": round(values["negative_max"], 3),
            "endpoint_max_absolute_error": int(values["endpoint_max_error"]),
        }
        for minutes, values in sorted(legacy_accumulators.items())
    ]
    legacy_by_minutes = {
        row["evaluation_minutes"]: row for row in legacy_summaries
    }
    rows = []
    for (minutes, policy), values in sorted(accumulators.items()):
        baseline = accumulators[(minutes, "no_rebalance")]
        requests = int(values["observed_requests"])
        fulfilled = int(values["fulfilled_requests"])
        comparisons = per_date[(minutes, policy)]
        legacy = legacy_by_minutes[minutes]
        legacy_low = legacy["empty_station_minutes_min"]
        legacy_high = legacy["empty_station_minutes_max"]
        rows.append(
            {
                "evaluation_minutes": minutes,
                "policy": policy,
                "date_count": len(comparisons),
                "observed_requests": requests,
                "fulfilled_requests": fulfilled,
                "unfulfilled_requests": int(values["unfulfilled_requests"]),
                "observed_demand_fulfillment_rate": (
                    fulfilled / requests if requests else 1.0
                ),
                "empty_station_minutes": round(values["empty_station_minutes"], 3),
                "empty_station_minutes_change_vs_no_rebalance_pct": (
                    None
                    if policy == "no_rebalance" or baseline["empty_station_minutes"] == 0
                    else round(
                        (values["empty_station_minutes"] - baseline["empty_station_minutes"])
                        / baseline["empty_station_minutes"]
                        * 100.0,
                        3,
                    )
                ),
                "unfulfilled_change_vs_no_rebalance": (
                    int(values["unfulfilled_requests"] - baseline["unfulfilled_requests"])
                ),
                "dates_fulfillment_better": sum(
                    row["fulfillment_delta"] > 1e-12 for row in comparisons
                ),
                "dates_fulfillment_equal": sum(
                    abs(row["fulfillment_delta"]) <= 1e-12 for row in comparisons
                ),
                "dates_fulfillment_worse": sum(
                    row["fulfillment_delta"] < -1e-12 for row in comparisons
                ),
                "moved_bikes": int(values["moved_bikes"]),
                "dispatched_routes": int(values["dispatched_routes"]),
                "vehicle_busy_minutes": round(values["vehicle_busy_minutes"], 3),
                "planned_bikes": int(values["planned_bikes"]),
                "movement_budget_used": int(values["movement_budget_used"]),
                "legacy_movement_budget_cap": legacy["balanced_movement_budget"],
                "empty_change_vs_legacy_timing_range_pct": (
                    None
                    if policy == "no_rebalance" or legacy_low == 0 or legacy_high == 0
                    else [
                        round(
                            (values["empty_station_minutes"] - legacy_high)
                            / legacy_high
                            * 100.0,
                            3,
                        ),
                        round(
                            (values["empty_station_minutes"] - legacy_low)
                            / legacy_low
                            * 100.0,
                            3,
                        ),
                    ]
                ),
                "per_date_comparison": comparisons,
            }
        )
    return {
        "schema_version": "point-in-time-policy-suite-v1",
        "dates": sorted(dates),
        "model_bundle_sha256": next(iter(model_hashes)),
        "result_count": len(documents),
        "publication_grade_system_claim_allowed": False,
        "legacy_summaries": legacy_summaries,
        "rows": rows,
    }


def _validate_contract_parity(documents: Sequence[dict[str, Any]]) -> None:
    """날짜 외 모든 운영 계약이 같은 결과만 집계하도록 강제한다."""
    reference = documents[0]["contracts"]
    for document in documents[1:]:
        candidate = document["contracts"]
        if len(candidate) != len(reference):
            raise ValueError("날짜별 평가 구간 수가 다릅니다.")
        for left, right in zip(reference, candidate, strict=True):
            left_contract = dict(left["contract"])
            right_contract = dict(right["contract"])
            left_contract.pop("target_date", None)
            right_contract.pop("target_date", None)
            if left_contract != right_contract:
                raise ValueError(
                    f"날짜 외 운영 계약이 다릅니다: {document['target_date']}"
                )


def aggregate_markdown(result: dict[str, Any]) -> str:
    """집계 결과를 날짜별 악화 횟수까지 보이는 Markdown 표로 만든다."""
    lines = [
        "# 재배치 정책 held-out 날짜 집계",
        "",
        f"- 날짜: {', '.join(result['dates'])}",
        f"- 모델 SHA-256: `{result['model_bundle_sha256']}`",
        "- 발표용 인과 주장 허용: **False**",
        "",
        "## 추정 기존 운영",
        "",
        "| 구간 | 균형 이동 예산 | 품절 대여소-분 범위 | 음수 재고 대여소-분 범위 | 종료 재고 최대 오차 |",
        "|---:|---:|---:|---:|---:|",
    ]
    for legacy in result["legacy_summaries"]:
        lines.append(
            f"| {legacy['evaluation_minutes']}분 | "
            f"{legacy['balanced_movement_budget']} | "
            f"{legacy['empty_station_minutes_min']:.1f}~"
            f"{legacy['empty_station_minutes_max']:.1f} | "
            f"{legacy['negative_station_minutes_min']:.1f}~"
            f"{legacy['negative_station_minutes_max']:.1f} | "
            f"{legacy['endpoint_max_absolute_error']} |"
        )
    lines.extend(
        (
            "",
            "## 정책 결과",
            "",
        "| 구간 | 정책 | 요청 | 충족률 | 미충족 변화 | 품절 대여소-분 변화 | 날짜별 충족률 개선/동일/악화 | 이동 | 차량 분 |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
        )
    )
    for row in result["rows"]:
        empty_change = row["empty_station_minutes_change_vs_no_rebalance_pct"]
        empty_text = "기준" if empty_change is None else f"{empty_change:+.2f}%"
        lines.append(
            f"| {row['evaluation_minutes']}분 | {row['policy']} | "
            f"{row['observed_requests']:,} | "
            f"{row['observed_demand_fulfillment_rate']:.4%} | "
            f"{row['unfulfilled_change_vs_no_rebalance']:+d} | {empty_text} | "
            f"{row['dates_fulfillment_better']}/"
            f"{row['dates_fulfillment_equal']}/"
            f"{row['dates_fulfillment_worse']} | "
            f"{row['moved_bikes']} | {row['vehicle_busy_minutes']:.1f} |"
        )
    lines.extend(
        (
            "",
            "> 여러 held-out 날짜 집계도 관측 성공 수요 replay다. 실패 수요와 기존 운영 "
            "작업 로그가 없으므로 실제 운영 대비 인과적 개선율로 인용하면 안 된다.",
            "",
        )
    )
    return "\n".join(lines)


def write_aggregate(
    result: dict[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> None:
    """집계 JSON과 Markdown을 저장한다."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(aggregate_markdown(result), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """날짜별 결과 파일과 출력 경로를 파싱한다."""
    parser = argparse.ArgumentParser(description="point-in-time 백테스트 결과 집계")
    parser.add_argument("--inputs", nargs="+", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """날짜별 JSON을 검증·집계하고 출력한다."""
    args = parse_args(argv)
    documents = [json.loads(path.read_text(encoding="utf-8")) for path in args.inputs]
    result = aggregate_results(documents)
    write_aggregate(
        result,
        json_path=args.output_json,
        markdown_path=args.output_markdown,
    )
    print(aggregate_markdown(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
