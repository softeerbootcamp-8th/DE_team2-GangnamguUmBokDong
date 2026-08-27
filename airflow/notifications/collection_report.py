"""수집 통계(collector --report-window-stats의 JSON)를 사람이 읽을 Slack 메시지로
바꾸는 순수 함수들. Airflow에 의존하지 않아 두 DAG(daily/hourly)와 테스트가 그대로
재사용한다.
"""

from __future__ import annotations

from typing import TypedDict

from config.alert_policy import load_thresholds
from notifications.slack import de2_group_mention


class SourceStatEvaluation(TypedDict):
    source_id: str
    stats: dict
    failure_rate: float
    missing_ratio: float
    outlier_ratio: float
    is_risky: bool


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def evaluate_source_stats(source_id: str, stats: dict) -> SourceStatEvaluation:
    """소스 하나의 통계를 alert_policy.yaml 기준과 비교해 위험 여부를 판정한다."""
    thresholds = load_thresholds(source_id)
    run_count = stats["run_count"]
    failed = stats["status_counts"].get("failed", 0)
    kept = stats["kept_count"]

    failure_rate = _ratio(failed, run_count)
    missing_ratio = _ratio(stats["missing_count"], kept)
    outlier_ratio = _ratio(stats["outlier_count"], kept)

    # run_count == 0이면 모든 비율이 0/0 → 0.0으로 계산돼 임계값 비교만으로는
    # "정상"과 구분이 안 된다 — 그런데 이건 collector/Airflow가 그 기간 내내
    # 완전히 멈춰 manifest가 하나도 안 남은, 비율 임계값보다 심한 장애다.
    # run_count == 0 자체를 별도 위험 조건으로 명시한다.
    no_runs = run_count == 0
    is_risky = (
        no_runs
        or failure_rate >= thresholds["failure_rate_threshold"]
        or missing_ratio >= thresholds["missing_ratio_threshold"]
        or outlier_ratio >= thresholds["outlier_ratio_threshold"]
    )
    return {
        "source_id": source_id,
        "stats": stats,
        "failure_rate": failure_rate,
        "missing_ratio": missing_ratio,
        "outlier_ratio": outlier_ratio,
        "is_risky": is_risky,
    }


_COLUMNS = ("STATUS", "SOURCE", "OK", "FAIL", "PART", "MISS", "OUT")
_STATUS_OK = "OK"
_STATUS_RISK = "RISK"
_STATUS_WIDTH = max(len(_STATUS_OK), len(_STATUS_RISK), len(_COLUMNS[0]))


def _table(evaluations: list[SourceStatEvaluation]) -> str:
    """소스별 통계를 Slack 코드 블록 안에 넣을 고정폭 표로 만든다.

    한글/이모지는 폰트마다 폭이 달라 고정폭 정렬이 어긋나므로, 표 안은 전부
    ASCII로 채운다. 정상/위험 둘 다 빈칸이 아니라 "OK"/"RISK" 텍스트로 명시해
    한쪽이 안 보이는 일이 없게 한다.
    """
    name_width = max(
        (len(e["source_id"]) for e in evaluations), default=len(_COLUMNS[1])
    )
    name_width = max(name_width, len(_COLUMNS[1]))
    header = (
        f"{_COLUMNS[0]:<{_STATUS_WIDTH}}  {_COLUMNS[1]:<{name_width}}  "
        f"{_COLUMNS[2]:>4} {_COLUMNS[3]:>4} {_COLUMNS[4]:>4} {_COLUMNS[5]:>5} {_COLUMNS[6]:>5}"
    )
    rows = [header, "-" * len(header)]
    for e in evaluations:
        stats = e["stats"]
        status_counts = stats["status_counts"]
        status = _STATUS_RISK if e["is_risky"] else _STATUS_OK
        rows.append(
            f"{status:<{_STATUS_WIDTH}}  {e['source_id']:<{name_width}}  "
            f"{status_counts.get('succeeded', 0):>4} {status_counts.get('failed', 0):>4} "
            f"{status_counts.get('partial', 0):>4} {stats['missing_count']:>5} "
            f"{stats['outlier_count']:>5}"
        )
    return "\n".join(rows)


def build_daily_report_message(day: str, evaluations: list[SourceStatEvaluation]) -> str:
    """일별 전체 소스 리포트 메시지를 만든다. 위험 소스가 있으면 맨 아래 @de2조를 태그한다."""
    lines = [f"*데이터 수집 일일 리포트 ({day})*", "```", _table(evaluations), "```"]

    risky = [e for e in evaluations if e["is_risky"]]
    if risky:
        names = ", ".join(f"`{e['source_id']}`" for e in risky)
        lines.append(f"{de2_group_mention()} 위험 수치를 초과한 소스가 있습니다: {names}")
    return "\n".join(lines)


def build_hourly_alert_message(
    hour_label: str, risky_evaluations: list[SourceStatEvaluation]
) -> str | None:
    """시간별 이상 감지 메시지를 만든다. 위험 소스가 없으면 None(전송하지 않음)."""
    if not risky_evaluations:
        return None

    lines = [
        f"{de2_group_mention()} *데이터 수집 이상 감지 ({hour_label})*",
        "```",
        _table(risky_evaluations),
        "```",
    ]
    return "\n".join(lines)
