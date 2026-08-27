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
    drop_rate: float
    is_risky: bool


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def evaluate_source_stats(source_id: str, stats: dict) -> SourceStatEvaluation:
    """소스 하나의 통계를 alert_policy.yaml 기준과 비교해 위험 여부를 판정한다.

    전부 행(row) 단위다 — 실행(run) 중 몇 번이 실패했는지, 가져온 행 중 몇 행이
    버려졌는지만 본다. 컬럼 값 단위 결측·이상치 비율은 쓰지 않는다: 소스마다
    "정상 기준선"이 크게 달라(예: 비회원 대여의 성별·생년 컬럼은 평소에도 결측이
    30%에 가깝다) 일률적인 임계값을 적용하면 노이즈가 컸다(2026-08-27 실측,
    bike_rental_history).
    """
    thresholds = load_thresholds(source_id)
    run_count = stats["run_count"]
    failed = stats["status_counts"].get("failed", 0)
    fetched = stats["fetched_count"]
    dropped = stats["dropped_count"]

    failure_rate = _ratio(failed, run_count)
    drop_rate = _ratio(dropped, fetched)

    # run_count == 0이면 failure_rate도 0/0 → 0.0으로 계산돼 임계값 비교만으로는
    # "정상"과 구분이 안 된다 — 그런데 이건 collector/Airflow가 그 기간 내내
    # 완전히 멈춰 manifest가 하나도 안 남은, 비율 임계값보다 심한 장애다.
    # run_count == 0 자체를 별도 위험 조건으로 명시한다.
    no_runs = run_count == 0
    is_risky = (
        no_runs
        or failure_rate >= thresholds["failure_rate_threshold"]
        or drop_rate >= thresholds["drop_rate_threshold"]
    )
    return {
        "source_id": source_id,
        "stats": stats,
        "failure_rate": failure_rate,
        "drop_rate": drop_rate,
        "is_risky": is_risky,
    }


_COLUMNS = ("STATUS", "SOURCE", "RUN", "SUCC", "FAIL", "PART", "KEEP", "DROP")
_STATUS_OK = "OK"
_STATUS_RISK = "RISK"
_STATUS_WIDTH = max(len(_STATUS_OK), len(_STATUS_RISK), len(_COLUMNS[0]))


def _table(evaluations: list[SourceStatEvaluation]) -> str:
    """소스별 통계를 Slack 코드 블록 안에 넣을 고정폭 표로 만든다.

    RUN=실행(수집 시도) 횟수, SUCC/FAIL/PART=그중 성공/실패/부분성공 횟수(행 개수가
    아니라 실행 횟수다 — "OK"라고 하면 유효 행 수처럼 오해하기 쉬워 SUCC로 명확히
    한다), KEEP=유효해서 살린 행, DROP=검증에 걸려 버린 행. 한글/이모지는 폰트마다
    폭이 달라 고정폭 정렬이 어긋나므로 표 안은 전부 ASCII로 채운다. STATUS 컬럼(소스
    전체 정상/위험 플래그)은 빈칸이 아니라 "OK"/"RISK" 텍스트로 명시해 한쪽이 안
    보이는 일이 없게 한다.
    """
    name_width = max(
        (len(e["source_id"]) for e in evaluations), default=len(_COLUMNS[1])
    )
    name_width = max(name_width, len(_COLUMNS[1]))
    header = (
        f"{_COLUMNS[0]:<{_STATUS_WIDTH}}  {_COLUMNS[1]:<{name_width}}  "
        f"{_COLUMNS[2]:>5} {_COLUMNS[3]:>5} {_COLUMNS[4]:>5} {_COLUMNS[5]:>5} "
        f"{_COLUMNS[6]:>9} {_COLUMNS[7]:>9}"
    )
    rows = [header, "-" * len(header)]
    for e in evaluations:
        stats = e["stats"]
        status_counts = stats["status_counts"]
        status = _STATUS_RISK if e["is_risky"] else _STATUS_OK
        rows.append(
            f"{status:<{_STATUS_WIDTH}}  {e['source_id']:<{name_width}}  "
            f"{stats['run_count']:>5} {status_counts.get('succeeded', 0):>5} "
            f"{status_counts.get('failed', 0):>5} {status_counts.get('partial', 0):>5} "
            f"{stats['kept_count']:>9} {stats['dropped_count']:>9}"
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
