"""데이터 수집 알림(daily_collection_report/hourly_collection_alert)의 위험 판단
기준을 alert_policy.yaml에서 읽는다. 실제 값 관리는 그 YAML 파일에서만 한다.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_POLICY_PATH = Path(__file__).resolve().parent / "alert_policy.yaml"


def load_thresholds(source_id: str) -> dict[str, float]:
    """소스별 위험 기준을 읽는다. sources: 아래 override가 없으면 default를 그대로 쓴다."""
    policy = yaml.safe_load(_POLICY_PATH.read_text())
    override = policy["sources"].get(source_id) or {}
    return {**policy["default"], **override}
