"""Slack Incoming Webhook으로 메시지를 보낸다.

이 저장소는 Airflow Variable/Connection을 쓰지 않고 모든 시크릿을 환경변수로만
관리한다(SEOUL_OPENAPI_KEY·KMA_APIHUB_KEY와 동일 패턴, 운영에서는
ops/deploy/render_env.sh가 내려받는 S3 config/secrets.env에서 채워진다). Webhook
URL도 같은 방식을 따른다. 별도 HTTP 클라이언트 의존성을 늘리지 않기 위해 표준
라이브러리 urllib만 사용한다.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)

_WEBHOOK_ENV = "SLACK_WEBHOOK_URL"
_DE2_GROUP_ENV = "SLACK_DE2_GROUP_ID"
_REQUEST_TIMEOUT_SECONDS = 10


def de2_group_mention() -> str:
    """DE 2조 Slack user group 멘션 문자열을 만든다.

    `SLACK_DE2_GROUP_ID`(Slack 워크스페이스의 실제 user group ID, `S`로 시작)가
    있어야 `<!subteam^...>` 형식으로 실제 멘션(핑)이 된다 — 코드로 알아낼 수 없는
    값이라 비어 있으면 멘션 없이 "@de2조" 텍스트로만 대체한다.
    """
    group_id = os.environ.get(_DE2_GROUP_ENV, "").strip()
    return f"<!subteam^{group_id}>" if group_id else "@de2조"


def send_message(text: str) -> None:
    """Slack Incoming Webhook으로 text를 보낸다.

    `SLACK_WEBHOOK_URL`이 없으면 아직 webhook이 설정되지 않은 것으로 보고 로그만
    남기고 조용히 건너뛴다 — 이 값이 없다고 모니터링 DAG 자체가 실패해서는 안 된다.
    """
    webhook_url = os.environ.get(_WEBHOOK_ENV, "").strip()
    if not webhook_url:
        logger.warning(
            "%s가 설정되지 않아 Slack 전송을 건너뜁니다: %s", _WEBHOOK_ENV, text
        )
        return

    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
        if response.status >= 300:
            raise RuntimeError(f"Slack 전송 실패: HTTP {response.status}")
