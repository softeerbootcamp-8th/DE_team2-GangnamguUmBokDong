"""rebalance CLI(routes_main.py)를 실행해 재배치 라우트를 계산하는 태스크 빌더.

urgency_task.py와 같은 이유로 `uv run --frozen python routes_main.py ...`로
호출한다. compute_urgency 뒤에 실행한다 — urgency_score/bike_qty를 다시 계산해야
하고(별도 프로세스라 결과 공유 불가), dispatched 넷팅을 위한 좁은 RDS 조회
하나를 제외하면 나머지는 여전히 S3만 읽는다.
"""

from __future__ import annotations

from config.schedules import ROUTES_EXECUTION_TIMEOUT
from orchestration.task_builder import build_module_task
from orchestration.templates import KST_DATE, KST_HOUR, KST_MINUTE
from orchestration.urgency_task import REBALANCE_DIR


def build_routes_task(dag):
    cmd = f"uv run --frozen python routes_main.py --date {KST_DATE} --hour {KST_HOUR} --minute {KST_MINUTE}"
    return build_module_task(
        dag,
        "compute_routes",
        REBALANCE_DIR,
        cmd,
        execution_timeout=ROUTES_EXECUTION_TIMEOUT,
    )
