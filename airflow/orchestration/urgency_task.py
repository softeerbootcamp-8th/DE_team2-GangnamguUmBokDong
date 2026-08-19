"""rebalance CLI(main.py)를 실행해 urgency_score를 계산하는 태스크 빌더.

rebalance는 loader/nowcaster/normalizer와 같은 flat 레이아웃 프로젝트라 그
태스크 빌더들과 동일하게 `uv run --frozen python main.py ...`로 호출한다
(ml/inference처럼 패키지 레이아웃이 아니라 `-m` 실행이 필요 없다). run_inference
뒤에 실행하지만 station_stock/forecast_points RDS 적재(load_station_stock,
load_forecast_points)를 기다리지 않는다 — S3(재고 이력·예측 결과)만 읽고 RDS는
건드리지 않기 때문이다.
"""

from __future__ import annotations

from config.schedules import URGENCY_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_DATE, KST_HOUR, KST_MINUTE

REBALANCE_DIR = str(REPO_ROOT / "rebalance")


def build_urgency_task(dag):
    cmd = f"uv run --frozen python main.py --date {KST_DATE} --hour {KST_HOUR} --minute {KST_MINUTE}"
    return build_module_task(
        dag,
        "compute_urgency",
        REBALANCE_DIR,
        cmd,
        execution_timeout=URGENCY_EXECUTION_TIMEOUT,
    )
