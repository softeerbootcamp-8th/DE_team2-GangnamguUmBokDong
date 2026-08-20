"""ml/inference CLI(predict_single.py --all-stations)를 실행하는 태스크 빌더.

실제 운영 진입점은 predict_single.py다 — predict_rental_demand.py/
predict_return_demand.py는 2025년 고정 테스트 구간만 읽는 백테스트 CLI이고,
run_full_pipeline.py는 로컬 데모 전용이라고 자체 docstring에 명시되어 있다.

호출 방식은 ml/inference/README.md 패턴을 개선하여 `uv run`을 직접 사용한다:
`cd ml && uv --project inference run python -m inference.predict_single ...`.
`inference` 패키지가 `ml/inference/`에 있어 `-m inference.predict_single`이
resolve되려면 cwd가 `ml/`이어야 하고, 환경은 `inference` 프로젝트의 것을 써야 하므로
`uv --project inference run` 명령어로 실행한다. Docker에서는 host bind mount의
``ml/inference/.venv``를 쓰지 않고 전용 named volume 환경을 사용한다.
"""

from __future__ import annotations

from airflow.task.trigger_rule import TriggerRule
from config.schedules import INFERENCE_EXECUTION_TIMEOUT

from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_DATE, KST_HOUR, KST_MINUTE

ML_DIR = str(REPO_ROOT / "ml")


def build_inference_task(dag):
    """정규화까지 끝난 실시간 입력이 모두 성공하면 실행할 추론 태스크를 만든다.

    두 운영 DAG는 단일 ``run_normalizer`` 태스크를 직접 upstream으로 둔다.
    현재 tick의 정규화 실패를 우회하는 분기가 없으므로 ``ALL_SUCCESS``가 올바른
    고정 계약이다.
    """
    cmd = (
        "uv --project inference run python -m inference.predict_single "
        f"--all-stations --date {KST_DATE} --hour {KST_HOUR} --minute {KST_MINUTE} "
        "--n-hours 12"
    )
    return build_module_task(
        dag,
        "run_inference",
        ML_DIR,
        cmd,
        execution_timeout=INFERENCE_EXECUTION_TIMEOUT,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        uv_environment_name="ml-inference",
    )
