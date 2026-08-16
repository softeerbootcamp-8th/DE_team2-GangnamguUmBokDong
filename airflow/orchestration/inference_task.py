"""ml/inference CLI(predict_single.py --all-stations)를 실행하는 태스크 빌더.

실제 운영 진입점은 predict_single.py다 — predict_rental_demand.py/
predict_return_demand.py는 2025년 고정 테스트 구간만 읽는 백테스트 CLI이고,
run_full_pipeline.py는 로컬 데모 전용이라고 자체 docstring에 명시되어 있다.

호출 방식은 ml/inference/README.md가 실제로 검증해 보여주는 패턴을 그대로 따른다:
`cd ml && ./inference/.venv/bin/python -m inference.predict_single ...`.
`inference` 패키지가 `ml/inference/`에 있어 `-m inference.predict_single`이
resolve되려면 cwd가 `ml/`이어야 하고, venv는 `ml/inference/`의 것이므로 `uv run
--project`가 아니라 venv 바이너리를 직접 호출한다(README에서 검증된 방식).
"""

from __future__ import annotations

from config.schedules import INFERENCE_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_DATE, KST_HOUR, KST_MINUTE

ML_DIR = str(REPO_ROOT / "ml")


def build_inference_task(dag):
    cmd = (
        "./inference/.venv/bin/python -m inference.predict_single "
        f"--all-stations --date {KST_DATE} --hour {KST_HOUR} --minute {KST_MINUTE}"
    )
    return build_module_task(
        dag,
        "run_inference",
        ML_DIR,
        cmd,
        execution_timeout=INFERENCE_EXECUTION_TIMEOUT,
    )
