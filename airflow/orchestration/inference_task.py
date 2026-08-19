"""ml/inference CLI(predict_single.py --all-stations)를 실행하는 태스크 빌더.

실제 운영 진입점은 predict_single.py다 — predict_rental_demand.py/
predict_return_demand.py는 2025년 고정 테스트 구간만 읽는 백테스트 CLI이고,
run_full_pipeline.py는 로컬 데모 전용이라고 자체 docstring에 명시되어 있다.

호출 방식은 ml/inference/README.md 패턴을 개선하여 `uv run`을 직접 사용한다:
`cd ml && uv --project inference run python -m inference.predict_single ...`.
`inference` 패키지가 `ml/inference/`에 있어 `-m inference.predict_single`이
resolve되려면 cwd가 `ml/`이어야 하고, 환경은 `inference` 프로젝트의 것을 써야 하므로
`uv --project inference run` 명령어로 실행한다. (로컬 Mac과 Docker(Linux) 환경을
오갈 때, 심볼릭 링크 등 가상환경이 깨져도 uv가 자동으로 감지해 복구해 준다.)
"""

from __future__ import annotations

from airflow.task.trigger_rule import TriggerRule

from config.schedules import INFERENCE_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_DATE, KST_HOUR, KST_MINUTE

ML_DIR = str(REPO_ROOT / "ml")


def build_inference_task(dag, *, trigger_rule: str = TriggerRule.ALL_SUCCESS):
    """`trigger_rule`은 realtime_5min.py가 normalizer 브랜치(strict/fallback) 뒤에
    붙일 때 `NONE_FAILED_MIN_ONE_SUCCESS`로 덮어쓴다 — `run_normalizer_fallback`이
    보통(strict 성공 시) SKIPPED로 끝나는데, 기본값 ALL_SUCCESS는 upstream이
    SKIPPED면 이 태스크도 그대로 SKIPPED로 전파시켜 버려서 정상 경로에서 추론이
    거의 항상 안 도는 사고가 난다(`realtime_5min.py` 모듈 docstring 참고).
    """
    cmd = (
        "uv --project inference run python -m inference.predict_single "
        f"--all-stations --date {KST_DATE} --hour {KST_HOUR} --minute {KST_MINUTE}"
    )
    return build_module_task(
        dag,
        "run_inference",
        ML_DIR,
        cmd,
        execution_timeout=INFERENCE_EXECUTION_TIMEOUT,
        trigger_rule=trigger_rule,
    )
