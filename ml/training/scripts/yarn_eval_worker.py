"""YARN Distributed Shell 컨테이너의 진입점 — 분산 평가 워커.

`monitor_performance.evaluate_recent_performance()`를 여러 컨테이너에 나눠
돌린다. 학습 분산(`yarn_worker_bootstrap.py`, ADR-0007)과 달리 워커끼리 소켓
통신을 할 필요가 없는 embarrassingly parallel 작업이다 — poisson_deviance/
RMSE/coverage가 전부 "행 단위 값의 평균"이라(`monitor_performance.
combine_evaluation_shards()` 참고) 부분합(행 수 + 합계)만 있으면 나중에
합쳐도 근사가 아니라 전체를 한 번에 계산한 것과 수학적으로 완전히 같다.

그럼에도 barrier(S3 자기등록 + 전원 등록 대기)는 `yarn_worker_bootstrap.py`와
같은 방식으로 재사용한다 — 이 워커가 실제로 필요한 건 "내가 몇 번째 조각을
맡을지"뿐이지만, `CONTAINER_ID`의 순번 형식에 기대는 새 방식을 만드는 대신
이미 실제 EMR로 검증된 메커니즘을 그대로 쓴다(중복 등록/자기 자신 못 찾음
등 fail-closed 안전장치도 그대로 얻는다). 단, 학습 barrier와 달리 서로의
host:port를 알 필요가 없어 payload는 `worker_id`만 담는다.

실행 예(YARN distributed-shell 컨테이너 안에서, `EVAL_RUN_ID`/`EVAL_MODEL`/
`EVAL_TARGET_COL`/`EVAL_EXPOSURE_COL`/`EVAL_HORIZON`/`EVAL_WINDOW_START`/
`EVAL_WINDOW_END`/`EVAL_NUM_WORKERS`는 스텝 제출 시 이미 환경에 있다고 가정):
    python -m training.scripts.yarn_eval_worker
"""

import os
import sys
import time

from core import s3 as s3_io
from ml_core.paths import TRAINING_RUNS_PREFIX, eval_shard_key

from .. import monitor_performance
from .yarn_worker_bootstrap import _discover_private_ip

_POLL_INTERVAL_SECONDS = 5
_BARRIER_TIMEOUT_SECONDS = 600.0


def _eval_barrier_key(run_id: str, model_name: str, worker_id: str) -> str:
    return f"{TRAINING_RUNS_PREFIX}/{run_id}/eval-barrier/{model_name}/{worker_id}.json"


def _register_self(run_id: str, model_name: str, worker_id: str) -> None:
    s3_io.write_json(_eval_barrier_key(run_id, model_name, worker_id), {"worker_id": worker_id})


def _poll_until_all_registered(run_id: str, model_name: str, num_workers: int, timeout_seconds: float) -> list[str]:
    """`training.scripts.yarn_worker_bootstrap._poll_until_all_registered()`와
    같은 정확히-일치 규칙을 따른다 — 이유는 그쪽 docstring 참고."""
    prefix = f"{TRAINING_RUNS_PREFIX}/{run_id}/eval-barrier/{model_name}/"
    deadline = time.monotonic() + timeout_seconds
    while True:
        keys = s3_io.list_keys(prefix)
        if len(keys) == num_workers:
            return keys
        if len(keys) > num_workers:
            raise RuntimeError(
                f"평가 barrier에 예상보다 많은 등록이 나타남: {len(keys)} > {num_workers}개 (run_id={run_id})"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(f"평가 barrier 타임아웃: {len(keys)}/{num_workers}개만 등록됨 (run_id={run_id})")
        time.sleep(_POLL_INTERVAL_SECONDS)


def _resolve_rank(keys: list[str], worker_id: str) -> int:
    """정렬된 등록 목록에서 자기 위치를 rank로 반환한다.

    학습 barrier(`_resolve_rank_and_machines()`)와 달리 "같은 worker_id 중복
    등록" 검사가 없다 — 등록 키 자체가 `{worker_id}.json`이라 같은 worker_id로
    두 번 등록하면 S3에서 같은 키를 덮어쓸 뿐이라(`_register_self()` 참고),
    `keys`에 중복이 나타나는 게 애초에 불가능하다(학습 쪽은 키가 CONTAINER_ID,
    중복 검사 대상이 host라 서로 다른 필드라 이 불변조건이 없었다).
    """
    registrations = sorted((s3_io.read_json(key) for key in keys), key=lambda r: r["worker_id"])
    for rank, registration in enumerate(registrations):
        if registration["worker_id"] == worker_id:
            return rank
    raise RuntimeError(f"barrier 등록 목록에서 자기 자신을 찾을 수 없음: worker_id={worker_id}")


def main() -> int:
    run_id = os.environ["EVAL_RUN_ID"]
    model_name = os.environ["EVAL_MODEL"]
    target_col = os.environ["EVAL_TARGET_COL"]
    exposure_col = os.environ.get("EVAL_EXPOSURE_COL") or None
    horizon = int(os.environ.get("EVAL_HORIZON", "1"))
    window_start = os.environ["EVAL_WINDOW_START"]
    window_end = os.environ["EVAL_WINDOW_END"]
    num_workers = int(os.environ["EVAL_NUM_WORKERS"])

    # host 자체는 barrier 사후 분석 외에는 안 쓰지만, 미리 조회해서 이 워커가
    # 실제로 EC2 인스턴스(IMDSv2)에서 실행되는지 조기에 확인한다 — 실패하면
    # worker_id 등록조차 없이 barrier가 타임아웃까지 조용히 기다리는 대신
    # 여기서 바로 에러가 난다.
    _discover_private_ip()
    worker_id = os.environ.get("CONTAINER_ID") or f"local-{os.getpid()}"

    _register_self(run_id, model_name, worker_id)
    keys = _poll_until_all_registered(run_id, model_name, num_workers, timeout_seconds=_BARRIER_TIMEOUT_SECONDS)
    rank = _resolve_rank(keys, worker_id)

    shard_ranges = monitor_performance._split_date_range(window_start, window_end, num_workers)
    shard = monitor_performance.evaluate_recent_performance_shard(
        model_name, target_col, exposure_col, shard_ranges[rank], horizon=horizon
    )
    s3_io.write_json(eval_shard_key(run_id, model_name, worker_id), shard)
    print(f"[yarn_eval_worker] rank={rank}/{num_workers} n_rows={shard['n_rows']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
