"""YARN Distributed Shell 컨테이너의 진입점 — 워커들이 서로를 찾아(barrier) 자기
순위(rank)를 정한 뒤 실제 학습 subprocess를 띄운다.

**왜 필요한가**: LightGBM 소켓 분산 학습(`LGB_TREE_LEARNER!="serial"`)은 모든
머신이 미리 정해진 동일한 `LGB_MACHINES="host:port,..."` 문자열을 알아야 하는데,
YARN distributed-shell은 컨테이너를 몇 대 띄워줄 뿐 컨테이너끼리 서로의 주소를
알려주는 기능이 없다(ADR-0007). 그래서 각 컨테이너가 이 스크립트로 시작해서
1) 자기 private IP를 알아내 S3에 등록하고, 2) 같은 `TRAINING_RUN_ID`를 쓰는
다른 워커들이 전부 등록할 때까지 기다린 뒤, 3) 정렬된 등록 목록에서 자기 위치를
`LGB_MACHINE_RANK`로 정하고 `LGB_MACHINES`를 조합해서, 4) 실제 학습 스크립트
(`training.train_rental_model`/`train_return_model`)를 그 환경변수로 새 프로세스로
띄운다.

**새 프로세스로 띄우는 이유(exec가 아니라 subprocess)**: `training.config`/
`ml_core.common_config`는 프로세스가 시작할 때 딱 한 번 환경변수를 읽어 모듈
전역 상수로 고정한다(`monthly_retrain_check.py`의 "프로필마다 별도 프로세스가
필요한 이유" 참고) — 이 스크립트 프로세스 안에서 `os.environ`을 바꿔봤자 이미
import된 `config` 모듈은 갱신되지 않는다. 그래서 rank/machines를 다 정한 뒤에는
반드시 새 인터프리터(`subprocess.run([sys.executable, "-m", ...])`)를 띄워야
`LGB_MACHINE_RANK`/`LGB_MACHINES`를 그 프로세스가 새로 읽는다.

실행 예(YARN distributed-shell 컨테이너 안에서, `LGB_NUM_MACHINES`/
`LGB_TREE_LEARNER`/`TRAINING_RUN_ID` 등은 스텝 제출 시 이미 환경에 있다고 가정):
    python -m training.scripts.yarn_worker_bootstrap --model rental
"""

import argparse
import os
import subprocess
import sys
import time
import urllib.request

from core import s3 as s3_io
from ml_core.paths import TRAINING_RUNS_PREFIX, training_run_worker_key

from .. import config

_TRAIN_MODULES = {"rental": "training.train_rental_model", "return": "training.train_return_model"}

_IMDS_BASE_URL = "http://169.254.169.254/latest"
_POLL_INTERVAL_SECONDS = 5


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="YARN distributed-shell 워커 barrier 후 학습 실행")
    parser.add_argument("--model", choices=sorted(_TRAIN_MODULES), required=True)
    return parser.parse_args(argv)


def _discover_private_ip() -> str:
    """이 EC2 인스턴스의 private IPv4를 IMDSv2(토큰 기반)로 조회한다.

    `terraform/compute_train.tf`가 EC2 인스턴스에 `http_tokens = "required"`
    (IMDSv2 강제)를 이미 걸어뒀으므로 토큰 없는 IMDSv1 요청은 여기서도 거부된다.
    """
    token_request = urllib.request.Request(
        f"{_IMDS_BASE_URL}/api/token",
        method="PUT",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
    )
    with urllib.request.urlopen(token_request, timeout=5) as response:
        token = response.read().decode()

    ip_request = urllib.request.Request(
        f"{_IMDS_BASE_URL}/meta-data/local-ipv4",
        headers={"X-aws-ec2-metadata-token": token},
    )
    with urllib.request.urlopen(ip_request, timeout=5) as response:
        return response.read().decode().strip()


def _register_self(run_id: str, worker_id: str, host: str, port: int) -> None:
    s3_io.write_json(training_run_worker_key(run_id, worker_id), {"host": host, "port": port})


def _poll_until_all_registered(run_id: str, num_machines: int, timeout_seconds: float) -> list[str]:
    """`run_id` 아래 워커 등록 파일이 정확히 `num_machines`개 모일 때까지 폴링한다.

    **정확히 일치해야 하는 이유**: 예전에는 "num_machines개 이상"이면 그 시점에
    보이는 목록을 그대로 반환했다 — 그런데 YARN이 실패한 컨테이너를 새
    `CONTAINER_ID`로 재시도하면 옛 컨테이너의 등록 파일이 S3에 orphan으로
    남을 수 있어, 워커마다 폴링 시각이 조금만 달라도(예: A는 8개를 보고 즉시
    반환, B는 0.1초 뒤 9개를 봄) 서로 다른 `machines` 문자열을 계산하게 된다 —
    LightGBM 소켓 프로토콜은 전 머신이 정확히 같은 `machines` 목록을 봐야
    하므로 이런 불일치는 조용한 분산 학습 corruption으로 이어진다. 정확히
    일치를 요구하면 초과 등록은 모든 워커가 일관되게(각자 에러로) 실패해,
    "일부는 성공한 줄 알고 진행, 일부만 무한 대기"하는 최악의 상황을 피한다.

    raises:
        TimeoutError: `timeout_seconds` 안에 정확히 다 모이지 않음
        RuntimeError: 등록이 `num_machines`보다 많음(컨테이너 재시도로 인한
            중복 등록 등 예상치 못한 상황 — 이 학습 시도를 신뢰할 수 없으므로
            계속 진행하지 않고 즉시 실패한다)
    """
    prefix = f"{TRAINING_RUNS_PREFIX}/{run_id}/workers/"
    deadline = time.monotonic() + timeout_seconds
    while True:
        keys = s3_io.list_keys(prefix)
        if len(keys) == num_machines:
            return keys
        if len(keys) > num_machines:
            raise RuntimeError(
                f"워커 barrier에 예상보다 많은 등록이 나타남: {len(keys)} > {num_machines}개 "
                f"(run_id={run_id}) — YARN 컨테이너 재시도 등으로 중복 등록됐을 가능성"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"워커 barrier 타임아웃: {len(keys)}/{num_machines}개만 등록됨 (run_id={run_id}, "
                f"{timeout_seconds:.0f}초 대기)"
            )
        time.sleep(_POLL_INTERVAL_SECONDS)


def _resolve_rank_and_machines(keys: list[str], host: str, port: int) -> tuple[int, str]:
    """등록된 워커들을 (host, port) 기준으로 정렬해 전 워커가 동일한 순서를
    계산하도록 하고, 그 순서에서 자기 위치를 rank로 반환한다.

    raises:
        RuntimeError: 같은 host가 두 번 이상 등록됨(컨테이너 두 개가 한 노드에
            배치돼 같은 포트를 두고 충돌할 것이 확실한 상황) — YARN 컨테이너
            메모리 요청을 노드 용량에 가깝게 잡아 노드당 1개만 배치되게 해야 함.
            또는 이 워커 자신의 등록을 찾지 못함(등록 직후 삭제되는 등 비정상 상태).
    """
    registrations = sorted(
        (s3_io.read_json(key) for key in keys),
        key=lambda r: (r["host"], r["port"]),
    )
    hosts = [r["host"] for r in registrations]
    if len(set(hosts)) != len(hosts):
        raise RuntimeError(f"같은 host가 중복 등록됨(노드당 컨테이너 1개 배치 가정이 깨짐): {hosts}")

    machines = ",".join(f"{r['host']}:{r['port']}" for r in registrations)
    for rank, registration in enumerate(registrations):
        if registration["host"] == host and registration["port"] == port:
            return rank, machines
    raise RuntimeError(f"barrier 등록 목록에서 자기 자신을 찾을 수 없음: host={host} port={port}")


def _launch_training(model: str, env: dict[str, str]) -> int:
    """학습 subprocess를 현재 작업 디렉터리를 그대로 물려받아 띄운다.

    `ml_core.paths.ML_ROOT`(로컬/EC2 repo clone 레이아웃 기준으로 계산됨)를
    `cwd`로 쓰지 않는다 — EMR bootstrap.sh는 `core`/`ml_core`/`training`을
    `/opt/gng` 바로 아래 평평하게 풀기 때문에(레포의 `ml/` 래퍼 디렉터리가
    없음) `ML_ROOT` 계산이 존재하지 않는 경로(`/opt/ml`)가 되어 `subprocess.run()`이
    `FileNotFoundError`로 즉시 죽는다. 이 스크립트를 감싸는 셸 명령(`monthly_retrain_check.
    _run_distributed_training_via_yarn()`)이 이미 `cd /opt/gng &&`로 진입한 뒤 이
    프로세스를 띄우므로, cwd를 지정하지 않고 그대로 상속받으면 항상 올바른 위치가 된다.
    """
    result = subprocess.run([sys.executable, "-m", _TRAIN_MODULES[model]], env=env, check=False)
    return result.returncode


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if config.LGB_NUM_MACHINES <= 1:
        raise RuntimeError(
            f"LGB_NUM_MACHINES>1일 때만 쓰는 스크립트인데 현재 값이 {config.LGB_NUM_MACHINES}입니다 "
            "— EMR 스텝 제출 시 환경변수를 확인하세요"
        )
    run_id = os.environ.get("TRAINING_RUN_ID")
    if not run_id:
        raise RuntimeError("TRAINING_RUN_ID 환경변수가 필요합니다 — 이 학습 시도의 모든 워커가 같은 값을 공유해야 합니다")

    worker_id = os.environ.get("CONTAINER_ID") or f"local-{os.getpid()}"
    host = _discover_private_ip()
    port = config.LGB_LOCAL_LISTEN_PORT

    _register_self(run_id, worker_id, host, port)
    keys = _poll_until_all_registered(run_id, config.LGB_NUM_MACHINES, timeout_seconds=config.LGB_TIME_OUT * 60)
    rank, machines = _resolve_rank_and_machines(keys, host, port)

    env = dict(os.environ)
    env["LGB_MACHINE_RANK"] = str(rank)
    env["LGB_MACHINES"] = machines
    print(f"[yarn_worker_bootstrap] rank={rank}/{config.LGB_NUM_MACHINES} machines={machines}", flush=True)
    return _launch_training(args.model, env)


if __name__ == "__main__":
    sys.exit(main())
