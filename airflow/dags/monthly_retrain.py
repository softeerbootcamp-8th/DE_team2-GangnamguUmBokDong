"""대여(rental) → 반납(return) 순서로 월별 점검·EMR 피처마트 생성·YARN 분산
학습·승격을 단일 EMR 클러스터 생애주기 안에서 오케스트레이션하는 단일 DAG.

**2026-08 재설계(ADR-0007)**: 예전에는 평가/학습을 EC2(SSM)로, 피처마트만
EMR(매번 새로 만들고 자동 종료)로 실행했다. 이 계정은 SSM(SendCommand 등)이
SCP로 전면 차단돼 있어 그 경로가 실제로는 동작하지 않았을 가능성이 높고, 학습용
EC2 자체도 더 이상 쓸 수 없게 됐다. 지금은 월 1회 EMR 클러스터 하나를 학습
단계 노드 수(`TRAINING_CORE_INSTANCE_COUNT`)로 처음부터 띄워 평가 → (필요 시)
후보 프로필 재학습 루프 → 종료까지 전부 EMR 스텝(`command-runner.jar`, 이미
실전에서 동작 중이던 유일한 원격 실행 경로)으로 실행한다. 원래는 피처마트(3노드)
→ 재학습 필요 시 학습용(8노드)으로 `resize_emr_cluster()`를 태우는 2단계
구성이었으나, 진행 중이던 스텝이 resize 중 죽거나 목표 개수까지 못 올라가는
사례가 의심돼(2026-08-26) 지금은 resize를 아예 없앴다. LightGBM 학습은 YARN
Distributed Shell로 컨테이너를 나눠 띄운다(`training/scripts/
yarn_worker_bootstrap.py`). 평가/학습 오케스트레이터(`training.scripts.
monthly_retrain_check`)도 master 노드에 bash 스텝으로 직접 올렸다가 master의
EMR 자체 데몬 메모리 압박으로 OOM(exitCode 137)이 나서, 지금은 이 오케스트레이터
자신도 YARN distributed-shell(`-num_containers 1`)로 core 노드에서 돌린다 —
실제 학습/평가 워커와 같은 메커니즘이라 별도 JVM 힙/executor 계산이 필요 없고,
자신이 먹는 노드도 AM 하나(1GB 남짓)뿐이다(`_yarn_python_module_step()`
docstring 참고 — spark-submit으로 감싸던 중간 단계에서 dynamic allocation·
executor 자원 경합 문제를 겪은 뒤 이 방식으로 정착했다).

**대여/반납을 한 DAG으로 합친 이유(2026-08)**: 원래는 `monthly_retrain_rental`/
`monthly_retrain_return` 두 DAG로 나눠 각자 다른 시각(03:00/06:00)에 스케줄했다.
하지만 두 DAG 모두 재학습이 실제로 필요해지면 각자 최대 8노드 EMR 클러스터를
띄우는데, 재시도나 지연으로 두 스케줄이 겹치면 클러스터 2개가 동시에 뜰 수
있었다 — 학습이 오래 걸릴 수 있다는 걸 감안해 타임아웃을 넉넉히(120시간) 늘린
뒤로는 3시간 간격의 스태거링으로는 겹침을 막을 수 없다. 그래서 한 DAG 안에서
대여 사이클(평가→재학습→클러스터 종료)이 완전히 끝난 뒤에만 반납 사이클이
시작하도록 강제한다.
"""

from __future__ import annotations

import json
import logging
import subprocess
from itertools import pairwise
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import Param
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import (
    CATCHUP,
    MAX_ACTIVE_RUNS,
    MONTHLY_CLUSTER_CREATE_TIMEOUT,
    MONTHLY_EVALUATION_TIMEOUT,
    MONTHLY_FEATURE_REFRESH_TIMEOUT,
    MONTHLY_RETRAIN_CRON,
    MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    MONTHLY_RETRAIN_TOTAL_TIMEOUT,
    MONTHLY_TRAINING_TIMEOUT,
    TIMEZONE,
)
from orchestration.aws_infra_task import (
    EMR_CORE_INSTANCE_TYPE,
    EMR_MLFLOW_TRACKING_URI,
    MOCK_OVERRIDE_FORCE_MOCK,
    MOCK_OVERRIDE_FORCE_REAL,
    MODELS_PREFIX,
    S3_BUCKET,
    TRAINING_RUNS_PREFIX,
    create_emr_cluster,
    read_s3_json,
    submit_emr_step,
    terminate_emr_cluster,
)

from airflow import DAG

logger = logging.getLogger(__name__)

# 대여 → 반납 순서로 고정 실행한다(동시에 두 EMR 클러스터가 뜨는 걸 막기 위함).
MODEL_EXECUTION_ORDER = ("rental", "return")

# 원래는 피처마트 단계(3노드) → 재학습 필요 시 학습용(8노드)으로 resize_emr_cluster()를
# 태우는 2단계 구성이었다. resize 중 진행 중이던 스텝이 죽거나(노드가 빠지는 동안
# 실행 중이던 executor/컨테이너가 유실) 리사이즈 자체가 목표 개수까지 안 올라가는
# 사례가 의심돼(사용자 지적, 2026-08-26), 지금은 처음부터 학습 단계 노드 수로
# 고정 생성해서 이 사이클 안에서는 resize를 아예 안 태운다 — m4.large 8대 기준
# (같은 학습 조건이 RAM 40GB 단일 서버에서는 이미 성공했으므로, 분산이 제대로
# 되면 8대로 충분해야 한다). 여전히 부족하면 최대 10대(이 AWS 계정 EMR 콘솔
# 제약)까지만 늘리고, 그래도 안 되면 노드 수 문제가 아니라 데이터 로딩/분배
# 로직(또는 메모리 미해제) 쪽을 봐야 한다.
FEATURE_MART_CORE_INSTANCE_COUNT = 8
TRAINING_CORE_INSTANCE_COUNT = 8
# Evaluate/Train 스텝은 `_yarn_python_module_step()`으로 자기 자신도 YARN
# distributed-shell로 core 노드에서 돈다 — AM(`_YARN_AM_MEMORY_MB`만큼의 작은
# 힙)과 실제 오케스트레이터 파이썬 코드가 도는 워커 컨테이너(`-container_memory
# 6144`, 노드 전체)는 별개다. 즉 이 wrapper 자신만으로 최소 노드 2개(AM 1 +
# 자기 워커 컨테이너 1)를 먹는다 — 1개(AM)만 빼고 계산했다가 barrier가 요청한
# 워커 수보다 실제 가용 노드가 하나 모자라 10분 타임아웃나는 걸 실제 EMR
# 실행에서 확인했다(2026-08-26). 그 "안"에서 다시 진짜 워커(평가/학습)용
# distributed-shell을 core 노드 수 그대로 요청하면 안 되고, 항상 이 예약분만큼
# 적게 잡아야 한다. (spark-submit으로 감싸던 예전 방식은 AM+정적 executor로
# 3노드를 먹었었다 — distributed-shell로 바꾸며 3 -> 2로 줄었다,
# `_yarn_python_module_step()` docstring 4번 참고.)
_WRAPPER_NODE_RESERVATION = 2
_EMR_PYTHONPATH = "/opt/gng"

# `test_profile_only` DAG 파라미터가 켜졌을 때 재평가를 건너뛰고 바로 재학습
# 대상으로 강제하는 프로필 — s3://{S3_BUCKET}/profiles/a-test-sparse-flat.json.
# mock_mode(AWS 호출을 흉내낼지 여부)와는 완전히 별개 축이다: test_profile_only는
# 실제 AWS 자원으로 전체 파이프라인(피처마트 생성 → 학습 → 승격 판정)을 작은
# 프로필 하나로 빠르게 스모크 테스트하려는 용도다.
#
# **주의**: 이 프로필의 ROLLING_WINDOW_MINUTES/ROLLING_EMBARGO_MINUTES(현재
# 65/45)는 프로덕션 챔피언 프로필의 값(기본 60/40)과 반드시 달라야 한다 —
# feature mart 출력 경로가 이 셋의 조합(w{window}_e{embargo}_t{tick})으로만
# 키잉되므로, 값이 같으면 서로 다른 TRAIN_LOOKBACK_MONTHS를 가진 프로필끼리
# 같은 물리 경로를 공유하게 된다. build_multi_horizon_features.py가
# partitionOverwriteMode=dynamic이라도 겹치는 날짜 파티션은 그대로 덮어쓰므로,
# 완전히 겹치지 않는 값으로 격리해야 안전하다 — 실제로 이 프로필이 챔피언과
# 같은 값을 썼다가 프로덕션 feature mart 365개 파티션 중 332개가 삭제되는
# 사고가 났다(2026-08-26, 원본 데이터에서 재생성해 복구함).
TEST_ONLY_PROFILE_NAME = "a-test-sparse-flat"

# DAG params의 "mock_mode" 값 → aws_infra_task의 override 인자로 변환하는 매핑.
# "auto"는 override 없이 환경변수 기반 자동 판별(is_mock_mode/is_emr_mock_mode)을 그대로 따른다.
_MOCK_MODE_PARAM_TO_OVERRIDE = {
    "force_mock": MOCK_OVERRIDE_FORCE_MOCK,
    "force_real": MOCK_OVERRIDE_FORCE_REAL,
}


def _mock_override_from_params(params: dict[str, Any]) -> str | None:
    """DAG trigger 시점 params의 mock_mode 선택값을 override 인자로 변환한다."""
    return _MOCK_MODE_PARAM_TO_OVERRIDE.get(params.get("mock_mode", "auto"))


def _result_s3_key(run_id: str, name: str) -> str:
    """EMR 스텝이 stdout 대신 결과를 남기는 위치 — SSM과 달리 EMR 스텝은 호출부가
    stdout을 직접 돌려받지 못하므로, `monthly_retrain_check.py --result-s3-key`가
    이 키에 요약 JSON을 쓰고 Airflow가 스텝 완료 후 다시 읽는다."""
    return f"{TRAINING_RUNS_PREFIX}/{run_id}/{name}.json"


def _spark_module_launcher_command(
    module: str,
    extra_args: list[str],
    app_args: list[str] | None = None,
) -> list[str]:
    """`module`(예: "feature_engine.spark.run_pipeline")을 spark-submit으로 돌리는
    bash 스텝 인자를 만든다.

    `run_pipeline.py`/`build_multi_horizon_features.py`는 패키지 내부 상대
    import(`from . import config`)를 쓴다 — spark-submit에 그 파일 경로를
    그대로 넘기면 Python이 `__main__`으로 실행해 패키지 컨텍스트를 잃고
    "ImportError: attempted relative import with no known parent package"로
    즉시 죽는다(실제 EMR 실행에서 확인, 2026-08-25). `python -m package.module`
    방식만 정상 동작하는데 spark-submit은 파일 경로만 받으므로, `runpy.run_module()`로
    감싼 launcher 스크립트를 실행 시점에 만들어 그걸 대신 제출한다.

    `app_args`는 spark-submit이 entry 스크립트 뒤에 그대로 붙여 전달하는
    애플리케이션 인자다 — `runpy.run_module(..., run_name="__main__")`이 대상
    모듈의 `if __name__ == "__main__":` 블록을 실행할 때 그 블록이 보는
    `sys.argv`는 spark-submit이 launcher 스크립트를 실행한 그 argv 그대로다.

    이 launcher는 실제로 Spark를 쓰는 모듈(`run_pipeline`/`build_multi_horizon_features`
    처럼 자기 코드 안에서 `get_spark()`로 SparkSession을 직접 만드는 것)에만 쓴다 —
    Spark를 전혀 안 쓰는 순수 파이썬 오케스트레이터는 `_yarn_python_module_step()`이
    YARN distributed-shell로 직접 띄운다(그 함수 docstring 4번 참고 — spark-submit으로
    감싸던 예전 방식은 SparkContext가 안 생겨 AM 등록이 미뤄지는 문제가 있었다)."""
    launcher = (
        f"cat > /tmp/_spark_entry_{module.rsplit('.', 1)[-1]}.py <<'PYEOF'\n"
        "import runpy\n"
        f'runpy.run_module("{module}", run_name="__main__")\n'
        "PYEOF\n"
    )
    entry_path = f"/tmp/_spark_entry_{module.rsplit('.', 1)[-1]}.py"
    spark_submit = " ".join(
        ["spark-submit", "--deploy-mode", "cluster", "--master", "yarn", *extra_args, entry_path, *(app_args or [])]
    )
    return ["bash", "-c", launcher + spark_submit]


_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS = ("/usr/lib/hadoop-yarn", "/usr/lib/hadoop", "/opt/hadoop")
# distributed-shell 자체 ApplicationMaster(워커 컨테이너와 별개)가 기본 100MB
# 힙으로 뜨면 클래스 로딩만 하다가도 OutOfMemoryError로 즉시 죽는다 — 겉보기엔
# "JNI error"라 Java 17 비호환처럼 보였지만 실제로는 순수 메모리 부족이었다
# (`ml/training/scripts/monthly_retrain_check.py`의 YARN_AM_MEMORY_MB 주석과
# 같은 근거, 실제 EMR 실행에서 -master_memory 512로 재현/해결 확인, 2026-08-26).
_YARN_AM_MEMORY_MB = 1024
_YARN_AM_VCORES = 1


def _yarn_python_module_step(
    name: str, module: str, app_args: list[str], env: dict[str, str] | None = None
) -> tuple[str, list[str]]:
    """master 노드가 아니라 core 노드의 YARN 컨테이너 안에서 순수 파이썬 모듈
    (`training.scripts.monthly_retrain_check` 등)을 돌리는 스텝을 만든다.

    **2026-08-26 실제 EMR 실행에서 네 번 시도 끝에 얻은 결론**:
    1) 처음엔 이 오케스트레이터를 `command-runner.jar` bash 스텝으로 master에서
       직접 돌렸다가 exitCode 137(OOM)로 죽었다 — master 노드는 이 계정
       제약상 m4.large(8GB)뿐인데, EMR 자체 데몬(ResourceManager/NameNode/
       instance-controller 등)만으로 이미 ~5.7GB를 쓰고 있어서(`free -h` 실측)
       우리 프로세스가 겨우 1.5GB(`dmesg`의 anon-rss:1544572kB)를 더 쓴 것만으로
       시스템 전체가 OOM-killer에 걸렸다.
    2) YARN distributed-shell로 core 노드에 옮겨봤는데, 이번엔 컨테이너에
       도달하기도 전에 distributed-shell 자체의 ApplicationMaster가 "WARNING:
       package javax.script not in java.base" + "Error: A JNI error has
       occurred"로 즉시 죽었다 — 처음엔 Java 17 비호환으로 오진했지만, 나중에
       실제 분산학습 워커(`yarn_worker_bootstrap.py`)로 격리 재현해보니 그냥
       기본 100MB AM 힙 부족이었다(`_YARN_AM_MEMORY_MB` 주석 참고).
    3) 그래서 spark-submit(YARN cluster 모드, 더미 SparkSession)으로 우회했으나,
       이 방식은 (a) Spark 자체가 아무 연산도 안 하는데도 EMR 기본 dynamic
       allocation이 executor를 최대 50개까지 미리 요청해 우리 distributed-shell
       워커와 자원을 놓고 경합하다 driver가 죽고(`spark.dynamicAllocation.
       enabled=false`로 완화는 됐지만), (b) 그렇게 완화해도 이 spark-submit
       자신이 AM(노드 1개) + 정적 최소 executor(스파크가 0개를 거부해서 강제로
       뜨는 executor, 노드 1개 이상)를 항상 먹어서, 그 "안"에서 다시 8노드
       그대로 distributed-shell을 요청하면 barrier가 나머지를 10분간 기다리다
       타임아웃나는 문제가 있었다(전부 실제 EMR 실행에서 확인, 2026-08-26).
    4) 결론: 2번의 "JNI error"가 사실 AM 힙 부족이었다는 걸 알게 된 뒤로는
       distributed-shell(`-num_containers 1`, `-master_memory`/`-master_vcores`
       명시)로 돌아오는 게 정답이다 — Spark를 아예 안 쓰므로 더미 SparkSession
       (`_spark_module_launcher_command`)도, JVM 힙/overhead 계산도,
       dynamicAllocation 문제도 전부 해당 사항이 없어지고, 이 오케스트레이터
       자신이 먹는 노드도 AM 하나(그것도 1GB 남짓, 노드 전체를 안 씀)뿐이라
       나머지 core 노드를 실제 distributed-shell 워커에 최대한 넘겨줄 수 있다.
    """
    shell_env = {"S3_BUCKET": S3_BUCKET, "PYTHONPATH": _EMR_PYTHONPATH, **(env or {})}
    if EMR_MLFLOW_TRACKING_URI:
        shell_env["MLFLOW_TRACKING_URI"] = EMR_MLFLOW_TRACKING_URI
    shell_env_args = " ".join(f"-shell_env {key}={value}" for key, value in shell_env.items())
    module_args = " ".join(app_args)
    # LightGBM predict()는 기본적으로 멀티스레드를 쓰려 하는데, vcores를 1만
    # 주면 스레드끼리 그 안에서 경합만 한다 — 실제 EMR 실행에서 챔피언 4개
    # 부스터(포아송/q10/q50/q90) 예측에 부스터당 최대 ~550초씩 걸리는 것으로
    # 확인(2026-08-26). m4.large는 물리 코어가 2개뿐이라(YARN엔 4 vCore로
    # 과다 표시돼 있지만 물리 한계는 2) 그 이상은 의미가 없다.
    shell_command = f"cd {_EMR_PYTHONPATH} && PYTHONPATH={_EMR_PYTHONPATH} python3.11 -m {module} {module_args}"
    search_roots = " ".join(_YARN_DISTRIBUTED_SHELL_JAR_SEARCH_ROOTS)
    script = (
        f"JAR=$(find {search_roots} -iname '*distributedshell*.jar' 2>/dev/null | head -1); "
        'if [ -z "$JAR" ]; then echo "distributed-shell jar를 찾을 수 없습니다" >&2; exit 1; fi; '
        "yarn org.apache.hadoop.yarn.applications.distributedshell.Client "
        f'-jar "$JAR" -shell_command \'{shell_command}\' '
        "-num_containers 1 -container_memory 6144 -container_vcores 2 "
        f"-master_memory {_YARN_AM_MEMORY_MB} -master_vcores {_YARN_AM_VCORES} {shell_env_args}"
    )
    return name, ["bash", "-c", script]


def _feature_mart_spark_steps(
    profile: str, core_instance_count: int = FEATURE_MART_CORE_INSTANCE_COUNT
) -> tuple[tuple[str, list[str]], tuple[str, list[str]]]:
    """`profile`로 feature mart(2차 정제)를 (재)생성하는 두 Spark 스텝(name, args)을
    반환한다 — run_pipeline.py는 watermark 기반 증분이라 매번 불러도 안전하다
    (평가 전 최신화용 `refresh_feature_mart`와 재학습 루프 양쪽에서 재사용).

    args:
        core_instance_count: 이 스텝이 실제로 돌 클러스터의 core 노드 수 —
            `--num-executors`를 여기 맞춰 노드마다 executor 하나씩 배치한다
            (노드 수보다 하나 적게 잡아 AM 자리를 남긴다). 하드코딩된 값을
            그대로 두면 노드 수가 늘어나도 병렬도가 그대로라 유휴 노드가
            생긴다(PR #248 리뷰 지적).
    """
    common_confs = [
        # feature_engine/spark/spark_session.py의 get_spark()가 "지금 진짜 yarn
        # 클러스터 실행인지"를 판단할 유일한 신호 — pyspark SparkConf()로 spark-submit
        # 인자를 되짚어 보는 시도는 SparkContext._jvm이 아직 없는 시점엔 항상 빈
        # 값을 봐서 실패했다(실제 EMR 실행으로 확인, 2026-08-26). 이 값이 있어야
        # get_spark()가 .master()를 안 건드리고 --master yarn을 그대로 살려 executor가
        # 실제로 뜬다.
        "--conf",
        "spark.yarn.appMasterEnv.SPARK_ON_YARN=1",
        "--conf",
        "spark.executorEnv.SPARK_ON_YARN=1",
        "--conf",
        f"spark.yarn.appMasterEnv.ML_PROFILE={profile}",
        "--conf",
        f"spark.executorEnv.ML_PROFILE={profile}",
        # S3_BUCKET도 ML_PROFILE과 같은 이유로 명시해야 한다 — 없으면
        # ml/feature_engine/spark/config.py가 "local-dev"로 떨어진다.
        "--conf",
        f"spark.yarn.appMasterEnv.S3_BUCKET={S3_BUCKET}",
        "--conf",
        f"spark.executorEnv.S3_BUCKET={S3_BUCKET}",
        # PYTHONPATH가 없으면 드라이버/executor가 core/ml_core/feature_engine을
        # 못 찾아 "No module named 'core'"로 즉시 죽는다(실제 EMR 실행에서 확인,
        # 2026-08-25) — `Makefile`의 `emr-features` 타겟이 이미 쓰던 값과 맞춘다.
        "--conf",
        f"spark.yarn.appMasterEnv.PYTHONPATH={_EMR_PYTHONPATH}",
        "--conf",
        f"spark.executorEnv.PYTHONPATH={_EMR_PYTHONPATH}",
        "--conf",
        "spark.pyspark.python=/usr/bin/python3.11",
        "--conf",
        "spark.pyspark.driver.python=/usr/bin/python3.11",
        # m4.large 노드당 YARN이 실제로 내주는 건 ~6GB뿐이다(EMR 자체 로그의
        # "6144 MB per container" 상한, 물리 8GB에서 OS/데몬 몫을 뺀 값). 기본
        # 드라이버/AM 힙(2g)로는 워터마크 없는 최초 실행(수개월치 전체 재생성)이
        # py4j 연결 끊김(OOM성 컨테이너 킬)으로 죽었다 — 4g로 넉넉히 올리고,
        # num-executors를 노드 수보다 적게 잡아 AM/executor가 노드마다 하나씩만
        # 배치되게 한다(실제 EMR 실행으로 검증, 2026-08-25).
        "--driver-memory",
        "4g",
        "--executor-memory",
        "4g",
        "--num-executors",
        str(max(core_instance_count - 1, 1)),
        # SparkContext 초기화 대기 기본값(100s, ApplicationMaster.scala의
        # AM_MAX_WAIT_TIME)이 이 작은 클러스터에서 컨테이너 배치가 늦어질 때
        # 너무 타이트해서 "Futures timed out after [100000 milliseconds]"로
        # 죽었다 — 30분으로 넉넉히 늘린다.
        "--conf",
        "spark.yarn.am.waitTime=1800s",
        "--conf",
        "spark.sql.shuffle.partitions=24",
        # emr-7.13.0(Hadoop 3.4.2)에서 AM 컨테이너가 시작 직후 "fs.s3a.buffer.dir"
        # 관련 오류로 exitCode 13으로 즉시 죽었다(실제 EMR 실행에서 확인,
        # 2026-08-25 — emr-7.2.0/Hadoop 3.3.6에서는 없던 문제). 컨테이너 로컬
        # 작업 디렉터리(YARN이 항상 보장하는 경로)를 명시해 기본값 해석 문제를
        # 피한다.
        "--conf",
        "spark.hadoop.fs.s3a.buffer.dir=/mnt/tmp",
        # emr-7.13.0(Hadoop 3.4.2)에서는 s3:// 스킴도 EMRFS 전용 구현이 아니라
        # 표준 S3A 커넥터를 타는데(emr-7.2.0/Hadoop 3.3.6과 다른 점), S3A의 기본
        # 자격증명 provider 체인이 "The AWS Access Key Id you provided does not
        # exist in our records"로 즉시 실패했다(실제 EMR 실행에서 확인,
        # 2026-08-25 — s3a://에서 s3://로 바꾼 이전 수정은 emr-7.13에서는 더 이상
        # 스킴을 구분하지 않아 무의미해졌다). EC2 인스턴스 프로필만 쓰는 provider로
        # 명시하면 정상 동작한다(같은 디버그 클러스터에서 재현 검증).
        "--conf",
        "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    ]
    return (
        (
            f"Spark-RunPipeline-{profile}",
            _spark_module_launcher_command("feature_engine.spark.run_pipeline", common_confs),
        ),
        (
            f"Spark-BuildMultiHorizon-{profile}",
            _spark_module_launcher_command("feature_engine.spark.build_multi_horizon_features", common_confs),
        ),
    )


def _champion_profile_name(model_name: str) -> str | None:
    """지금 챔피언이 학습될 때 쓴 프로필 이름 — `training.scripts.monthly_retrain_check.
    _champion_profile_name()`과 같은 로직이다. airflow venv는 ml_core/core를 설치하지
    않으므로(파일 상단 S3_BUCKET 주석 참고) `read_champion_prefix()`/`model_json_key()`를
    import하지 못하고, 그 두 함수가 만드는 S3 키 형식을 `read_s3_json()`으로 직접
    재현한다 — 키 형식이 바뀌면 (`libs/ml_core/paths.py`) 이쪽도 같이 고칠 것."""
    pointer = read_s3_json(f"{MODELS_PREFIX}/champion/{model_name}.json")
    archive_prefix = (pointer or {}).get("archive_prefix")
    if not archive_prefix:
        return None
    payload = read_s3_json(f"{archive_prefix}/{model_name}_profile.json")
    return (payload or {}).get("profile_name")


def _bash_step(name: str, command: str) -> tuple[str, list[str]]:
    # command-runner.jar이 물려주는 환경(EMR controller.gz 실측 확인, 2026-08-25)에는
    # S3_BUCKET이 없어 core.s3/ml_core가 잘못된 기본 버킷("gangnamgu")으로 떨어진다
    # — Airflow 쪽 aws_infra_task.S3_BUCKET과 반드시 같은 값을 명시적으로 넘긴다.
    exports = f"export S3_BUCKET={S3_BUCKET}"
    if EMR_MLFLOW_TRACKING_URI:
        # 기본값("http://mlflow:5000/mlflow")은 docker 네트워크 이름이라 EMR
        # 노드에서 안 풀린다 — 학습 스텝(train_common.py의 mlflow.start_run())이
        # 여기 못 붙으면 예외 없이 바로 실패한다(PR 리뷰 지적, 2026-08).
        exports += f" && export MLFLOW_TRACKING_URI={EMR_MLFLOW_TRACKING_URI}"
    return name, ["bash", "-c", f"{exports} && {command}"]


def _wait_for_yarn_nodes_step(count: int) -> tuple[str, list[str]]:
    """`count`개 이상 YARN 노드가 RUNNING으로 보일 때까지 대기하는 스텝을 만든다.

    EMR 클러스터가 WAITING 상태가 됐다고 보고된 시점과 YARN
    ResourceManager/NodeManager가 실제로 AM 등록을 받을 준비가 된 시점 사이에
    간극이 있다 — 그 간극에서 바로 spark-submit을 돌리면 AM이 RM에 영원히
    등록되지 못하고 "Futures timed out"/"ApplicationMaster ... timed out"으로
    죽는다(실제 EMR 실행에서 재현 확인, 2026-08-25 — 서로 다른 두 클러스터에서
    똑같은 방식으로, 리사이즈 뒤에는 이미 Wait-YARN-Nodes가 있어 안 겪던 문제).
    클러스터 생성 직후에도 같은 대기를 걸어야 한다."""
    return _bash_step(
        "Wait-YARN-Nodes",
        f"until [ $(yarn node -list -all 2>/dev/null | grep -c RUNNING) -ge {count} ]; do sleep 15; done",
    )


def _task_id(model_name: str, name: str) -> str:
    """모델별 태스크 체인의 task_id — 한 DAG 안에 대여/반납 두 체인이 공존하므로
    겹치지 않게 전부 모델 접미사를 붙인다."""
    return f"{name}_{model_name}"


def make_task_create_cluster(model_name: str) -> Any:
    """월간 사이클용 상시 EMR 클러스터만 생성하는 callable을 반환한다.

    평가(evaluate)와 반드시 별도 태스크여야 한다 — 평가 스텝이 멈추거나
    실패해도 "클러스터 생성 자체는 성공했다"는 사실이 `terminate_cluster`의
    teardown 조건(setup 성공)에서 오염되면 안 되기 때문이다(2026-08, PR 리뷰
    지적: 원래 한 태스크였을 때는 평가가 타임아웃되면 태스크 전체가 FAILED로
    기록돼 teardown이 스킵되고, 클러스터가 계속 떠 있는데도 EMR 스텝이 여전히
    RUNNING으로 보여 orphan reaper도 건드리지 않는 — 클러스터가 영원히
    안 죽는 경로가 있었다)."""

    def task_create_cluster(**context: Any) -> str:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)

        logger.info("[%s 월별 재학습] 1단계: 상시 EMR 클러스터 생성(피처마트용 %d노드)", model_name, FEATURE_MART_CORE_INSTANCE_COUNT)
        cluster_id = create_emr_cluster(
            cluster_name=f"ml-monthly-retrain-{model_name}",
            core_instance_count=params.get("emr_core_instance_count") or FEATURE_MART_CORE_INSTANCE_COUNT,
            core_instance_type=params.get("emr_core_instance_type") or None,
            mock_override=mock_override,
        )
        ti.xcom_push(key="cluster_id", value=cluster_id)
        return cluster_id

    return task_create_cluster


def make_task_refresh_feature_mart(model_name: str) -> Any:
    """평가(evaluate) 직전에, 지금 챔피언이 실제로 학습된 프로필 기준으로 feature
    mart를 증분 최신화하는 callable을 반환한다.

    **왜 필요한가(2026-08 발견)**: `evaluate`는 이미 만들어진 feature mart 테이블을
    그냥 읽기만 하고 절대 스스로 최신화하지 않는다 — feature mart를 실제로 만드는
    Spark 스텝은 원래 `orchestrate_retrain_loop`(재학습이 필요하다고 판단된 *뒤*)
    안에만 있었다. 즉 evaluate가 참고할 최근 구간(`MONITOR_LOOKBACK_MONTHS`,
    `TRAINING_SAFETY_MARGIN_DAYS`만큼 뺀 최근 구간)이 최신인지 아무도 보장하지
    않는 채로 순환 참조에 걸려 있었다: feature mart가 없거나 오래되면 evaluate가
    실패/구식 데이터로 판단하는데, feature mart를 새로 만드는 유일한 경로가 그
    evaluate의 판단 결과에 갇혀 있었다. `run_pipeline.py`는 watermark 기반
    증분이라(`feature_engine/spark/run_pipeline.py` 모듈 docstring) 매 사이클마다
    불러도 이미 최신이면 비용이 작다."""

    def task_refresh_feature_mart(**context: Any) -> None:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        cluster_id = ti.xcom_pull(task_ids=_task_id(model_name, "create_cluster"), key="cluster_id")

        # 클러스터가 막 WAITING이 된 직후라 YARN이 실제로 AM 등록을 받을 준비가
        # 안 됐을 수 있다 — _wait_for_yarn_nodes_step() docstring 참고. 여기서
        # 기다려야 첫 Spark 스텝의 AM이 등록 타임아웃으로 죽지 않는다. test_profile_only일
        # 때도 이 대기는 그대로 해야 한다 — 뒤에 오는 orchestrate_retrain_loop의 첫
        # 스텝이 이 대기를 대신 해주지 않기 때문이다.
        params_core_count = params.get("emr_core_instance_count") or FEATURE_MART_CORE_INSTANCE_COUNT
        # submit_emr_step()의 기본 timeout_seconds(5400s=90분)는 무거운 Spark
        # 스텝 기준이다 — 이 대기는 그냥 `yarn node -list` 폴링이라 노드가
        # 정상 등록되면 1분 안에 끝난다. 기본값을 그대로 물려받으면 이 스텝을
        # 포함해 총 3개 스텝(대기+RunPipeline+BuildMultiHorizon)이 각각 90분
        # 한도를 쓸 수 있어 최악 270분 > MONTHLY_FEATURE_REFRESH_TIMEOUT(4시간)
        # 이 된다(PR #248 리뷰 지적) — 노드가 끝내 안 붙는 실패 케이스도 90분씩
        # 기다릴 이유가 없으므로 짧게 별도로 잡는다.
        submit_emr_step(
            cluster_id, *_wait_for_yarn_nodes_step(params_core_count),
            timeout_seconds=1200, mock_override=mock_override,
        )

        if params.get("test_profile_only"):
            # 챔피언 프로필 feature mart는 필요 없다 — orchestrate_retrain_loop가
            # TEST_ONLY_PROFILE_NAME으로 어차피 다시 만든다(make_task_evaluate
            # 참고). 여기서 또 만들면 그냥 낭비다.
            logger.info(
                "[%s 월별 재학습] test_profile_only=True — 챔피언 feature mart 갱신 스킵", model_name
            )
            return

        profile = _champion_profile_name(model_name) or "builtin-default"
        logger.info(
            "[%s 월별 재학습] 1.5단계: 챔피언 프로필 '%s' 기준으로 feature mart 증분 갱신", model_name, profile
        )
        for step_name, spark_args in _feature_mart_spark_steps(profile, params_core_count):
            submit_emr_step(cluster_id, step_name, spark_args, mock_override=mock_override)
        ti.xcom_push(key="profile", value=profile)

    return task_refresh_feature_mart


def make_task_evaluate(model_name: str) -> Any:
    """이미 떠 있는 상시 EMR 클러스터 위에서 챔피언 성능 점검 스텝을 실행하는
    callable을 반환한다 — `make_task_create_cluster()`와 분리된 이유는 그
    docstring 참고."""

    def task_evaluate(**context: Any) -> dict[str, Any]:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        run_id = context["run_id"]
        cluster_id = ti.xcom_pull(task_ids=_task_id(model_name, "create_cluster"), key="cluster_id")

        if params.get("test_profile_only"):
            # 챔피언 성능 점검을 건너뛰고 TEST_ONLY_PROFILE_NAME 하나만으로
            # orchestrate_retrain_loop(피처마트 생성 -> 학습 -> 승격 판정)를 강제
            # 실행한다 — 실제 AWS 자원으로 전체 파이프라인을 빠르게 스모크
            # 테스트할 때 쓴다(evaluate/refresh_feature_mart 단계 자체를 아예
            # 건너뛰므로 mock_mode와는 독립적인 축이다).
            logger.info(
                "[%s 월별 재학습] test_profile_only=True — 재평가 건너뛰고 '%s' 프로필로 바로 재학습",
                model_name,
                TEST_ONLY_PROFILE_NAME,
            )
            summary = {"needs_retrain": True, "candidate_profiles": [TEST_ONLY_PROFILE_NAME]}
            ti.xcom_push(key="needs_retrain", value=True)
            ti.xcom_push(key="candidate_profiles", value=[TEST_ONLY_PROFILE_NAME])
            return summary

        logger.info("[%s 월별 재학습] 2단계: EMR 스텝으로 %s 챔피언 성능 점검 (--check-only)", model_name, model_name)
        result_key = _result_s3_key(run_id, f"eval-{model_name}")
        # 이 시점엔 create_cluster가 이미 core 노드를 다 띄워둔 상태다(피처마트용/
        # 학습용 노드 수를 더 이상 분리하지 않음 — FEATURE_MART_CORE_INSTANCE_COUNT ==
        # TRAINING_CORE_INSTANCE_COUNT). 평가(predict)는 학습과 달리 워커끼리 통신할
        # 필요가 없는 embarrassingly parallel 작업이라(monitor_performance.
        # combine_evaluation_shards() 참고) 놀고 있는 core 노드 수만큼 그대로
        # 나눠 돌릴 수 있다 — create_cluster와 같은 override 규칙을 써서 실제
        # 띄운 노드 수와 어긋나지 않게 한다. 이 Evaluate 스텝 자신도 spark-submit
        # 래퍼로 노드를 먹으므로 `_WRAPPER_NODE_RESERVATION`만큼 빼야 한다
        # (위 상수 주석 참고 — 안 빼면 barrier가 10분 타임아웃난다).
        core_count = params.get("emr_core_instance_count") or FEATURE_MART_CORE_INSTANCE_COUNT
        eval_num_workers = max(core_count - _WRAPPER_NODE_RESERVATION, 1)
        # refresh_feature_mart가 방금 갱신한 feature mart는 챔피언 프로필 경로
        # (FEATURE_PARAM_COMBO_ID = w/e/t, 프로필에서 파생)에 쓰였다. 여기서
        # ML_PROFILE을 안 넘기면 monitor_performance가 기본(builtin-default)
        # 경로를 읽어, 챔피언 프로필의 w/e/t가 기본값과 다를 때 방금 갱신한
        # mart를 못 보거나(구버전 데이터) 아예 없는 테이블을 봐서
        # FileNotFoundError/ValueError로 죽는다(PR #248 리뷰 지적) — 같은
        # xcom 값을 그대로 재사용해 두 태스크가 항상 같은 프로필을 본다.
        profile = ti.xcom_pull(task_ids=_task_id(model_name, "refresh_feature_mart"), key="profile") or "builtin-default"
        name, command = _yarn_python_module_step(
            f"Evaluate-{model_name}",
            "training.scripts.monthly_retrain_check",
            [
                "--check-only", "--models", model_name, "--result-s3-key", result_key,
                "--eval-num-workers", str(eval_num_workers),
            ],
            env={"ML_PROFILE": profile},
        )
        submit_emr_step(cluster_id, name, command, mock_override=mock_override)

        # mock 모드에서는 실제 스텝이 결과를 쓰지 않으므로, 기존 SSM mock과 같은 취지로
        # "재학습 필요"를 기본값으로 삼아 dry-run에서도 재학습 루프 구조를 계속 검증할 수
        # 있게 한다(운영 real 경로에서는 항상 스텝이 쓴 실제 값을 읽는다).
        summary = read_s3_json(result_key) or {
            "needs_retrain": True,
            "retrain_models": [model_name],
            "candidate_profiles": ["builtin-default"],
        }
        ti.xcom_push(key="needs_retrain", value=summary.get("needs_retrain", False))
        ti.xcom_push(
            key="candidate_profiles", value=summary.get("candidate_profiles") or ["builtin-default"]
        )
        return summary

    return task_evaluate


def make_task_check_retrain_branch(model_name: str) -> Any:
    """재학습 진행 여부를 분기하는 callable을 반환한다."""

    def task_check_retrain_branch(**context: Any) -> str:
        ti = context["ti"]
        needs_retrain = ti.xcom_pull(
            task_ids=_task_id(model_name, "evaluate"), key="needs_retrain"
        )
        if needs_retrain:
            logger.info("[%s 월별 재학습] 기준 미달 발견 — 재학습 오케스트레이션으로 분기", model_name)
            return _task_id(model_name, "orchestrate_retrain_loop")
        logger.info("[%s 월별 재학습] 성능 정상 — 재학습 건너뜀", model_name)
        return _task_id(model_name, "skip_monthly_retrain")

    return task_check_retrain_branch


def make_task_orchestrate_retrain_loop(model_name: str) -> Any:
    """상시 EMR 클러스터 위에서 후보 프로필을 순환하며 피처마트 → YARN
    distributed-shell 학습을 반복하는 callable을 반환한다 — 클러스터가 이미
    `create_cluster` 단계에서 학습용 노드 수(`TRAINING_CORE_INSTANCE_COUNT`)로
    생성돼 있으므로 여기서 별도 resize는 하지 않는다."""

    def task_orchestrate_retrain_loop(**context: Any) -> dict[str, Any]:
        ti = context["ti"]
        params = context.get("params", {})
        mock_override = _mock_override_from_params(params)
        run_id = context["run_id"]
        cluster_id = ti.xcom_pull(task_ids=_task_id(model_name, "create_cluster"), key="cluster_id")
        candidate_profiles = (
            ti.xcom_pull(task_ids=_task_id(model_name, "evaluate"), key="candidate_profiles")
            or ["builtin-default"]
        )

        logger.info(
            "[%s 월별 재학습] 3단계: 챌린저 재학습 시작 (대상 모델: %s, 후보 프로필: %s)",
            model_name,
            model_name,
            candidate_profiles,
        )

        results_by_profile: dict[str, Any] = {}
        core_instance_count = params.get("emr_core_instance_count") or TRAINING_CORE_INSTANCE_COUNT

        for profile in candidate_profiles:
            logger.info("=== [%s 프로필: %s] EMR 피처마트 스텝 제출 ===", model_name, profile)
            try:
                for step_name, spark_args in _feature_mart_spark_steps(profile, core_instance_count):
                    submit_emr_step(cluster_id, step_name, spark_args, mock_override=mock_override)
                logger.info("=== [%s 프로필: %s] EMR 피처마트 완료 ===", model_name, profile)

                logger.info("=== [%s 프로필: %s] YARN distributed-shell 학습 스텝 제출 ===", model_name, profile)
                train_result_key = _result_s3_key(run_id, f"train-{model_name}-{profile}")
                train_args = [
                    "--execute",
                    "--skip-feature-pipeline",
                    "--profile-name",
                    profile,
                    "--models",
                    model_name,
                    "--result-s3-key",
                    train_result_key,
                ]
                if params.get("test_profile_only"):
                    # test_profile_only는 "재평가만 건너뛴다"는 뜻이지 "승격을
                    # 막는다"는 뜻이 아니다 — 프로필이 우연히 챔피언보다 좋게
                    # 나오면 이 플래그 없이는 진짜 models/champion/*.json이
                    # 바뀐다. 스모크 테스트 목적과 안 맞으므로 명시적으로 막는다.
                    train_args.append("--no-promote")
                # 이 학습 스텝도 spark-submit 래퍼 "안"에서 자기 자신의 distributed-shell을
                # core_instance_count 그대로 요청하면 barrier가 영원히 기다리다 타임아웃
                # 난다 — Evaluate와 같은 이유(`_WRAPPER_NODE_RESERVATION` 주석 참고).
                train_name, train_command = _yarn_python_module_step(
                    f"Train-{model_name}-{profile}",
                    "training.scripts.monthly_retrain_check",
                    train_args,
                    env={
                        "LGB_NUM_MACHINES": str(max(core_instance_count - _WRAPPER_NODE_RESERVATION, 1)),
                        "LGB_TREE_LEARNER": "data",
                    },
                )
                submit_emr_step(
                    cluster_id,
                    train_name,
                    train_command,
                    timeout_seconds=int(MONTHLY_TRAINING_TIMEOUT.total_seconds()),
                    mock_override=mock_override,
                )
                train_summary = read_s3_json(train_result_key) or {"promoted": {model_name: False}}
                promoted = bool(train_summary.get("promoted", {}).get(model_name))
                results_by_profile[profile] = {"status": "success", "promoted": promoted}
                if promoted:
                    logger.info("=== [%s 프로필: %s] 챔피언 승격 성공 — 루프 종료 ===", model_name, profile)
                    break
            except (RuntimeError, OSError, ValueError, TimeoutError) as exc:
                logger.error("[%s 프로필: %s] 재학습 루프 중 오류 발생: %s", model_name, profile, exc)
                results_by_profile[profile] = {"status": "failed", "error": str(exc)}

        return {"status": "completed", "profiles": results_by_profile}

    return task_orchestrate_retrain_loop


def make_task_terminate_emr_cluster(model_name: str) -> Any:
    """사이클이 어떻게 끝났든(성공/실패/스킵) 상시 EMR 클러스터를 반드시 종료하는
    callable을 반환한다 — 이 안전망이 없으면 태스크가 kill돼도 클러스터가 계속
    과금된다(2026-08 재설계 전 실제로 없던 안전망)."""

    def task_terminate_emr_cluster(**context: Any) -> None:
        ti = context["ti"]
        mock_override = _mock_override_from_params(context.get("params", {}))
        cluster_id = ti.xcom_pull(task_ids=_task_id(model_name, "create_cluster"), key="cluster_id")
        if not cluster_id:
            logger.warning("[%s 월별 재학습] 4단계: cluster_id 없음(생성 자체가 실패) — 종료할 대상 없음", model_name)
            return
        logger.info("[%s 월별 재학습] 4단계: EMR 클러스터 '%s' 종료", model_name, cluster_id)
        terminate_emr_cluster(cluster_id, mock_override=mock_override)

    return task_terminate_emr_cluster


# 이 값과 정확히 일치하는 label(docker-compose가 서비스마다 자동으로 붙임)을
# 가진 컨테이너를 찾는다 — 정확한 컨테이너 이름(프로젝트명 접두사 등)에 의존하면
# compose 프로젝트 이름이 바뀔 때 조용히 깨진다.
_MLFLOW_COMPOSE_SERVICE_LABEL = "com.docker.compose.service=mlflow"


def _docker_mlflow_container_action(action: str) -> None:
    """호스트 Docker 데몬(마운트된 소켓)에 직접 HTTP로 붙어 mlflow 컨테이너를
    시작/정지한다.

    이 프로젝트는 SSM(SendCommand 등)이 SCP로 전면 차단돼 있어(terraform/emr.tf
    참고) 상시 EC2에 원격으로 명령을 실행할 방법이 이것뿐이다 — airflow-scheduler
    컨테이너에만 호스트 docker.sock을 마운트해뒀다(docker-compose.prod.yml).
    `docker` CLI를 이미지에 새로 설치하는 대신 Docker Engine API를 curl로 직접
    호출한다(엔진 API와 CLI는 같은 REST 인터페이스를 공유한다).

    args:
        action: "start" 또는 "stop"
    """
    find_cmd = [
        "curl", "-sf", "--unix-socket", "/var/run/docker.sock",
        f"http://localhost/containers/json?all=1&filters=%7B%22label%22%3A%5B%22{_MLFLOW_COMPOSE_SERVICE_LABEL}%22%5D%7D",
    ]
    result = subprocess.run(find_cmd, capture_output=True, text=True, check=True)
    containers = json.loads(result.stdout)
    if not containers:
        logger.warning("[mlflow %s] label=%s인 컨테이너를 못 찾음 — 건너뜀", action, _MLFLOW_COMPOSE_SERVICE_LABEL)
        return
    container_id = containers[0]["Id"]
    subprocess.run(
        ["curl", "-sf", "-X", "POST", "--unix-socket", "/var/run/docker.sock", f"http://localhost/containers/{container_id}/{action}"],
        check=True,
    )
    logger.info("[mlflow %s] 컨테이너 %s에 적용 완료", action, container_id[:12])


def make_task_start_mlflow() -> Any:
    """monthly_retrain DAG 실행 구간에만 mlflow를 띄우는 callable을 반환한다 —
    상시 켜두는 대신 이 DAG가 도는 동안만 켜서 EC2 리소스를 아낀다. mlflow가
    안 떠도 재학습 자체의 정확성과는 무관하므로(모니터링/로깅 부가 기능,
    `monitor_performance._log_to_mlflow()`도 실패를 삼킴) 실패해도 DAG를 막지
    않는다."""

    def task_start_mlflow(**context: Any) -> None:
        mock_override = _mock_override_from_params(context.get("params", {}))
        if mock_override == MOCK_OVERRIDE_FORCE_MOCK:
            logger.info("[mlflow] force_mock — 실제로 켜지 않음")
            return
        try:
            _docker_mlflow_container_action("start")
        except Exception:
            logger.exception("[mlflow] 시작 실패 — 재학습은 계속 진행(모니터링 부가 기능일 뿐)")

    return task_start_mlflow


def make_task_stop_mlflow() -> Any:
    """DAG가 어떻게 끝났든(성공/실패/수동 중단) mlflow를 반드시 정지하는 callable을
    반환한다 — `make_task_terminate_emr_cluster()`와 같은 이유로 teardown으로
    표시해야 운영자가 DAG Run을 수동으로 실패 처리해도 실행된다."""

    def task_stop_mlflow(**context: Any) -> None:
        mock_override = _mock_override_from_params(context.get("params", {}))
        if mock_override == MOCK_OVERRIDE_FORCE_MOCK:
            logger.info("[mlflow] force_mock — 실제로 끄지 않음")
            return
        try:
            _docker_mlflow_container_action("stop")
        except Exception:
            logger.exception("[mlflow] 정지 실패 — 다음 성공 실행 때 재시도되지 않으니 수동 확인 필요")

    return task_stop_mlflow


def build_model_task_chain(model_name: str) -> dict[str, Any]:
    """모델 하나(대여 또는 반납)의 생성→평가→재학습→클러스터 종료 태스크 체인을
    만들어 반환한다 — `build_monthly_retrain_dag()`가 두 모델을 순서대로
    이어붙이는 데 쓴다.

    returns:
        dict[str, Any]: 이 체인의 첫 태스크("create_cluster")와 마지막
            태스크("terminate_cluster") — 다른 모델의 체인과 이어붙일 때 이 두
            개만 있으면 된다.
    """
    create_cluster = PythonOperator(
        task_id=_task_id(model_name, "create_cluster"),
        python_callable=make_task_create_cluster(model_name),
        execution_timeout=MONTHLY_CLUSTER_CREATE_TIMEOUT,
    )

    refresh_feature_mart = PythonOperator(
        task_id=_task_id(model_name, "refresh_feature_mart"),
        python_callable=make_task_refresh_feature_mart(model_name),
        execution_timeout=MONTHLY_FEATURE_REFRESH_TIMEOUT,
    )

    evaluate = PythonOperator(
        task_id=_task_id(model_name, "evaluate"),
        python_callable=make_task_evaluate(model_name),
        execution_timeout=MONTHLY_EVALUATION_TIMEOUT,
    )

    check_retrain_branch = BranchPythonOperator(
        task_id=_task_id(model_name, "check_retrain_branch"),
        python_callable=make_task_check_retrain_branch(model_name),
    )

    skip_monthly_retrain = EmptyOperator(
        task_id=_task_id(model_name, "skip_monthly_retrain"),
    )

    orchestrate_retrain_loop = PythonOperator(
        task_id=_task_id(model_name, "orchestrate_retrain_loop"),
        python_callable=make_task_orchestrate_retrain_loop(model_name),
        execution_timeout=MONTHLY_RETRAIN_ORCHESTRATION_TIMEOUT,
    )

    terminate_cluster = PythonOperator(
        task_id=_task_id(model_name, "terminate_cluster"),
        python_callable=make_task_terminate_emr_cluster(model_name),
    )

    # 태스크 흐름 정의 (비순환 단방향 그래프)
    create_cluster >> refresh_feature_mart >> evaluate >> check_retrain_branch
    check_retrain_branch >> [orchestrate_retrain_loop, skip_monthly_retrain]
    orchestrate_retrain_loop >> terminate_cluster
    skip_monthly_retrain >> terminate_cluster
    # trigger_rule=ALL_DONE만으로는 안전하지 않다 — 운영자가 DAG Run 전체를
    # 수동으로 "Mark Failed" 처리하면 Airflow는 아직 실행 안 된 일반 태스크를
    # 스케줄러의 trigger_rule 평가 없이 그냥 SKIPPED로 강제 전환하고 끝내버린다
    # (Airflow 3.3.1 `_set_dag_run_terminal_state()` 실측 확인, 2026-08).
    # `is_teardown=True`인 태스크만 이 강제 skip에서 예외로 남아 실제로 실행될
    # 기회를 얻는다 — 그래서 trigger_rule 대신 setup/teardown API로 이 태스크를
    # 표시한다.
    #
    # setup은 반드시 `create_cluster` 하나여야 하고 `evaluate`를 같이 넣으면 안
    # 된다(PR 리뷰 지적, 2026-08) — ALL_DONE_SETUP_SUCCESS는 "지정된 setup이
    # 전부 성공했을 때만" teardown을 실행한다. evaluate 스텝이 EMR 쪽에서 멈추거나
    # (RUNNING 상태로 안 끝남) 실패하면 evaluate 태스크 자체가 FAILED로 끝나는데,
    # 만약 evaluate도 setup에 포함돼 있었다면 teardown이 스킵되고, 클러스터는
    # 이미 떠 있는데도 EMR 스텝이 여전히 RUNNING으로 보여 emr_orphan_reaper의
    # "활성 스텝 있으면 절대 안 건드림" 보호까지 겹쳐 클러스터가 영원히 안 죽는
    # 경로가 생긴다. create_cluster만 setup으로 지정하면 evaluate 이후 무슨 일이
    # 있어도(성공/실패/타임아웃) "클러스터 생성 자체는 성공했다"는 사실만으로
    # teardown 조건이 충족돼 terminate_cluster가 반드시 실행된다.
    terminate_cluster.as_teardown(setups=create_cluster)

    return {
        "create_cluster": create_cluster,
        "terminate_cluster": terminate_cluster,
    }


def build_monthly_retrain_dag(cron_schedule: str) -> DAG:
    """대여 → 반납 순서로 순차 실행하는 단일 월간 재학습 DAG를 생성한다.

    두 모델을 한 DAG로 합친 이유는 모듈 docstring 참고 — 각자 최대 8노드 EMR
    클러스터를 띄우므로 동시에 두 개가 뜨면 안 된다.

    args:
        cron_schedule: cron 표현식 문자열
    returns:
        DAG: 구성된 Airflow DAG 객체
    """
    with DAG(
        dag_id="monthly_retrain",
        schedule=CronTriggerTimetable(cron_schedule, timezone=TIMEZONE),
        start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
        catchup=CATCHUP,
        max_active_runs=MAX_ACTIVE_RUNS,
        dagrun_timeout=MONTHLY_RETRAIN_TOTAL_TIMEOUT,
        tags=["ml", "monthly", "retrain", "emr", "yarn", "rental", "return"],
        params={
            "mock_mode": Param(
                "auto",
                type="string",
                enum=["auto", "force_mock", "force_real"],
                description=(
                    "auto: 환경변수(AWS_ACCESS_KEY_ID 등)로 자동 판별. "
                    "force_mock: EMR 호출 없이 무조건 mock. "
                    "force_real: mock 판별을 무시하고 무조건 실제 AWS 호출."
                ),
            ),
            "emr_core_instance_type": Param(
                EMR_CORE_INSTANCE_TYPE,
                type="string",
                description=(
                    "EMR core 노드 인스턴스 타입. 이 AWS 계정은 EMR에 m4.large 외 타입을 "
                    "허용하지 않으니 변경 전 계정 제약을 먼저 확인할 것."
                ),
            ),
            "emr_core_instance_count": Param(
                FEATURE_MART_CORE_INSTANCE_COUNT,
                type="integer",
                minimum=1,
                maximum=10,
                description=(
                    "클러스터 생성 시점 core 노드 개수 — master 1대는 별도. 피처마트/학습 "
                    "단계 모두 이 개수를 그대로 쓴다(resize 없음). 대여/반납 두 사이클 모두 "
                    "동일하게 적용된다."
                ),
            ),
            "test_profile_only": Param(
                False,
                type="boolean",
                description=(
                    f"켜면 재평가(evaluate)를 건너뛰고 '{TEST_ONLY_PROFILE_NAME}' 프로필 "
                    "하나만으로 바로 재학습 루프(피처마트 생성 → 학습 → 승격 판정)를 실행한 "
                    "뒤 종료한다. mock_mode와는 별개 축이다 — mock_mode=force_real과 같이 "
                    "켜면 실제 AWS 자원으로 전체 파이프라인을 작은 프로필로 빠르게 스모크"
                    "테스트할 수 있다."
                ),
            ),
        },
    ) as dag:
        start_mlflow = PythonOperator(
            task_id="start_mlflow",
            python_callable=make_task_start_mlflow(),
        )
        stop_mlflow = PythonOperator(
            task_id="stop_mlflow",
            python_callable=make_task_stop_mlflow(),
        )
        # terminate_cluster와 같은 이유로 teardown 표시 — 운영자가 DAG Run을
        # 수동으로 실패 처리해도(Airflow 3.3.1 강제 skip 실측, 위 주석 참고) 반드시
        # 실행돼 mlflow가 계속 켜진 채 남지 않게 한다.
        stop_mlflow.as_teardown(setups=start_mlflow)

        chains = {model_name: build_model_task_chain(model_name) for model_name in MODEL_EXECUTION_ORDER}
        for upstream_model, downstream_model in pairwise(MODEL_EXECUTION_ORDER):
            chains[upstream_model]["terminate_cluster"] >> chains[downstream_model]["create_cluster"]
        start_mlflow >> chains[MODEL_EXECUTION_ORDER[0]]["create_cluster"]
        chains[MODEL_EXECUTION_ORDER[-1]]["terminate_cluster"] >> stop_mlflow

    return dag


dag: DAG = build_monthly_retrain_dag(MONTHLY_RETRAIN_CRON)
