"""대여(rental) → 반납(return) 순서로 월별 점검·EMR 피처마트 생성·YARN 분산
학습·승격을 단일 EMR 클러스터 생애주기 안에서 오케스트레이션하는 단일 DAG.

**2026-08 재설계(ADR-0007)**: 예전에는 평가/학습을 EC2(SSM)로, 피처마트만
EMR(매번 새로 만들고 자동 종료)로 실행했다. 이 계정은 SSM(SendCommand 등)이
SCP로 전면 차단돼 있어 그 경로가 실제로는 동작하지 않았을 가능성이 높고, 학습용
EC2 자체도 더 이상 쓸 수 없게 됐다. 지금은 월 1회 EMR 클러스터 하나를 띄워
(피처마트 3노드로 시작) 평가 → (필요 시) 후보 프로필 재학습 루프 → 종료까지
전부 EMR 스텝(`command-runner.jar`, 이미 실전에서 동작 중이던 유일한 원격 실행
경로)으로 실행한다. 재학습이 실제로 필요해지면 그 시점에 한 번만 8노드로
리사이즈하고, LightGBM 학습은 YARN Distributed Shell로 8개 컨테이너에 나눠
띄운다(`training/scripts/yarn_worker_bootstrap.py`).

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

import logging
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
_EMR_PYTHONPATH = "/opt/gng"
_EMR_PYTHON = "python3.11"

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


def _spark_module_launcher_command(module: str, extra_args: list[str]) -> list[str]:
    """`module`(예: "feature_engine.spark.run_pipeline")을 spark-submit으로 돌리는
    bash 스텝 인자를 만든다.

    `run_pipeline.py`/`build_multi_horizon_features.py`는 패키지 내부 상대
    import(`from . import config`)를 쓴다 — spark-submit에 그 파일 경로를
    그대로 넘기면 Python이 `__main__`으로 실행해 패키지 컨텍스트를 잃고
    "ImportError: attempted relative import with no known parent package"로
    즉시 죽는다(실제 EMR 실행에서 확인, 2026-08-25). `python -m package.module`
    방식만 정상 동작하는데 spark-submit은 파일 경로만 받으므로, `runpy.run_module()`로
    감싼 launcher 스크립트를 실행 시점에 만들어 그걸 대신 제출한다."""
    launcher = (
        f"cat > /tmp/_spark_entry_{module.rsplit('.', 1)[-1]}.py <<'PYEOF'\n"
        "import runpy\n"
        f'runpy.run_module("{module}", run_name="__main__")\n'
        "PYEOF\n"
    )
    entry_path = f"/tmp/_spark_entry_{module.rsplit('.', 1)[-1]}.py"
    spark_submit = " ".join(
        ["spark-submit", "--deploy-mode", "cluster", "--master", "yarn", *extra_args, entry_path]
    )
    return ["bash", "-c", launcher + spark_submit]


def _feature_mart_spark_steps(profile: str) -> tuple[tuple[str, list[str]], tuple[str, list[str]]]:
    """`profile`로 feature mart(2차 정제)를 (재)생성하는 두 Spark 스텝(name, args)을
    반환한다 — run_pipeline.py는 watermark 기반 증분이라 매번 불러도 안전하다
    (평가 전 최신화용 `refresh_feature_mart`와 재학습 루프 양쪽에서 재사용)."""
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
        # num-executors를 노드 수(3)보다 적게(2) 잡아 AM/executor가 노드마다
        # 하나씩만 배치되게 한다(실제 EMR 실행으로 검증, 2026-08-25).
        "--driver-memory",
        "4g",
        "--executor-memory",
        "4g",
        "--num-executors",
        "2",
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
        # 기다려야 첫 Spark 스텝의 AM이 등록 타임아웃으로 죽지 않는다.
        params_core_count = params.get("emr_core_instance_count") or FEATURE_MART_CORE_INSTANCE_COUNT
        submit_emr_step(cluster_id, *_wait_for_yarn_nodes_step(params_core_count), mock_override=mock_override)

        profile = _champion_profile_name(model_name) or "builtin-default"
        logger.info(
            "[%s 월별 재학습] 1.5단계: 챔피언 프로필 '%s' 기준으로 feature mart 증분 갱신", model_name, profile
        )
        for step_name, spark_args in _feature_mart_spark_steps(profile):
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

        logger.info("[%s 월별 재학습] 2단계: EMR 스텝으로 %s 챔피언 성능 점검 (--check-only)", model_name, model_name)
        result_key = _result_s3_key(run_id, f"eval-{model_name}")
        name, command = _bash_step(
            f"Evaluate-{model_name}",
            f"cd {_EMR_PYTHONPATH} && PYTHONPATH={_EMR_PYTHONPATH} {_EMR_PYTHON} -m "
            f"training.scripts.monthly_retrain_check --check-only --models {model_name} "
            f"--result-s3-key {result_key}",
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

        for profile in candidate_profiles:
            logger.info("=== [%s 프로필: %s] EMR 피처마트 스텝 제출 ===", model_name, profile)
            try:
                for step_name, spark_args in _feature_mart_spark_steps(profile):
                    submit_emr_step(cluster_id, step_name, spark_args, mock_override=mock_override)
                logger.info("=== [%s 프로필: %s] EMR 피처마트 완료 ===", model_name, profile)

                logger.info("=== [%s 프로필: %s] YARN distributed-shell 학습 스텝 제출 ===", model_name, profile)
                train_result_key = _result_s3_key(run_id, f"train-{model_name}-{profile}")
                train_name, train_command = _bash_step(
                    f"Train-{model_name}-{profile}",
                    f"cd {_EMR_PYTHONPATH} && LGB_NUM_MACHINES={TRAINING_CORE_INSTANCE_COUNT} "
                    f"LGB_TREE_LEARNER=data PYTHONPATH={_EMR_PYTHONPATH} {_EMR_PYTHON} -m "
                    f"training.scripts.monthly_retrain_check --execute --skip-feature-pipeline "
                    f"--profile-name {profile} --models {model_name} --result-s3-key {train_result_key}",
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
        },
    ) as dag:
        chains = {model_name: build_model_task_chain(model_name) for model_name in MODEL_EXECUTION_ORDER}
        for upstream_model, downstream_model in pairwise(MODEL_EXECUTION_ORDER):
            chains[upstream_model]["terminate_cluster"] >> chains[downstream_model]["create_cluster"]

    return dag


dag: DAG = build_monthly_retrain_dag(MONTHLY_RETRAIN_CRON)
