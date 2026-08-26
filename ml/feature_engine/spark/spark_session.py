"""로컬 개발/EMR 공용 SparkSession 생성.

EMR에서 `spark-submit`으로 실행할 때는 이미 클러스터 매니저(YARN)가 세션을
관리하므로 `SparkSession.builder.getOrCreate()`만으로 충분하다 — 이 함수는 로컬
개발 시(예: `./.venv/bin/python -m feature_engine.spark.run_pipeline`) master를
명시적으로 지정해주는 것 하나만 다르고, 나머지 설정은 EMR/로컬 어디서 돌아가든
동일한 코드 경로를 타게 한다.
"""

import os
import sys

from pyspark.sql import SparkSession

# 드라이버와 워커(로컬 모드에선 서브프로세스)가 같은 Python 인터프리터를 쓰도록 고정한다.
# 지정 안 하면 PATH상의 `python3`(=이 프로젝트의 메인 .venv, pyspark가 지원하지 않는
# 최신 버전일 수 있음)를 워커가 집어서 "PYTHON_VERSION_MISMATCH"로 죽는다 — pyspark는
# 로컬 dev용으로 별도 venv(.venv-spark, Spark가 공식 지원하는 Python 버전)를 쓰는 걸
# 전제로 한다. EMR에서는 spark-submit이 클러스터 전체에 동일한 Python을 이미 보장하므로
# 이 값이 없어도 되지만, setdefault라 있어도 무해하다.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# 이 프로젝트는 타임존을 KST(Asia/Seoul)로 쓰기로 했다 — 원본 데이터(트립 시각 등)
# 자체가 한국 로컬 wall-clock이라 서비스/운영 타임존과 맞추는 게 자연스럽다. JVM
# 기본 타임존을 세션 타임존(아래 get_spark())과 반드시 같은 값으로 맞춰야 한다 —
# 안 맞추면 `F.unix_timestamp()`/`F.timestamp_seconds()`가 timestamp_ntz(parquet에서
# 읽은 값, 실제 배치 실행 경로)와 timestamp(tz-aware, pandas DataFrame을
# spark.createDataFrame()한 값, 테스트에서 흔함)를 서로 다르게 취급해 왕복 변환이
# 조용히 어긋난다(실제로 이 개발 머신에서 9시간 밀린 채 발견함). 이 값 자체는 UTC든
# KST든 상관없이 안전하다 — `rolling_window_features.py`의 `_unix_seconds_ntz()`/
# `_seconds_to_ntz()`가 세션 타임존과 무관하게 정확한 왕복을 보장하도록 이미 고쳐져
# 있기 때문(그 파일 docstring 참고) — 여기서는 "이 프로젝트가 실제로 쓰기로 한 값"을
# 반영할 뿐이다. JVM은 이미 띄워진 뒤엔 타임존을 못 바꾸므로, SparkSession을 만들기
# 전에 프로세스 환경변수로 설정해야 한다(py4j가 이 환경을 물려받아 JVM 서브프로세스를 띄움).
os.environ.setdefault("TZ", "Asia/Seoul")


def get_spark(app_name: str = "ttareungyi-feature-engineering") -> SparkSession:
    """SparkSession을 만들거나(로컬) 기존 세션을 가져온다(EMR, spark-submit이 이미 만들어둠).

    args:
        app_name: Spark UI/로그에 표시될 애플리케이션 이름
    returns:
        SparkSession: builder.getOrCreate() 결과
    """
    # 위 os.environ["TZ"]와 짝을 이루는 세션 설정 — 반드시 같은 값(KST)으로 고정해야
    # timestamp_ntz/timestamp(tz-aware) 왕복 변환이 어긋나지 않는다.
    builder = SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "Asia/Seoul")

    # "EMR인지"를 SPARK_HOME/EMR_RELEASE_LABEL 환경변수 유무로 추측하던 예전 방식은
    # EMR 스텝을 자체 Spark 스텝 타입이 아니라 command-runner.jar(bash -c "spark-submit
    # ...")로 직접 띄우면 깨진다 — 그 실행 경로에는 이 두 환경변수가 아예 없어서
    # "로컬"로 오판했고, `--master yarn`을 조용히 `local[*]`로 덮어써 executor가 하나도
    # 안 뜬 채 드라이버 혼자 로컬 코어로 전체 데이터를 처리하다 YARN AM 하트비트를
    # 놓쳐 죽었다(실제 EMR 실행 + yarn logs로 확인, 2026-08-25/26 — BlockManager가
    # "driver" 하나만 등록되고 executor 등록 로그가 전혀 없었다).
    #
    # 그 뒤 "spark-submit이 이미 넘긴 --master를 SparkConf()로 되짚어보면 되지
    # 않나"로 고쳐봤지만 이것도 실제 EMR 실행에서 또 실패했다(2026-08-26) —
    # pyspark의 `SparkConf()`는 `SparkContext._jvm`이 아직 None일 때(=이 함수이
    # 처음 호출되는 시점, 아직 게이트웨이/SparkContext가 하나도 없는 상태)는 JVM
    # System property를 전혀 못 읽고 그냥 빈 로컬 dict를 반환한다(pyspark
    # `conf.py`) — 그래서 언제나 None으로 보였고 매번 local[*]로 강제됐다(yarn
    # logs에 "local-<timestamp>" 형식 appId, driver만 등록된 BlockManager로 재확인).
    #
    # 결론: 파이썬 쪽에서 "지금 yarn인지"를 추측할 방법이 없다 — 우리가 직접
    # 제출하는 모든 실제 spark-submit 호출(monthly_retrain.py, 디버그 스크립트)이
    # `spark.yarn.appMasterEnv.SPARK_ON_YARN=1`/`spark.executorEnv.SPARK_ON_YARN=1`을
    # 명시적으로 심어주기로 하고, 그 신호가 있으면 `.master()`를 아예 안 불러서
    # spark-submit이 넘긴 `--master yarn`이 그대로 살아남게 둔다.
    if os.environ.get("SPARK_ON_YARN"):
        return builder.getOrCreate()

    # 로컬 개발 편의를 위한 명시적 override(예: "local[2]") — 없으면 기본 local[*].
    builder = builder.master(os.environ.get("SPARK_MASTER", "local[*]"))

    # local[*] 모드는 드라이버 JVM 하나가 곧 "executor"이기도 해서, 기본
    # driver.memory(보통 1g)로는 실 데이터(3,500만 트립)에서 바로 OutOfMemoryError가
    # 난다. EMR에서는 클러스터/step 설정(--driver-memory, --executor-memory 등)이
    # 이 값을 결정하므로 여기서 건드리지 않는다 — 로컬 실행에서만 넉넉하게 잡아준다.
    builder = builder.config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
    # shuffle partition 기본값(200)은 로컬 코어 수 대비 과도하게 쪼개서 작은 파티션이
    # 너무 많아지는 오버헤드가 있다 — 로컬 코어 수 정도로 줄인다.
    builder = builder.config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
    # 로컬 개발도 항상 S3(MinIO)를 거치므로 Hadoop-S3A 커넥터가 필요하다 — EMR은
    # EMRFS가 이미 내장돼 있어서 이 설정 자체가 필요 없다(SPARK_ON_YARN 분기에서
    # 이미 return했으므로 여기 도달했다는 것 자체가 로컬 실행이라는 뜻). 버전은
    # pyspark 3.5.3이 내장한 Hadoop 3.3.4와 맞춘 것 — 다른 버전을 섞으면 클래스
    # 충돌(NoSuchMethodError 등)이 난다. 최초 실행 시 Maven에서 내려받으므로
    # 인터넷 연결이 필요하다(이후엔 로컬 ivy 캐시 재사용).
    builder = builder.config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # MinIO는 버킷을 서브도메인이 아니라 경로로 구분한다(virtual-hosted-style 미지원).
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
    builder = builder.config(
        "spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    )

    return builder.getOrCreate()
