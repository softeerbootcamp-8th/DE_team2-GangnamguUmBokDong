"""feature_engineering 테스트 전용 pytest 설정.

SparkSession 기동(JVM 부트스트랩)은 테스트 로직 자체보다 훨씬 비싸다(수~수십 초).
예전에는 dev_spark_build_features.py/dev_spark_incremental.py/dev_spark_rolling_parity.py가
각자 `scope="module"` spark 픽스처를 따로 정의하고 teardown에서 `session.stop()`까지
불렀는데, 그러면 모듈이 바뀔 때마다(파일마다) JVM이 통째로 재기동됐다(issue #46 —
테스트 12개에 253초, 평균 21초/테스트로 사실상 Spark 기동 비용이 대부분). 여기서
`scope="session"` 픽스처 하나로 통일해 pytest 프로세스 전체가 SparkSession을 한 번만
기동/종료하도록 한다 — 개별 테스트 파일의 `spark` 픽스처 정의는 지웠다(있으면 이
conftest보다 우선 적용돼 무의미해짐).
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# timestamp_ntz/timestamp(tz-aware) 왕복 어긋남 방지 — feature_engineering/spark_session.py 참고.
os.environ.setdefault("TZ", "Asia/Seoul")


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-feature-engineering")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    yield session
    session.stop()
