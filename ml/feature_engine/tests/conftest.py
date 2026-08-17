"""feature_engine 테스트 전용 pytest 설정.

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

import boto3
import pytest
from moto import mock_aws
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
# timestamp_ntz/timestamp(tz-aware) 왕복 어긋남 방지 — feature_engine/spark_session.py 참고.
os.environ.setdefault("TZ", "Asia/Seoul")

# watermark.py는 Spark가 아니라 ml_core.s3_io(boto3)로 직접 S3를 두드린다 — 테스트가
# WATERMARK_PATH를 로컬 tmp_path 문자열로 monkeypatch해도(dev_spark_incremental.py),
# s3_io는 그 문자열을 그대로 S3 키로 써버려 실제 MinIO를 오염시킨다(실제로 한 번
# `private/var/folders/.../_watermark.json` 키로 새 나간 적이 있음). moto로 완전히
# 격리된 가짜 S3를 매 테스트마다 만들어 이 오염을 막는다 — Spark 자신의 로컬 parquet
# 읽기/쓰기는 이 픽스처와 무관하게(boto3를 안 거치므로) 그대로 tmp_path를 쓴다.
_TEST_BUCKET = "test-bucket"


@pytest.fixture(autouse=True)
def _s3_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("S3_BUCKET", _TEST_BUCKET)


@pytest.fixture(autouse=True)
def _mock_bucket():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=_TEST_BUCKET)
        yield


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
