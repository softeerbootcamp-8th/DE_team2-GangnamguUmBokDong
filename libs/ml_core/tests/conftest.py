"""S3 관련 테스트(dev_s3_io.py 등)가 공유하는 moto 기반 가짜 S3 환경 픽스처.

`collector/tests/conftest.py`와 동일한 패턴 — 실제 MinIO 컨테이너 없이
`moto.mock_aws()`로 완전히 격리된 가짜 S3를 매 테스트마다 새로 만든다.
"""

import boto3
import pytest
from moto import mock_aws

TEST_BUCKET = "test-bucket"


@pytest.fixture(autouse=True)
def _s3_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("S3_BUCKET", TEST_BUCKET)


@pytest.fixture(autouse=True)
def _bucket():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=TEST_BUCKET)
        yield
