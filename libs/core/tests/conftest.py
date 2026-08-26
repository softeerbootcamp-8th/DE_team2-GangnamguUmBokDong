"""테스트 공용 픽스처. moto S3 목킹은 collector/tests/conftest.py 패턴을 그대로 따른다."""

import boto3
import pytest
from core import s3
from moto import mock_aws

TEST_BUCKET = "test-bucket"


@pytest.fixture(autouse=True)
def _s3_env(monkeypatch):
    s3._clear_client_cache()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("S3_BUCKET", TEST_BUCKET)
    yield
    s3._clear_client_cache()


@pytest.fixture(autouse=True)
def _bucket():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=TEST_BUCKET)
        yield
