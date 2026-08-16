"""테스트 공용 픽스처: moto 기반 S3 환경."""

from zoneinfo import ZoneInfo

import boto3
import pytest
from moto import mock_aws

TEST_BUCKET = "test-bucket"
KST = ZoneInfo("Asia/Seoul")


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
