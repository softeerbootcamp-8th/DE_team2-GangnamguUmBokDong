"""S3 관련 테스트(dev_s3_io.py 등)가 공유하는 moto 기반 가짜 S3 환경 픽스처.

`collector/tests/conftest.py`와 동일한 패턴 — 실제 MinIO 컨테이너 없이
`moto.mock_aws()`로 완전히 격리된 가짜 S3를 매 테스트마다 새로 만든다.
"""

import boto3
import pytest
from core import s3 as s3_io
from moto import mock_aws

from ml_core import mlflow_tracking

TEST_BUCKET = "test-bucket"


@pytest.fixture(autouse=True)
def _s3_env(monkeypatch):
    """환경과 process-local S3 client cache를 테스트마다 격리한다."""
    s3_io._clear_client_cache()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("S3_BUCKET", TEST_BUCKET)
    yield
    s3_io._clear_client_cache()


@pytest.fixture(autouse=True)
def _bucket():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=TEST_BUCKET)
        yield


@pytest.fixture(autouse=True)
def _no_real_mlflow_server(monkeypatch):
    """`ml/training/tests/conftest.py`와 같은 이유 — 기본값(http://localhost:5000)이
    로컬 개발 중엔 실제로 떠 있을 수 있어(ops/compose의 mlflow 서비스), 아무 설정
    없이 테스트를 돌리면 실수로 진짜 서버에 run이 쌓일 수 있다(예:
    `profile_registry.push_profile()`을 부르는 테스트). 존재하지 않는 포트로
    가리켜 막는다 — 실제 동작을 검증하는 테스트는 자기 fixture에서 로컬 파일
    경로로 다시 덮어써서 쓴다.
    """
    monkeypatch.setattr(mlflow_tracking, "MLFLOW_TRACKING_URI", "http://localhost:0")
