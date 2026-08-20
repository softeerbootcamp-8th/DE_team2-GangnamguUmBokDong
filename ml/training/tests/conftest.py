"""테스트 공용 픽스처. moto S3 목킹은 collector/tests/conftest.py 패턴을 그대로 따른다."""

import boto3
import pytest
from ml_core import mlflow_tracking
from moto import mock_aws

TEST_BUCKET = "test-bucket"


@pytest.fixture(autouse=True)
def _s3_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("S3_BUCKET", TEST_BUCKET)


@pytest.fixture(autouse=True)
def _no_real_mlflow_server(monkeypatch):
    """기본값(http://localhost:5000)이 로컬 개발 중엔 실제로 떠 있을 수 있어(이
    저장소의 ops/compose mlflow 서비스), 아무 설정 없이 테스트를 돌리면 진짜
    서버에 실험 run이 쌓여버릴 수 있다 — 존재하지 않는 포트로 가리켜 실수로도
    네트워크를 안 타게 막는다. 실제 mlflow 동작을 검증하는 테스트(예:
    dev_train_target_mlflow.py)는 이 값을 자기 fixture에서 다시 로컬 파일
    경로로 덮어써서 쓴다(monkeypatch는 나중에 setattr한 값이 우선이므로 문제없음).
    """
    monkeypatch.setattr(mlflow_tracking, "MLFLOW_TRACKING_URI", "http://localhost:0")


@pytest.fixture(autouse=True)
def _bucket():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=TEST_BUCKET)
        yield
