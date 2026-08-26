"""Inference 테스트의 process-local runtime 자원을 격리한다."""

import pytest
from core import s3 as s3_io


@pytest.fixture(autouse=True)
def _clear_s3_client_cache():
    """Inline moto context를 포함해 테스트마다 S3 client cache를 비운다."""
    s3_io._clear_client_cache()
    yield
    s3_io._clear_client_cache()
