"""#2·#3·#4 테스트 공용 픽스처.

`make_spec`은 `ColumnSpec.model_construct()`로 만든다 — 일반 생성자를 쓰면 validator가
돌아서 `range`의 부분 선언이나 `range`+`enum` 동시 선언 같은 "일부러 위반된" spec을
만들 수 없다. 정책 함수(#3)의 방어 코드는 이런 잘못된 spec에서도 안전한지를 테스트해야
하므로, 이 픽스처만 validator를 우회한다. 실제 YAML 로딩 경로(config.loader.load)는
`SourceConfig.model_validate`를 쓰므로 이 우회와 무관하게 여전히 막힌다.

storage.py·manifest.py 테스트가 공유하는 moto S3 환경 픽스처도 함께 둔다.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import pytest
from moto import mock_aws

from config.schema import ColumnSpec, Range
from validation import registry
from validation.types import Issue, RunContext

TEST_BUCKET = "test-bucket"
KST = ZoneInfo("Asia/Seoul")


@pytest.fixture
def make_spec():
    """ColumnSpec을 만든다. `range=(0, 200)`처럼 튜플로 주면 Range로 감싼다."""

    def _make(types=("int",), required=False, range=None, enum=None, default=None):
        bounds = Range.model_construct(min=range[0], max=range[1]) if isinstance(range, tuple) else range
        return ColumnSpec.model_construct(
            types=tuple(types),
            required=required,
            range=bounds,
            enum=enum,
            default=default,
            on_missing=None,
            on_outlier=None,
        )

    return _make


@pytest.fixture
def make_issue(make_spec):
    """Issue를 만든다. `required`를 생략하면 spec의 값을 따른다."""

    def _make(kind, spec=None, column="col", raw=None, required=None):
        spec = make_spec() if spec is None else spec
        return Issue(
            column=column,
            kind=kind,
            required=spec.required if required is None else required,
            raw_value=raw,
            spec=spec,
        )

    return _make


@pytest.fixture
def ctx():
    return RunContext(
        source_id="bike_station_realtime",
        window_start=datetime(2026, 8, 12, 14, 10, tzinfo=KST),
        window_end=datetime(2026, 8, 12, 14, 15, tzinfo=KST),
        attempt=1,
    )


@pytest.fixture
def clean_registry():
    """레지스트리는 전역 상태다. 테스트가 등록한 이름이 다른 테스트로 새지 않게 복원한다."""
    saved_policies = dict(registry._POLICIES)
    saved_row_policies = dict(registry._ROW_POLICIES)
    yield registry
    registry._POLICIES.clear()
    registry._POLICIES.update(saved_policies)
    registry._ROW_POLICIES.clear()
    registry._ROW_POLICIES.update(saved_row_policies)


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
