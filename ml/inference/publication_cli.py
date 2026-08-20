"""Serving plan에 결합된 immutable inference authority를 실행하는 운영 CLI다."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import redirect_stdout
from typing import Any
from urllib.parse import urlsplit

import boto3
from core.gold_publication import ContractViolation, S3ImmutableObjectStore
from core.inference_catalog import S3InferenceRevisionCatalog
from core.serving_plan_input import read_serving_plan_inference_inputs
from ml_core.serving_release import S3ServingReleasePointerStore

from .publication import run_and_publish_inference


def run(*, plan_uri: str, plan_sha256: str) -> dict[str, dict[str, str]]:
    """Plan actual bytes에서 inference 입력을 얻어 manifest ref 하나를 반환한다."""
    bucket = _required_bucket()
    _require_uri_bucket(plan_uri, bucket, "serving plan")
    client = _s3_client()
    object_store = S3ImmutableObjectStore(client)
    inputs = read_serving_plan_inference_inputs(
        object_store,
        plan_uri=plan_uri,
        plan_sha256=plan_sha256,
    )
    _require_uri_bucket(inputs.object_base_uri, bucket, "inference object base")
    catalog = S3InferenceRevisionCatalog(
        client,
        object_store,
        bucket=bucket,
        object_base_uri=inputs.object_base_uri,
    )
    published = run_and_publish_inference(
        logical_dttm=inputs.logical_dttm,
        station_dependency=inputs.station_dependency,
        serving_plan=inputs.serving_plan,
        expected_sta_ids=inputs.expected_sta_ids,
        object_base_uri=inputs.object_base_uri,
        object_store=object_store,
        pointer_store=S3ServingReleasePointerStore(client, bucket),
        revision_catalog=catalog,
    )
    return {
        "inference": {
            "byte_sha256": published.manifest_sha256,
            "uri": published.manifest_uri,
        }
    }


def _required_bucket() -> str:
    """필수 S3_BUCKET을 안전한 bucket name으로 검증한다."""
    try:
        value = os.environ["S3_BUCKET"]
    except KeyError as exc:
        raise ContractViolation("필수 환경변수가 없습니다: S3_BUCKET") from exc
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or "/" in value
        or any(character.isspace() for character in value)
    ):
        raise ContractViolation("S3_BUCKET 형식이 잘못됐습니다.")
    return value


def _require_uri_bucket(uri: str, bucket: str, name: str) -> None:
    """S3 ref/base가 운영 bucket과 정확히 같은지 검증한다."""
    if type(uri) is not str:
        raise ContractViolation(f"{name} URI는 exact string이어야 합니다.")
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "s3"
        or parsed.netloc != bucket
        or not parsed.path.lstrip("/")
        or parsed.query
        or parsed.fragment
    ):
        raise ContractViolation(f"{name} URI가 S3_BUCKET과 다릅니다.")


def _s3_client() -> Any:
    """표준 credential chain과 선택적 S3 endpoint로 단일 client를 만든다."""
    return boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None)


def main() -> int:
    """운영 인자를 실행하고 마지막 stdout 한 줄에 compact JSON ref만 쓴다."""
    parser = argparse.ArgumentParser(
        description="Pinned inference를 immutable 게시한다."
    )
    parser.add_argument("--plan-uri", required=True)
    parser.add_argument("--plan-sha256", required=True)
    args = parser.parse_args()
    try:
        with redirect_stdout(sys.stderr):
            result = run(plan_uri=args.plan_uri, plan_sha256=args.plan_sha256)
    except Exception as exc:  # noqa: BLE001 - CLI 경계에서 nonzero로 변환한다.
        print(f"Inference publication failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
