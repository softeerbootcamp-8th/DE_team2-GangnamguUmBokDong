from __future__ import annotations

import argparse
import json
import os

import boto3


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get(
            "AWS_ACCESS_KEY_ID",
            "minioadmin",
        ),
        aws_secret_access_key=os.environ.get(
            "AWS_SECRET_ACCESS_KEY",
            "minioadmin",
        ),
        region_name="ap-northeast-2",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--run-key", required=True)
    args = parser.parse_args()

    payload = {
        "window_start": args.window_start,
        "station_id": 102,
        "station_name": "102. 망원역 1번출구 앞",
        "current_bikes": 8,
        "temperature": 27.5,
        "population": 3200,
    }

    bucket = os.environ.get("S3_BUCKET", "local-dev")
    key = f"mock/silver/{args.run_key}.json"

    client = get_s3_client()

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"[collector] wrote s3://{bucket}/{key}")


if __name__ == "__main__":
    main()