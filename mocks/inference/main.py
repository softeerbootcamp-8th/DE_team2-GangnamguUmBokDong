from __future__ import annotations

import argparse
import json
import os

import boto3


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
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

    bucket = os.environ.get("S3_BUCKET", "local-dev")

    input_key = (
        f"mock/inference_input/{args.run_key}.json"
    )
    prediction_key = (
        f"mock/prediction/{args.run_key}.json"
    )

    client = get_s3_client()

    response = client.get_object(
        Bucket=bucket,
        Key=input_key,
    )

    inference_input = json.loads(
        response["Body"].read().decode("utf-8")
    )

    output = {
        "window_start": args.window_start,
        "station_id": inference_input["station_id"],
        "predicted_dttm": args.window_start,
        "predicted_rent_cnt": 5.2,
        "predicted_return_cnt": 3.1,
    }

    client.put_object(
        Bucket=bucket,
        Key=prediction_key,
        Body=json.dumps(
            output,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"[inference] read s3://{bucket}/{input_key}")
    print(f"[inference] wrote s3://{bucket}/{prediction_key}")


if __name__ == "__main__":
    main()