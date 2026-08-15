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


def read_json(client, bucket: str, key: str) -> dict:
    response = client.get_object(
        Bucket=bucket,
        Key=key,
    )

    return json.loads(
        response["Body"].read().decode("utf-8")
    )


def write_json(
    client,
    bucket: str,
    key: str,
    data: dict,
) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
    )


def build_inference_input(run_key: str) -> None:
    bucket = os.environ.get("S3_BUCKET", "local-dev")

    silver_key = f"mock/silver/{run_key}.json"
    output_key = f"mock/inference_input/{run_key}.json"

    client = get_s3_client()

    silver = read_json(
        client,
        bucket,
        silver_key,
    )

    output = {
        "window_start": silver["window_start"],
        "station_id": silver["station_id"],
        "current_bikes": silver["current_bikes"],
        "temperature": silver["temperature"],
        "population": silver["population"],
    }

    write_json(
        client,
        bucket,
        output_key,
        output,
    )

    print(f"[combiner] read s3://{bucket}/{silver_key}")
    print(f"[combiner] wrote s3://{bucket}/{output_key}")


def build_serving_output(run_key: str) -> None:
    bucket = os.environ.get("S3_BUCKET", "local-dev")

    silver_key = f"mock/silver/{run_key}.json"
    prediction_key = f"mock/prediction/{run_key}.json"
    gold_key = f"mock/gold/{run_key}.json"

    client = get_s3_client()

    silver = read_json(
        client,
        bucket,
        silver_key,
    )

    prediction = read_json(
        client,
        bucket,
        prediction_key,
    )

    output = {
        "sta_id": silver["station_id"],
        "predicted_dttm": prediction["predicted_dttm"],
        "predicted_rent_cnt": prediction["predicted_rent_cnt"],
        "predicted_return_cnt": prediction["predicted_return_cnt"],
        "batch_run_at": prediction["window_start"],
    }

    write_json(
        client,
        bucket,
        gold_key,
        output,
    )

    print(f"[combiner] read s3://{bucket}/{silver_key}")
    print(f"[combiner] read s3://{bucket}/{prediction_key}")
    print(f"[combiner] wrote s3://{bucket}/{gold_key}")


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--job",
        required=True,
        choices=[
            "inference-input",
            "serving-output",
        ],
    )

    parser.add_argument(
        "--run-key",
        required=True,
    )

    args = parser.parse_args()

    if args.job == "inference-input":
        build_inference_input(args.run_key)
    else:
        build_serving_output(args.run_key)


if __name__ == "__main__":
    main()