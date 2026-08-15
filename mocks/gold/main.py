from __future__ import annotations

import argparse
import json
import os

import boto3
import psycopg2


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


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_INTERNAL_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_APP_DB", "app"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-key", required=True)
    args = parser.parse_args()

    bucket = os.environ.get("S3_BUCKET", "local-dev")
    gold_key = f"mock/gold/{args.run_key}.json"

    s3 = get_s3_client()
    response = s3.get_object(
        Bucket=bucket,
        Key=gold_key,
    )

    gold = json.loads(
        response["Body"].read().decode("utf-8")
    )

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO forecast_points (
                    sta_id,
                    predicted_dttm,
                    predicted_rent_cnt,
                    predicted_return_cnt,
                    batch_run_at
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sta_id, predicted_dttm)
                DO UPDATE SET
                    predicted_rent_cnt = EXCLUDED.predicted_rent_cnt,
                    predicted_return_cnt = EXCLUDED.predicted_return_cnt,
                    batch_run_at = EXCLUDED.batch_run_at
                """,
                (
                    gold["sta_id"],
                    gold["predicted_dttm"],
                    gold["predicted_rent_cnt"],
                    gold["predicted_return_cnt"],
                    gold["batch_run_at"],
                ),
            )

    print(f"[gold] read s3://{bucket}/{gold_key}")
    print(
        "[gold] upserted forecast_points "
        f"sta_id={gold['sta_id']} "
        f"predicted_dttm={gold['predicted_dttm']}"
    )


if __name__ == "__main__":
    main()