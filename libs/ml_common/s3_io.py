"""S3/MinIO 입출력 공통 헬퍼.

`collector/storage.py`와 같은 패턴(f-string으로 키를 직접 만들고, boto3에는
`Bucket=`/`Key=`를 항상 별도 인자로 넘김 — URI 문자열을 이어붙이는 추상화는
안 씀)을 따른다. `ml_common`은 `collector`를 import하지 않는다(두 모듈은
서로 다른 인스턴스에 독립 배포되므로 의존 관계를 만들면 안 됨) — 그래서
같은 스타일의 코드를 여기 독립적으로 둔다. `collector/storage.py`가 bytes
단위로만 주고받는 것과 달리, 여기는 `feature_engineering`/`training`/
`inference`가 바로 쓸 수 있게 pandas DataFrame/dict/LightGBM Booster 단위로
한 겹 더 감싼다.
"""

from __future__ import annotations

import io
import json
import os
import re
import tempfile
from pathlib import Path

import boto3
import lightgbm as lgb
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def _load_env_file() -> None:
    """저장소 루트의 .env 파일을 파싱하여 환경 변수에 로드한다 (표준 라이브러리만 사용).

    `dev/s3_client.py`와 동일한 로직이다 — `ml_common`은 `dev/`를 import하지
    않으므로(배포 대상이 다른 독립 패키지) 같은 파서를 복제해서 쓴다.
    """
    # 이 파일은 libs/ml_common/s3_io.py에 있고 저장소 루트는 그 조상 2단계 위.
    root_dir = Path(__file__).resolve().parent.parent.parent
    env_path = root_dir / ".env"
    if not env_path.exists():
        return

    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("'\"")
                val = re.sub(
                    r"\$\{([A-Za-z0-9_]+)(?::-([^}]+))?\}",
                    lambda m: os.environ.get(m.group(1), m.group(2) or ""),
                    val,
                )
                if key not in os.environ:
                    os.environ[key] = val


_load_env_file()


def _client():
    """S3 호환 클라이언트를 생성한다."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _bucket() -> str:
    """대상 S3 버킷 이름을 환경 변수에서 읽어 반환한다."""
    return os.environ["S3_BUCKET"]


def get_object_bytes(key: str) -> bytes | None:
    """S3 객체를 bytes로 읽는다.

    args:
        key: 읽을 객체의 전체 키
    returns:
        객체 본문 bytes, 키가 없으면 None
    raises:
        ClientError: NoSuchKey가 아닌 다른 S3 오류가 발생했을 때
    """
    try:
        return _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def put_object_bytes(key: str, body: bytes) -> None:
    """bytes를 S3 객체로 저장한다."""
    _client().put_object(Bucket=_bucket(), Key=key, Body=body)


def read_parquet(key: str, columns: list[str] | None = None) -> pd.DataFrame | None:
    """S3의 parquet 객체를 pandas DataFrame으로 읽는다.

    args:
        key: 읽을 parquet 객체의 전체 키
        columns: 읽을 컬럼만 지정(None이면 전체)
    returns:
        pd.DataFrame, 키가 없으면 None
    """
    body = get_object_bytes(key)
    if body is None:
        return None
    return pq.read_table(io.BytesIO(body), columns=columns).to_pandas()


def write_parquet(df: pd.DataFrame, key: str) -> None:
    """pandas DataFrame을 parquet으로 직렬화해 S3에 저장한다."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    put_object_bytes(key, buffer.getvalue())


def read_json(key: str):
    """S3의 JSON 객체를 읽는다 (dict 또는 list — JSON 최상위 값 그대로). 키가 없으면 None."""
    body = get_object_bytes(key)
    if body is None:
        return None
    return json.loads(body)


def write_json(key: str, data) -> None:
    """dict 또는 list를 JSON으로 직렬화해 S3에 저장한다."""
    put_object_bytes(key, json.dumps(data, ensure_ascii=False).encode("utf-8"))


def list_keys(prefix: str) -> list[str]:
    """주어진 prefix 아래 모든 객체 키를 나열한다.

    `list_objects_v2`는 한 번에 최대 1000개까지만 반환하므로 paginator로 전체를 모은다
    (`collector/storage.py`의 `clear_bronze`/`list_retry_markers`와 동일 패턴).

    args:
        prefix: 나열할 키 prefix
    returns:
        prefix로 시작하는 모든 객체 키 목록
    """
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix)
        for obj in page.get("Contents", [])
    ]


def stage_and_upload_booster(booster: lgb.Booster, key: str) -> None:
    """LightGBM Booster를 로컬 임시 파일에 저장한 뒤 S3에 업로드한다.

    LightGBM의 `save_model()`은 로컬 파일 경로 문자열만 받고 S3 URI를 모른다
    (이 라이브러리 자체의 한계 — S3 전환과 무관하게 EMR/EC2 운영 환경에서도
    항상 필요한 어댑터다). 저장 직후 별도로 다시 열어 읽는 이유는, LightGBM이
    파일을 자기 쪽에서 직접 쓰기 때문에 우리가 들고 있던 파일 객체의 버퍼/위치
    상태를 신뢰할 수 없어서다 — 경로만 빌리고 실제 바이트는 새로 읽는다.
    """
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        booster.save_model(str(tmp_path))
        put_object_bytes(key, tmp_path.read_bytes())
    finally:
        tmp_path.unlink(missing_ok=True)


def download_and_load_booster(key: str) -> lgb.Booster:
    """S3의 LightGBM 모델 파일을 로컬 임시 파일로 내려받아 Booster로 로드한다."""
    body = get_object_bytes(key)
    if body is None:
        raise FileNotFoundError(f"모델 파일 없음: {key}")
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        tmp_path.write_bytes(body)
        return lgb.Booster(model_file=str(tmp_path))
    finally:
        tmp_path.unlink(missing_ok=True)
