import tempfile
from pathlib import Path

import lightgbm as lgb
from core.s3 import get_object_bytes, put_object_bytes


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
