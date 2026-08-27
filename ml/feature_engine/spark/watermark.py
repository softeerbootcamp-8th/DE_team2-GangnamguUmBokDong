"""파라미터 조합(피처 뷰)별로 "피처마트가 어디까지(어느 시각까지) 계산됐는지" 추적한다.

`src/experiment_log.py`(학습 실행 기록, append-only 로그)와는 목적이 다르다 — 여기는
"지금 상태" 하나만 알면 되는 워터마크라 파일 하나에 최신값만 덮어쓴다. Spark의
파일 리더/라이터를 쓰지 않고 `ml_core.s3_io`(boto3)로 직접 처리한다 — 워터마크
파일은 초소형(몇십 바이트)이라 Spark를 띄울 이유가 없다. `path`는 `config.
WATERMARK_PATH`(s3a:// 스킴이 아니라 순수 S3 키 — config.py의 `OUTPUT_ROOT_KEY`
참고)를 그대로 받는다.
"""

from __future__ import annotations

from datetime import UTC, datetime

from core import s3 as s3_io


def read_watermark(path: str) -> dict | None:
    """워터마크를 읽는다. 없으면(=이 파라미터 조합으로 처음 만드는 것) None을 반환한다.

    args:
        path: config.WATERMARK_PATH (파라미터 조합별 S3 키)
    returns:
        dict | None: {"max_hour_ts": "YYYY-MM-DDTHH:MM:SS", "params": {...}, "updated_at": "..."}
            또는 객체가 없으면 None
    """
    return s3_io.read_json(path)


def write_watermark(path: str, max_hour_ts: str, params: dict) -> dict:
    """워터마크를 기록한다 (기존 값을 덮어씀 — "지금 상태" 하나만 유지).

    args:
        path: config.WATERMARK_PATH
        max_hour_ts: 이 피처마트에 반영된 가장 최신 hour_ts (ISO 문자열)
        params: 이 피처마트를 만든 파라미터 조합 (window/embargo/tick 등, 기록용)
    returns:
        dict: 실제로 기록된 내용
    """
    record = {
        "max_hour_ts": max_hour_ts,
        "params": params,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    s3_io.write_json(path, record)
    return record


def is_fresh(watermark: dict | None, max_age_hours: float = 24.0) -> bool:
    """워터마크의 updated_at이 최근 max_age_hours시간 이내인지 확인한다.

    args:
        watermark: read_watermark()로 읽은 dict 또는 None
        max_age_hours: 신선하다고 간주할 최대 경과 시간 (시간 단위)
    returns:
        bool: 워터마크가 존재하고 max_age_hours 이내에 갱신되었으면 True
    """
    if watermark is None or not watermark.get("updated_at"):
        return False
    try:
        updated_at = datetime.fromisoformat(watermark["updated_at"])
        age_seconds = (datetime.now(UTC) - updated_at).total_seconds()
        return age_seconds <= max_age_hours * 3600
    except (ValueError, TypeError):
        return False


def multi_horizon_watermark_path(model_name: str | None = None) -> str:
    """Multi-horizon feature mart의 워터마크 S3 키를 반환한다.

    args:
        model_name: "rental", "return", 또는 None(공통)
    returns:
        str: S3 키 경로
    """
    from . import config

    if model_name:
        return (
            f"{config.OUTPUT_ROOT_KEY}/training_anchor_a{config.TRAIN_ANCHOR_TICK_MINUTES}/"
            f"_multi_horizon_{model_name}_watermark.json"
        )
    return config.MULTI_HORIZON_WATERMARK_PATH

