"""S3/MinIO 입출력과 경로 규칙 생성.

S3와 연결되는 창구다. 경로 문자열을 만드는 곳도 여기 하나뿐이다.
dict·bytes 단위로만 주고받고, 그 값이 무엇을 뜻하는지는 해석하지 않는다.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Sequence
from datetime import date, datetime

import pyarrow.parquet as pq

from core.layout import archive_key
from core.s3 import (
    S3Object,
    delete_object,
    delete_objects,
    get_object_bytes,
    list_keys,
    list_objects,
    put_object_bytes,
    read_json,
    write_json,
    write_parquet,
)


def _bronze_prefix(source_id: str, window_start: datetime) -> str:
    """bronze 조각들이 모이는 공통 prefix를 만든다."""
    return (
        f"bronze/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}/"
    )


def _bronze_part_key(source_id: str, window_start: datetime, chunk_key: str) -> str:
    """bronze 조각 하나의 전체 키를 만든다."""
    return f"{_bronze_prefix(source_id, window_start)}part={chunk_key}.json.gz"


def write_bronze_part(
    source_id: str, window_start: datetime, chunk_key: str, chunk: bytes
) -> None:
    """bronze 조각을 gzip으로 압축해 저장한다."""
    key = _bronze_part_key(source_id, window_start, chunk_key)
    put_object_bytes(key, gzip.compress(chunk))


def read_bronze(source_id: str, window_start: datetime, parts: Sequence[str]) -> list[bytes]:
    """지정된 bronze 조각들을 읽어 압축을 해제한 뒤 순서대로 반환한다."""
    result = []
    for chunk_key in parts:
        key = _bronze_part_key(source_id, window_start, chunk_key)
        body = get_object_bytes(key)
        if body:
            result.append(gzip.decompress(body))
    return result


def clear_bronze(source_id: str, window_start: datetime) -> None:
    """해당 윈도우의 bronze 조각을 모두 삭제한다.
        수집 파이프라인이 에러로 뻗었을 때, 쓰레기 데이터가 남지 않도록 임시 조각들을 지워주는 역할."""
    prefix = _bronze_prefix(source_id, window_start)
    keys = list_keys(prefix)
    delete_objects(keys)


def _layer_key(layer: str, source_id: str, window_start: datetime, ext: str) -> str:
    """silver·quarantine처럼 윈도우당 파일 하나로 떨어지는 계층의 경로를 만든다."""
    return (
        f"{layer}/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.{ext}"
    )


def write_silver(source_id: str, window_start: datetime, table: pq.Table) -> str:
    """silver 테이블을 parquet으로 직렬화해 저장하고, 저장된 키를 반환한다."""
    key = _layer_key("silver", source_id, window_start, "parquet")
    write_parquet(table, key)
    return key


def write_quarantine(source_id: str, window_start: datetime, rows: list[dict]) -> str | None:
    """검증에 실패한 row들을 jsonl로 저장하고, 저장된 키를 반환한다."""
    if not rows:
        return None
    key = _layer_key("quarantine", source_id, window_start, "jsonl")
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    put_object_bytes(key, body.encode("utf-8"))
    return key


def _silver_date_prefix(source_id: str, day: date) -> str:
    """해당 날짜의 silver 조각들이 모이는 prefix를 만든다."""
    return f"silver/{source_id}/dt={day:%Y-%m-%d}/"


def list_silver_objects(source_id: str, day: date) -> list[S3Object]:
    """해당 날짜의 silver parquet을 메타(size·last_modified)와 함께 나열한다.

    compaction이 "이 날짜가 지난번 압축 이후 바뀌었는가"를 본문 없이 판정하는 근거다.
    parquet이 아닌 객체는 거른다 — nowcaster가 같은 prefix 아래에 자기 산출물을
    두는 경우가 있어(`silver/{src}/dt=.../hh=00/nowcast.parquet`) 확장자만으로는
    부족할 수 있으나, 그 소스는 compaction 대상이 아니다.
    """
    return [o for o in list_objects(_silver_date_prefix(source_id, day)) if o.key.endswith(".parquet")]


def write_archive(source_id: str, day: date, table: pq.Table) -> str:
    """하루치를 묶은 테이블을 archive parquet으로 저장하고 저장된 키를 반환한다.

    경로 규칙은 `core.layout`이 갖는다 — nowcaster도 같은 계층을 읽고 쓰므로
    한쪽만 바뀌면 조용히 어긋난다.
    """
    key = archive_key(source_id, day)
    write_parquet(table, key)
    return key


def _archive_manifest_key(source_id: str, day: date) -> str:
    """해당 날짜의 archive manifest 객체 키를 만든다."""
    return f"_archive_manifest/{source_id}/dt={day:%Y-%m-%d}.json"


def write_archive_manifest(source_id: str, day: date, data: dict) -> None:
    """해당 날짜의 압축 결과 요약을 저장한다."""
    write_json(_archive_manifest_key(source_id, day), data)


def read_archive_manifest(source_id: str, day: date) -> dict | None:
    """해당 날짜의 압축 결과 요약을 읽는다. 없으면 None(= 아직 압축한 적 없음)."""
    return read_json(_archive_manifest_key(source_id, day))


def _manifest_key(source_id: str, window_start: datetime) -> str:
    """해당 윈도우의 manifest 객체 키를 만든다."""
    return (
        f"_manifest/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.json"
    )


def _retry_marker_key(source_id: str, window_start: datetime) -> str:
    """해당 윈도우의 retry marker 객체 키를 만든다."""
    return f"_retry_queue/{source_id}/{window_start.isoformat()}.json"


def write_manifest(source_id: str, window_start: datetime, data: dict) -> None:
    """해당 윈도우의 manifest를 json으로 저장한다."""
    key = _manifest_key(source_id, window_start)
    write_json(key, data)


def read_manifest(source_id: str, window_start: datetime) -> dict | None:
    """해당 윈도우의 manifest를 읽는다. 없으면 None을 반환한다."""
    return read_json(_manifest_key(source_id, window_start))


def write_retry_marker(source_id: str, window_start: datetime, data: dict) -> None:
    """해당 윈도우의 retry marker를 JSON으로 저장한다."""
    key = _retry_marker_key(source_id, window_start)
    write_json(key, data)


def list_retry_markers(source_id: str) -> list[dict]:
    """해당 소스에 쌓인 retry marker를 모두 읽어 반환한다."""
    prefix = f"_retry_queue/{source_id}/"
    markers = []
    for key in list_keys(prefix):
        data = read_json(key)
        if data:
            markers.append(data)
    return markers


def delete_retry_marker(source_id: str, window_start: datetime) -> None:
    """해당 윈도우의 retry marker를 삭제한다."""
    key = _retry_marker_key(source_id, window_start)
    delete_object(key)
