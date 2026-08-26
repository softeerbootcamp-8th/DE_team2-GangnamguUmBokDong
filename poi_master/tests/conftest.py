"""POI Master 테스트가 공유하는 공식 fixture 로더를 제공한다."""

from pathlib import Path

import pytest
from core import s3 as s3_io

from source import Attachment, SourceAssets

_ROOT = Path(__file__).resolve().parents[2]
_DATA = _ROOT / "normalizer" / "data"


@pytest.fixture(autouse=True)
def _clear_s3_client_cache():
    """각 POI publication 테스트의 moto client를 완전히 격리한다."""
    s3_io._clear_client_cache()
    yield
    s3_io._clear_client_cache()


def real_source_assets() -> SourceAssets:
    """저장소에 동결된 현재 121장소 XLSX·ZIP을 source 응답 형태로 반환한다."""
    return SourceAssets(
        list_attachment=Attachment(
            role="list",
            filename="서울시 주요 121장소 목록.xlsx",
            sequence="23",
            modified_date="2026.04.02.",
            declared_place_count=121,
        ),
        areas_attachment=Attachment(
            role="areas",
            filename="서울시 주요 121장소 영역.zip",
            sequence="24",
            modified_date="2026.04.02.",
            declared_place_count=121,
        ),
        list_bytes=(_DATA / "서울시 주요 121장소 목록.xlsx").read_bytes(),
        areas_bytes=(_DATA / "서울시 주요 121장소 영역.zip").read_bytes(),
    )
