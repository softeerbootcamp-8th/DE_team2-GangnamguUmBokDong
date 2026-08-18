"""테이블별 S3 읽기, 변환 함수 및 DB Upsert 스펙 레지스트리를 정의한다."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

import reader
import transform

_TABLES_YAML_PATH = Path(__file__).parent / "tables.yaml"


def _read_silver_as_pandas(source_id: str, window_start: datetime) -> pd.DataFrame:
    """지정된 Silver 소스의 Parquet 데이터를 Pandas DataFrame으로 읽어온다."""
    return reader.read_silver(source_id, window_start).to_pandas()


@dataclass(frozen=True)
class TableSpec:
    """Gold 테이블별 S3 소스, 변환 함수, DB Upsert 충돌 및 갱신 컬럼 스펙."""

    source_id: str
    transform: Callable
    conflict_cols: list[str]
    update_cols: list[str]
    reader: Callable[[datetime], pd.DataFrame] | None = None
    guard_col: str | None = None

    def read(self, window_start: datetime) -> pd.DataFrame:
        """지정된 윈도우 시각의 S3 데이터를 읽어 Pandas DataFrame으로 반환한다."""
        if self.reader is not None:
            return self.reader(window_start)
        return _read_silver_as_pandas(self.source_id, window_start)


def _load_table_specs() -> dict[str, TableSpec]:
    """tables.yaml을 읽어 TableSpec 레지스트리 딕셔너리를 생성한다."""
    raw_specs = yaml.safe_load(_TABLES_YAML_PATH.read_text(encoding="utf-8"))
    specs: dict[str, TableSpec] = {}

    for table_name, raw in raw_specs.items():
        transform_fn = getattr(transform, raw["transform"])

        reader_fn = None
        if raw.get("reader") == "read_predictions":
            reader_fn = lambda ws: reader.read_predictions(ws).to_pandas()

        specs[table_name] = TableSpec(
            source_id=raw["source_id"],
            transform=transform_fn,
            conflict_cols=raw["conflict_cols"],
            update_cols=raw["update_cols"],
            reader=reader_fn,
            guard_col=raw.get("guard_col"),
        )
    return specs


TABLE_SPECS: dict[str, TableSpec] = _load_table_specs()
