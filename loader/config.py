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
from retention_config import RETENTION_GRACE

_TABLES_YAML_PATH = Path(__file__).parent / "tables.yaml"

# 논리 spec 이름 → 물리 DB 테이블 이름 매핑
# 서로 다른 여러 데이터 소스가 단일 Gold 테이블로 통합 적재되는 경우를 처리한다:
# 1) 문화/공연 행사: cultural_event(문화행사)와 performance_event(공연행사) → cultural_events 테이블로 병합
# 2) 날씨 예보: weather_short_term_forecast(단기)와 weather_ultra_short_forecast(초단기) → weather_forecast 테이블로 병합
TABLE_ALIASES = {
    "cultural_events_performance": "cultural_events",
    "weather_forecast_ultra": "weather_forecast",
}


def target_table_for(table: str) -> str:
    """논리 spec 이름을 실제 적재/정리 대상인 물리 테이블 이름으로 해소한다."""
    return TABLE_ALIASES.get(table, table)


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
    expire_col: str | None = None
    """만료 판정에 쓰는 컬럼(예: forecast_dttm, predicted_dttm, end_date). 지정된
    테이블은 적재 직후 유예기간이 지난 행을 정리한다(loader/retention_config.py,
    main.py의 _delete_expired 참고). None이면 정리 대상이 아니다(마스터 데이터나
    최신 1건만 유지하는 테이블 등)."""
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
        if raw.get("reader"):
            reader_name = raw["reader"]
            # getattr을 람다 밖에서 한 번만 실행해 캡처하면 안 된다 — 테스트가
            # `monkeypatch.setattr("config.reader.read_predictions", ...)`처럼
            # 모듈 속성을 나중에 바꿔치기하므로, 호출 시점마다 다시 조회해야
            # 그 패치가 반영된다(기존 하드코딩 버전의 lambda ws: reader.read_predictions(ws)와
            # 동일한 지연 조회 방식을 유지).
            reader_fn = lambda ws, reader_name=reader_name: getattr(reader, reader_name)(ws).to_pandas()

        specs[table_name] = TableSpec(
            source_id=raw["source_id"],
            transform=transform_fn,
            conflict_cols=raw["conflict_cols"],
            update_cols=raw["update_cols"],
            reader=reader_fn,
            expire_col=raw.get("expire_col"),
            guard_col=raw.get("guard_col"),
        )
    return specs


def _validate_retention_config(specs: dict[str, TableSpec]) -> None:
    """expire_col이 선언된 모든 스펙에 유예기간이 있는지 임포트 시점에 확인한다.

    적재 시점(main.run)에 검사하면 upsert 뒤 commit 전에 터져 그 실행의 적재까지
    통째로 롤백되므로, 설정 오류는 여기서 미리 잡는다."""
    missing = sorted(
        {target_table_for(name) for name, spec in specs.items() if spec.expire_col} - set(RETENTION_GRACE)
    )
    if missing:
        raise ValueError(
            f"tables.yaml에 expire_col이 선언된 테이블 {missing}의 유예기간이 없다. "
            f"loader/retention_config.py의 RETENTION_GRACE에 추가하라."
        )


TABLE_SPECS: dict[str, TableSpec] = _load_table_specs()
_validate_retention_config(TABLE_SPECS)
