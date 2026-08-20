"""Legacy loader registry가 source·derived authority를 fail closed하는지 검증한다."""

from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa
import pytest

import config
from config import (
    RETIRED_DERIVED_SOURCE_IDS,
    RETIRED_DERIVED_TABLES,
    RETIRED_SOURCE_IDS,
    RETIRED_SOURCE_TABLES,
    TABLE_SPECS,
    RetiredSourceGoldPathError,
    TableSpec,
    target_table_for,
)


def test_legacy_registry_is_empty() -> None:
    """모든 운영 Gold 적재가 publication publisher로 전환돼 YAML registry는 비어 있다."""
    assert TABLE_SPECS == {}


@pytest.mark.parametrize(
    "table",
    sorted(
        RETIRED_SOURCE_TABLES
        | RETIRED_SOURCE_IDS
        | RETIRED_DERIVED_TABLES
        | RETIRED_DERIVED_SOURCE_IDS
    ),
)
def test_retired_path_cannot_resolve_target(table: str) -> None:
    """Source와 derived legacy 이름을 우회 target으로도 해소하지 않는다."""
    with pytest.raises(RetiredSourceGoldPathError, match="publication publisher"):
        target_table_for(table)


@pytest.mark.parametrize(
    "yaml_text",
    [
        """forecast_points:
  source_id: replacement
  transform: forecast_points_from_predictions
  conflict_cols: [sta_id]
  update_cols: []
""",
        """renamed_event_target:
  source_id: cultural_event
  transform: cultural_events_from_silver
  conflict_cols: [event_id]
  update_cols: []
""",
        """renamed_prediction_target:
  source_id: ml_predictions
  transform: forecast_points_from_predictions
  conflict_cols: [sta_id]
  update_cols: []
""",
        """station_demand_forecast:
  source_id: replacement
  transform: forecast_points_from_predictions
  conflict_cols: [sta_id]
  update_cols: []
""",
    ],
)
def test_registry_rejects_reintroduced_authority(
    tmp_path,
    monkeypatch,
    yaml_text: str,
) -> None:
    """Retired table명이나 source ID를 YAML에 다시 넣어도 import부터 실패한다."""
    path = tmp_path / "tables.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setattr(config, "_TABLES_YAML_PATH", path)

    with pytest.raises(RetiredSourceGoldPathError, match="폐기된 source publication"):
        config._load_table_specs()


def test_generic_non_authority_table_spec_reader_is_preserved(monkeypatch) -> None:
    """향후 비권한 utility가 쓰는 generic TableSpec read 동작은 보존한다."""
    captured = {}

    def fake_read_silver(source_id, window_start):
        """호출 인자를 기록하고 최소 Arrow table을 반환한다."""
        captured.update(source_id=source_id, window_start=window_start)
        return pa.table({"id": ["101"]})

    monkeypatch.setattr("config.reader.read_silver", fake_read_silver)
    spec = TableSpec(
        source_id="non_authoritative_fixture",
        transform=lambda frame: [],
        conflict_cols=["id"],
        update_cols=[],
    )
    window = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)

    result = spec.read(window)

    assert captured == {
        "source_id": "non_authoritative_fixture",
        "window_start": window,
    }
    assert isinstance(result, pd.DataFrame)
