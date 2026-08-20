"""Loader 레지스트리의 파생 경로와 폐기된 원천 권한 경계를 검증한다."""

from datetime import UTC, datetime

import config
import pandas as pd
import pyarrow as pa
import pytest
from config import (
    RETIRED_SOURCE_IDS,
    RETIRED_SOURCE_TABLES,
    TABLE_SPECS,
    RetiredSourceGoldPathError,
    TableSpec,
    target_table_for,
)


def test_only_derived_table_specs_remain_registered() -> None:
    """#153에서 전환할 파생·ML 적재 스펙만 legacy 레지스트리에 남긴다."""
    assert set(TABLE_SPECS) == {
        "forecast_points",
        "station_urgency",
        "rebalance_routes",
        "rebalance_route_stops",
    }
    assert RETIRED_SOURCE_TABLES.isdisjoint(TABLE_SPECS)
    assert RETIRED_SOURCE_IDS.isdisjoint(
        spec.source_id for spec in TABLE_SPECS.values()
    )


@pytest.mark.parametrize("table", sorted(RETIRED_SOURCE_TABLES | RETIRED_SOURCE_IDS))
def test_retired_source_path_cannot_resolve_a_target_table(table: str) -> None:
    """폐기된 테이블명과 source_id는 우회 이름으로도 Gold target이 될 수 없다."""
    with pytest.raises(RetiredSourceGoldPathError, match="publication publisher"):
        target_table_for(table)


@pytest.mark.parametrize(
    "yaml_text",
    [
        """stations:
  source_id: replacement_source
  transform: stations_from_silver
  conflict_cols: [sta_id]
  update_cols: []
""",
        """renamed_event_target:
  source_id: cultural_event
  transform: cultural_events_from_silver
  conflict_cols: [event_id]
  update_cols: []
""",
    ],
)
def test_registry_fails_closed_when_retired_authority_is_reintroduced(
    tmp_path,
    monkeypatch,
    yaml_text: str,
) -> None:
    """폐기 table명이나 source_id를 tables.yaml에 다시 넣으면 import 구성이 실패한다."""
    tables_path = tmp_path / "tables.yaml"
    tables_path.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setattr(config, "_TABLES_YAML_PATH", tables_path)

    with pytest.raises(RetiredSourceGoldPathError, match="폐기된 source publication"):
        config._load_table_specs()


def test_forecast_points_table_spec_registered() -> None:
    """예측 publisher 전환 전까지 forecast_points 파생 스펙을 보존한다."""
    spec = TABLE_SPECS["forecast_points"]

    assert spec.conflict_cols == ["sta_id", "predicted_dttm"]
    assert spec.update_cols == [
        "predicted_rent_cnt",
        "predicted_return_cnt",
        "batch_run_at",
    ]
    assert spec.reader is not None
    assert spec.expire_col == "predicted_dttm"


def test_forecast_points_reader_delegates_to_read_predictions(monkeypatch) -> None:
    """forecast_points는 별도 predictions key reader를 계속 사용한다."""
    captured = {}

    def fake_read_predictions(window_start):
        """호출 시각을 기록하고 최소 예측 fixture를 반환한다."""
        captured["window_start"] = window_start
        return pa.table({"station_id": ["101"]})

    monkeypatch.setattr("config.reader.read_predictions", fake_read_predictions)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["forecast_points"].read(window_start)

    assert captured["window_start"] == window_start
    assert isinstance(result, pd.DataFrame)
    assert result["station_id"].tolist() == ["101"]


def test_station_urgency_table_spec_registered() -> None:
    """긴급도 publisher 전환 전까지 station_urgency 파생 스펙을 보존한다."""
    spec = TABLE_SPECS["station_urgency"]

    assert spec.conflict_cols == ["sta_id"]
    assert spec.update_cols == [
        "urgency_score",
        "minutes_until_critical",
        "action_type",
        "bike_qty",
        "batch_run_at",
    ]
    assert spec.reader is not None


def test_station_urgency_reader_delegates_to_read_urgency(monkeypatch) -> None:
    """station_urgency는 별도 urgency batch reader를 계속 사용한다."""
    captured = {}

    def fake_read_urgency(window_start):
        """호출 시각을 기록하고 최소 긴급도 fixture를 반환한다."""
        captured["window_start"] = window_start
        return pa.table({"sta_id": ["101"]})

    monkeypatch.setattr("config.reader.read_urgency", fake_read_urgency)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["station_urgency"].read(window_start)

    assert captured["window_start"] == window_start
    assert isinstance(result, pd.DataFrame)
    assert result["sta_id"].tolist() == ["101"]


def test_rebalance_routes_table_spec_registered() -> None:
    """운영 상태를 덮지 않는 route 파생 스펙을 보존한다."""
    spec = TABLE_SPECS["rebalance_routes"]

    assert spec.conflict_cols == ["route_id"]
    assert spec.update_cols == []
    assert spec.reader is not None


def test_rebalance_routes_reader_delegates_to_read_routes(monkeypatch) -> None:
    """route 스펙은 별도 route batch reader를 계속 사용한다."""
    captured = {}

    def fake_read_routes(window_start):
        """호출 시각을 기록하고 최소 route fixture를 반환한다."""
        captured["window_start"] = window_start
        return pa.table({"route_id": ["r1"]})

    monkeypatch.setattr("config.reader.read_routes", fake_read_routes)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["rebalance_routes"].read(window_start)

    assert captured["window_start"] == window_start
    assert isinstance(result, pd.DataFrame)
    assert result["route_id"].tolist() == ["r1"]


def test_rebalance_route_stops_table_spec_registered() -> None:
    """route stop 파생 스펙을 보존한다."""
    spec = TABLE_SPECS["rebalance_route_stops"]

    assert spec.conflict_cols == ["route_id", "visit_order"]
    assert spec.update_cols == []
    assert spec.reader is not None


def test_rebalance_route_stops_reader_delegates_to_read_route_stops(
    monkeypatch,
) -> None:
    """route stop 스펙은 별도 route-stop batch reader를 계속 사용한다."""
    captured = {}

    def fake_read_route_stops(window_start):
        """호출 시각을 기록하고 최소 route-stop fixture를 반환한다."""
        captured["window_start"] = window_start
        return pa.table({"route_id": ["r1"]})

    monkeypatch.setattr("config.reader.read_route_stops", fake_read_route_stops)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["rebalance_route_stops"].read(window_start)

    assert captured["window_start"] == window_start
    assert isinstance(result, pd.DataFrame)
    assert result["route_id"].tolist() == ["r1"]


def test_non_authoritative_default_reader_behavior_is_preserved(monkeypatch) -> None:
    """원천 권한 목록 밖 TableSpec의 일반 Silver reader 동작은 변경하지 않는다."""
    captured = {}

    def fake_read_silver(source_id, window_start):
        """요청 source와 시각을 기록하고 최소 fixture를 반환한다."""
        captured["source_id"] = source_id
        captured["window_start"] = window_start
        return pa.table({"id": ["101"]})

    monkeypatch.setattr("config.reader.read_silver", fake_read_silver)
    spec = TableSpec(
        source_id="non_authoritative_fixture",
        transform=lambda frame: [],
        conflict_cols=["id"],
        update_cols=[],
    )
    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)

    result = spec.read(window_start)

    assert captured == {
        "source_id": "non_authoritative_fixture",
        "window_start": window_start,
    }
    assert isinstance(result, pd.DataFrame)
