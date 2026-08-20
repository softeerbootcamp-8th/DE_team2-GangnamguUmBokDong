"""Gold station·station_stock publisher 경계를 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from core.gold_publication import ContractViolation, sha256_hex
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from gold.common import parquet_bytes
from gold.source_catalog import SourceManifestArtifact
from gold.station import StationRecord
from gold.station_release import (
    _STATION_SCHEMA,
    _STATION_STOCK_SCHEMA,
    _build_window_set,
    _delete_affected_proposed_routes,
    _direct_payloads,
    _point_from_ewkb,
    _postgis_distance,
    _require_master_only_last_seen,
    _require_positive_lookback,
    _route_invalidating_station_ids,
    _station_records_from_parquet,
    _station_records_to_parquet,
    _stock_records_from_parquet,
    _stock_records_to_parquet,
    _validate_station_ids,
)
from gold.station_stock import StationStockRecord

_ROOT = Path(__file__).resolve().parents[3]
_LOGICAL = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)


def _station(**overrides: object) -> StationRecord:
    """테스트용 exact station record를 반환한다."""
    values: dict[str, object] = {
        "sta_id": "ST-1",
        "sta_nm": "강남 대여소",
        "sta_addr": "서울시 강남구",
        "hold_cnt": 20,
        "longitude": 127.0473,
        "latitude": 37.5172,
        "sta_point_source_cd": "bike_station_master",
        "weather_grid_id": "61_126",
        "dispatch_center_id": "gangnam",
        "master_base_dttm": _LOGICAL - timedelta(days=1),
        "last_seen_dttm": _LOGICAL,
        "is_active": False,
    }
    values.update(overrides)
    return StationRecord(**values)  # type: ignore[arg-type]


def _source_artifact(
    logical: datetime,
    revision: int = 0,
) -> SourceManifestArtifact:
    """checked-in realtime policy를 만족하는 manifest artifact를 만든다."""
    config = _ROOT / "collector/sources/bike_station_realtime.yaml"
    manifest = build_source_snapshot_manifest(
        source_id="bike_station_realtime",
        logical_dttm=logical,
        revision_no=revision,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=f"sha256:{sha256_hex(config.read_bytes())}",
        silver_uri=f"s3://fixture/silver/sha256={'1' * 64}.parquet",
        silver_byte_sha256="1" * 64,
        counts=SourceSnapshotCounts(1, 1, 1, 0, 0),
        planned_parts=("page-00001-01000", "page-01001-02000"),
        completed_parts=("page-00001-01000", "page-01001-02000"),
    )
    return SourceManifestArtifact(
        manifest=manifest,
        uri=f"s3://fixture/manifests/{logical:%H%M}-{revision}.json",
        byte_sha256=manifest.sha256,
        payload=manifest.canonical_bytes,
    )


def test_station_and_stock_fixed_schema_parquet_round_trip() -> None:
    """station·stock output이 exact schema·Point·business time을 보존한다."""
    station = (_station(),)
    stock = (StationStockRecord("ST-1", _LOGICAL, 27),)

    assert (
        _station_records_from_parquet(_station_records_to_parquet(station)) == station
    )
    assert _stock_records_from_parquet(_stock_records_to_parquet(stock)) == stock


def test_station_output_rejects_schema_and_point_tamper() -> None:
    """column nullable/type 또는 EWKB SRID 변조를 output으로 받지 않는다."""
    wrong_schema = pa.schema(
        [
            *list(_STATION_SCHEMA)[:-1],
            pa.field("is_active", pa.bool_(), nullable=True),
        ]
    )
    payload = parquet_bytes(
        pa.Table.from_pylist(
            [
                {
                    "sta_id": "ST-1",
                    "sta_nm": "강남",
                    "sta_addr": "서울",
                    "hold_cnt": 1,
                    "sta_point_ewkb": bytes.fromhex(_station().point_ewkb),
                    "sta_point_source_cd": "bike_station_master",
                    "weather_grid_id": "61_126",
                    "dispatch_center_id": "gangnam",
                    "master_base_dttm": _LOGICAL,
                    "last_seen_dttm": _LOGICAL,
                    "is_active": False,
                }
            ],
            schema=wrong_schema,
        )
    )
    with pytest.raises(ContractViolation, match="schema"):
        _station_records_from_parquet(payload)

    tampered = bytearray(bytes.fromhex(_station().point_ewkb))
    tampered[8:9] = b"\x00"
    with pytest.raises(ContractViolation, match="EWKB"):
        _point_from_ewkb(bytes(tampered[:-1]))


def test_stock_output_rejects_nullable_schema() -> None:
    """stock output의 DDL NOT NULL schema drift를 거부한다."""
    wrong = pa.schema(
        (
            pa.field("sta_id", pa.string(), nullable=False),
            pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("parking_bike_tot_cnt", pa.int32(), nullable=True),
        )
    )
    payload = parquet_bytes(
        pa.Table.from_pylist(
            [{"sta_id": "ST-1", "base_dttm": _LOGICAL, "parking_bike_tot_cnt": 1}],
            schema=wrong,
        )
    )
    assert wrong != _STATION_STOCK_SCHEMA
    with pytest.raises(ContractViolation, match="schema"):
        _stock_records_from_parquet(payload)


@pytest.mark.parametrize("station_id", ["1", "ST-A", "ST-1 ", "ST-"])
def test_release_boundary_enforces_target_station_id_regex(station_id: str) -> None:
    """pure projection보다 좁은 target DDL station ID 규식을 release에서 강제한다."""
    with pytest.raises(ContractViolation, match=r"ST-\[0-9\]"):
        _validate_station_ids((station_id,))


def test_realtime_window_set_keeps_one_two_three_distinct_and_correction() -> None:
    """window-set이 1..3 logical DESC와 candidate correction identity를 보존한다."""
    candidate = _source_artifact(_LOGICAL, revision=1)
    older = tuple(
        _source_artifact(_LOGICAL - timedelta(minutes=5 * offset)) for offset in (1, 2)
    )
    for artifacts in ((candidate,), (candidate, older[0]), (candidate, *older)):
        window_set = _build_window_set(artifacts, candidate)
        assert 1 <= len(window_set.windows) <= 3
        assert window_set.windows[0].revision_no == 1
        assert tuple(window.logical_dttm for window in window_set.windows) == tuple(
            sorted((item.manifest.logical_dttm for item in artifacts), reverse=True)
        )


def test_window_set_rejects_same_logical_correction_twice() -> None:
    """같은 logical window의 v0·v1을 두 번 invalid streak으로 세지 않는다."""
    candidate = _source_artifact(_LOGICAL, revision=1)
    old_revision = _source_artifact(_LOGICAL, revision=0)
    with pytest.raises(ContractViolation, match="하나만"):
        _build_window_set((candidate, old_revision), candidate)


def test_master_only_requires_identical_identity_and_last_seen() -> None:
    """master-only correction이 신규 ID나 realtime last-seen을 혼합하지 않는다."""
    previous = (_station(),)
    same_last_seen = (_station(sta_addr="서울시 강남구 수정"),)
    _require_master_only_last_seen(previous, same_last_seen)

    with pytest.raises(ContractViolation, match="last_seen"):
        _require_master_only_last_seen(
            previous,
            (_station(last_seen_dttm=_LOGICAL + timedelta(minutes=5)),),
        )
    with pytest.raises(ContractViolation, match="identity"):
        _require_master_only_last_seen(
            previous, (*same_last_seen, _station(sta_id="ST-2"))
        )


def test_route_cleanup_ids_only_include_deactivation_center_or_point_change() -> None:
    """표시 속성 변경은 제외하고 route topology 변화 ID만 결정한다."""
    previous = (
        _station(sta_id="ST-1", is_active=True),
        _station(sta_id="ST-2", is_active=True),
        _station(sta_id="ST-3", is_active=True),
        _station(sta_id="ST-4", is_active=True),
    )
    incoming = (
        _station(sta_id="ST-1", is_active=False),
        _station(sta_id="ST-2", is_active=True, dispatch_center_id="other"),
        _station(sta_id="ST-3", is_active=True, longitude=127.0474),
        _station(
            sta_id="ST-4",
            is_active=True,
            sta_nm="표시명 수정",
            sta_addr="주소 수정",
            hold_cnt=21,
        ),
        _station(sta_id="ST-5", is_active=True),
    )

    assert _route_invalidating_station_ids(previous, incoming) == (
        "ST-1",
        "ST-2",
        "ST-3",
    )


def test_route_cleanup_sql_targets_proposed_headers_by_stop_station() -> None:
    """route cleanup이 proposed header만 지워 CASCADE stop 삭제를 사용한다."""
    cursor = _RouteCleanupCursor()

    _delete_affected_proposed_routes(cursor, ())  # type: ignore[arg-type]
    assert cursor.calls == []
    _delete_affected_proposed_routes(cursor, ("ST-1", "ST-2"))  # type: ignore[arg-type]

    [(statement, parameters)] = cursor.calls
    assert "DELETE FROM rebalance_route AS route" in statement
    assert "route.route_status_cd = 'proposed'" in statement
    assert "FROM rebalance_route_stop AS stop" in statement
    assert parameters == (["ST-1", "ST-2"],)


def test_postgis_distance_callback_uses_geography_and_exact_boundary() -> None:
    """relocation·center 거리가 haversine 대신 PostGIS geography를 사용한다."""
    cursor = _DistanceCursor((100.0, 100.000001))
    distance = _postgis_distance(cursor)  # type: ignore[arg-type]

    assert distance(127.0, 37.5, 127.001, 37.5) == 100.0
    assert distance(127.0, 37.5, 127.002, 37.5) > 100.0
    assert all("::geography" in statement for statement in cursor.statements)


def test_direct_prior_payload_uses_actual_immutable_bytes() -> None:
    """prior projection을 재직렬화하지 않고 manifest URI·SHA actual bytes를 사용한다."""
    actual = b"prior-parquet-from-an-older-pyarrow-version"
    output = SimpleNamespace(uri="s3://fixture/prior.parquet", byte_sha256="a" * 64)
    prior = SimpleNamespace(output_artifact=output, records=(_station(),))
    store = _ExactStore(actual)
    master = SimpleNamespace(uri="s3://fixture/master.json", payload=b"master")
    window_set = SimpleNamespace(canonical_bytes=b"windows")

    payloads = _direct_payloads(
        store,  # type: ignore[arg-type]
        master_artifact=master,  # type: ignore[arg-type]
        window_set=window_set,  # type: ignore[arg-type]
        prior=prior,  # type: ignore[arg-type]
        relocation_payload=None,
    )

    assert payloads[output.uri] is actual
    assert store.reads == [(output.uri, output.byte_sha256)]


@pytest.mark.parametrize("lookback", [timedelta(0), timedelta(seconds=-1)])
def test_catalog_lookback_must_be_explicitly_positive(lookback: timedelta) -> None:
    """publisher가 unbounded authority history 탐색으로 되돌아가지 않는다."""
    with pytest.raises(ContractViolation, match="양의 timedelta"):
        _require_positive_lookback(lookback, "realtime_lookback")


class _DistanceCursor:
    """ST_Distance query별 결과를 반환하는 cursor fake이다."""

    def __init__(self, values: tuple[float, ...]) -> None:
        """거리 결과 순서를 복사한다."""
        self.values = list(values)
        self.statements: list[str] = []

    def execute(self, statement: str, _parameters: object) -> None:
        """SQL을 기록한다."""
        self.statements.append(statement)

    def fetchone(self) -> tuple[float]:
        """다음 거리 결과를 반환한다."""
        return (self.values.pop(0),)


class _RouteCleanupCursor:
    """proposed route cleanup SQL과 parameter를 기록하는 cursor fake이다."""

    def __init__(self) -> None:
        """빈 execute 기록을 만든다."""
        self.calls: list[tuple[str, object]] = []

    def execute(self, statement: str, parameters: object) -> None:
        """실행할 SQL과 station ID array를 기록한다."""
        self.calls.append((statement, parameters))


class _ExactStore:
    """prior output exact-read만 기록하는 immutable store fake이다."""

    def __init__(self, payload: bytes) -> None:
        """actual prior bytes를 고정한다."""
        self.payload = payload
        self.reads: list[tuple[str, str]] = []

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """URI·SHA read를 기록하고 actual bytes를 반환한다."""
        assert require_canonical_json is False
        self.reads.append((uri, expected_sha256))
        return self.payload

    def put_once(self, *_args: Any, **_kwargs: Any) -> None:
        """이 테스트에서 write를 금지한다."""
        raise AssertionError("put_once must not be called")
