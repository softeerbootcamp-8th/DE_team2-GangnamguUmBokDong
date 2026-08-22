"""master·realtime window·이전 projection을 Gold station으로 결합한다."""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.gold_publication import (
    ContractViolation,
    StationRelocationApproval,
    format_utc_dttm,
    point_ewkb_xdr_hex,
)
from core.weather_grid import latlon_to_grid

CENTER_ASSIGNMENT_VERSION = "gold-nearest-active-center-v1"
GRID_CONVERSION_VERSION = "kma-lcc-v1"
STATION_POLICY_VERSION = "gold-station-policy-v1"
RELOCATION_THRESHOLD_M = 100.0
_STATION_ID = re.compile(r"ST-[0-9]+\Z")


@dataclass(frozen=True, slots=True)
class DispatchCenterReference:
    """station 최근 센터 배정에 쓰는 seed 읽기 모델을 표현한다."""

    dispatch_center_id: str
    longitude: float
    latitude: float
    is_active: bool

    def __post_init__(self) -> None:
        """center ID·Point·active 타입을 검증한다."""
        _nonblank_text(self.dispatch_center_id, "dispatch_center_id")
        _point(self.longitude, self.latitude, "dispatch center")
        if type(self.is_active) is not bool:
            raise ContractViolation("dispatch center is_active는 bool이어야 합니다.")


@dataclass(frozen=True, slots=True)
class MasterSnapshot:
    """station publication이 사용하는 완전 master snapshot을 표현한다."""

    logical_dttm: datetime
    rows: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        """master logical time과 row container를 검증한다."""
        object.__setattr__(
            self, "logical_dttm", _utc_dttm(self.logical_dttm, "master logical_dttm")
        )
        if type(self.rows) is not tuple or any(
            not isinstance(row, Mapping) for row in self.rows
        ):
            raise ContractViolation("master rows는 mapping tuple이어야 합니다.")


@dataclass(frozen=True, slots=True)
class RealtimeWindowSnapshot:
    """authoritative realtime logical window와 correction rows를 표현한다."""

    logical_dttm: datetime
    revision_no: int
    rows: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        """window logical time·revision·row container를 검증한다."""
        object.__setattr__(
            self, "logical_dttm", _utc_dttm(self.logical_dttm, "realtime logical_dttm")
        )
        if type(self.revision_no) is not int or self.revision_no < 0:
            raise ContractViolation("realtime revision_no는 0 이상 integer여야 합니다.")
        if type(self.rows) is not tuple or any(
            not isinstance(row, Mapping) for row in self.rows
        ):
            raise ContractViolation("realtime rows는 mapping tuple이어야 합니다.")


@dataclass(frozen=True, slots=True)
class StationRecord:
    """Gold station 전체 projection의 대여소 행을 표현한다."""

    sta_id: str
    sta_nm: str
    sta_addr: str
    hold_cnt: int
    longitude: float
    latitude: float
    sta_point_source_cd: str
    weather_grid_id: str
    dispatch_center_id: str
    master_base_dttm: datetime
    last_seen_dttm: datetime
    is_active: bool

    def __post_init__(self) -> None:
        """station DDL의 필수값·Point·lineage 계약을 검증한다."""
        _station_id(self.sta_id)
        _nonblank_text(self.sta_nm, "sta_nm")
        _nonblank_text(self.sta_addr, "sta_addr")
        if type(self.hold_cnt) is not int or self.hold_cnt <= 0:
            raise ContractViolation("hold_cnt는 0보다 큰 integer여야 합니다.")
        _point(self.longitude, self.latitude, "station")
        if self.sta_point_source_cd not in {
            "bike_station_master",
            "bike_station_realtime_fallback",
        }:
            raise ContractViolation("sta_point_source_cd가 SSOT allowlist에 없습니다.")
        _weather_grid_id(self.weather_grid_id)
        _nonblank_text(self.dispatch_center_id, "dispatch_center_id")
        object.__setattr__(
            self,
            "master_base_dttm",
            _utc_dttm(self.master_base_dttm, "master_base_dttm"),
        )
        object.__setattr__(
            self,
            "last_seen_dttm",
            _utc_dttm(self.last_seen_dttm, "last_seen_dttm"),
        )
        if type(self.is_active) is not bool:
            raise ContractViolation("station is_active는 bool이어야 합니다.")

    @property
    def point_ewkb(self) -> str:
        """station Point의 contract big-endian EWKB hex를 반환한다."""
        return point_ewkb_xdr_hex(self.longitude, self.latitude)


@dataclass(frozen=True, slots=True)
class StationProjection:
    """station target 전체 교체 행과 relocation 결과를 표현한다."""

    records: tuple[StationRecord, ...]
    relocation_applied: bool
    lkg_retained_count: int

    def __post_init__(self) -> None:
        """projection의 nonempty·정렬·중복·metric 계약을 검증한다."""
        if type(self.records) is not tuple or any(
            type(record) is not StationRecord for record in self.records
        ):
            raise ContractViolation(
                "station records는 StationRecord tuple이어야 합니다."
            )
        if not self.records:
            raise ContractViolation("station publication은 EMPTY를 허용하지 않습니다.")
        ids = tuple(record.sta_id for record in self.records)
        if ids != tuple(sorted(ids, key=lambda value: value.encode("utf-8"))):
            raise ContractViolation("station records는 sta_id UTF-8 순이어야 합니다.")
        if len(ids) != len(set(ids)):
            raise ContractViolation("station projection에 중복 sta_id가 있습니다.")
        if type(self.relocation_applied) is not bool:
            raise ContractViolation("relocation_applied는 bool이어야 합니다.")
        if type(self.lkg_retained_count) is not int or self.lkg_retained_count < 0:
            raise ContractViolation(
                "retained relocation count는 0 이상 integer여야 합니다."
            )


def build_station_projection(
    *,
    master_snapshot: MasterSnapshot,
    realtime_windows: tuple[RealtimeWindowSnapshot, ...],
    previous_records: tuple[StationRecord, ...] | None,
    weather_grid_ids: tuple[str, ...],
    dispatch_centers: tuple[DispatchCenterReference, ...],
    activation_ready_station_ids: tuple[str, ...],
    relocation_approval: StationRelocationApproval | None = None,
    distance_meters: Callable[[float, float, float, float], float] | None = None,
) -> StationProjection:
    """최신 1..3 window와 prior LKG로 station 전체 projection을 만든다.

    `activation_ready_station_ids`는 lock 안에서 13시간 weather와 model-support
    demand 동시 게시 조건을 재검증해 만든 집합이어야 한다. 여기서는
    임의로 active를 추론하지 않고 그 집합만 소비한다.
    """
    if type(master_snapshot) is not MasterSnapshot:
        raise ContractViolation("master_snapshot은 MasterSnapshot이어야 합니다.")
    window_maps = _validate_and_index_windows(realtime_windows)
    candidate = realtime_windows[0]
    master_by_id = _index_unique_rows(master_snapshot.rows, "RNTLS_ID", "master")
    previous_by_id = _index_previous_records(previous_records)
    grid_ids = _validated_grid_ids(weather_grid_ids)
    centers = _validated_centers(dispatch_centers)
    activation_ready = _validated_station_id_set(
        activation_ready_station_ids, "activation ready station"
    )
    distance = distance_meters or _haversine_meters
    if not callable(distance):
        raise ContractViolation("distance_meters는 callable이어야 합니다.")
    approvals = _approval_lookup(relocation_approval)
    used_approvals: set[tuple[str, str]] = set()
    lkg_retained = 0
    records: list[StationRecord] = []
    candidate_by_id = window_maps[0]
    candidate_ids = set(candidate_by_id)
    for station_id in sorted(
        set(previous_by_id) | candidate_ids,
        key=lambda value: value.encode("utf-8"),
    ):
        previous = previous_by_id.get(station_id)
        realtime_row = candidate_by_id.get(station_id)
        realtime_values = _serving_realtime_values(realtime_row)
        if previous is None and realtime_values is None:
            continue
        master_row = master_by_id.get(station_id)
        master_values = _master_values(master_row)
        point_choice = _choose_master_values_and_point(
            station_id=station_id,
            master_values=master_values,
            master_base_dttm=master_snapshot.logical_dttm,
            realtime_row=realtime_row,
            previous=previous,
            approvals=approvals,
            used_approvals=used_approvals,
            distance_meters=distance,
        )
        if point_choice is None:
            if previous is None:
                continue
            address = previous.sta_addr
            longitude = previous.longitude
            latitude = previous.latitude
            point_source = previous.sta_point_source_cd
            master_base = previous.master_base_dttm
            lkg_retained += 1
        else:
            (
                address,
                longitude,
                latitude,
                point_source,
                master_base,
                used_lkg,
            ) = point_choice
            lkg_retained += int(used_lkg)
        if realtime_values is not None:
            station_name, hold_count = realtime_values
            last_seen = candidate.logical_dttm
        else:
            assert previous is not None
            station_name = previous.sta_nm
            hold_count = previous.hold_cnt
            last_seen = (
                candidate.logical_dttm
                if realtime_row is not None
                else previous.last_seen_dttm
            )
        weather_nx, weather_ny = latlon_to_grid(latitude, longitude)
        weather_grid_id = f"{weather_nx}_{weather_ny}"
        if weather_grid_id not in grid_ids:
            raise ContractViolation(
                f"station이 weather_grid seed 밖 격자를 참조합니다: "
                f"{station_id} -> {weather_grid_id}"
            )
        center_id = _nearest_active_center(longitude, latitude, centers, distance)
        invalid_streak = _leading_invalid_window_count(station_id, window_maps)
        if realtime_values is not None:
            is_active = bool(
                (previous is not None and previous.is_active)
                or station_id in activation_ready
            )
        else:
            assert previous is not None
            is_active = previous.is_active and invalid_streak < 3
        records.append(
            StationRecord(
                sta_id=station_id,
                sta_nm=station_name,
                sta_addr=address,
                hold_cnt=hold_count,
                longitude=longitude,
                latitude=latitude,
                sta_point_source_cd=point_source,
                weather_grid_id=weather_grid_id,
                dispatch_center_id=center_id,
                master_base_dttm=master_base,
                last_seen_dttm=last_seen,
                is_active=is_active,
            )
        )
    expected_approvals = set(approvals)
    if used_approvals != expected_approvals:
        extras = sorted(expected_approvals - used_approvals)
        raise ContractViolation(
            "station relocation approval에 실제 반영하지 않은 여분이 있습니다: "
            f"{extras}"
        )
    return StationProjection(tuple(records), bool(used_approvals), lkg_retained)


def _validate_and_index_windows(
    windows: tuple[RealtimeWindowSnapshot, ...],
) -> tuple[dict[str, Mapping[str, Any]], ...]:
    """realtime window set의 1..3개·DESC·logical unique 계약을 검증한다."""
    if type(windows) is not tuple or any(
        type(window) is not RealtimeWindowSnapshot for window in windows
    ):
        raise ContractViolation(
            "realtime_windows는 RealtimeWindowSnapshot tuple이어야 합니다."
        )
    if not 1 <= len(windows) <= 3:
        raise ContractViolation(
            "station realtime window는 candidate를 포함해 1..3개여야 합니다."
        )
    logical_times = tuple(window.logical_dttm for window in windows)
    if len(logical_times) != len(set(logical_times)):
        raise ContractViolation(
            "같은 realtime logical window는 correction 최신 하나만 남겨야 합니다."
        )
    if logical_times != tuple(sorted(logical_times, reverse=True)):
        raise ContractViolation("realtime windows는 logical_dttm DESC여야 합니다.")
    return tuple(
        _index_unique_rows(window.rows, "stationId", "realtime") for window in windows
    )


def _index_unique_rows(
    rows: tuple[Mapping[str, Any], ...],
    id_field: str,
    source_name: str,
) -> dict[str, Mapping[str, Any]]:
    """source rows를 station ID로 index하고 natural-key 중복을 거부한다."""
    indexed: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        station_id = _station_id(row.get(id_field))
        if station_id in indexed:
            raise ContractViolation(
                f"{source_name} snapshot에 중복 station ID가 있습니다: {station_id}"
            )
        indexed[station_id] = row
    return indexed


def _index_previous_records(
    records: tuple[StationRecord, ...] | None,
) -> dict[str, StationRecord]:
    """prior station artifact를 ID index로 변환한다."""
    if records is None:
        return {}
    if type(records) is not tuple or any(
        type(record) is not StationRecord for record in records
    ):
        raise ContractViolation(
            "previous_records는 StationRecord tuple 또는 None이어야 합니다."
        )
    indexed = {record.sta_id: record for record in records}
    if len(indexed) != len(records):
        raise ContractViolation("previous station projection에 중복 ID가 있습니다.")
    return indexed


def _validated_grid_ids(values: tuple[str, ...]) -> set[str]:
    """published weather_grid dependency의 exact ID 집합을 검증한다."""
    if type(values) is not tuple:
        raise ContractViolation("weather_grid_ids는 tuple이어야 합니다.")
    validated = {_weather_grid_id(value) for value in values}
    if len(validated) != len(values):
        raise ContractViolation("weather_grid_ids에 중복이 있습니다.")
    return validated


def _validated_centers(
    values: tuple[DispatchCenterReference, ...],
) -> tuple[DispatchCenterReference, ...]:
    """dispatch center dependency에서 active center를 exact ID로 검증한다."""
    if type(values) is not tuple or any(
        type(value) is not DispatchCenterReference for value in values
    ):
        raise ContractViolation(
            "dispatch_centers는 DispatchCenterReference tuple이어야 합니다."
        )
    ids = tuple(value.dispatch_center_id for value in values)
    if len(ids) != len(set(ids)):
        raise ContractViolation("dispatch center ID가 중복됩니다.")
    active = tuple(value for value in values if value.is_active)
    if not active:
        raise ContractViolation("station 배정에 쓸 active dispatch center가 없습니다.")
    return active


def _validated_station_id_set(values: tuple[str, ...], name: str) -> set[str]:
    """station ID tuple을 exact unique set으로 검증한다."""
    if type(values) is not tuple:
        raise ContractViolation(f"{name} IDs는 tuple이어야 합니다.")
    result = {_station_id(value) for value in values}
    if len(result) != len(values):
        raise ContractViolation(f"{name} IDs에 중복이 있습니다.")
    return result


def _serving_realtime_values(
    row: Mapping[str, Any] | None,
) -> tuple[str, int] | None:
    """realtime 행이 serving-valid이면 표시명·정원을 반환한다."""
    if row is None:
        return None
    name = _optional_nonblank_text(row.get("stationName"))
    hold_count = _positive_int_or_none(row.get("rackTotCnt"))
    if name is None or hold_count is None:
        return None
    return name, hold_count


def _master_values(
    row: Mapping[str, Any] | None,
) -> tuple[str | None, tuple[float, float] | None]:
    """master 행의 주소와 유효 Point를 서로 독립적으로 판정한다."""
    if row is None:
        return None, None
    address = _optional_nonblank_text(row.get("ADDR1"))
    point = _optional_point(row.get("LOT"), row.get("LAT"))
    return address, point


def _choose_master_values_and_point(
    *,
    station_id: str,
    master_values: tuple[str | None, tuple[float, float] | None],
    master_base_dttm: datetime,
    realtime_row: Mapping[str, Any] | None,
    previous: StationRecord | None,
    approvals: Mapping[tuple[str, str], Any],
    used_approvals: set[tuple[str, str]],
    distance_meters: Callable[[float, float, float, float], float],
) -> tuple[str, float, float, str, datetime, bool] | None:
    """master LKG·fallback·>100m 승인으로 주소와 Point를 선택한다."""
    address, master_point = master_values
    realtime_point = (
        _optional_point(
            realtime_row.get("stationLongitude"),
            realtime_row.get("stationLatitude"),
        )
        if realtime_row is not None
        else None
    )
    if address is None:
        return None
    if master_point is None:
        if previous is not None:
            return None
        if realtime_point is None:
            return None
        longitude, latitude = realtime_point
        return (
            address,
            longitude,
            latitude,
            "bike_station_realtime_fallback",
            master_base_dttm,
            False,
        )
    master_lon, master_lat = master_point
    comparisons: list[tuple[str, tuple[float, float], tuple[float, float]]] = []
    if previous is not None:
        comparisons.append(
            (
                "gold_vs_master",
                (master_lon, master_lat),
                (previous.longitude, previous.latitude),
            )
        )
    if realtime_point is not None:
        comparisons.append(
            (
                "master_vs_realtime",
                (master_lon, master_lat),
                realtime_point,
            )
        )
    approved_keys: list[tuple[str, str]] = []
    # 비교 쌍은 최대 2개이고 승인 판정에 앞서 전부 필요하지 않을 수도 있지만,
    # 쌍마다 RDS를 왕복하지 않도록 한 번에 계산한다 — 값은 스칼라 경로와 같다.
    comparison_meters = _checked_distances(
        distance_meters,
        tuple(
            (candidate_point, reference_point)
            for _, candidate_point, reference_point in comparisons
        ),
    )
    for meters, (comparison_cd, candidate_point, reference_point) in zip(
        comparison_meters, comparisons, strict=True
    ):
        if meters <= RELOCATION_THRESHOLD_M:
            continue
        key = (station_id, comparison_cd)
        approval = approvals.get(key)
        if approval is None or not _approval_matches(
            approval,
            station_id=station_id,
            comparison_cd=comparison_cd,
            candidate_point=candidate_point,
            reference_point=reference_point,
        ):
            return None
        approved_keys.append(key)
    used_approvals.update(approved_keys)
    return (
        address,
        master_lon,
        master_lat,
        "bike_station_master",
        master_base_dttm,
        False,
    )


def _approval_lookup(
    document: StationRelocationApproval | None,
) -> dict[tuple[str, str], Any]:
    """relocation approval document를 station·comparison exact key로 index한다."""
    if document is None:
        return {}
    if type(document) is not StationRelocationApproval:
        raise ContractViolation(
            "relocation_approval은 StationRelocationApproval이어야 합니다."
        )
    return {
        (approval.sta_id, approval.comparison_cd): approval
        for approval in document.approvals
    }


def _approval_matches(
    approval: Any,
    *,
    station_id: str,
    comparison_cd: str,
    candidate_point: tuple[float, float],
    reference_point: tuple[float, float],
) -> bool:
    """승인의 station·comparison·candidate/reference Point bytes를 exact 비교한다."""
    return (
        approval.sta_id == station_id
        and approval.comparison_cd == comparison_cd
        and approval.candidate_point_ewkb
        == point_ewkb_xdr_hex(candidate_point[0], candidate_point[1])
        and approval.reference_point_ewkb
        == point_ewkb_xdr_hex(reference_point[0], reference_point[1])
    )


def _leading_invalid_window_count(
    station_id: str,
    window_maps: tuple[dict[str, Mapping[str, Any]], ...],
) -> int:
    """최신부터 연속한 미관측·serving-invalid authoritative window 수를 반환한다."""
    count = 0
    for rows in window_maps:
        if _serving_realtime_values(rows.get(station_id)) is not None:
            break
        count += 1
    return count


def _nearest_active_center(
    longitude: float,
    latitude: float,
    centers: tuple[DispatchCenterReference, ...],
    distance_meters: Callable[[float, float, float, float], float],
) -> str:
    """geography 거리 최소 center를, 동률이면 ID UTF-8 순으로 선택한다."""
    meters = _checked_distances(
        distance_meters,
        tuple(
            ((longitude, latitude), (center.longitude, center.latitude))
            for center in centers
        ),
    )
    ranked = [
        (
            value,
            center.dispatch_center_id.encode("utf-8"),
            center.dispatch_center_id,
        )
        for value, center in zip(meters, centers, strict=True)
    ]
    return min(ranked)[2]


def _checked_distances(
    distance_meters: Callable[[float, float, float, float], float],
    pairs: tuple[tuple[tuple[float, float], tuple[float, float]], ...],
) -> tuple[float, ...]:
    """Point 쌍 목록의 거리를 계산하고 스칼라 경로와 같은 계약으로 검증한다.

    callback이 `batch`를 노출하면(PostGIS 경로) 한 번의 round trip으로 묶어
    계산한다 — 쌍마다 SQL을 던지면 prepare_serving_plan이 RDS 왕복 대기로만
    151초를 쓴다(2026-08-22 실측). `batch`가 없는 callback(테스트 stub·
    `_haversine_meters`)은 기존과 동일하게 순차 계산한다.
    """
    batch = getattr(distance_meters, "batch", None)
    if batch is None:
        return tuple(
            _checked_distance(distance_meters, candidate, reference)
            for candidate, reference in pairs
        )
    values = batch(pairs)
    if len(values) != len(pairs):
        raise ContractViolation("distance_meters batch 결과 수가 입력과 다릅니다.")
    for value in values:
        _validate_distance(value)
    return tuple(float(value) for value in values)


def _checked_distance(
    distance_meters: Callable[[float, float, float, float], float],
    candidate_point: tuple[float, float],
    reference_point: tuple[float, float],
) -> float:
    """Point 쌍의 거리 callback 결과를 유한 비음수 meter로 검증한다."""
    value = distance_meters(
        candidate_point[0],
        candidate_point[1],
        reference_point[0],
        reference_point[1],
    )
    _validate_distance(value)
    return float(value)


def _validate_distance(value: Any) -> None:
    """거리 값이 유한 비음수 meter인지 검증한다."""
    if type(value) not in {int, float} or not math.isfinite(float(value)) or value < 0:
        raise ContractViolation("distance_meters 결과는 유한 비음수여야 합니다.")


def _haversine_meters(
    longitude_a: float,
    latitude_a: float,
    longitude_b: float,
    latitude_b: float,
) -> float:
    """pre-lock projection용 WGS84 Point 근사 대권거리를 meter로 반환한다."""
    radius_m = 6_371_008.8
    lat_a = math.radians(latitude_a)
    lat_b = math.radians(latitude_b)
    delta_lat = lat_b - lat_a
    delta_lon = math.radians(longitude_b - longitude_a)
    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat_a) * math.cos(lat_b) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * radius_m * math.asin(math.sqrt(haversine))


def _station_id(value: Any) -> str:
    """station ID를 exact NFC nonblank 문자열로 검증한다."""
    if type(value) is not str:
        raise ContractViolation("station ID는 문자열이어야 합니다.")
    normalized = unicodedata.normalize("NFC", value.strip())
    if normalized != value or _STATION_ID.fullmatch(normalized) is None:
        raise ContractViolation("station ID는 canonical ST-숫자 형식이어야 합니다.")
    return normalized


def _nonblank_text(value: Any, name: str) -> str:
    """값을 NFC nonblank 문자열로 검증해 반환한다."""
    result = _optional_nonblank_text(value)
    if result is None:
        raise ContractViolation(f"{name}은 nonblank 문자열이어야 합니다.")
    return result


def _optional_nonblank_text(value: Any) -> str | None:
    """값이 있으면 NFC·trim·연속 공백 정규화 문자열을 반환한다."""
    if value is None:
        return None
    if type(value) is not str:
        value = str(value)
    normalized = unicodedata.normalize("NFC", " ".join(value.strip().split()))
    return normalized or None


def _positive_int_or_none(value: Any) -> int | None:
    """값이 양의 integer면 반환하고 결측·무효하면 None을 반환한다."""
    if value is None or value == "" or type(value) is bool:
        return None
    try:
        converted = int(value)
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric != converted or converted <= 0:
        return None
    return converted


def _optional_point(longitude: Any, latitude: Any) -> tuple[float, float] | None:
    """값이 DDL safe box의 유한 Point면 반환하고 아니면 None을 반환한다."""
    try:
        return _point(longitude, latitude, "source")
    except ContractViolation:
        return None


def _point(longitude: Any, latitude: Any, name: str) -> tuple[float, float]:
    """값을 DDL safe box의 유한 WGS84 Point로 검증한다."""
    if type(longitude) is bool or type(latitude) is bool:
        raise ContractViolation(f"{name} Point 좌표는 숫자여야 합니다.")
    try:
        lon = float(longitude)
        lat = float(latitude)
    except (TypeError, ValueError) as exc:
        raise ContractViolation(f"{name} Point 좌표는 숫자여야 합니다.") from exc
    if not math.isfinite(lon) or not math.isfinite(lat):
        raise ContractViolation(f"{name} Point 좌표는 유한해야 합니다.")
    if not 126.5 <= lon <= 127.5 or not 37.0 <= lat <= 38.0:
        raise ContractViolation(f"{name} Point가 Gold DDL safe box 밖입니다.")
    return lon, lat


def _weather_grid_id(value: Any) -> str:
    """weather_grid_id를 DDL canonical x_y 형식으로 검증한다."""
    if type(value) is not str or value.count("_") != 1:
        raise ContractViolation("weather_grid_id는 x_y 문자열이어야 합니다.")
    x_text, y_text = value.split("_")
    try:
        x_no = int(x_text)
        y_no = int(y_text)
    except ValueError as exc:
        raise ContractViolation(
            "weather_grid_id 격자번호가 integer가 아닙니다."
        ) from exc
    if x_no <= 0 or y_no <= 0 or value != f"{x_no}_{y_no}":
        raise ContractViolation("weather_grid_id가 DDL canonical 형식과 다릅니다.")
    return value


def _utc_dttm(value: Any, name: str) -> datetime:
    """exact aware datetime을 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise ContractViolation(f"{name}은 datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)
