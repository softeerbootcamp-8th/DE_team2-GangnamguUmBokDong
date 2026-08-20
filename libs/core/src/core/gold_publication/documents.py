"""Gold publication의 station·route 보조 canonical 문서를 정의한다."""

from __future__ import annotations

import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid5

from .canonical import (
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    parse_utc_dttm,
    sha256_hex,
    validate_point_ewkb_xdr_hex,
    validate_sha256_hex,
)
from .errors import ContractViolation

STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION = "gold-station-realtime-window-set-v1"
STATION_RELOCATION_APPROVAL_SCHEMA_VERSION = "gold-station-relocation-approval-v1"
ROUTE_COVERAGE_SCHEMA_VERSION = "gold-route-coverage-v1"
ROUTE_PUBLICATION_KEY = "rebalance_route"
ROUTE_UUID_NAMESPACE = UUID("d0d59897-9e72-541f-bb05-bd3d113c2639")

_MAX_SAFE_INTEGER = 2**53 - 1
_WINDOW_SET_KEYS = frozenset(("schema_version", "windows"))
_WINDOW_KEYS = frozenset(("byte_sha256", "logical_dttm", "revision_no", "uri"))
_RELOCATION_DOCUMENT_KEYS = frozenset(("approvals", "schema_version"))
_RELOCATION_APPROVAL_KEYS = frozenset(
    (
        "approval_id",
        "approved_by",
        "approved_dttm",
        "candidate_point_ewkb",
        "comparison_cd",
        "reference_point_ewkb",
        "sta_id",
    )
)
_ROUTE_COVERAGE_KEYS = frozenset(("routes", "schema_version", "stock_anchor_dttm"))
_ROUTE_KEYS = frozenset(
    ("completed_dttm", "dispatched_dttm", "route_id", "status", "stops")
)
_ROUTE_STOP_KEYS = frozenset(("action", "bike_cnt", "sta_id", "visit_no"))
_RELOCATION_COMPARISON_CODES = frozenset(("gold_vs_master", "master_vs_realtime"))
_ROUTE_COVERAGE_STATUSES = frozenset(("dispatched", "completed"))
_ROUTE_ACTIONS = frozenset(("pickup", "dropoff"))


@dataclass(frozen=True, slots=True)
class StationRealtimeWindow:
    """authoritative station realtime manifest 한 window를 표현한다."""

    byte_sha256: str
    logical_dttm: datetime
    revision_no: int
    uri: str

    def __post_init__(self) -> None:
        """window의 hash·시각·revision·URI 계약을 검증한다."""
        validate_sha256_hex(self.byte_sha256)
        object.__setattr__(self, "logical_dttm", _utc_dttm(self.logical_dttm))
        _require_nonnegative_integer(self.revision_no, "window revision_no")
        _require_nfc_nonblank(self.uri, "window URI")


@dataclass(frozen=True, slots=True)
class StationRealtimeWindowSet:
    """최신 authoritative station realtime window 최대 세 개를 표현한다."""

    schema_version: str
    windows: tuple[StationRealtimeWindow, ...]

    def __post_init__(self) -> None:
        """schema·cardinality·logical time 역순·중복을 검증한다."""
        _require_exact_value(
            self.schema_version,
            STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION,
            "station realtime window set schema_version",
        )
        _require_tuple(self.windows, "station realtime windows")
        _require_instances(
            self.windows,
            StationRealtimeWindow,
            "station realtime window",
        )
        if not 1 <= len(self.windows) <= 3:
            raise ContractViolation(
                "station realtime window set은 candidate를 포함해 1..3개여야 합니다."
            )
        logical_dttms = tuple(window.logical_dttm for window in self.windows)
        if len(logical_dttms) != len(set(logical_dttms)):
            raise ContractViolation(
                "같은 station realtime logical window는 하나만 허용합니다."
            )
        if logical_dttms != tuple(sorted(logical_dttms, reverse=True)):
            raise ContractViolation(
                "station realtime windows는 logical_dttm 내림차순이어야 합니다."
            )

    @property
    def canonical_bytes(self) -> bytes:
        """window set의 exact canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_station_window_set_document(self))

    @property
    def sha256(self) -> str:
        """window set canonical bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


@dataclass(frozen=True, slots=True)
class RelocationApproval:
    """station Point 변경 후보 한 건의 명시적 승인을 표현한다."""

    approval_id: str
    approved_by: str
    approved_dttm: datetime
    candidate_point_ewkb: str
    comparison_cd: str
    reference_point_ewkb: str
    sta_id: str

    def __post_init__(self) -> None:
        """승인 identity·시각·Point·comparison code를 검증한다."""
        _require_nfc_nonblank(self.approval_id, "relocation approval_id")
        _require_nfc_nonblank(self.approved_by, "relocation approved_by")
        object.__setattr__(self, "approved_dttm", _utc_dttm(self.approved_dttm))
        validate_point_ewkb_xdr_hex(self.candidate_point_ewkb)
        _require_exact_member(
            self.comparison_cd,
            _RELOCATION_COMPARISON_CODES,
            "relocation comparison_cd",
        )
        validate_point_ewkb_xdr_hex(self.reference_point_ewkb)
        _require_nfc_nonblank(self.sta_id, "relocation sta_id")


@dataclass(frozen=True, slots=True)
class StationRelocationApproval:
    """한 station publication에서 실제 적용한 relocation 승인 집합을 표현한다."""

    schema_version: str
    approvals: tuple[RelocationApproval, ...]

    def __post_init__(self) -> None:
        """schema·정렬·중복과 후보별 승인 단일성을 검증한다."""
        _require_exact_value(
            self.schema_version,
            STATION_RELOCATION_APPROVAL_SCHEMA_VERSION,
            "station relocation approval schema_version",
        )
        _require_tuple(self.approvals, "station relocation approvals")
        _require_instances(
            self.approvals,
            RelocationApproval,
            "relocation approval",
        )
        if not self.approvals:
            raise ContractViolation(
                "station_relocation_approval artifact에는 승인이 하나 이상 있어야 합니다."
            )
        sort_keys = tuple(_relocation_sort_key(approval) for approval in self.approvals)
        if len(sort_keys) != len(set(sort_keys)):
            raise ContractViolation(
                "중복 relocation approval tuple은 허용하지 않습니다."
            )
        if sort_keys != tuple(sorted(sort_keys)):
            raise ContractViolation(
                "relocation approvals는 (sta_id, comparison_cd, approval_id) "
                "UTF-8 byte 오름차순이어야 합니다."
            )
        candidate_keys = tuple(
            (approval.sta_id, approval.comparison_cd) for approval in self.approvals
        )
        if len(candidate_keys) != len(set(candidate_keys)):
            raise ContractViolation(
                "같은 station relocation 비교 후보에는 승인이 정확히 하나여야 합니다."
            )

    @property
    def canonical_bytes(self) -> bytes:
        """relocation approval의 exact canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_station_relocation_document(self))

    @property
    def sha256(self) -> str:
        """relocation approval canonical bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


@dataclass(frozen=True, slots=True)
class RouteCoverageStop:
    """coverage route에서 이미 실행된 station 작업 하나를 표현한다."""

    action: str
    bike_cnt: int
    sta_id: str
    visit_no: int

    def __post_init__(self) -> None:
        """작업 코드·양수 수량·station·1-based 순서를 검증한다."""
        _require_exact_member(self.action, _ROUTE_ACTIONS, "route coverage action")
        _require_positive_integer(self.bike_cnt, "route coverage bike_cnt")
        _require_nfc_nonblank(self.sta_id, "route coverage sta_id")
        _require_positive_integer(self.visit_no, "route coverage visit_no")


@dataclass(frozen=True, slots=True)
class RouteCoverageRoute:
    """stock에 아직 반영되지 않은 dispatched 또는 completed route를 표현한다."""

    completed_dttm: datetime | None
    dispatched_dttm: datetime
    route_id: str
    status: str
    stops: tuple[RouteCoverageStop, ...]

    def __post_init__(self) -> None:
        """route identity·lifecycle·status와 연속 stop을 검증한다."""
        dispatched = _utc_dttm(self.dispatched_dttm)
        completed = (
            None if self.completed_dttm is None else _utc_dttm(self.completed_dttm)
        )
        object.__setattr__(self, "dispatched_dttm", dispatched)
        object.__setattr__(self, "completed_dttm", completed)
        _require_canonical_uuid(self.route_id, "route coverage route_id")
        _require_exact_member(self.status, _ROUTE_COVERAGE_STATUSES, "route status")
        _require_tuple(self.stops, "route coverage stops")
        _require_instances(self.stops, RouteCoverageStop, "route coverage stop")
        if not self.stops:
            raise ContractViolation(
                "coverage route에는 stop이 하나 이상 있어야 합니다."
            )
        visit_nos = tuple(stop.visit_no for stop in self.stops)
        if visit_nos != tuple(range(1, len(self.stops) + 1)):
            raise ContractViolation(
                "coverage route stop visit_no는 중복 없이 1..N으로 이어져야 합니다."
            )
        if self.status == "dispatched" and completed is not None:
            raise ContractViolation(
                "dispatched coverage route의 completed_dttm은 null이어야 합니다."
            )
        if self.status == "completed":
            if completed is None:
                raise ContractViolation(
                    "completed coverage route에는 completed_dttm이 필요합니다."
                )
            if completed < dispatched:
                raise ContractViolation(
                    "completed_dttm은 dispatched_dttm보다 빠를 수 없습니다."
                )


@dataclass(frozen=True, slots=True)
class RouteCoverageDocument:
    """stock anchor에 아직 반영되지 않은 route 실행 상태 문서를 표현한다."""

    schema_version: str
    stock_anchor_dttm: datetime
    routes: tuple[RouteCoverageRoute, ...]

    def __post_init__(self) -> None:
        """schema·anchor·route 정렬과 completed coverage 범위를 검증한다."""
        _require_exact_value(
            self.schema_version,
            ROUTE_COVERAGE_SCHEMA_VERSION,
            "route coverage schema_version",
        )
        anchor = _utc_dttm(self.stock_anchor_dttm)
        object.__setattr__(self, "stock_anchor_dttm", anchor)
        _require_tuple(self.routes, "route coverage routes")
        _require_instances(self.routes, RouteCoverageRoute, "route coverage route")
        route_ids = tuple(route.route_id for route in self.routes)
        if len(route_ids) != len(set(route_ids)):
            raise ContractViolation("중복 route coverage route_id는 허용하지 않습니다.")
        if route_ids != tuple(sorted(route_ids, key=_utf8_key)):
            raise ContractViolation(
                "route coverage routes는 route_id 오름차순이어야 합니다."
            )
        for route in self.routes:
            if (
                route.status == "completed"
                and route.completed_dttm is not None
                and route.completed_dttm <= anchor
            ):
                raise ContractViolation(
                    "completed coverage route는 stock anchor 뒤에 완료되어야 합니다."
                )

    @property
    def canonical_bytes(self) -> bytes:
        """route coverage의 exact canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_route_coverage_document(self))

    @property
    def sha256(self) -> str:
        """route coverage canonical bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


def build_station_realtime_window_set(
    windows: Iterable[StationRealtimeWindow],
    *,
    expected_candidate: StationRealtimeWindow,
) -> StationRealtimeWindowSet:
    """window를 logical time 내림차순으로 정렬하고 candidate가 첫 원소인지 검증한다."""
    values = tuple(windows)
    _require_instances(values, StationRealtimeWindow, "station realtime window")
    ordered = tuple(
        sorted(values, key=lambda window: window.logical_dttm, reverse=True)
    )
    document = StationRealtimeWindowSet(
        STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION,
        ordered,
    )
    validate_station_realtime_window_set(
        document,
        expected_candidate=expected_candidate,
    )
    return document


def parse_station_realtime_window_set(
    payload: bytes,
    *,
    expected_candidate: StationRealtimeWindow | None = None,
) -> StationRealtimeWindowSet:
    """exact canonical window-set bytes를 파싱하고 선택 candidate를 검증한다.

    ``expected_candidate``가 없으면 문서의 구조·정렬·canonical bytes만
    검증한다. station publisher는 lock 안에서 선택한 candidate를 반드시
    전달해 첫 window identity까지 검증해야 한다.
    """
    document = _parse_object(payload, _WINDOW_SET_KEYS, "station realtime window set")
    windows = tuple(
        _parse_station_realtime_window(value)
        for value in _require_array(document["windows"], "station realtime windows")
    )
    result = StationRealtimeWindowSet(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        windows=windows,
    )
    if expected_candidate is not None:
        validate_station_realtime_window_set(
            result,
            expected_candidate=expected_candidate,
        )
    return result


def validate_station_realtime_window_set(
    document: StationRealtimeWindowSet,
    *,
    expected_candidate: StationRealtimeWindow,
) -> None:
    """window set 첫 원소가 publisher가 선택한 candidate와 정확히 같은지 검증한다."""
    if type(document) is not StationRealtimeWindowSet:
        raise ContractViolation("document는 StationRealtimeWindowSet이어야 합니다.")
    if type(expected_candidate) is not StationRealtimeWindow:
        raise ContractViolation(
            "expected_candidate는 StationRealtimeWindow여야 합니다."
        )
    if document.windows[0] != expected_candidate:
        raise ContractViolation(
            "station realtime window set 첫 원소가 expected candidate와 다릅니다."
        )


def build_station_relocation_approval(
    approvals: Iterable[RelocationApproval],
) -> StationRelocationApproval:
    """relocation 승인을 SSOT UTF-8 tuple 순서로 정렬해 문서를 만든다."""
    values = tuple(approvals)
    _require_instances(values, RelocationApproval, "relocation approval")
    ordered = tuple(sorted(values, key=_relocation_sort_key))
    return StationRelocationApproval(
        STATION_RELOCATION_APPROVAL_SCHEMA_VERSION,
        ordered,
    )


def parse_station_relocation_approval(payload: bytes) -> StationRelocationApproval:
    """exact canonical station relocation approval bytes를 typed 문서로 파싱한다."""
    document = _parse_object(
        payload,
        _RELOCATION_DOCUMENT_KEYS,
        "station relocation approval",
    )
    approvals = tuple(
        _parse_relocation_approval(value)
        for value in _require_array(document["approvals"], "relocation approvals")
    )
    return StationRelocationApproval(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        approvals=approvals,
    )


def build_route_coverage_route(
    *,
    completed_dttm: datetime | None,
    dispatched_dttm: datetime,
    route_id: str,
    status: str,
    stops: Iterable[RouteCoverageStop],
) -> RouteCoverageRoute:
    """route stop을 visit_no 순으로 정렬해 coverage route를 만든다."""
    values = tuple(stops)
    _require_instances(values, RouteCoverageStop, "route coverage stop")
    ordered = tuple(sorted(values, key=lambda stop: stop.visit_no))
    return RouteCoverageRoute(
        completed_dttm=completed_dttm,
        dispatched_dttm=dispatched_dttm,
        route_id=route_id,
        status=status,
        stops=ordered,
    )


def build_route_coverage(
    *,
    stock_anchor_dttm: datetime,
    routes: Iterable[RouteCoverageRoute],
) -> RouteCoverageDocument:
    """coverage route를 route_id UTF-8 byte 순서로 정렬해 문서를 만든다."""
    values = tuple(routes)
    _require_instances(values, RouteCoverageRoute, "route coverage route")
    ordered = tuple(sorted(values, key=lambda route: _utf8_key(route.route_id)))
    return RouteCoverageDocument(
        schema_version=ROUTE_COVERAGE_SCHEMA_VERSION,
        stock_anchor_dttm=stock_anchor_dttm,
        routes=ordered,
    )


def parse_route_coverage(payload: bytes) -> RouteCoverageDocument:
    """exact canonical route coverage bytes를 typed 문서로 파싱한다."""
    document = _parse_object(payload, _ROUTE_COVERAGE_KEYS, "route coverage")
    routes = tuple(
        _parse_route_coverage_route(value)
        for value in _require_array(document["routes"], "route coverage routes")
    )
    return RouteCoverageDocument(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        stock_anchor_dttm=parse_utc_dttm(
            _require_string(document["stock_anchor_dttm"], "stock_anchor_dttm")
        ),
        routes=routes,
    )


def route_uuid_v5(
    dispatch_center_id: str,
    logical_dttm: datetime,
    revision_no: int,
    route_ordinal: int,
) -> UUID:
    """고정 namespace와 exact canonical JSON name으로 route UUIDv5를 만든다."""
    center_id = _require_nfc_nonblank(dispatch_center_id, "dispatch_center_id")
    logical = _utc_dttm(logical_dttm)
    revision = _require_nonnegative_integer(revision_no, "revision_no")
    ordinal = _require_positive_integer(route_ordinal, "route_ordinal")
    name_bytes = canonical_json_bytes(
        {
            "dispatch_center_id": center_id,
            "logical_dttm": format_utc_dttm(logical),
            "publication_key": ROUTE_PUBLICATION_KEY,
            "revision_no": revision,
            "route_ordinal": ordinal,
        }
    )
    return uuid5(ROUTE_UUID_NAMESPACE, name_bytes.decode("utf-8"))


def _station_window_set_document(
    document: StationRealtimeWindowSet,
) -> dict[str, Any]:
    """station realtime window set을 exact JSON object로 바꾼다."""
    return {
        "schema_version": document.schema_version,
        "windows": [_station_window_document(window) for window in document.windows],
    }


def _station_window_document(window: StationRealtimeWindow) -> dict[str, Any]:
    """station realtime window를 exact JSON object로 바꾼다."""
    return {
        "byte_sha256": window.byte_sha256,
        "logical_dttm": format_utc_dttm(window.logical_dttm),
        "revision_no": window.revision_no,
        "uri": window.uri,
    }


def _station_relocation_document(
    document: StationRelocationApproval,
) -> dict[str, Any]:
    """station relocation approval을 exact JSON object로 바꾼다."""
    return {
        "approvals": [
            _relocation_approval_document(approval) for approval in document.approvals
        ],
        "schema_version": document.schema_version,
    }


def _relocation_approval_document(approval: RelocationApproval) -> dict[str, Any]:
    """relocation approval 한 건을 exact JSON object로 바꾼다."""
    return {
        "approval_id": approval.approval_id,
        "approved_by": approval.approved_by,
        "approved_dttm": format_utc_dttm(approval.approved_dttm),
        "candidate_point_ewkb": approval.candidate_point_ewkb,
        "comparison_cd": approval.comparison_cd,
        "reference_point_ewkb": approval.reference_point_ewkb,
        "sta_id": approval.sta_id,
    }


def _route_coverage_document(document: RouteCoverageDocument) -> dict[str, Any]:
    """route coverage를 exact JSON object로 바꾼다."""
    return {
        "routes": [_route_document(route) for route in document.routes],
        "schema_version": document.schema_version,
        "stock_anchor_dttm": format_utc_dttm(document.stock_anchor_dttm),
    }


def _route_document(route: RouteCoverageRoute) -> dict[str, Any]:
    """coverage route를 exact JSON object로 바꾼다."""
    return {
        "completed_dttm": (
            None
            if route.completed_dttm is None
            else format_utc_dttm(route.completed_dttm)
        ),
        "dispatched_dttm": format_utc_dttm(route.dispatched_dttm),
        "route_id": route.route_id,
        "status": route.status,
        "stops": [_route_stop_document(stop) for stop in route.stops],
    }


def _route_stop_document(stop: RouteCoverageStop) -> dict[str, Any]:
    """coverage route stop을 exact JSON object로 바꾼다."""
    return {
        "action": stop.action,
        "bike_cnt": stop.bike_cnt,
        "sta_id": stop.sta_id,
        "visit_no": stop.visit_no,
    }


def _parse_station_realtime_window(value: Any) -> StationRealtimeWindow:
    """JSON 값을 exact-key StationRealtimeWindow로 파싱한다."""
    document = _require_exact_object(value, _WINDOW_KEYS, "station realtime window")
    return StationRealtimeWindow(
        byte_sha256=_require_string(document["byte_sha256"], "window byte_sha256"),
        logical_dttm=parse_utc_dttm(
            _require_string(document["logical_dttm"], "window logical_dttm")
        ),
        revision_no=_require_nonnegative_integer(
            document["revision_no"],
            "window revision_no",
        ),
        uri=_require_string(document["uri"], "window URI"),
    )


def _parse_relocation_approval(value: Any) -> RelocationApproval:
    """JSON 값을 exact-key RelocationApproval로 파싱한다."""
    document = _require_exact_object(
        value,
        _RELOCATION_APPROVAL_KEYS,
        "relocation approval",
    )
    return RelocationApproval(
        approval_id=_require_string(document["approval_id"], "approval_id"),
        approved_by=_require_string(document["approved_by"], "approved_by"),
        approved_dttm=parse_utc_dttm(
            _require_string(document["approved_dttm"], "approved_dttm")
        ),
        candidate_point_ewkb=_require_string(
            document["candidate_point_ewkb"],
            "candidate_point_ewkb",
        ),
        comparison_cd=_require_string(document["comparison_cd"], "comparison_cd"),
        reference_point_ewkb=_require_string(
            document["reference_point_ewkb"],
            "reference_point_ewkb",
        ),
        sta_id=_require_string(document["sta_id"], "sta_id"),
    )


def _parse_route_coverage_route(value: Any) -> RouteCoverageRoute:
    """JSON 값을 exact-key RouteCoverageRoute로 파싱한다."""
    document = _require_exact_object(value, _ROUTE_KEYS, "route coverage route")
    completed_value = document["completed_dttm"]
    if completed_value is not None:
        completed = parse_utc_dttm(_require_string(completed_value, "completed_dttm"))
    else:
        completed = None
    stops = tuple(
        _parse_route_coverage_stop(stop)
        for stop in _require_array(document["stops"], "route coverage stops")
    )
    return RouteCoverageRoute(
        completed_dttm=completed,
        dispatched_dttm=parse_utc_dttm(
            _require_string(document["dispatched_dttm"], "dispatched_dttm")
        ),
        route_id=_require_string(document["route_id"], "route_id"),
        status=_require_string(document["status"], "status"),
        stops=stops,
    )


def _parse_route_coverage_stop(value: Any) -> RouteCoverageStop:
    """JSON 값을 exact-key RouteCoverageStop으로 파싱한다."""
    document = _require_exact_object(value, _ROUTE_STOP_KEYS, "route coverage stop")
    return RouteCoverageStop(
        action=_require_string(document["action"], "route stop action"),
        bike_cnt=_require_positive_integer(document["bike_cnt"], "route stop bike_cnt"),
        sta_id=_require_string(document["sta_id"], "route stop sta_id"),
        visit_no=_require_positive_integer(document["visit_no"], "route stop visit_no"),
    )


def _parse_object(
    payload: bytes,
    expected_keys: frozenset[str],
    name: str,
) -> dict[str, Any]:
    """canonical bytes의 root를 exact-key object로 파싱한다."""
    return _require_exact_object(parse_canonical_json(payload), expected_keys, name)


def _require_exact_object(
    value: Any,
    expected_keys: frozenset[str],
    name: str,
) -> dict[str, Any]:
    """값이 정확한 key 집합의 JSON object인지 확인한다."""
    if type(value) is not dict:
        raise ContractViolation(f"{name}은 JSON object여야 합니다.")
    document = cast(dict[str, Any], value)
    actual_keys = frozenset(document)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys.difference(actual_keys))
        extra = sorted(actual_keys.difference(expected_keys))
        raise ContractViolation(
            f"{name} key가 정확하지 않습니다: missing={missing}, extra={extra}"
        )
    return document


def _require_array(value: Any, name: str) -> list[Any]:
    """값이 JSON array인지 확인한다."""
    if type(value) is not list:
        raise ContractViolation(f"{name}은 JSON array여야 합니다.")
    return cast(list[Any], value)


def _require_string(value: Any, name: str) -> str:
    """값이 NFC 문자열인지 확인한다."""
    return _require_nfc_string(value, name)


def _require_nfc_string(value: Any, name: str) -> str:
    """값이 surrogate·noncharacter 없는 NFC 문자열인지 확인한다."""
    if type(value) is not str:
        raise ContractViolation(f"{name}은 문자열이어야 합니다.")
    for character in value:
        codepoint = ord(character)
        if 0xD800 <= codepoint <= 0xDFFF or _is_noncharacter(codepoint):
            raise ContractViolation(
                f"{name}에 Unicode surrogate 또는 noncharacter를 사용할 수 없습니다."
            )
    if unicodedata.normalize("NFC", value) != value:
        raise ContractViolation(f"{name}은 Unicode NFC여야 합니다.")
    return value


def _require_nfc_nonblank(value: Any, name: str) -> str:
    """값이 NFC이며 공백뿐이지 않은 문자열인지 확인한다."""
    result = _require_nfc_string(value, name)
    if not result.strip():
        raise ContractViolation(f"{name}은 nonblank 문자열이어야 합니다.")
    return result


def _require_nonnegative_integer(value: Any, name: str) -> int:
    """값이 canonical JSON 안전 범위의 0 이상 integer인지 확인한다."""
    if type(value) is not int:
        raise ContractViolation(f"{name}은 integer여야 합니다.")
    if not 0 <= value <= _MAX_SAFE_INTEGER:
        raise ContractViolation(
            f"{name}은 0 이상 {_MAX_SAFE_INTEGER} 이하의 integer여야 합니다."
        )
    return value


def _require_positive_integer(value: Any, name: str) -> int:
    """값이 canonical JSON 안전 범위의 양수 integer인지 확인한다."""
    result = _require_nonnegative_integer(value, name)
    if result == 0:
        raise ContractViolation(f"{name}은 1 이상의 integer여야 합니다.")
    return result


def _require_tuple(value: Any, name: str) -> None:
    """typed document의 배열 field가 immutable tuple인지 확인한다."""
    if type(value) is not tuple:
        raise ContractViolation(f"{name}은 tuple이어야 합니다.")


def _require_instances(
    values: tuple[Any, ...],
    expected_type: type[Any],
    name: str,
) -> None:
    """tuple의 모든 값이 기대 dataclass 인스턴스인지 확인한다."""
    if any(type(value) is not expected_type for value in values):
        raise ContractViolation(
            f"모든 {name} 값은 {expected_type.__name__}이어야 합니다."
        )


def _require_exact_value(value: Any, expected: str, name: str) -> None:
    """문자열 field가 contract 고정값과 정확히 같은지 확인한다."""
    if value != expected:
        raise ContractViolation(f"{name}은 정확히 {expected!r}이어야 합니다.")


def _require_exact_member(value: Any, allowed: frozenset[str], name: str) -> str:
    """문자열 field가 정확한 allowlist 원소인지 확인한다."""
    result = _require_nfc_nonblank(value, name)
    if result not in allowed:
        raise ContractViolation(
            f"{name}은 다음 값 중 하나여야 합니다: {sorted(allowed)}"
        )
    return result


def _require_canonical_uuid(value: Any, name: str) -> str:
    """값이 lowercase hyphenated canonical UUID 문자열인지 확인한다."""
    result = _require_nfc_nonblank(value, name)
    try:
        parsed = UUID(result)
    except ValueError as exc:
        raise ContractViolation(f"{name}은 올바른 UUID여야 합니다.") from exc
    if str(parsed) != result:
        raise ContractViolation(f"{name}은 lowercase canonical UUID여야 합니다.")
    return result


def _utc_dttm(value: Any) -> datetime:
    """aware datetime을 contract UTC instant로 변환한다."""
    if type(value) is not datetime:
        raise ContractViolation("시각은 datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)


def _utf8_key(value: str) -> bytes:
    """NFC 문자열의 UTF-8 bytes sort key를 반환한다."""
    return _require_nfc_string(value, "sort key").encode("utf-8")


def _relocation_sort_key(
    approval: RelocationApproval,
) -> tuple[bytes, bytes, bytes]:
    """relocation approval의 SSOT UTF-8 tuple sort key를 반환한다."""
    return (
        _utf8_key(approval.sta_id),
        _utf8_key(approval.comparison_cd),
        _utf8_key(approval.approval_id),
    )


def _is_noncharacter(codepoint: int) -> bool:
    """Unicode code point가 I-JSON noncharacter인지 반환한다."""
    return 0xFDD0 <= codepoint <= 0xFDEF or codepoint & 0xFFFF in {0xFFFE, 0xFFFF}
