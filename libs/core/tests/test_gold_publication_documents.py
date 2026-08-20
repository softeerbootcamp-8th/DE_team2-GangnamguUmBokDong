"""Gold station·route 보조 canonical 문서 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from core.gold_publication.canonical import canonical_json_bytes, point_ewkb_hex
from core.gold_publication.documents import (
    ROUTE_COVERAGE_SCHEMA_VERSION,
    ROUTE_UUID_NAMESPACE,
    STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION,
    STATION_RELOCATION_APPROVAL_SCHEMA_VERSION,
    RelocationApproval,
    RouteCoverageDocument,
    RouteCoverageRoute,
    RouteCoverageStop,
    StationRealtimeWindow,
    StationRealtimeWindowSet,
    StationRelocationApproval,
    build_route_coverage,
    build_route_coverage_route,
    build_station_realtime_window_set,
    build_station_relocation_approval,
    parse_route_coverage,
    parse_station_realtime_window_set,
    parse_station_relocation_approval,
    route_uuid_v5,
    validate_station_realtime_window_set,
)
from core.gold_publication.errors import ContractViolation

_UTC_1550 = datetime(2026, 8, 19, 15, 50, tzinfo=UTC)
_UTC_1555 = datetime(2026, 8, 19, 15, 55, tzinfo=UTC)
_UTC_1600 = datetime(2026, 8, 19, 16, 0, tzinfo=UTC)
_UTC_1602 = datetime(2026, 8, 19, 16, 2, tzinfo=UTC)
_UTC_1610 = datetime(2026, 8, 19, 16, 10, tzinfo=UTC)

_REFERENCE_POINT = "0020000001000010e6405fc000000000004042c00000000000"
_CANDIDATE_POINT = "0020000001000010e6405fc020c49ba5e34042c04189374bc7"

_WINDOW_SET_BYTES = (
    b'{"schema_version":"gold-station-realtime-window-set-v1","windows":['
    b'{"byte_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
    b'"logical_dttm":"2026-08-19T16:00:00.000000Z","revision_no":0,'
    b'"uri":"s3://fixture/bike-station-realtime-20260819T160000Z.json"}]}'
)
_WINDOW_SET_SHA256 = "ad7674bc8e3b0ddc6ac06a0939b9a23519d34132efe28936c9c28fc764740132"

_RELOCATION_BYTES = (
    b'{"approvals":[{"approval_id":"REL-20260820-001","approved_by":"data-owner",'
    b'"approved_dttm":"2026-08-20T00:00:00.000000Z","candidate_point_ewkb":'
    b'"0020000001000010e6405fc020c49ba5e34042c04189374bc7","comparison_cd":'
    b'"gold_vs_master","reference_point_ewkb":'
    b'"0020000001000010e6405fc000000000004042c00000000000","sta_id":"ST-1"}],'
    b'"schema_version":"gold-station-relocation-approval-v1"}'
)
_RELOCATION_SHA256 = "210d13ebc01aae9ae6941eb6b159c98d477f80ae06f4b3ece8616d09367eeed1"

_ROUTE_COVERAGE_BYTES = (
    b'{"routes":[{"completed_dttm":null,"dispatched_dttm":'
    b'"2026-08-19T16:02:00.000000Z","route_id":'
    b'"00000000-0000-0000-0000-000000000001","status":"dispatched","stops":['
    b'{"action":"pickup","bike_cnt":3,"sta_id":"ST-9001","visit_no":1}]}],'
    b'"schema_version":"gold-route-coverage-v1","stock_anchor_dttm":'
    b'"2026-08-19T16:00:00.000000Z"}'
)
_ROUTE_COVERAGE_SHA256 = (
    "13cd1f4fe82d4b09370fd4141d1ee1a727f25c5b109de11f06bb904f9c001e8b"
)


def _window(
    logical_dttm: datetime = _UTC_1600,
    revision_no: int = 0,
    *,
    suffix: str = "1600",
) -> StationRealtimeWindow:
    """테스트용 authoritative realtime window를 만든다."""
    return StationRealtimeWindow(
        byte_sha256=(suffix[-1].lower() if suffix[-1].lower() in "abcdef" else "b")
        * 64,
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        uri=f"s3://fixture/window-{suffix}.json",
    )


def _regression_window() -> StationRealtimeWindow:
    """SSOT station realtime window 회귀 fixture를 만든다."""
    return StationRealtimeWindow(
        byte_sha256="b" * 64,
        logical_dttm=_UTC_1600,
        revision_no=0,
        uri="s3://fixture/bike-station-realtime-20260819T160000Z.json",
    )


def _approval(
    *,
    approval_id: str = "REL-20260820-001",
    comparison_cd: str = "gold_vs_master",
    sta_id: str = "ST-1",
) -> RelocationApproval:
    """테스트용 station relocation approval을 만든다."""
    return RelocationApproval(
        approval_id=approval_id,
        approved_by="data-owner",
        approved_dttm=datetime(2026, 8, 20, tzinfo=UTC),
        candidate_point_ewkb=_CANDIDATE_POINT,
        comparison_cd=comparison_cd,
        reference_point_ewkb=_REFERENCE_POINT,
        sta_id=sta_id,
    )


def _stop(visit_no: int = 1, action: str = "pickup") -> RouteCoverageStop:
    """테스트용 route coverage stop을 만든다."""
    return RouteCoverageStop(
        action=action,
        bike_cnt=3,
        sta_id=f"ST-{9000 + visit_no}",
        visit_no=visit_no,
    )


def _route(
    route_id: str = "00000000-0000-0000-0000-000000000001",
) -> RouteCoverageRoute:
    """테스트용 dispatched coverage route를 만든다."""
    return build_route_coverage_route(
        completed_dttm=None,
        dispatched_dttm=_UTC_1602,
        route_id=route_id,
        status="dispatched",
        stops=(_stop(),),
    )


def test_station_window_set_matches_ssot_regression_vector() -> None:
    """station realtime window-set bytes와 SHA를 SSOT 값으로 고정한다."""
    candidate = _regression_window()
    document = build_station_realtime_window_set(
        (candidate,),
        expected_candidate=candidate,
    )

    assert document.canonical_bytes == _WINDOW_SET_BYTES
    assert document.sha256 == _WINDOW_SET_SHA256
    assert parse_station_realtime_window_set(_WINDOW_SET_BYTES) == document
    assert (
        parse_station_realtime_window_set(
            _WINDOW_SET_BYTES,
            expected_candidate=candidate,
        )
        == document
    )


def test_station_window_builder_sorts_latest_first_and_requires_candidate() -> None:
    """builder는 logical time DESC로 정렬하고 실제 candidate를 첫 원소로 요구한다."""
    candidate = _window(_UTC_1600, suffix="a")
    previous = _window(_UTC_1555, suffix="b")
    oldest = _window(_UTC_1550, suffix="c")

    document = build_station_realtime_window_set(
        (previous, oldest, candidate),
        expected_candidate=candidate,
    )

    assert document.windows == (candidate, previous, oldest)
    with pytest.raises(ContractViolation, match="expected candidate"):
        validate_station_realtime_window_set(
            document,
            expected_candidate=previous,
        )


def test_station_window_set_rejects_zero_more_than_three_and_duplicate_logical() -> (
    None
):
    """window set은 1..3개와 서로 다른 logical window만 허용한다."""
    with pytest.raises(ContractViolation, match="1..3"):
        StationRealtimeWindowSet(STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION, ())

    four = tuple(
        _window(_UTC_1600 - timedelta(minutes=index * 5), suffix=str(index))
        for index in range(4)
    )
    with pytest.raises(ContractViolation, match="1..3"):
        StationRealtimeWindowSet(STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION, four)

    same_logical = (
        _window(_UTC_1600, 1, suffix="a"),
        _window(_UTC_1600, 0, suffix="b"),
    )
    with pytest.raises(ContractViolation, match="logical window"):
        StationRealtimeWindowSet(
            STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION,
            same_logical,
        )


def test_station_window_parser_rejects_wrong_order_and_extra_key() -> None:
    """wire parser는 array 순서와 모든 nested exact key를 그대로 검증한다."""
    candidate = _window(_UTC_1600, suffix="a")
    previous = _window(_UTC_1555, suffix="b")
    wrong_order = canonical_json_bytes(
        {
            "schema_version": STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION,
            "windows": [
                {
                    "byte_sha256": previous.byte_sha256,
                    "logical_dttm": "2026-08-19T15:55:00.000000Z",
                    "revision_no": 0,
                    "uri": previous.uri,
                },
                {
                    "byte_sha256": candidate.byte_sha256,
                    "logical_dttm": "2026-08-19T16:00:00.000000Z",
                    "revision_no": 0,
                    "uri": candidate.uri,
                },
            ],
        }
    )
    with pytest.raises(ContractViolation, match="내림차순"):
        parse_station_realtime_window_set(
            wrong_order,
            expected_candidate=candidate,
        )

    extra = canonical_json_bytes(
        {
            "byte_sha256": candidate.byte_sha256,
            "extra": None,
            "logical_dttm": "2026-08-19T16:00:00.000000Z",
            "revision_no": 0,
            "uri": candidate.uri,
        }
    )
    invalid_root = canonical_json_bytes(
        {
            "schema_version": STATION_REALTIME_WINDOW_SET_SCHEMA_VERSION,
            "windows": [parse_json_object(extra)],
        }
    )
    with pytest.raises(ContractViolation, match="extra"):
        parse_station_realtime_window_set(
            invalid_root,
            expected_candidate=candidate,
        )


def test_relocation_approval_matches_ssot_regression_vector() -> None:
    """station relocation approval bytes와 SHA를 SSOT 값으로 고정한다."""
    document = build_station_relocation_approval((_approval(),))

    assert document.canonical_bytes == _RELOCATION_BYTES
    assert document.sha256 == _RELOCATION_SHA256
    assert parse_station_relocation_approval(_RELOCATION_BYTES) == document


def test_relocation_builder_uses_utf8_tuple_order() -> None:
    """approval 배열을 sta_id·comparison·approval_id UTF-8 byte 순으로 정렬한다."""
    later = _approval(
        approval_id="REL-2",
        comparison_cd="master_vs_realtime",
        sta_id="ST-2",
    )
    first = _approval(approval_id="REL-1", sta_id="ST-1")

    document = build_station_relocation_approval((later, first))

    assert document.approvals == (first, later)


def test_relocation_approval_rejects_empty_duplicate_candidate_and_bad_code() -> None:
    """artifact 없는 상태와 후보당 복수 승인 및 미등록 comparison을 거부한다."""
    with pytest.raises(ContractViolation, match="하나 이상"):
        StationRelocationApproval(STATION_RELOCATION_APPROVAL_SCHEMA_VERSION, ())

    duplicate_candidate = (
        _approval(approval_id="REL-1"),
        _approval(approval_id="REL-2"),
    )
    with pytest.raises(ContractViolation, match="정확히 하나"):
        build_station_relocation_approval(duplicate_candidate)

    with pytest.raises(ContractViolation, match="comparison_cd"):
        _approval(comparison_cd="unknown")


@pytest.mark.parametrize(
    "point",
    [
        _REFERENCE_POINT.upper(),
        "00" * 25,
        "",
    ],
)
def test_relocation_approval_rejects_non_contract_point_ewkb(point: str) -> None:
    """승인 Point는 lowercase SRID 4326 XDR EWKB여야 한다."""
    with pytest.raises(ContractViolation):
        RelocationApproval(
            approval_id="REL-1",
            approved_by="owner",
            approved_dttm=_UTC_1600,
            candidate_point_ewkb=point,
            comparison_cd="gold_vs_master",
            reference_point_ewkb=_REFERENCE_POINT,
            sta_id="ST-1",
        )


def test_relocation_parser_rejects_extra_and_missing_keys() -> None:
    """relocation wire document의 root와 approval key 집합을 정확히 고정한다."""
    approval = {
        "approval_id": "REL-1",
        "approved_by": "owner",
        "approved_dttm": "2026-08-20T00:00:00.000000Z",
        "candidate_point_ewkb": _CANDIDATE_POINT,
        "comparison_cd": "gold_vs_master",
        "reference_point_ewkb": _REFERENCE_POINT,
        "sta_id": "ST-1",
    }
    approval["extra"] = None
    payload = canonical_json_bytes(
        {
            "approvals": [approval],
            "schema_version": STATION_RELOCATION_APPROVAL_SCHEMA_VERSION,
        }
    )

    with pytest.raises(ContractViolation, match="extra"):
        parse_station_relocation_approval(payload)


def test_route_coverage_matches_ssot_regression_vector() -> None:
    """route coverage bytes와 SHA를 SSOT 값으로 고정한다."""
    document = build_route_coverage(
        stock_anchor_dttm=_UTC_1600,
        routes=(_route(),),
    )

    assert document.canonical_bytes == _ROUTE_COVERAGE_BYTES
    assert document.sha256 == _ROUTE_COVERAGE_SHA256
    assert parse_route_coverage(_ROUTE_COVERAGE_BYTES) == document


def test_route_coverage_allows_empty_routes() -> None:
    """실행 중이거나 미반영 완료 route가 없으면 coverage routes는 빈 배열이다."""
    document = build_route_coverage(stock_anchor_dttm=_UTC_1600, routes=())

    assert document.routes == ()
    assert parse_route_coverage(document.canonical_bytes) == document


def test_route_builders_sort_routes_and_stops() -> None:
    """builder는 route_id와 stop visit_no를 각각 오름차순으로 정렬한다."""
    route_a = build_route_coverage_route(
        completed_dttm=None,
        dispatched_dttm=_UTC_1602,
        route_id="00000000-0000-0000-0000-000000000001",
        status="dispatched",
        stops=(_stop(2, "dropoff"), _stop(1)),
    )
    route_b = _route("00000000-0000-0000-0000-000000000002")

    document = build_route_coverage(
        stock_anchor_dttm=_UTC_1600,
        routes=(route_b, route_a),
    )

    assert tuple(route.route_id for route in document.routes) == (
        route_a.route_id,
        route_b.route_id,
    )
    assert tuple(stop.visit_no for stop in route_a.stops) == (1, 2)


@pytest.mark.parametrize(
    ("status", "completed_dttm", "message"),
    [
        ("dispatched", _UTC_1610, "null"),
        ("completed", None, "필요"),
        ("unknown", None, "route status"),
    ],
)
def test_route_rejects_invalid_status_timestamp_matrix(
    status: str,
    completed_dttm: datetime | None,
    message: str,
) -> None:
    """coverage status와 nullable lifecycle time 조합을 exact matrix로 제한한다."""
    with pytest.raises(ContractViolation, match=message):
        RouteCoverageRoute(
            completed_dttm=completed_dttm,
            dispatched_dttm=_UTC_1602,
            route_id="00000000-0000-0000-0000-000000000001",
            status=status,
            stops=(_stop(),),
        )


def test_completed_route_requires_ordered_time_after_stock_anchor() -> None:
    """completed route는 dispatch 이후이면서 stock anchor 뒤에 완료되어야 한다."""
    with pytest.raises(ContractViolation, match="빠를"):
        RouteCoverageRoute(
            completed_dttm=_UTC_1600,
            dispatched_dttm=_UTC_1602,
            route_id="00000000-0000-0000-0000-000000000001",
            status="completed",
            stops=(_stop(),),
        )

    completed_at_anchor = RouteCoverageRoute(
        completed_dttm=_UTC_1600,
        dispatched_dttm=_UTC_1555,
        route_id="00000000-0000-0000-0000-000000000001",
        status="completed",
        stops=(_stop(),),
    )
    with pytest.raises(ContractViolation, match="stock anchor 뒤"):
        build_route_coverage(
            stock_anchor_dttm=_UTC_1600,
            routes=(completed_at_anchor,),
        )


def test_route_rejects_missing_noncontiguous_or_invalid_stops() -> None:
    """coverage route마다 양수 수량의 pickup/dropoff stop이 1..N으로 필요하다."""
    with pytest.raises(ContractViolation, match="하나 이상"):
        RouteCoverageRoute(
            completed_dttm=None,
            dispatched_dttm=_UTC_1602,
            route_id="00000000-0000-0000-0000-000000000001",
            status="dispatched",
            stops=(),
        )
    with pytest.raises(ContractViolation, match="1..N"):
        RouteCoverageRoute(
            completed_dttm=None,
            dispatched_dttm=_UTC_1602,
            route_id="00000000-0000-0000-0000-000000000001",
            status="dispatched",
            stops=(_stop(2),),
        )
    with pytest.raises(ContractViolation, match="action"):
        RouteCoverageStop(action="move", bike_cnt=1, sta_id="ST-1", visit_no=1)
    with pytest.raises(ContractViolation, match="1 이상"):
        RouteCoverageStop(action="pickup", bike_cnt=0, sta_id="ST-1", visit_no=1)


@pytest.mark.parametrize(
    "route_id",
    [
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "not-a-uuid",
    ],
)
def test_route_rejects_noncanonical_uuid(route_id: str) -> None:
    """route_id는 lowercase hyphenated canonical UUID 문자열이어야 한다."""
    with pytest.raises(ContractViolation, match="UUID"):
        RouteCoverageRoute(
            completed_dttm=None,
            dispatched_dttm=_UTC_1602,
            route_id=route_id,
            status="dispatched",
            stops=(_stop(),),
        )


def test_route_coverage_rejects_duplicate_unsorted_and_extra_keys() -> None:
    """route 배열 중복·역순과 wire extra key를 모두 거부한다."""
    route_a = _route("00000000-0000-0000-0000-000000000001")
    route_b = _route("00000000-0000-0000-0000-000000000002")
    with pytest.raises(ContractViolation, match="오름차순"):
        RouteCoverageDocument(
            ROUTE_COVERAGE_SCHEMA_VERSION,
            _UTC_1600,
            (route_b, route_a),
        )
    with pytest.raises(ContractViolation, match="중복"):
        RouteCoverageDocument(
            ROUTE_COVERAGE_SCHEMA_VERSION,
            _UTC_1600,
            (route_a, route_a),
        )

    route_value = parse_json_object(_ROUTE_COVERAGE_BYTES)["routes"][0]
    route_value["extra"] = None
    payload = canonical_json_bytes(
        {
            "routes": [route_value],
            "schema_version": ROUTE_COVERAGE_SCHEMA_VERSION,
            "stock_anchor_dttm": "2026-08-19T16:00:00.000000Z",
        }
    )
    with pytest.raises(ContractViolation, match="extra"):
        parse_route_coverage(payload)


def test_route_uuid_v5_matches_exact_name_regression() -> None:
    """고정 namespace와 five-key name의 SSOT UUIDv5를 재현한다."""
    route_id = route_uuid_v5("center_a", _UTC_1600, 0, 1)

    assert str(ROUTE_UUID_NAMESPACE) == "d0d59897-9e72-541f-bb05-bd3d113c2639"
    assert str(route_id) == "7dd58c8d-7dc7-5279-8845-7673c9c87be2"
    assert route_id.version == 5


@pytest.mark.parametrize(
    ("dispatch_center_id", "revision_no", "route_ordinal"),
    [
        ("", 0, 1),
        ("center_a", -1, 1),
        ("center_a", 0, 0),
    ],
)
def test_route_uuid_rejects_invalid_identity_parts(
    dispatch_center_id: str,
    revision_no: int,
    route_ordinal: int,
) -> None:
    """route UUID identity는 nonblank center·nonnegative revision·1-based ordinal이다."""
    with pytest.raises(ContractViolation):
        route_uuid_v5(
            dispatch_center_id,
            _UTC_1600,
            revision_no,
            route_ordinal,
        )


def test_documents_reject_non_nfc_and_noncanonical_wire_bytes() -> None:
    """typed string과 parser 원본 모두 NFC exact canonical이어야 한다."""
    with pytest.raises(ContractViolation, match="NFC"):
        StationRealtimeWindow("b" * 64, _UTC_1600, 0, "s3://fixture/e\u0301")

    candidate = _regression_window()
    with pytest.raises(ContractViolation):
        parse_station_realtime_window_set(
            b'{"schema_version":"gold-station-realtime-window-set-v1", "windows":[]}',
            expected_candidate=candidate,
        )


def test_point_helper_fixture_is_same_xdr_contract() -> None:
    """문서 테스트 Point fixture가 공통 XDR encoder와 같은지 확인한다."""
    assert point_ewkb_hex(127.0, 37.5) == _REFERENCE_POINT


def parse_json_object(payload: bytes) -> dict[str, object]:
    """테스트에서 canonical JSON root object를 mutable dict로 돌려준다."""
    from core.gold_publication.canonical import parse_canonical_json

    value = parse_canonical_json(payload)
    assert isinstance(value, dict)
    return value
