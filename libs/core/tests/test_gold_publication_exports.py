"""Gold publication package root의 공개 API를 검증한다."""

from core import gold_publication


def test_package_root_exports_publication_boundary() -> None:
    """publisher가 package root에서 문서·evidence·transaction API를 가져온다."""
    expected_names = {
        "BusinessTimeEvidence",
        "RouteCoverageDocument",
        "StationRealtimeWindowSet",
        "VerifiedPublicationEvidence",
        "parse_route_coverage",
        "parse_station_realtime_window_set",
        "validate_linked_dependency_manifests",
        "validate_point_ewkb_xdr_hex",
        "validate_station_stock_release",
        "verify_publication_evidence",
    }

    assert expected_names <= set(gold_publication.__all__)
    assert all(hasattr(gold_publication, name) for name in expected_names)
