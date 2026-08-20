"""Gold event source projection 계약을 검증한다."""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from core.gold_publication import ContractViolation
from gold.event import (
    CULTURAL_EVENT_SOURCE,
    PERFORMANCE_EVENT_SOURCE,
    STADIUM_COORDINATE_SHA256,
    _records_from_parquet,
    _records_to_parquet,
    build_cultural_event_projection,
    build_performance_event_projection,
    cultural_source_event_id,
    load_stadium_coordinates,
    parse_stadium_coordinates,
)

OBSERVED_AT = datetime(2026, 8, 20, 1, 2, 3, tzinfo=UTC)
TODAY = date(2026, 8, 20)
ASSET_PATH = Path(__file__).parents[2] / "assets" / "stadium_coords.json"


def _cultural_row(**overrides: object) -> dict[str, object]:
    """문화행사 기본 원천 행을 반환한다."""
    row: dict[str, object] = {
        "TITLE": "서울 축제",
        "PLACE": None,
        "STRTDATE": "2026-08-20",
        "END_DATE": "2026-08-21",
        "LOT": 127.0473,
        "LAT": 37.5172,
    }
    row.update(overrides)
    return row


def _performance_row(**overrides: object) -> dict[str, object]:
    """체육시설 공연행사 기본 원천 행을 반환한다."""
    row: dict[str, object] = {
        "SCH_SEQ": "event-1",
        "TITLE": "야구 경기",
        "SDATE": "2026-08-20",
        "EDATE": "2026-08-21",
        "SCH_CODE_B": "8",
        "CODE_TITLE_B": "잠실야구장",
    }
    row.update(overrides)
    return row


def test_cultural_identity_matches_ssot_regression_vector() -> None:
    """문화행사 canonical ID가 SSOT SHA-256 회귀값과 같다."""
    assert cultural_source_event_id("서울 축제", None, "2026-08-20", "2026-08-21") == (
        "v1:ca93c5fd090e1423f1923d31cca0b27cc5811e4c8e5710fc12f2861cb3f44e06"
    )


def test_cultural_identity_normalizes_nfc_whitespace_and_empty_place() -> None:
    """identity 문자열의 NFC·trim·연속 공백·빈 장소 규칙을 고정한다."""
    expected = cultural_source_event_id("서울 축제", None, "2026-08-20", "2026-08-21")
    actual = cultural_source_event_id(
        "  서울\t  축제  ",
        " \n ",
        "2026-08-20 00:00:00.0",
        "2026-08-21",
    )
    assert actual == expected


def test_cultural_projection_keeps_source_scoped_current_point() -> None:
    """유효한 현재 문화행사를 source-qualified event로 게시한다."""
    projection = build_cultural_event_projection(
        (_cultural_row(),), last_seen_dttm=OBSERVED_AT, today=TODAY
    )
    [record] = projection.records
    assert record.event_source_cd == CULTURAL_EVENT_SOURCE
    assert record.event_id == f"{CULTURAL_EVENT_SOURCE}:{record.source_event_id}"
    assert record.event_point_source_cd == "source_reported"
    assert record.location_accuracy_cd == "source_reported"
    assert record.last_seen_dttm == OBSERVED_AT
    assert projection.rejected_row_count == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"LOT": None},
        {"LAT": 38.1},
        {"END_DATE": "2026-08-19"},
        {"STRTDATE": "2028-08-21", "END_DATE": "2028-08-21"},
    ],
)
def test_cultural_projection_preserves_but_rejects_nonserving_rows(
    overrides: dict[str, object],
) -> None:
    """Point·현재 기간·2년 horizon 위반 행을 Gold에 넣지 않는다."""
    projection = build_cultural_event_projection(
        (_cultural_row(**overrides),), last_seen_dttm=OBSERVED_AT, today=TODAY
    )
    assert projection.records == ()
    assert projection.rejected_row_count == 1


def test_cultural_projection_deduplicates_only_equivalent_payload() -> None:
    """같은 canonical ID와 같은 Gold payload만 dedupe한다."""
    row = _cultural_row()
    projection = build_cultural_event_projection(
        (row, dict(row)), last_seen_dttm=OBSERVED_AT, today=TODAY
    )
    assert len(projection.records) == 1
    assert projection.rejected_row_count == 0


def test_cultural_projection_rejects_identity_collision() -> None:
    """같은 canonical ID의 서로 다른 Point는 snapshot 전체를 거부한다."""
    with pytest.raises(ContractViolation, match="payload가 충돌"):
        build_cultural_event_projection(
            (_cultural_row(), _cultural_row(LOT=127.0573)),
            last_seen_dttm=OBSERVED_AT,
            today=TODAY,
        )


def test_empty_cultural_snapshot_is_explicit_empty_projection() -> None:
    """0건 source snapshot을 정상 EMPTY projection으로 보존한다."""
    projection = build_cultural_event_projection(
        (), last_seen_dttm=OBSERVED_AT, today=TODAY
    )
    assert projection.records == ()
    assert projection.rejected_row_count == 0


def test_stadium_asset_has_exact_ssot_sha_and_eleven_codes() -> None:
    """공연 시설 Point asset의 exact bytes와 11개 코드를 고정한다."""
    coordinates = load_stadium_coordinates(ASSET_PATH)
    assert STADIUM_COORDINATE_SHA256 == (
        "0e0c047bd08f77e82bbccda969c0e726af6998ceaa92979081506cb2140a969b"
    )
    assert len(coordinates) == 11
    assert {item.code for item in coordinates} == {str(value) for value in range(5, 16)}


def test_stadium_asset_rejects_tampered_actual_bytes() -> None:
    """manifest에 넣을 stadium input의 actual bytes 변조를 거부한다."""
    with pytest.raises(ContractViolation, match="SHA-256"):
        parse_stadium_coordinates(ASSET_PATH.read_bytes() + b"\n")


def test_performance_projection_joins_curated_coordinate() -> None:
    """공연행사를 exact 시설 코드·명칭으로 Point와 결합한다."""
    projection = build_performance_event_projection(
        (_performance_row(),),
        last_seen_dttm=OBSERVED_AT,
        today=TODAY,
        coordinates=load_stadium_coordinates(ASSET_PATH),
    )
    [record] = projection.records
    assert record.event_source_cd == PERFORMANCE_EVENT_SOURCE
    assert record.event_id == "performance_event:event-1"
    assert record.event_spot_nm == "잠실야구장"
    assert record.event_point_source_cd == "curated_osm_nominatim"
    assert record.location_accuracy_cd == "approximate"


@pytest.mark.parametrize(
    "overrides",
    [
        {"SCH_SEQ": None},
        {"SCH_CODE_B": None},
        {"SCH_CODE_B": "999"},
    ],
)
def test_performance_projection_keeps_unmappable_rows_silver_only(
    overrides: dict[str, object],
) -> None:
    """안정 ID 또는 검수 Point가 없는 행을 Gold에 넣지 않는다."""
    projection = build_performance_event_projection(
        (_performance_row(**overrides),),
        last_seen_dttm=OBSERVED_AT,
        today=TODAY,
        coordinates=load_stadium_coordinates(ASSET_PATH),
    )
    assert projection.records == ()
    assert projection.rejected_row_count == 1


def test_performance_projection_rejects_code_name_mismatch_for_snapshot() -> None:
    """시설 코드·명칭 불일치를 임의 Point 결합 없이 전체 거부한다."""
    with pytest.raises(ContractViolation, match="코드·명칭"):
        build_performance_event_projection(
            (_performance_row(CODE_TITLE_B="다른 시설"),),
            last_seen_dttm=OBSERVED_AT,
            today=TODAY,
            coordinates=load_stadium_coordinates(ASSET_PATH),
        )


def test_event_sources_remain_isolated() -> None:
    """두 source가 같은 source ID를 써도 event identity를 병합하지 않는다."""
    cultural = build_cultural_event_projection(
        (_cultural_row(TITLE="event-1"),),
        last_seen_dttm=OBSERVED_AT,
        today=TODAY,
    ).records[0]
    performance = build_performance_event_projection(
        (_performance_row(),),
        last_seen_dttm=OBSERVED_AT,
        today=TODAY,
        coordinates=load_stadium_coordinates(ASSET_PATH),
    ).records[0]
    assert cultural.event_id.startswith("cultural_event:v1:")
    assert performance.event_id == "performance_event:event-1"
    assert cultural.event_id != performance.event_id


def test_event_output_parquet_round_trips_exact_projection() -> None:
    """immutable event output Parquet이 Point EWKB와 행 정렬을 보존한다."""
    records = build_cultural_event_projection(
        (_cultural_row(),),
        last_seen_dttm=OBSERVED_AT,
        today=TODAY,
    ).records
    assert _records_from_parquet(_records_to_parquet(records)) == records
