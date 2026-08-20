"""Gold publication correction revision allocator의 순수 판정을 검증한다."""

from datetime import UTC, datetime, timedelta

import pytest
from core.gold_publication.errors import ContractViolation
from gold.versioning import (
    CurrentPublication,
    PublicationCandidate,
    choose_revision,
)

LOGICAL_DTTM = datetime(2026, 8, 19, tzinfo=UTC)


def _candidate(**overrides: object) -> PublicationCandidate:
    """테스트용 weather_grid candidate를 만든다."""
    values = {
        "publication_key": "weather_grid",
        "logical_dttm": LOGICAL_DTTM,
        "artifact_set_sha256": "1" * 64,
        "input_fingerprint_sha256": "2" * 64,
        "published_row_cnt": 34,
    }
    values.update(overrides)
    return PublicationCandidate(**values)  # type: ignore[arg-type]


def _current(**overrides: object) -> CurrentPublication:
    """테스트용 current publication state를 만든다."""
    values = {
        "logical_dttm": LOGICAL_DTTM,
        "revision_no": 3,
        "artifact_set_sha256": "1" * 64,
        "input_fingerprint_sha256": "2" * 64,
        "published_row_cnt": 34,
    }
    values.update(overrides)
    return CurrentPublication(**values)  # type: ignore[arg-type]


def test_revision_starts_at_zero_per_key_and_logical_time() -> None:
    """state 없음·새 logical·과거 logical은 upstream revision과 무관하게 0을 쓴다."""
    assert choose_revision(_candidate(), None) == 0
    assert (
        choose_revision(
            _candidate(logical_dttm=LOGICAL_DTTM + timedelta(minutes=1)),
            _current(),
        )
        == 0
    )
    assert (
        choose_revision(
            _candidate(logical_dttm=LOGICAL_DTTM - timedelta(minutes=1)),
            _current(),
        )
        == 0
    )


def test_same_content_replays_current_revision() -> None:
    """같은 logical과 content는 새 correction을 만들지 않고 exact replay한다."""
    assert choose_revision(_candidate(), _current()) == 3


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("artifact_set_sha256", "3" * 64),
        ("input_fingerprint_sha256", "4" * 64),
        ("published_row_cnt", 33),
    ),
)
def test_same_logical_changed_content_allocates_next_correction(
    field: str,
    value: object,
) -> None:
    """같은 logical의 output·input·count 변경은 current+1 correction을 만든다."""
    assert choose_revision(_candidate(**{field: value}), _current()) == 4


def test_revision_overflow_fails_closed() -> None:
    """PostgreSQL INTEGER 한계 뒤 correction을 조용히 wrap하지 않는다."""
    with pytest.raises(ContractViolation, match="한계"):
        choose_revision(
            _candidate(input_fingerprint_sha256="3" * 64),
            _current(revision_no=2_147_483_647),
        )
