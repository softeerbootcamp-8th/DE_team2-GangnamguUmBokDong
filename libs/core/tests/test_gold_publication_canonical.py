"""Gold publication canonical byte contract를 검증한다."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from core.gold_publication.canonical import (
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    parse_utc_dttm,
    point_ewkb_hex,
    point_ewkb_xdr_hex,
    sha256_hex,
    validate_point_ewkb_xdr_hex,
    validate_sha256_hex,
)
from core.gold_publication.errors import (
    CanonicalizationError,
    CanonicalParseError,
    GeometryEncodingError,
    HashFormatError,
    TimestampFormatError,
)

_ARTIFACT_SET = {
    "schema_version": "gold-artifact-set-v1",
    "artifacts": [
        {
            "role": "route_stops",
            "uri": "s3://fixture/route-stops.parquet",
            "row_count": 1,
            "byte_sha256": "c" * 64,
        },
        {
            "role": "routes",
            "uri": "s3://fixture/routes.parquet",
            "row_count": 1,
            "byte_sha256": "d" * 64,
        },
    ],
}
_ARTIFACT_SET_BYTES = (
    b'{"artifacts":[{"byte_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccc'
    b'cccccccccccccccc","role":"route_stops","row_count":1,"uri":"s3://fixture/route-'
    b'stops.parquet"},{"byte_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddd'
    b'dddddddddddddddd","role":"routes","row_count":1,"uri":"s3://fixture/routes.parquet"'
    b'}],"schema_version":"gold-artifact-set-v1"}'
)


class _EvilInteger(int):
    """문자열 표현을 float token으로 바꾸는 악성 int subclass다."""

    def __str__(self) -> str:
        """canonical integer가 아닌 token을 반환한다."""
        return "1.0"


class _EvilString(str):
    """정렬용 byte encoding을 뒤집는 악성 str subclass다."""

    def encode(self, *_args: object, **_kwargs: object) -> bytes:
        """실제 문자열과 무관한 bytes를 반환한다."""
        return b"z"


class _EvilBytes(bytes):
    """decode와 equality를 바꾸는 악성 bytes subclass다."""

    def decode(self, *_args: object, **_kwargs: object) -> str:
        """underlying bytes와 무관한 canonical JSON을 반환한다."""
        return '{"safe":true}'

    def __ne__(self, _other: object) -> bool:
        """모든 canonical byte 비교가 같다고 가장한다."""
        return False


def test_canonical_json_matches_artifact_set_regression_vector() -> None:
    """SSOT artifact-set bytes와 SHA-256 회귀값을 그대로 재현한다."""
    payload = canonical_json_bytes(_ARTIFACT_SET)

    assert payload == _ARTIFACT_SET_BYTES
    assert sha256_hex(payload) == (
        "576eec2c53f1be8985ce531f512f4f4014fe05879d1f53714128dd774d8abf87"
    )


def test_canonical_json_matches_empty_artifact_set_vector() -> None:
    """정상 EMPTY artifact-set bytes와 SHA-256을 재현한다."""
    payload = canonical_json_bytes(
        {"schema_version": "gold-artifact-set-v1", "artifacts": []}
    )

    assert payload == b'{"artifacts":[],"schema_version":"gold-artifact-set-v1"}'
    assert sha256_hex(payload) == (
        "98f11969010a550c3b20fd37879e45ec1682b3b05d4c7a25e590a7f0874a4cdb"
    )


def test_canonical_json_uses_utf16_key_order() -> None:
    """supplementary key를 Unicode code point가 아닌 UTF-16 단위로 정렬한다."""
    payload = canonical_json_bytes({"\ue000": "é", "\U0001f600": "ok"})

    assert payload == '{"😀":"ok","\ue000":"é"}'.encode()


def test_canonical_json_uses_rfc8785_control_escapes() -> None:
    """control 문자는 RFC 8785가 지정한 짧은 escape와 lowercase hex를 쓴다."""
    payload = canonical_json_bytes({"control": "\b\t\n\f\r\x00\x0f"})

    assert payload == b'{"control":"\\b\\t\\n\\f\\r\\u0000\\u000f"}'


@pytest.mark.parametrize("value", [1.0, float("nan"), float("inf"), ("not", "json")])
def test_canonical_json_rejects_non_contract_types(value: object) -> None:
    """float와 JSON 외 container는 canonical 문서에 들어갈 수 없다."""
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    [
        _EvilInteger(1),
        _EvilString("text"),
        [_EvilInteger(1)],
        {_EvilString("key"): 1},
    ],
)
def test_canonical_json_rejects_builtin_subclass_overrides(value: object) -> None:
    """override로 canonical bytes를 바꿀 수 있는 builtin subclass를 거부한다."""
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes(value)  # type: ignore[arg-type]


def test_parse_and_hash_reject_bytes_subclass_overrides() -> None:
    """parser와 hash가 서로 다른 payload를 보게 하는 bytes subclass를 거부한다."""
    payload = _EvilBytes(b"not-json")

    with pytest.raises(CanonicalParseError, match="bytes"):
        parse_canonical_json(payload)
    with pytest.raises(TypeError, match="bytes"):
        sha256_hex(payload)


@pytest.mark.parametrize("value", [2**53, -(2**53)])
def test_canonical_json_rejects_non_interoperable_integers(value: int) -> None:
    """IEEE-754에서 정확히 왕복되지 않는 integer를 거부한다."""
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes(value)


def test_canonical_json_rejects_surrogates_and_non_nfc_strings() -> None:
    """Unicode scalar가 아니거나 NFC로 입력되지 않은 key와 value를 거부한다."""
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes("\ud800")
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes("e\u0301")
    with pytest.raises(CanonicalizationError):
        canonical_json_bytes({"e\u0301": 1})


@pytest.mark.parametrize("value", ["\ufdd0", "\ufffe", "\U0010ffff"])
def test_canonical_json_rejects_unicode_noncharacters(value: str) -> None:
    """I-JSON이 금지하는 Unicode noncharacter를 key와 value에서 거부한다."""
    with pytest.raises(CanonicalizationError, match="noncharacter"):
        canonical_json_bytes(value)
    with pytest.raises(CanonicalizationError, match="noncharacter"):
        canonical_json_bytes({value: "invalid"})


def test_parse_canonical_json_rejects_escaped_unicode_noncharacter() -> None:
    """escape로 표현된 noncharacter도 canonical parser에서 거부한다."""
    with pytest.raises(CanonicalParseError, match="noncharacter"):
        parse_canonical_json(b'"\\ufdd0"')


def test_canonical_json_rejects_cycles() -> None:
    """순환 container를 무한 재귀 대신 명시적인 계약 위반으로 거부한다."""
    value: list[object] = []
    value.append(value)

    with pytest.raises(CanonicalizationError):
        canonical_json_bytes(value)  # type: ignore[arg-type]


def test_parse_canonical_json_roundtrips_exact_bytes() -> None:
    """canonical bytes는 손실 없이 지원 JSON 값으로 돌아온다."""
    payload = b'{"array":[null,true,false,-1],"text":"Gold"}'

    assert parse_canonical_json(payload) == {
        "array": [None, True, False, -1],
        "text": "Gold",
    }


@pytest.mark.parametrize(
    "payload",
    [
        b'{"a":1, "b":2}',
        b'{"b":2,"a":1}',
        b'{"a":1,"a":1}',
        b'{"text":"\\u0061"}',
        '{"text":"e\u0301"}'.encode(),
        b'{"number":1.0}',
        b'{"number":NaN}',
        b'{"number":-0}',
        b"\xff",
    ],
)
def test_parse_canonical_json_rejects_noncanonical_bytes(payload: bytes) -> None:
    """문법상 읽히더라도 원본 bytes가 exact canonical이 아니면 거부한다."""
    with pytest.raises(CanonicalParseError):
        parse_canonical_json(payload)


def test_utc_dttm_has_exact_six_digits_and_converts_to_utc() -> None:
    """offset datetime을 UTC Z와 마이크로초 6자리로 고정한다."""
    value = datetime(
        2026,
        8,
        20,
        9,
        1,
        2,
        3,
        tzinfo=timezone(timedelta(hours=9)),
    )

    assert format_utc_dttm(value) == "2026-08-20T00:01:02.000003Z"
    assert parse_utc_dttm("2026-08-20T00:01:02.000003Z") == value.astimezone(UTC)


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 8, 20, tzinfo=UTC).replace(tzinfo=None),
        "2026-08-20T00:00:00.000000Z",
    ],
)
def test_format_utc_dttm_rejects_naive_or_non_datetime(value: object) -> None:
    """timezone 정보가 없거나 datetime이 아닌 입력을 거부한다."""
    with pytest.raises(TimestampFormatError):
        format_utc_dttm(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    [
        "2026-08-20T00:00:00Z",
        "2026-08-20T00:00:00.00000Z",
        "2026-08-20T00:00:00.000000+00:00",
        "2026-02-30T00:00:00.000000Z",
    ],
)
def test_parse_utc_dttm_rejects_non_contract_format(value: str) -> None:
    """UTC Z와 마이크로초 정확히 6자리가 아닌 시각을 거부한다."""
    with pytest.raises(TimestampFormatError):
        parse_utc_dttm(value)


def test_sha256_validation_requires_lowercase_64_hex() -> None:
    """SHA-256 validator는 lowercase 64 hex만 통과시킨다."""
    digest = "a" * 64

    assert validate_sha256_hex(digest) == digest
    with pytest.raises(HashFormatError):
        validate_sha256_hex("A" * 64)
    with pytest.raises(HashFormatError):
        validate_sha256_hex("a" * 63)


def test_point_ewkb_matches_postgis_xdr_regression_vector() -> None:
    """POINT(127.0 37.5)의 SRID 4326 XDR EWKB를 정확히 재현한다."""
    assert point_ewkb_xdr_hex(127.0, 37.5) == (
        "0020000001000010e6405fc000000000004042c00000000000"
    )
    assert point_ewkb_hex(127.0, 37.5) == point_ewkb_xdr_hex(127.0, 37.5)
    assert validate_point_ewkb_xdr_hex(point_ewkb_hex(127.0, 37.5)) == (
        "0020000001000010e6405fc000000000004042c00000000000"
    )


@pytest.mark.parametrize(
    ("longitude", "latitude"),
    [(float("nan"), 37.5), (127.0, float("inf")), (True, 37.5)],
)
def test_point_ewkb_rejects_nonfinite_or_boolean_coordinates(
    longitude: float, latitude: float
) -> None:
    """EWKB encoder는 유한한 숫자 좌표만 허용한다."""
    with pytest.raises(GeometryEncodingError):
        point_ewkb_xdr_hex(longitude, latitude)

    with pytest.raises(GeometryEncodingError):
        point_ewkb_xdr_hex(10**1000, 37.5)


@pytest.mark.parametrize(
    "value",
    [
        "0020000001000010E6405fc000000000004042c00000000000",
        "0020000001000010e6405fc000000000004042c0000000000",
        "0101000020e61000000000000000c05f400000000000c04240",
        "0020000001000010e67ff80000000000004042c00000000000",
    ],
)
def test_point_ewkb_validator_rejects_non_contract_bytes(value: str) -> None:
    """validator는 lowercase XDR SRID 4326 유한 Point만 허용한다."""
    with pytest.raises(GeometryEncodingError):
        validate_point_ewkb_xdr_hex(value)
