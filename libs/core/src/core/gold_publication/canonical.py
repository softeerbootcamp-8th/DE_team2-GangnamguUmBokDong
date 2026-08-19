"""Gold publication v1의 canonical byte primitive를 제공한다."""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
import unicodedata
from datetime import UTC, datetime
from typing import Any, NoReturn, TypeAlias, cast

from .errors import (
    CanonicalizationError,
    CanonicalParseError,
    GeometryEncodingError,
    HashFormatError,
    TimestampFormatError,
)

JsonScalar: TypeAlias = str | int | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]

_MIN_SAFE_INTEGER = -(2**53) + 1
_MAX_SAFE_INTEGER = 2**53 - 1
_LOWERCASE_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_UTC_DTTM_PATTERN = re.compile(
    r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})\."
    r"(?P<microsecond>\d{6})Z\Z"
)
_EWKB_SRID_FLAG = 0x20000000
_EWKB_POINT_TYPE = 1
_WGS84_SRID = 4326


def canonical_json_bytes(value: JsonValue) -> bytes:
    """지원하는 JSON 값을 RFC 8785 순서의 NFC UTF-8 bytes로 직렬화한다."""
    try:
        return _encode_json(value, set()).encode("utf-8")
    except CanonicalizationError:
        raise
    except (RecursionError, TypeError, UnicodeError, ValueError) as exc:
        raise CanonicalizationError("canonical JSON 직렬화에 실패했습니다.") from exc


def parse_canonical_json(payload: bytes) -> JsonValue:
    """정확히 canonical한 UTF-8 JSON bytes만 파싱한다.

    args:
        payload: canonical 여부를 검증할 원본 bytes
    returns:
        contract가 허용하는 JSON 값
    raises:
        CanonicalParseError: UTF-8, JSON 문법, 중복 key 또는 byte 표현이 틀릴 때
    """
    if type(payload) is not bytes:
        raise CanonicalParseError("canonical JSON 입력은 bytes여야 합니다.")

    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CanonicalParseError(
            "canonical JSON은 올바른 UTF-8이어야 합니다."
        ) from exc

    try:
        value = json.loads(
            text,
            object_pairs_hook=_object_from_pairs,
            parse_float=_reject_json_number,
            parse_int=_parse_json_integer,
            parse_constant=_reject_json_constant,
        )
    except CanonicalParseError:
        raise
    except (json.JSONDecodeError, RecursionError) as exc:
        raise CanonicalParseError("올바른 JSON 문서가 아닙니다.") from exc

    try:
        canonical = canonical_json_bytes(cast(JsonValue, value))
    except CanonicalizationError as exc:
        raise CanonicalParseError(str(exc)) from exc
    if canonical != payload:
        raise CanonicalParseError(
            "입력 JSON bytes가 canonical 표현과 정확히 같지 않습니다."
        )
    return cast(JsonValue, value)


def sha256_hex(payload: bytes) -> str:
    """bytes의 lowercase SHA-256 hex digest를 반환한다."""
    if type(payload) is not bytes:
        raise TypeError("SHA-256 입력은 bytes여야 합니다.")
    return hashlib.sha256(payload).hexdigest()


def validate_sha256_hex(value: str) -> str:
    """값이 정확한 lowercase SHA-256이면 그대로 반환한다."""
    if type(value) is not str or _LOWERCASE_SHA256_PATTERN.fullmatch(value) is None:
        raise HashFormatError("SHA-256은 정확히 64자리 lowercase hex여야 합니다.")
    return value


def format_utc_dttm(value: datetime) -> str:
    """timezone-aware 시각을 UTC 마이크로초 6자리 문자열로 변환한다."""
    if type(value) is not datetime:
        raise TimestampFormatError("시각은 datetime이어야 합니다.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise TimestampFormatError("시각은 timezone-aware datetime이어야 합니다.")

    utc_value = value.astimezone(UTC)
    return (
        f"{utc_value.year:04d}-{utc_value.month:02d}-{utc_value.day:02d}T"
        f"{utc_value.hour:02d}:{utc_value.minute:02d}:{utc_value.second:02d}."
        f"{utc_value.microsecond:06d}Z"
    )


def parse_utc_dttm(value: str) -> datetime:
    """contract의 UTC 마이크로초 6자리 문자열을 aware datetime으로 파싱한다."""
    if type(value) is not str:
        raise TimestampFormatError("UTC 시각은 문자열이어야 합니다.")
    match = _UTC_DTTM_PATTERN.fullmatch(value)
    if match is None:
        raise TimestampFormatError(
            "UTC 시각은 YYYY-MM-DDTHH:MM:SS.ffffffZ 형식이어야 합니다."
        )
    try:
        parsed = datetime(
            year=int(match.group("year")),
            month=int(match.group("month")),
            day=int(match.group("day")),
            hour=int(match.group("hour")),
            minute=int(match.group("minute")),
            second=int(match.group("second")),
            microsecond=int(match.group("microsecond")),
            tzinfo=UTC,
        )
    except ValueError as exc:
        raise TimestampFormatError(
            "UTC 시각의 날짜 또는 시간 값이 유효하지 않습니다."
        ) from exc
    return parsed


def point_ewkb_xdr_hex(longitude: float, latitude: float) -> str:
    """WGS84 Point를 SRID 4326 big-endian EWKB lowercase hex로 직렬화한다."""
    x = _finite_coordinate(longitude, "longitude")
    y = _finite_coordinate(latitude, "latitude")
    ewkb = struct.pack(
        ">BIIdd",
        0,
        _EWKB_SRID_FLAG | _EWKB_POINT_TYPE,
        _WGS84_SRID,
        x,
        y,
    )
    return ewkb.hex()


def point_ewkb_hex(longitude: float, latitude: float) -> str:
    """WGS84 Point의 contract EWKB hex를 반환한다."""
    return point_ewkb_xdr_hex(longitude, latitude)


def validate_point_ewkb_xdr_hex(value: str) -> str:
    """값이 SRID 4326 big-endian Point EWKB lowercase hex인지 검증한다."""
    if type(value) is not str or len(value) != 50:
        raise GeometryEncodingError(
            "Point EWKB는 정확히 25-byte lowercase hex여야 합니다."
        )
    if value != value.lower() or re.fullmatch(r"[0-9a-f]{50}", value) is None:
        raise GeometryEncodingError("Point EWKB는 lowercase hex여야 합니다.")
    try:
        byte_order, geometry_type, srid, longitude, latitude = struct.unpack(
            ">BIIdd", bytes.fromhex(value)
        )
    except (ValueError, struct.error) as exc:
        raise GeometryEncodingError("Point EWKB를 해석할 수 없습니다.") from exc
    if (
        byte_order != 0
        or geometry_type != _EWKB_SRID_FLAG | _EWKB_POINT_TYPE
        or srid != _WGS84_SRID
    ):
        raise GeometryEncodingError(
            "Point EWKB는 SRID 4326 big-endian 2D Point여야 합니다."
        )
    _finite_coordinate(longitude, "longitude")
    _finite_coordinate(latitude, "latitude")
    return value


def _encode_json(value: JsonValue, active_containers: set[int]) -> str:
    """JSON 값을 공백 없는 canonical 문자열로 재귀 직렬화한다."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if type(value) is str:
        return _encode_string(value)
    if type(value) is int:
        if not _MIN_SAFE_INTEGER <= value <= _MAX_SAFE_INTEGER:
            raise CanonicalizationError(
                "JSON integer는 IEEE-754에서 정확히 표현 가능한 범위여야 합니다."
            )
        return str(value)
    if type(value) in {list, dict}:
        return _encode_container(value, active_containers)
    raise CanonicalizationError(
        "canonical JSON은 dict, list, string, integer, boolean, null만 허용합니다."
    )


def _encode_container(
    value: list[JsonValue] | dict[str, JsonValue], active_containers: set[int]
) -> str:
    """순환 참조를 차단하며 JSON 배열 또는 object를 직렬화한다."""
    identity = id(value)
    if identity in active_containers:
        raise CanonicalizationError("순환 참조가 있는 JSON 값은 허용하지 않습니다.")
    active_containers.add(identity)
    try:
        if type(value) is list:
            items = (_encode_json(item, active_containers) for item in value)
            return "[" + ",".join(items) + "]"
        return _encode_object(value, active_containers)
    finally:
        active_containers.remove(identity)


def _encode_object(value: dict[str, JsonValue], active_containers: set[int]) -> str:
    """object key의 NFC를 검증하고 UTF-16 code unit 순으로 직렬화한다."""
    normalized_items: list[tuple[str, JsonValue]] = []
    normalized_keys: set[str] = set()
    for key, item in value.items():
        if type(key) is not str:
            raise CanonicalizationError("JSON object key는 문자열이어야 합니다.")
        normalized_key = _normalize_string(key)
        if normalized_key in normalized_keys:
            raise CanonicalizationError("NFC 정규화 뒤 중복되는 object key가 있습니다.")
        normalized_keys.add(normalized_key)
        normalized_items.append((normalized_key, item))

    normalized_items.sort(key=lambda item: item[0].encode("utf-16-be"))
    fields = (
        f"{_encode_string(key)}:{_encode_json(item, active_containers)}"
        for key, item in normalized_items
    )
    return "{" + ",".join(fields) + "}"


def _encode_string(value: str) -> str:
    """문자열의 NFC를 검증하고 RFC 8785 escape 규칙으로 인코딩한다."""
    normalized = _normalize_string(value)
    return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


def _normalize_string(value: str) -> str:
    """문자열이 Unicode scalar로 구성된 NFC인지 검증한다."""
    if type(value) is not str:
        raise CanonicalizationError("canonical JSON 문자열은 builtin str이어야 합니다.")
    for character in value:
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            raise CanonicalizationError(
                "Unicode surrogate code point는 허용하지 않습니다."
            )
        if _is_unicode_noncharacter(code_point):
            raise CanonicalizationError("Unicode noncharacter는 허용하지 않습니다.")
    normalized = unicodedata.normalize("NFC", value)
    if normalized != value:
        raise CanonicalizationError("canonical JSON 문자열은 이미 NFC여야 합니다.")
    return value


def _object_from_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """JSON object를 만들며 원문 중복 key를 거부한다."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CanonicalParseError("JSON object에 중복 key가 있습니다.")
        result[key] = value
    return result


def _parse_json_integer(value: str) -> int:
    """JSON integer token을 안전한 정수로 파싱한다."""
    parsed = int(value)
    if not _MIN_SAFE_INTEGER <= parsed <= _MAX_SAFE_INTEGER:
        raise CanonicalParseError(
            "JSON integer는 IEEE-754에서 정확히 표현 가능한 범위여야 합니다."
        )
    return parsed


def _reject_json_number(value: str) -> NoReturn:
    """float 형태의 JSON number를 항상 거부한다."""
    raise CanonicalParseError(f"float JSON number는 허용하지 않습니다: {value}")


def _reject_json_constant(value: str) -> NoReturn:
    """NaN과 Infinity 같은 비표준 JSON 상수를 항상 거부한다."""
    raise CanonicalParseError(f"비표준 JSON 상수는 허용하지 않습니다: {value}")


def _finite_coordinate(value: float, name: str) -> float:
    """좌표를 유한한 float로 검증한다."""
    if type(value) not in {int, float}:
        raise GeometryEncodingError(f"{name} 좌표는 숫자여야 합니다.")
    try:
        coordinate = float(value)
    except (OverflowError, ValueError) as exc:
        raise GeometryEncodingError(f"{name} 좌표는 유한한 숫자여야 합니다.") from exc
    if not math.isfinite(coordinate):
        raise GeometryEncodingError(f"{name} 좌표는 유한해야 합니다.")
    return coordinate


def _is_unicode_noncharacter(code_point: int) -> bool:
    """I-JSON에서 금지하는 Unicode noncharacter인지 반환한다."""
    return 0xFDD0 <= code_point <= 0xFDEF or code_point & 0xFFFF in {
        0xFFFE,
        0xFFFF,
    }
