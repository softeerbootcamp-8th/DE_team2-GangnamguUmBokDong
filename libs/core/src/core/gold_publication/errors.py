"""Gold publication 공통 예외 계층을 정의한다."""


class PublicationError(Exception):
    """Gold publication 처리 중 발생하는 모든 도메인 오류의 기반 예외다."""


class ContractViolation(PublicationError):
    """publication contract를 충족하지 못한 입력을 나타낸다."""


class CanonicalizationError(ContractViolation):
    """값을 contract의 canonical JSON으로 표현할 수 없음을 나타낸다."""


class CanonicalParseError(CanonicalizationError):
    """입력 bytes가 정확한 canonical JSON이 아님을 나타낸다."""


class TimestampFormatError(ContractViolation):
    """시각이 contract의 UTC 6자리 형식과 맞지 않음을 나타낸다."""


class HashFormatError(ContractViolation):
    """해시가 contract의 lowercase SHA-256 형식과 맞지 않음을 나타낸다."""


class GeometryEncodingError(ContractViolation):
    """Point를 contract의 XDR EWKB로 표현할 수 없음을 나타낸다."""


class PublicationStateError(PublicationError):
    """publication state를 읽거나 전진시키는 규칙이 충족되지 않음을 나타낸다."""


class PublicationConflictError(PublicationStateError):
    """같은 publication identity에 서로 다른 내용이 충돌함을 나타낸다."""


class PublicationDependencyError(PublicationStateError):
    """검증한 dependency tuple이 현재 publication state와 다름을 나타낸다."""


class PublicationEmptyError(ContractViolation):
    """EMPTY를 허용하지 않는 publication 입력이 비어 있음을 나타낸다."""


class PublicationTimeError(ContractViolation):
    """logical 또는 business 시각이 publication 시간 계약을 위반함을 나타낸다."""


class PublicationTransactionError(PublicationError):
    """target과 state를 함께 반영하는 원자적 transaction이 실패했음을 나타낸다."""


class ImmutableObjectError(PublicationError):
    """immutable publication object 처리 실패의 기반 예외다."""


class InvalidObjectUriError(ImmutableObjectError):
    """object URI가 지원하는 immutable URI 형식이 아님을 나타낸다."""


class ObjectMissingError(ImmutableObjectError):
    """요청한 immutable object가 존재하지 않음을 나타낸다."""


class ObjectChecksumMismatchError(ImmutableObjectError):
    """읽은 object bytes가 기대한 SHA-256과 다름을 나타낸다."""


class ObjectPartialReadError(ImmutableObjectError):
    """object를 완전한 bytes로 읽지 못했음을 나타낸다."""


class ObjectNotCanonicalError(ImmutableObjectError):
    """JSON object bytes가 canonical contract를 위반함을 나타낸다."""


class ObjectCollisionError(ImmutableObjectError):
    """immutable URI에 서로 다른 bytes를 쓰려는 충돌을 나타낸다."""


class ObjectStoreAccessError(ImmutableObjectError):
    """object store 접근 자체가 실패했음을 나타낸다."""
