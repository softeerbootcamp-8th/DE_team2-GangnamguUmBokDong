"""서울 열린데이터광장 공용 페이지네이션 규약 어댑터.

RESULT.CODE → 실패 범주:

| 코드 | 범주 | 근거 |
| --- | --- | --- |
| `INFO-000` | 성공 | |
| `INFO-200` (해당 데이터 없음) | 성공(빈 결과) | 정상 상황을 누락으로 세면 완결도가 왜곡된다 |
| `INFO-100` (인증키 오류) | `FATAL` | 모든 조각이 같은 키를 쓰므로 재시도·백필이 무의미 |
| `ERROR-500` 계열 (서버 오류) | `TRANSIENT` | 라운드로 회복 가능 |
| 그 외 요청 오류 | `PERMANENT` | 그 조각만 누락 확정 |

주의:
- 인증키(`SEOUL_OPENAPI_KEY`)는 URL 경로에 그대로 실리므로 로그·예외 메시지·bronze·
  조각 키 어디에도 남지 않게 마스킹한다.
- 응답 필드는 전부 문자열로 내려온다. 캐스팅은 검증 엔진의 `types`가 맡고 어댑터는
  값에 손대지 않는다.
- 기본 `pagination: total`은 전체 건수인 `list_total_count`를 사용한다. 현재 페이지
  행 수를 돌려주는 `bikeList`는 `pagination: probe_until_empty`로 빈 페이지까지
  순차 탐색한다.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

import httpx
from core.forecast import POPULATION_FORECAST_SLOT_COUNT

from adapters.base import FetchErrorKind, FetchResult, adapter, run_concurrent

if TYPE_CHECKING:
    from config.schema import SourceConfig

_BASE_URL = "http://openapi.seoul.go.kr:8088"

_FATAL_CODES = {"INFO-100"}
_SUCCESS_CODES = {"INFO-000", "INFO-200"}

# adapter_params에 `concurrency`를 선언하지 않은 소스는 순차로 돈다. 병렬은 소스별
# opt-in이다 — 페이지가 몇 개뿐인 소스는 이득이 없고, 한 API에 동시 요청을 늘리는
# 것이라 필요한 곳에만 켠다.
_DEFAULT_CONCURRENCY = 1


def _api_key() -> str:
    # URL 경로에 그대로 박히는 값이라, 호출한 곳(fetch)이 알아서 마스킹해야 한다.
    # 이 함수는 조각 키에도, 예외 메시지에도 이 값을 노출하지 않는다.
    return os.environ["SEOUL_OPENAPI_KEY"]


def _classify(code: str | None) -> FetchErrorKind | None:
    """RESULT.CODE를 실패 범주로 매핑한다. None은 성공이다."""

    # 코드가 없거나 문자열이 아니면 영구 실패로 취급한다.
    if not isinstance(code, str):
        return FetchErrorKind.PERMANENT
    # INFO-000(성공)과 INFO-200(빈 결과)는 성공으로 취급한다.
    if code in _SUCCESS_CODES:
        return None
    # INFO-100(인증키 오류)는 치명적 에러로 취급한다.
    if code in _FATAL_CODES:
        return FetchErrorKind.FATAL
    # ERROR-5xx(서버 오류)는 일시적 에러로 취급한다.
    if code.startswith("ERROR-5"):
        return FetchErrorKind.TRANSIENT
    # 그 외 에러들은 영구적 에러로 취급한다.
    return FetchErrorKind.PERMANENT


def _extract(body: dict, wrapper_key: str) -> dict:
    """서울 API 응답은 서비스명을 키로 감싸서 온다."""
    return body.get(wrapper_key, {})


def _result_code(body: dict, wrapper_key: str) -> str | None:
    """본문에서 RESULT.CODE를 꺼낸다. 서울 API는 두 형태를 쓴다.

    보통은 서비스명 래퍼 안에 `RESULT.CODE`가 있다. 그런데 **시작 인덱스가
    `list_total_count`를 넘으면 래퍼 없이 최상단에 `CODE`만 온다**
    (실측: `{"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}`).
    래퍼만 보면 `None`이 되어 `_classify`가 PERMANENT로 오판한다.
    """
    wrapper = _extract(body, wrapper_key)
    if isinstance(wrapper, dict):
        code = wrapper.get("RESULT", {}).get("CODE")
        if isinstance(code, str):
            return code
    top_level = body.get("CODE")
    return top_level if isinstance(top_level, str) else None


# citydata_ppltn 응답의 `FCST_PPLTN`은 `{FCST_TIME, FCST_CONGEST_LVL, FCST_PPLTN_MIN,
# FCST_PPLTN_MAX}`가 12개 들어있는 중첩 배열이다. 검증 엔진의 `types`는 스칼라만
# 다루므로 여기서 평탄한 슬롯 컬럼으로 펼친다 — 어댑터가 값에 손대지 않는다는 원칙은
# 지킨다(문자열을 그대로 옮기고 캐스팅은 엔진이 한다).
_FCST_ARRAY_KEY = "FCST_PPLTN"
_FCST_SLOT_FIELDS = (
    "FCST_TIME",
    "FCST_CONGEST_LVL",
    "FCST_PPLTN_MIN",
    "FCST_PPLTN_MAX",
)
# 실측(2026-08-19 POI001): 1시간 간격 12개. 슬롯 번호는 "n시간 후"가 아니다 —
# 20:55 관측의 첫 예측이 22:00이었다. 몇 시의 예측인지는 `FCST_n_TIME`이 말해주고,
# 소비자(normalizer)는 슬롯 번호가 아니라 그 값으로 시각을 맞춘다.


def _flatten_forecast(row: dict) -> dict:
    """`FCST_PPLTN` 중첩 배열을 `FCST_1_TIME`~`FCST_12_PPLTN_MAX` 슬롯 컬럼으로 펼친다.

    `FCST_TIME` 오름차순으로 슬롯 번호를 매긴다 — API가 시간순으로 준다는 보장이
    문서에 없다. 값이 없는 슬롯은 키를 만들지 않아 엔진의 `optional_missing`이
    결측으로 처리하게 둔다. 원본 중첩 키는 제거한다(parquet·격리 저장이 스칼라만 받는다).
    """
    forecasts = row.pop(_FCST_ARRAY_KEY, None)
    if not isinstance(forecasts, list):
        return row

    datable = [
        f
        for f in forecasts
        if isinstance(f, dict) and isinstance(f.get("FCST_TIME"), str)
    ]
    for slot, forecast in enumerate(
        sorted(datable, key=lambda f: f["FCST_TIME"])[:POPULATION_FORECAST_SLOT_COUNT],
        start=1,
    ):
        for field in _FCST_SLOT_FIELDS:
            value = forecast.get(field)
            if value is not None:
                row[f"FCST_{slot}_{field.removeprefix('FCST_')}"] = value
    return row


@dataclass(frozen=True, slots=True)
class _PageOutcome:
    """페이지 하나의 조회 결과. 스레드풀 워커가 돌려주는 값이라 순수 데이터로 둔다."""

    payload: bytes | None
    total: int | None
    error: FetchErrorKind | None
    row_count: int | None = None
    terminal: bool = False


class NaturalKeyCardinalityError(ValueError):
    """원천 행 수와 유효한 고유 자연키 수가 일치하지 않을 때 발생한다."""


def _request_json(
    client: httpx.Client, url: str
) -> tuple[bytes, dict] | FetchErrorKind:
    """URL을 요청해 JSON 객체로 파싱한다. 예외를 밖으로 던지지 않는다.

    스레드풀에서 병렬 호출되므로 공유 상태를 두지 않는다. `httpx.Client`는 스레드
    안전하고, 하나를 공유하면 커넥션 풀도 재사용된다.
    """
    try:
        response = client.get(url)
        response.raise_for_status()
        body = json.loads(response.content)
        if not isinstance(body, dict):
            raise json.JSONDecodeError("응답이 JSON 객체가 아님", "", 0)
    except (httpx.RequestError, httpx.HTTPStatusError, json.JSONDecodeError):
        # 타임아웃·커넥션 에러, 또는 서버가 5xx와 함께 HTML을 내려보내 파싱에 실패한 경우
        return FetchErrorKind.TRANSIENT
    return response.content, body


def _page_rows(body: dict, wrapper_key: str, row_path: str) -> list | None:
    """설정된 wrapper와 row 경로에서 원천 행 배열을 꺼낸다."""
    node = _extract(body, wrapper_key)
    for segment in row_path.split(".") if row_path else []:
        if not isinstance(node, dict):
            return None
        node = node.get(segment)
    return node if isinstance(node, list) else None


def _optional_nonnegative_int(value: object) -> int | None:
    """응답 메타를 음수가 아닌 정수로 읽고 malformed 값을 거부한다."""
    if value is None:
        return None
    if type(value) is int:
        parsed = value
    elif type(value) is str and value.strip().isdigit():
        parsed = int(value.strip())
    else:
        raise ValueError("list_total_count는 음수가 아닌 정수여야 합니다")
    if parsed < 0:
        raise ValueError("list_total_count는 음수가 아닌 정수여야 합니다")
    return parsed


def _fetch_page(
    client: httpx.Client,
    url: str,
    wrapper_key: str,
    row_path: str,
    *,
    ignore_total: bool = False,
) -> _PageOutcome:
    """페이지 하나를 받아 실패 범주까지 판정한다."""
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(
            payload=None,
            total=None,
            row_count=None,
            terminal=False,
            error=result,
        )
    content, body = result

    # 서울 API는 보통 HTTP 200으로 응답하고, 논리적 성공/실패는 본문 코드에 담아 보낸다
    code = _result_code(body, wrapper_key)
    category = _classify(code)
    if category is not None:
        return _PageOutcome(
            payload=None,
            total=None,
            row_count=None,
            terminal=False,
            error=category,
        )

    wrapper = _extract(body, wrapper_key)
    raw_total = wrapper.get("list_total_count") if isinstance(wrapper, dict) else None
    rows = _page_rows(body, wrapper_key, row_path)
    terminal = code == "INFO-200" or rows == []
    if rows is None and not terminal:
        return _PageOutcome(
            payload=None,
            total=None,
            row_count=None,
            terminal=False,
            error=FetchErrorKind.PERMANENT,
        )
    try:
        total = None if ignore_total else _optional_nonnegative_int(raw_total)
    except ValueError:
        return _PageOutcome(
            payload=None,
            total=None,
            row_count=None,
            terminal=False,
            error=FetchErrorKind.PERMANENT,
        )
    return _PageOutcome(
        payload=content,
        total=total,
        row_count=len(rows) if rows is not None else 0,
        terminal=terminal,
        error=None,
    )


def _fetch_probe_page(client: httpx.Client, url: str, wrapper_key: str) -> _PageOutcome:
    """전체 건수를 신뢰하지 않는 페이지를 받아 행 수와 종료 여부를 판정한다.

    `bikeList`의 `list_total_count`는 전체가 아니라 현재 응답의 행 수다. probe
    모드에서는 이 값을 완전히 무시하고, 정상 응답의 `row` 길이와 INFO-200만
    사용한다. INFO-000인데 row가 없거나 list가 아니면 빈 끝으로 오인해 수집을
    조용히 자를 수 있으므로 영구 조각 오류로 돌린다.
    """
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(payload=None, total=None, error=result)
    content, body = result

    code = _result_code(body, wrapper_key)
    category = _classify(code)
    if category is not None:
        return _PageOutcome(payload=None, total=None, error=category)
    if code == "INFO-200":
        return _PageOutcome(
            payload=content,
            total=None,
            error=None,
            row_count=0,
            terminal=True,
        )

    wrapper = _extract(body, wrapper_key)
    rows = wrapper.get("row") if isinstance(wrapper, dict) else None
    if not isinstance(rows, list):
        return _PageOutcome(payload=None, total=None, error=FetchErrorKind.PERMANENT)
    return _PageOutcome(
        payload=content,
        total=None,
        error=None,
        row_count=len(rows),
        terminal=not rows,
    )


def _fetch_poi(client: httpx.Client, url: str, wrapper_key: str) -> _PageOutcome:
    """실시간 인구 POI 하나를 받아 실패 범주까지 판정한다."""
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(
            payload=None,
            total=None,
            row_count=None,
            terminal=False,
            error=result,
        )
    content, body = result

    # citydata_ppltn은 일반 페이지 응답과 달리 RESULT.CODE가 최상단 RESULT 객체의
    # `RESULT.CODE` 키로 내려온다. 빈 POI(INFO-200)도 성공 조각으로 보존한다.
    code = body.get("RESULT.CODE") or body.get("RESULT", {}).get("RESULT.CODE")
    category = _classify(code)
    if category is not None:
        return _PageOutcome(
            payload=None,
            total=None,
            row_count=None,
            terminal=False,
            error=category,
        )
    return _PageOutcome(
        payload=content,
        total=None,
        row_count=None,
        terminal=False,
        error=None,
    )


def _validate_natural_key(
    rows: list[dict], natural_key: tuple[str, ...] | None
) -> None:
    """모든 raw 행에 자연키가 있고 행 수만큼 고유한지 검증한다."""
    if natural_key is None:
        return

    identities: list[tuple[object, ...]] = []
    for index, row in enumerate(rows):
        identity = tuple(row.get(column) for column in natural_key)
        if any(
            value is None
            or value == ""
            or (isinstance(value, str) and not value.strip())
            for value in identity
        ):
            columns = ", ".join(natural_key)
            raise NaturalKeyCardinalityError(
                f"raw row {index}의 natural_key({columns})가 비어 있습니다"
            )
        try:
            hash(identity)
        except TypeError as exc:
            raise NaturalKeyCardinalityError(
                f"raw row {index}의 natural_key 값은 scalar여야 합니다"
            ) from exc
        identities.append(identity)

    unique_count = len(set(identities))
    if unique_count != len(rows):
        raise NaturalKeyCardinalityError(
            "raw row count와 unique natural_key count가 다릅니다: "
            f"rows={len(rows)}, unique={unique_count}"
        )



def _probe_page_key(start: int, page_size: int) -> str:
    """probe 모드의 고정 폭 페이지 키를 만든다."""
    return f"page-{start:05d}-{start + page_size - 1:05d}"


def _minimum_total_covered_by_probe_parts(parts: frozenset[str]) -> int:
    """이미 저장된 probe data part들이 보장하는 최소 row 위치를 반환한다.

    ``page-N-M``이 성공 저장됐다는 것은 적어도 N번째 row가 그 snapshot에 있었다는
    뜻이다. 종료 probe 재시도 중 더 작은 total이 관측되면 서로 다른 snapshot의
    payload와 metadata를 섞는 것이므로 성공으로 확정할 수 없다.
    """
    starts: list[int] = []
    for key in parts:
        if not key.startswith("page-"):
            continue
        try:
            starts.append(int(key.split("-", 2)[1]))
        except (IndexError, ValueError):
            continue
    return max(starts, default=0)


def _recover_probe_total(
    *,
    page_start: int,
    page_size: int,
    skip: frozenset[str],
    client: httpx.Client,
    page_url,
    wrapper_key: str,
) -> int | None:
    """종료 페이지만 재시도한 경우 직전 성공 페이지에서 실제 행 수를 복구한다.

    직전 라운드에서 종료 요청만 일시 실패했다면 이번 라운드의 데이터 페이지는 전부
    skip에 들어 있다. 이때 직전 페이지 하나만 다시 읽어 마지막 페이지의 실제 행 수를
    알아낸다. 데이터 payload를 다시 yield하지 않으므로 Bronze는 중복 저장되지 않는다.
    """
    previous_start = page_start - page_size
    if previous_start < 1:
        return 0
    previous_key = _probe_page_key(previous_start, page_size)
    if previous_key not in skip:
        return None

    outcome = _fetch_probe_page(
        client,
        page_url(previous_start, previous_start + page_size - 1),
        wrapper_key,
    )
    if outcome.error is not None or outcome.terminal or outcome.row_count is None:
        return None
    return previous_start - 1 + outcome.row_count


def _fetch_probe_pages(
    *,
    params: dict,
    client: httpx.Client,
    page_url,
    wrapper_key: str,
    skip: frozenset[str],
    expected_total: int | None,
):
    """응답 total 대신 빈 페이지까지 순차 탐색하는 페이지 결과를 생성한다.

    처음에는 끝을 모르므로 순차 탐색한다. 빈 row 또는 INFO-200 종료 응답도 source
    snapshot의 완결성 증거인 Bronze part로 보존하며 실제 expected_total을 함께
    전달한다. 이후 라운드에는 그 expected_total로 안정적인 고정 폭 페이지 목록을
    복원해 성공 조각은 skip하고 실패 조각만 다시 요청한다.
    """
    page_size = int(params["page_size"])
    max_probe_pages = int(params["max_probe_pages"])

    if expected_total is not None:
        page_count = (expected_total + page_size - 1) // page_size
        if page_count > max_probe_pages:
            start = max_probe_pages * page_size + 1
            yield FetchResult(
                key=_probe_page_key(start, page_size),
                payload=None,
                error=FetchErrorKind.PERMANENT,
                expected_total=None,
            )
            return

        pages = [
            (start, _probe_page_key(start, page_size))
            for start in range(1, page_count * page_size + 1, page_size)
        ]
        for index, (start, key) in enumerate(pages):
            if key in skip:
                continue
            outcome = _fetch_probe_page(
                client,
                page_url(start, start + page_size - 1),
                wrapper_key,
            )
            if outcome.terminal:
                # 이전 라운드에서 확정한 행 수가 있는데 데이터 페이지가 사라지면
                # 스냅샷이 호출 사이에 바뀐 것이다. 남은 범위를 모두 transient로
                # 남겨 부분 snapshot을 완결로 오인하지 않는다.
                for missing_start, missing_key in pages[index:]:
                    if missing_key not in skip:
                        yield FetchResult(
                            key=missing_key,
                            payload=None,
                            error=FetchErrorKind.TRANSIENT,
                            expected_total=None,
                        )
                return
            yield FetchResult(
                key=key,
                payload=outcome.payload,
                error=outcome.error,
                expected_total=None,
            )
            if outcome.error is FetchErrorKind.FATAL:
                return

        # 고정된 expected_total만 믿고 끝내면 재시도 사이에 뒤로 늘어난 행을 놓친다.
        # 기존 범위 바로 다음 페이지부터 terminal까지 다시 탐색해 growth를 수집하고
        # 실제 total을 상위 라운드에 갱신한다. 정적 snapshot이면 추가 호출 한 번으로
        # INFO-200을 확인하고 끝난다.
        page_start = page_count * page_size + 1
        last_data_start: int | None = None
        last_data_rows: int | None = None
        while True:
            page_number = ((page_start - 1) // page_size) + 1
            key = _probe_page_key(page_start, page_size)
            outcome = _fetch_probe_page(
                client,
                page_url(page_start, page_start + page_size - 1),
                wrapper_key,
            )
            if outcome.error is not None:
                yield FetchResult(
                    key=key,
                    payload=None,
                    error=outcome.error,
                    expected_total=None,
                )
                return
            if outcome.terminal:
                total = (
                    expected_total
                    if last_data_start is None or last_data_rows is None
                    else last_data_start - 1 + last_data_rows
                )
                yield FetchResult(
                    key=key,
                    payload=outcome.payload,
                    error=None,
                    expected_total=total,
                )
                return
            if page_number > max_probe_pages:
                yield FetchResult(
                    key=key,
                    payload=None,
                    error=FetchErrorKind.PERMANENT,
                    expected_total=None,
                )
                return
            yield FetchResult(
                key=key,
                payload=outcome.payload,
                error=None,
                expected_total=None,
            )
            last_data_start = page_start
            last_data_rows = outcome.row_count
            page_start += page_size
        return

    page_start = 1
    last_data_start: int | None = None
    last_data_rows: int | None = None
    received_rows = 0
    skipped_data_page = False
    while True:
        page_number = ((page_start - 1) // page_size) + 1
        key = _probe_page_key(page_start, page_size)

        # max_probe_pages개의 데이터 페이지 뒤 딱 한 번 더 호출해 빈 끝을 확인한다.
        # 그 호출에도 데이터가 있으면 설정 상한이 낮은 것이므로 조용히 자르지 않고
        # 명시적인 누락으로 실패시킨다.
        is_boundary_probe = page_number == max_probe_pages + 1
        if page_number > max_probe_pages + 1:
            raise AssertionError("probe 페이지 상한 검사가 누락됨")
        if key in skip and not is_boundary_probe:
            skipped_data_page = True
            page_start += page_size
            continue

        outcome = _fetch_probe_page(
            client,
            page_url(page_start, page_start + page_size - 1),
            wrapper_key,
        )
        if outcome.error is not None:
            yield FetchResult(
                key=key,
                payload=None,
                error=outcome.error,
                expected_total=None,
            )
            # 끝을 모르는 상태에서 실패 page 너머를 탐색하면 중간 공백을 둔 채 뒤의
            # terminal을 완결 증거로 오인할 수 있다. 다음 fetch round가 이 page부터
            # 이어받도록 즉시 멈춘다.
            return

        if outcome.terminal:
            if page_start == 1:
                total = 0
            elif not skipped_data_page:
                total = received_rows
            elif (
                last_data_start == page_start - page_size and last_data_rows is not None
            ):
                total = last_data_start - 1 + last_data_rows
            else:
                total = _recover_probe_total(
                    page_start=page_start,
                    page_size=page_size,
                    skip=skip,
                    client=client,
                    page_url=page_url,
                    wrapper_key=wrapper_key,
                )
            minimum_covered_total = _minimum_total_covered_by_probe_parts(skip)
            if total is None or total < minimum_covered_total:
                # 직전 라운드의 성공 payload는 그대로 두고 더 작아진 snapshot의
                # terminal/row_count만 metadata로 확정하면 fetched>expected인 혼합
                # Bronze가 정상 완료될 수 있다. 복구 불가능한 이번 라운드는 transient로
                # 남겨 상위 라운드/quality gate가 fail-closed하게 처리한다.
                yield FetchResult(
                    key=key,
                    payload=None,
                    error=FetchErrorKind.TRANSIENT,
                    expected_total=None,
                )
                return
            yield FetchResult(
                key=key,
                payload=outcome.payload,
                error=None,
                expected_total=total,
            )
            return

        if is_boundary_probe:
            yield FetchResult(
                key=key,
                payload=None,
                error=FetchErrorKind.PERMANENT,
                expected_total=None,
            )
            return

        yield FetchResult(
            key=key,
            payload=outcome.payload,
            error=None,
            expected_total=None,
        )
        last_data_start = page_start
        last_data_rows = outcome.row_count
        received_rows += outcome.row_count or 0
        page_start += page_size


@adapter("seoul_openapi")
class SeoulOpenApiAdapter:
    """서울 열린데이터광장 공용 페이지네이션 규약 어댑터."""

    @staticmethod
    def _root_location(params: dict) -> tuple[str, str]:
        """설정에 따라 응답 wrapper 키와 그 아래 row 경로를 분리한다."""
        root_key = params["root_key"]
        if params.get("root_key_literal", False):
            return root_key, ""
        wrapper_key, _, row_path = root_key.partition(".")
        return wrapper_key, row_path

    @staticmethod
    def planned_parts(config: SourceConfig, window) -> frozenset[str] | None:
        """요청 전에 전체 키를 아는 POI 소스의 조각 계획을 반환한다."""
        params = config.adapter_params
        if params["service"] != "citydata_ppltn":
            return None
        poi_start = int(params.get("poi_start", 1))
        poi_end = int(params["poi_end"])
        return frozenset(f"poi-POI{i:03d}" for i in range(poi_start, poi_end + 1))

    @staticmethod
    def fetch(
        config: SourceConfig,
        window,
        *,
        client: httpx.Client,
        skip: frozenset[str] = frozenset(),
        expected_total: int | None = None,
    ):
        params = config.adapter_params
        service = params["service"]
        page_size = params["page_size"]

        wrapper_key, row_path = SeoulOpenApiAdapter._root_location(params)
        path_suffix_template = params.get("path_suffix", "")
        suffix = ""
        if path_suffix_template:
            # `window_last`는 이 윈도우가 시작하기 직전 순간이다. 시간 단위 파라미터를
            # 받는 API에서 매시 끝자락이 누락되는 것을 막는다 — 19:00 윈도우가
            # window_start의 시를 쓰면 19시대를 요청해 18:55~19:00 데이터를 아무도
            # 가져가지 않는다. window_last를 쓰면 그 윈도우가 18시대를 한 번 더
            # 완결시키고 19:05부터 19시대로 넘어간다.
            suffix = path_suffix_template.format(
                window_start=window.window_start,
                window_end=window.window_end,
                window_last=window.window_start - timedelta(seconds=1),
            )
        # citydata_ppltn은 페이지네이션 대신 YAML에 선언된 POI 범위를 순회한다.
        # 장소가 늘어날 때 공통 코드를 고치지 않고 poi_end만 갱신할 수 있게 한다.
        if service == "citydata_ppltn":
            poi_start = int(params.get("poi_start", 1))
            poi_end = int(params["poi_end"])
            if poi_start < 1 or poi_end < poi_start:
                raise ValueError(
                    "citydata_ppltn의 poi_start/poi_end 범위가 올바르지 않습니다"
                )

            pois = []
            for i in range(poi_start, poi_end + 1):
                poi_id = f"POI{i:03d}"
                key = f"poi-{poi_id}"
                if key not in skip:
                    url = f"{_BASE_URL}/{_api_key()}/json/{service}/1/5/{poi_id}/"
                    pois.append((key, url))

            # expected_total은 pipeline에서 기대 row 수를 뜻한다. POI 범위 크기는
            # 요청 조각 수이고 INFO-200 조각은 정상적으로 0행일 수 있으므로, 여기서는
            # None을 유지해 실제 요청 실패만 조각 기준 missing_ratio로 계산한다.
            concurrency = max(1, int(params.get("concurrency", _DEFAULT_CONCURRENCY)))
            if concurrency == 1:
                for key, url in pois:
                    outcome = _fetch_poi(client, url, wrapper_key)
                    yield FetchResult(
                        key=key,
                        payload=outcome.payload,
                        error=outcome.error,
                        expected_total=None,
                    )
                    if outcome.error is FetchErrorKind.FATAL:
                        return
                return

            # 조각 순서는 유지하되 네트워크 요청은 미리 병렬로 시작한다.
            def fetch_one_poi(item: tuple[str, str]) -> _PageOutcome:
                _, url = item
                return _fetch_poi(client, url, wrapper_key)

            for (key, _url), outcome in run_concurrent(
                pois, concurrency, fetch_one_poi, "seoul-poi"
            ):
                yield FetchResult(
                    key=key,
                    payload=outcome.payload,
                    error=outcome.error,
                    expected_total=None,
                )
            return

        def page_url(start: int, end: int) -> str:
            return f"{_BASE_URL}/{_api_key()}/json/{service}/{start}/{end}{suffix}/"

        if params.get("pagination", "total") in {"probe", "probe_until_empty"}:
            yield from _fetch_probe_pages(
                params=params,
                client=client,
                page_url=page_url,
                wrapper_key=wrapper_key,
                skip=skip,
                expected_total=expected_total,
            )
            return

        # total을 모르면 몇 페이지를 더 돌아야 하는지 알 수 없다.
        # expected_total로 이미 받았으면(라운드 재시도·백필) 그 값을 그대로 쓴다.
        total = expected_total
        page_start = 1

        # (1) total 발견 — 모르는 동안은 병렬화할 수 없다. 페이지 목록 자체를 만들 수
        #     없기 때문이고, 범위를 넘겨 요청하면 서울 API가 래퍼 없이 INFO-200을
        #     내려보내기 때문이다(`_result_code` 참고). 그래서 한 페이지만 순차로 받는다.
        if total is None:
            # skip에 있는 키는 네트워크 호출 없이 건너뛴다. total을 모르는 구간에서는
            # 마지막 페이지를 clamp할 수 없으므로 page_end를 그대로 쓴다.
            while f"page-{page_start:05d}-{page_start + page_size - 1:05d}" in skip:
                page_start += page_size

            key = f"page-{page_start:05d}-{page_start + page_size - 1:05d}"
            outcome = _fetch_page(
                client,
                page_url(page_start, page_start + page_size - 1),
                wrapper_key,
                row_path,
            )

            if outcome.error is not None:
                yield FetchResult(
                    key=key, payload=None, error=outcome.error, expected_total=None
                )
                # total을 못 구했으면 남은 페이지 수를 알 방법이 없으므로 여기서 멈춘다.
                return

            total = outcome.total or 0
            yield FetchResult(
                key=key,
                payload=outcome.payload,
                error=None,
                # expected_total은 pipeline이 기억해뒀다가 다음 라운드·백필에 되돌려주는
                # 값이라 처음 알아낸 순간에만 싣는다. 그 뒤로는 pipeline이 이미 갖고 있다.
                expected_total=total if page_start == 1 else None,
            )
            page_start += page_size

        # (2) 남은 페이지 목록. total을 알므로 마지막 페이지를 clamp한다 — 조각 키가
        #     이 clamp에 의존하므로 total이 라운드 사이에 흔들리면 skip이 어긋난다.
        #     그래서 pipeline이 expected_total을 persist해 되돌려준다.
        pages: list[tuple[str, int, int]] = []
        start = page_start
        while start <= total:
            end = min(start + page_size - 1, total)
            key = f"page-{start:05d}-{end:05d}"
            if key not in skip:
                pages.append((key, start, end))
            start = end + 1

        if not pages:
            return

        # (3) 조회. concurrency를 선언하지 않은 소스는 순차 그대로다(동작 무변화).
        concurrency = max(1, int(params.get("concurrency", _DEFAULT_CONCURRENCY)))

        if concurrency == 1:
            for key, start, end in pages:
                outcome = _fetch_page(
                    client, page_url(start, end), wrapper_key, row_path
                )
                yield FetchResult(
                    key=key,
                    payload=outcome.payload,
                    error=outcome.error,
                    expected_total=total if start == 1 else None,
                )
                if outcome.error is FetchErrorKind.FATAL:
                    # 모든 조각이 같은 인증키를 쓰므로 나머지도 같은 이유로 실패한다.
                    return
            return

        # living_population_grid는 254페이지라 페이지당 1.4MB면 수백 MB가 된다 —
        # 그래서 run_concurrent가 미리 요청하는 개수를 concurrency의 2배로 묶는다.
        def fetch_one_page(item: tuple[str, int, int]) -> _PageOutcome:
            _, start, end = item
            return _fetch_page(client, page_url(start, end), wrapper_key, row_path)

        for (key, start, _end), outcome in run_concurrent(
            pages, concurrency, fetch_one_page, "seoul-page"
        ):
            yield FetchResult(
                key=key,
                payload=outcome.payload,
                error=outcome.error,
                expected_total=total if start == 1 else None,
            )

    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]:
        """조각들을 순서대로 이어붙여 행 = 레코드 리스트로 만든다.
        네트워크를 타지 않는 순수 함수이다. bronze에서 다시 읽어 언제든 재호출된다.
        """
        params = config.adapter_params
        wrapper_key, row_path = SeoulOpenApiAdapter._root_location(params)

        rows: list[dict] = []
        for chunk in chunks:
            wrapper = _extract(json.loads(chunk), wrapper_key)
            node = wrapper
            for segment in row_path.split(".") if row_path else []:
                if not isinstance(node, dict):
                    node = None
                    break
                node = node.get(segment)
            if isinstance(node, list):
                rows.extend(node)
        if params.get("flatten_forecast", False):
            rows = [_flatten_forecast(row) for row in rows if isinstance(row, dict)]
        _validate_natural_key(rows, getattr(config, "natural_key", None))
        return rows
