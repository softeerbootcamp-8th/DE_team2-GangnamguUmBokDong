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
"""

from __future__ import annotations

import json
import os
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

import httpx

from adapters.base import FetchErrorKind, FetchResult, adapter

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


@dataclass(frozen=True, slots=True)
class _PageOutcome:
    """페이지 하나의 조회 결과. 스레드풀 워커가 돌려주는 값이라 순수 데이터로 둔다."""

    payload: bytes | None
    total: int | None
    error: FetchErrorKind | None


def _request_json(client: httpx.Client, url: str) -> tuple[bytes, dict] | FetchErrorKind:
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


def _fetch_page(client: httpx.Client, url: str, wrapper_key: str) -> _PageOutcome:
    """페이지 하나를 받아 실패 범주까지 판정한다."""
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(payload=None, total=None, error=result)
    content, body = result

    # 서울 API는 보통 HTTP 200으로 응답하고, 논리적 성공/실패는 본문 코드에 담아 보낸다
    category = _classify(_result_code(body, wrapper_key))
    if category is not None:
        return _PageOutcome(payload=None, total=None, error=category)

    wrapper = _extract(body, wrapper_key)
    raw_total = wrapper.get("list_total_count", 0) if isinstance(wrapper, dict) else 0
    return _PageOutcome(payload=content, total=int(raw_total), error=None)


def _fetch_poi(client: httpx.Client, url: str, wrapper_key: str) -> _PageOutcome:
    """실시간 인구 POI 하나를 받아 실패 범주까지 판정한다."""
    result = _request_json(client, url)
    if isinstance(result, FetchErrorKind):
        return _PageOutcome(payload=None, total=None, error=result)
    content, body = result

    # citydata_ppltn은 일반 페이지 응답과 달리 RESULT.CODE가 최상단 RESULT 객체의
    # `RESULT.CODE` 키로 내려온다. 빈 POI(INFO-200)도 성공 조각으로 보존한다.
    code = body.get("RESULT.CODE") or body.get("RESULT", {}).get("RESULT.CODE")
    category = _classify(code)
    if category is not None:
        return _PageOutcome(payload=None, total=None, error=category)
    return _PageOutcome(payload=content, total=None, error=None)


def _run_concurrent(items, concurrency: int, fetch_one, thread_name_prefix: str):
    """items 순서를 유지하며 fetch_one(item)을 동시 실행해 (item, outcome) 순서대로 yield한다.

    완료 순서대로 내보내면(as_completed) 조각 키 순서가 흔들리므로, 앞 항목을 기다리는
    동안에도 뒤 요청은 이미 나가 있게 해 전체 소요를 완료순과 같게 만든다. 미리 요청하는
    개수를 concurrency의 2배로 묶는 이유는 메모리다 — 전부 던져두면 완료됐지만 아직
    내보내지 않은 응답이 쌓인다.
    """
    pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix=thread_name_prefix)
    try:
        queued = iter(items)
        inflight: deque[tuple[object, Future[_PageOutcome]]] = deque()

        def submit_next() -> bool:
            item = next(queued, None)
            if item is None:
                return False
            inflight.append((item, pool.submit(fetch_one, item)))
            return True

        for _ in range(concurrency * 2):
            if not submit_next():
                break

        while inflight:
            item, future = inflight.popleft()
            outcome = future.result()
            submit_next()
            yield item, outcome
            if outcome.error is FetchErrorKind.FATAL:
                # 모든 조각이 같은 인증키를 쓰므로 나머지도 같은 이유로 실패한다.
                return
    finally:
        # `with ThreadPoolExecutor(...)`를 쓰면 __exit__이 shutdown(wait=True)라
        # 큐에 남은 항목이 끝날 때까지 블록한다. fetch_with_rounds가 마감 시한을
        # 넘겨 순회를 중단하고 이 제너레이터를 버릴 때 그 대기가 마감 시한 방어를
        # 무력화한다. 실행 중인 요청은 끊을 수 없지만 대기 큐는 즉시 비운다.
        pool.shutdown(wait=False, cancel_futures=True)


@adapter("seoul_openapi")
class SeoulOpenApiAdapter:
    """서울 열린데이터광장 공용 페이지네이션 규약 어댑터."""

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

        # . 기준으로 문자열을 쪼개서 앞부분만 떼어낸다.
        # "SeoulRtd.citydata_ppltn" 처럼 마침표가 포함된 단일 키가 응답의 최상단에 존재할 수 있다.
        if params["root_key"] in {"SeoulRtd.citydata_ppltn"}:
            wrapper_key = params["root_key"]
        else:
            wrapper_key = params["root_key"].split(".", 1)[0]
        
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
                raise ValueError("citydata_ppltn의 poi_start/poi_end 범위가 올바르지 않습니다")

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
                        key=key, payload=outcome.payload, error=outcome.error,
                        expected_total=None,
                    )
                    if outcome.error is FetchErrorKind.FATAL:
                        return
                return

            # 조각 순서는 유지하되 네트워크 요청은 미리 병렬로 시작한다.
            def fetch_one_poi(item: tuple[str, str]) -> _PageOutcome:
                _, url = item
                return _fetch_poi(client, url, wrapper_key)

            for (key, _url), outcome in _run_concurrent(pois, concurrency, fetch_one_poi, "seoul-poi"):
                yield FetchResult(
                    key=key, payload=outcome.payload, error=outcome.error,
                    expected_total=None,
                )
            return

        def page_url(start: int, end: int) -> str:
            return f"{_BASE_URL}/{_api_key()}/json/{service}/{start}/{end}{suffix}/"

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
            outcome = _fetch_page(client, page_url(page_start, page_start + page_size - 1), wrapper_key)

            if outcome.error is not None:
                yield FetchResult(key=key, payload=None, error=outcome.error, expected_total=None)
                # total을 못 구했으면 남은 페이지 수를 알 방법이 없으므로 여기서 멈춘다.
                return

            total = outcome.total or 0
            yield FetchResult(
                key=key, payload=outcome.payload, error=None,
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
                outcome = _fetch_page(client, page_url(start, end), wrapper_key)
                yield FetchResult(
                    key=key, payload=outcome.payload, error=outcome.error,
                    expected_total=total if start == 1 else None,
                )
                if outcome.error is FetchErrorKind.FATAL:
                    # 모든 조각이 같은 인증키를 쓰므로 나머지도 같은 이유로 실패한다.
                    return
            return

        # living_population_grid는 254페이지라 페이지당 1.4MB면 수백 MB가 된다 —
        # 그래서 _run_concurrent가 미리 요청하는 개수를 concurrency의 2배로 묶는다.
        def fetch_one_page(item: tuple[str, int, int]) -> _PageOutcome:
            _, start, end = item
            return _fetch_page(client, page_url(start, end), wrapper_key)

        for (key, start, _end), outcome in _run_concurrent(pages, concurrency, fetch_one_page, "seoul-page"):
            yield FetchResult(
                key=key, payload=outcome.payload, error=outcome.error,
                expected_total=total if start == 1 else None,
            )

    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]:
        """조각들을 순서대로 이어붙여 행 = 레코드 리스트로 만든다.
        네트워크를 타지 않는 순수 함수이다. bronze에서 다시 읽어 언제든 재호출된다.
        """
        root_key = config.adapter_params["root_key"]
        if root_key in {"SeoulRtd.citydata_ppltn"}:
            wrapper_key = root_key
            row_path = ""
        else:
            wrapper_key, _, row_path = root_key.partition(".")

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
        return rows
