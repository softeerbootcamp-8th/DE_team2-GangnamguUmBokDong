"""서울 열린데이터광장 공용 페이지네이션 규약 어댑터.

## 데이터 소스 
따릉이 실시간 대여정보 · 따릉이 대여이력 정보 · 서울 실시간 인구 데이터 ·
서울시 문화행사·공연행사 · 서울 생활인구(250m).

## RESULT.CODE → 실패 범주

| 코드 | 범주 | 근거 |
| --- | --- | --- |
| `INFO-000` | 성공 | |
| `INFO-200` (해당 데이터 없음) | **성공(빈 결과)** | 정상 상황을 누락으로 세면 완결도가 왜곡된다 |
| `INFO-100` (인증키 오류) | `FATAL` | 모든 조각이 같은 키를 쓰므로 재시도·백필이 무의미 |
| `ERROR-500` 계열 (서버 오류) | `TRANSIENT` | 라운드로 회복 가능 |
| 그 외 요청 오류 | `PERMANENT` | 그 조각만 누락 확정 |

`yield`하는 값은 **가공되지 않은 응답 원본**이다. 여기서 행을 꺼내거나 페이지를 합치지
않는다. 조각을 S3에 저장하는 것은 pipeline이다.

라운드 재시도 · 안전장치 · HTTP 클라이언트는 base의 공통 유틸을 쓴다.

## 주의

- 인증키는 `SEOUL_OPENAPI_KEY`에서 읽는다. **키가 URL 경로에 들어가므로** 로그 ·
  예외 메시지 · bronze · **조각 키**에 남지 않게 마스킹한다.
- 전 필드가 문자열로 내려온다. 캐스팅은 검증 엔진의 `types`가 담당하고 어댑터는
  값에 손대지 않는다.
"""


from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import httpx

from adapters.base import FetchErrorKind, FetchResult, adapter

if TYPE_CHECKING:
    from config.schema import SourceConfig

_BASE_URL = "http://openapi.seoul.go.kr:8088"

_FATAL_CODES = {"INFO-100"}
_SUCCESS_CODES = {"INFO-000", "INFO-200"}


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


@adapter("seoul_openapi")
class SeoulOpenApiAdapter:
    """서울 열린데이터광장 공용 페이지네이션 규약 어댑터."""

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
            suffix = path_suffix_template.format(window_start=window.window_start)

        # total을 모르면 몇 페이지를 더 돌아야 하는지 알 수 없다.
        # expected_total로 이미 받았으면(라운드 재시도·백필) 그 값을 그대로 쓴다.
        total = expected_total
        page_start = 1

        # total을 아직 모르는 동안은 무조건 계속 돈다(첫 페이지를 반드시 부른다).
        # 알고 나면 그 값을 넘어서는 순간 멈춘다.

        # (1) 동적 페이지네이션
        while total is None or page_start <= total:
            page_end = page_start + page_size - 1
            if total is not None:
                page_end = min(page_end, total)
            key = f"page-{page_start:05d}-{page_end:05d}"

            # (2) skip 목록에 있는 키는 네트워크 호출을 건너뛴다.
            if key in skip:
                page_start = page_end + 1
                continue

            url = f"{_BASE_URL}/{_api_key()}/json/{service}/{page_start}/{page_end}{suffix}/"
            try:
                response = client.get(url)
                response.raise_for_status()
                wrapper = _extract(json.loads(response.content), wrapper_key)
            except (httpx.RequestError, httpx.HTTPStatusError, json.JSONDecodeError):
                
                # 타임아웃, 커넥션 에러, 또는 서버가 5xx 에러와 함께 HTML 등을 내려보내 JSON 파싱에 실패한 경우
                yield FetchResult(key=key, payload=None, error=FetchErrorKind.TRANSIENT, expected_total=None)
                if total is None:
                    # 첫 페이지에서 네트워크 에러가 나면 전체 건수를 알 길이 없으므로 그대로 중단
                    return
                page_start = page_end + 1
                continue

            # 서울 API는 보통 HTTP 200으로 응답하고, 논리적 성공/실패는 본문의 RESULT.CODE에 담아 보낸다
            code = wrapper.get("RESULT", {}).get("CODE")
            category = _classify(code)

            if category is None:  # 성공 (INFO-000 또는 INFO-200)
                if total is None:
                    # list_total_count는 첫 응답에만 실려 오기 때문에 
                    # 여기서 이 값을 한 번 잡아야 남은 페이지 수를 계산할 수 있다.
                    total = int(wrapper.get("list_total_count", 0))
                yield FetchResult(
                    key=key, payload=response.content, error=None,
                    # expected_total은 pipeline이 기억해뒀다가 다음 라운드·백필에 되돌려주는 값이므로, 
                    # 처음 알아낸 순간에만 실어 보내고, 그 뒤로는 pipeline이 이미 갖고 있다.
                    expected_total=total if page_start == 1 else None,
                )
                page_start = page_end + 1
                if total is None:
                    # 첫 페이지가 성공했는데도 list_total_count를 못 읽었다면 몇 페이지가 더 있는지 알 방법이 없으므로 멈춘다.
                    return
            elif category is FetchErrorKind.FATAL:
                # 인증키 오류 등 확정적 원인이면 즉시 중단한다(라운드 재시도도 무의미).
                yield FetchResult(key=key, payload=None, error=category, expected_total=None)
                return
            else:  # TRANSIENT 또는 PERMANENT 조각만 실패로 보고하고 계속 진행
                yield FetchResult(key=key, payload=None, error=category, expected_total=None)
                if total is None:
                    # 첫 페이지가 실패해 total을 못 구했다 
                    # 남은 페이지 수를 알 수 없으므로 여기서 멈춘다. 
                    return
                page_start = page_end + 1

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
