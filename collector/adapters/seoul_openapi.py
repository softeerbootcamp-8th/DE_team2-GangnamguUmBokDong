"""서울 열린데이터광장 어댑터 — 소스 5종이 공유한다.

## 공유하는 소스 5종

따릉이 실시간 대여정보 · 따릉이 대여이력 정보 · 서울 실시간 인구 데이터 ·
서울시 문화행사·공연행사 · 서울 생활인구(250m).

5종이 `{인증키}/{포맷}/{서비스명}/{시작}/{끝}/` 형태의 **동일한 페이지네이션 규약**을
쓰므로 어댑터 하나로 커버된다. 소스별 차이는 `adapter_params`로만 표현한다.

## adapter_params

- `service` — 서비스명(예: `bikeList`)
- `page_size` — 한 번에 가져올 건수. 서울 API 상한은 1,000건이다.
- `root_key` — 응답에서 행 배열을 꺼낼 경로(예: `rentBikeStatus.row`)

## fetch 구현할 것 — 페이지마다 yield

1. 첫 페이지를 호출해 `list_total_count`를 읽고, **그 응답을 바로 `yield`한다.**
   이때 `FetchResult.expected_total`에 `list_total_count`를 실어 보낸다. pipeline이
   이 값을 기억했다가 다음 라운드·백필에 `expected_total`로 되돌려주므로, **첫 페이지를
   skip해도 계획을 세울 수 있다.** 이 값은 manifest의 `counts.expected`가 되어 완결도
   계산에 쓰인다.
2. `page_size` 단위로 `{시작}/{끝}` 인덱스를 만들어 남은 페이지를 순회하며 각 응답을
   차례로 `yield`한다. 인덱스는 **1부터 시작하고 끝 값을 포함**한다.
3. `skip`에 든 조각 키는 **호출하지 않는다.** 라운드 재순회와 백필이 같은 인자를 쓴다.
4. **`RESULT.CODE` 검사** — 서울 API는 HTTP 200으로 응답하면서 본문에 에러 코드를
   담는다. `rentBikeStatus.RESULT.CODE`가 `INFO-000`이 아니면 코드에 따라 범주를
   나눈다(아래).

## 조각 키

    part=page-00001-01000.json.gz
    part=page-01001-02000.json.gz
    part=page-02001-02765.json.gz

`page-{시작:05d}-{끝:05d}` 형식이다. 제로 패딩하는 이유는 문자열 정렬이 호출 순서와
일치하게 하기 위해서다(읽는 순서 자체는 manifest `parts`가 정하지만, S3 콘솔에서
사람이 볼 때 뒤섞이지 않는 편이 낫다).

`list_total_count`가 2,765 → 2,770으로 변하면 마지막 페이지 키가
`page-02001-02765`에서 `page-02001-02770`으로 바뀐다. 이 경우 백필은 그 조각을
"없는 것"으로 보고 새로 받는다 — 순번이었다면 같은 번호에 다른 범위를 덮어써 조용히
어긋났을 상황이다.

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

라운드 재시도 · 안전장치 · HTTP 클라이언트는 base의 공통 유틸을 쓴다. **이 어댑터는
몇 번 다시 시도할지 알지 못한다** — 실패를 `FetchResult`로 보고할 뿐이다.

## normalize 구현할 것

- 조각 목록을 순서대로 돌며 `root_key`를 점 표기 경로로 따라가 행 배열을 꺼내고, 그
  결과를 이어 붙인다.
- 이 API는 이미 "행 = 레코드"이므로 pivot이 필요 없다. 구조 변환은 이것뿐이다.
- **조각이 빠져 있어도 동작해야 한다.** 부분 수집된 window는 조각 목록에 구멍이 있는
  채로 들어온다. 페이지를 이어 붙이는 방식이라 자연히 성립하지만, "N번째 페이지"를
  가정하는 코드를 넣지 않는다.

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

from adapters.base import FetchErrorKind, FetchResult, adapter

if TYPE_CHECKING:
    import httpx

    from config.schema import SourceConfig

_BASE_URL = "http://openapi.seoul.go.kr:8088"

_FATAL_CODES = {"INFO-100"}
_SUCCESS_CODES = {"INFO-000", "INFO-200"}


def _api_key() -> str:
    # URL 경로에 그대로 박히는 값이라, 호출한 곳(fetch)이 알아서 마스킹해야 한다 —
    # 이 함수는 조각 키에도, 예외 메시지에도 이 값을 노출하지 않는다.
    return os.environ["SEOUL_OPENAPI_KEY"]


def _classify(code: str) -> FetchErrorKind | None:
    """RESULT.CODE를 실패 범주로 매핑한다. `None`은 성공(빈 결과 포함)이다."""
    if code in _SUCCESS_CODES:
        return None  # INFO-000(성공)과 INFO-200(빈 결과)을 같이 취급한다
    if code in _FATAL_CODES:
        return FetchErrorKind.FATAL  # INFO-100: 인증키 오류 — 모든 조각이 같은 키다
    if code.startswith("ERROR-5"):
        return FetchErrorKind.TRANSIENT  # 서버 쪽 문제라 재시도하면 회복될 수 있다
    return FetchErrorKind.PERMANENT  # 그 외 요청 오류는 재시도해도 똑같이 실패한다


def _extract(body: dict, wrapper_key: str) -> dict:
    """서울 API 응답은 서비스명을 키로 감싸서 온다 — `{wrapper_key: {...}}`."""
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
        # root_key의 첫 세그먼트가 응답을 감싸는 wrapper 키
        wrapper_key = params["root_key"].split(".", 1)[0]

        # total을 모르면 몇 페이지를 더 돌아야 하는지 알 수 없다.
        # expected_total로 이미 받았으면(라운드 재시도·백필) 그 값을 그대로 쓴다.
        total = expected_total
        page_start = 1

        # total을 아직 모르는 동안은 무조건 계속 돈다(첫 페이지를 반드시 부른다).
        # 알고 나면 그 값을 넘어서는 순간 멈춘다.
        while total is None or page_start <= total:
            page_end = page_start + page_size - 1
            if total is not None:
                page_end = min(page_end, total)
            key = f"page-{page_start:05d}-{page_end:05d}"

            if key in skip:
                # 이미 확보했거나 영구 실패로 확정된 조각은 호출하지 않고 다음 구간으로 넘어간다.
                page_start = page_end + 1
                continue

            # 인증키가 URL 경로에 그대로 들어가지만, 조각 키에는 절대 섞이지 않는다 
            # 로그·bronze·manifest 어디에도 인증키가 남지 않는다.
            url = f"{_BASE_URL}/{_api_key()}/json/{service}/{page_start}/{page_end}/"
            response = client.get(url)

            # 서울 API는 HTTP 200으로만 응답하고, 성공/실패는 본문의 RESULT.CODE에 담아 보낸다
            wrapper = _extract(json.loads(response.content), wrapper_key)
            code = wrapper.get("RESULT", {}).get("CODE")
            category = _classify(code)

            if category is None:  # 성공 (INFO-000 또는 빈 결과 INFO-200)
                if total is None:
                    # list_total_count는 첫 응답에만 실려 오기 때문에 
                    # 여기서 이 값을 한 번 잡아야 남은 페이지 수를 계산할 수 있다.
                    total = wrapper.get("list_total_count")
                yield FetchResult(
                    key=key, payload=response.content, error=None,
                    # expected_total은 pipeline이 기억해뒀다가 
                    # 다음 라운드·백필에 되돌려주는 값이므로, 
                    # 정말 처음 알아낸 순간에만 실어 보내고, 그 뒤로는 pipeline이 이미 갖고 있다.
                    expected_total=total if page_start == 1 else None,
                )
                page_start = page_end + 1
                if total is None:
                    # 첫 페이지가 성공했는데도 list_total_count를 못 읽었다면 몇 페이지가 더 있는지 알 방법이 없으므로 멈춘다
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
        # row_path에 점이 더 있으면 그만큼 더 깊이 내려간다.
        wrapper_key, _, row_path = root_key.partition(".")

        rows: list[dict] = []
        for chunk in chunks:
            wrapper = _extract(json.loads(chunk), wrapper_key)
            node = wrapper
            for segment in row_path.split(".") if row_path else []:
                if not isinstance(node, dict):
                    # 이 조각엔 기대한 경로가 없고, 조용히 건너뛴다. 
                    # 부분 수집된 window는 조각에 구멍이 있는 게 정상이라 여기서 죽으면 안 된다.
                    node = None
                    break
                node = node.get(segment)
            if isinstance(node, list):
                rows.extend(node)
        return rows
