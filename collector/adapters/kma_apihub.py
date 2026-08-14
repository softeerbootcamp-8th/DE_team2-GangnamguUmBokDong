"""기상청 API 허브 어댑터 

## 공유하는 소스 2종

기상청 초단기 실황·예보(10분) · 기상청 단기예보(3시간).

## adapter_params

- `endpoint` — 호출할 엔드포인트(예: `getUltraSrtNcst`)
- `root_key` — 행 배열 경로(예: `response.body.items.item`)
- `pivot` — long → wide 변환 기준(예: `{key: category, value: obsrValue}`)
- `grids` — 서울을 커버할 격자 목록(예: `[[60, 127], [61, 127]]`)

## fetch 구현할 것 — 격자마다 yield

- **격자 반복** — 기상청은 격자(`nx`, `ny`)마다 별도 호출이 필요하다. `grids`를 순서대로
  돌며 각 격자의 응답 원본을 그대로 `yield`한다. 조각 저장은 pipeline이 맡는다.
- `skip`에 든 조각 키는 호출하지 않는다. **격자는 config에 고정이라 계획을 미리 세울 수
  있으므로** 서울 어댑터와 달리 첫 호출 없이도 남은 조각을 알 수 있다.
- `expected_total`은 **`None`이다.** 이 API는 전체 행 수를 미리 알려주지 않는다. 그래서
  완결도가 행이 아니라 **조각 기준**(성공 격자 / 계획 격자)으로 계산되고, manifest의
  `missing.basis`가 `parts`가 된다. 격자당 행 수가 일정하므로 행 기준과 거의 같은 값이
  나온다.
- `base_date` · `base_time`은 window로 계산한다. 발표 주기와 window 경계가 어긋날 때
  어느 발표분을 집을지는 #9에서 확정한다.
- 격자 수만큼 호출이 선형으로 늘어난다. 격자 목록을 코드에 박지 않고 config에 두는
  이유가 이것이다. 동시 호출이 필요해지면 base의 async 클라이언트를 쓰되 `asyncio.run`은
  이 `fetch` 안에 가둔다(ADR 0003).

## 조각 키

    part=grid-060x127.json.gz
    part=grid-061x127.json.gz

`grid-{nx:03d}x{ny:03d}` 형식이다. **격자는 서로 독립이라 순서가 결과에 영향을 주지
않는다** — pivot 그룹 키에 `nx`·`ny`가 들어가므로 어떤 순서로 읽어도 같은 silver가
나온다. 페이지네이션과 달리 이어 붙이는 개념이 없기 때문이다.

격자 목록이 config에 고정이므로 조각 키도 실행 간에 안정적이다. 백필이 지목하기 가장
쉬운 소스다.

## normalize 구현할 것 — long → wide pivot

기상청 응답은 기온 · 습도 · 풍속이 **각각 별도 행으로 쌓인 long format**이다.

    # 원본 (bronze 조각에 저장되는 형태)
    {"category": "T1H", "obsrValue": "31.6", "nx": 60, "ny": 127, ...}
    {"category": "REH", "obsrValue": "42",   ...}

    # normalize 결과 (검증 엔진이 받는 형태)
    {"baseDate": ..., "baseTime": ..., "nx": 60, "ny": 127,
     "T1H": "31.6", "REH": "42", "WSD": "3.2", "PTY": "0", ...}

- **조각 목록 전체를 가로질러** 처리한다. 그룹 키는 `(baseDate, baseTime, nx, ny)`이고,
  같은 키의 행들을 한 행으로 합쳐 `pivot.key`의 값을 컬럼명으로, `pivot.value`의 값을
  값으로 올린다.
- 예보 엔드포인트는 값 필드가 `fcstValue`이고 예보 시각(`fcstDate` · `fcstTime`)이
  그룹 키에 추가된다. **코드 분기가 아니라 `pivot` 설정으로 흡수한다.**
- **격자가 빠져 있어도 동작해야 한다.** 부분 수집된 window는 격자 몇 개가 없는 채로
  들어오고, 그 격자의 행이 결과에서 빠질 뿐이다. 그룹 키 기반이라 자연히 성립한다.

이 정규화 덕분에 config가 `T1H` · `REH`처럼 컬럼별로 서로 다른 정상 범위를 선언할 수
있고, 검증 엔진에는 조건부 range 같은 개념이 필요 없어진다.

## 주의

- 인증키는 `KMA_APIHUB_KEY`에서 읽고 로그 · bronze에 남지 않게 마스킹한다.
- 캐스팅은 검증 엔진의 `types`가 담당한다. pivot은 구조만 바꾸고 값은 문자열 그대로
  둔다.
- 실패 범주 판정은 base의 규칙을 따른다. HTTP 상태 기반이며, 인증 실패는 `FATAL`이라
  격자 20개를 헛되이 돌지 않는다.

## base_date · base_time — window을 그대로 쓴다 (발표 주기 보정 없음)
`window.window_start`를 가공 없이 `%Y%m%d`·`%H%M`으로 포맷해 요청한다. 기상청 발표
주기(초단기실황은 매시 40분 발표 등)와 트리거 시각이 어긋날 경우, 응답 본문에
`resultCode="03"`(데이터 없음/미발표)이 반환된다.
이 어댑터는 HTTP 상태 코드(200) 뿐만 아니라 이 `resultCode`를 적극적으로 파싱하여
미발표 응답(`03`)을 `TRANSIENT` 에러로 분류한다. 이를 통해 Airflow DAG의 트리거
타이밍이 미세하게 어긋나더라도, 파이프라인의 라운드 재시도 메커니즘이 데이터를 
기다렸다가 스스로 복구할 수 있게 한다.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import httpx

from adapters.base import FetchErrorKind, FetchResult, adapter, classify_http_status

if TYPE_CHECKING:
    from adapters.base import Window
    from config.schema import SourceConfig

_BASE_URL = "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0"
_NUM_OF_ROWS = 1000  # 격자 하나당 한 시각의 관측·예보 항목 수를 넉넉히 덮는 상한


def _api_key() -> str:
    # 조각 키(grid-{nx}x{ny})에는 이 값이 섞이지 않는다
    return os.environ["KMA_APIHUB_KEY"]


def _classify_result_code(code: str | None) -> FetchErrorKind | None:
    """본문의 resultCode를 실패 범주로 매핑한다."""
    if not isinstance(code, str):
        return FetchErrorKind.PERMANENT

    if code == "00":
        return None  # NORMAL_SERVICE (성공)
    if code in {"03", "04", "05", "22"}:
        # 03: 데이터 없음(미발표), 04: HTTP 에러, 05: 연결 실패, 22: 제한 초과 -> 재시도
        return FetchErrorKind.TRANSIENT 
    if code in {"20", "21", "30", "31", "32", "33"}:
        # 인증/권한 에러 -> 즉시 중단
        return FetchErrorKind.FATAL 
    
    # 01, 02, 10, 11, 12 등 파라미터/DB 에러 -> 영구 실패
    return FetchErrorKind.PERMANENT 


def _extract(body: dict, root_key: str) -> list[dict]:
    """`root_key`의 점 표기 경로를 따라가 행 배열을 꺼낸다. 경로가 없으면 빈 리스트."""
    node: object = body
    for segment in root_key.split("."):
        if not isinstance(node, dict):
            return []
        node = node.get(segment)
    return node if isinstance(node, list) else []


@adapter("kma_apihub")
class KmaApiHubAdapter:
    """기상청 API 허브 공용 격자 반복 어댑터."""

    @staticmethod
    def fetch(
        config: SourceConfig,
        window: Window,
        *,
        client: httpx.Client,
        skip: frozenset[str] = frozenset(),
        expected_total: int | None = None,
    ):
        params = config.adapter_params
        endpoint = params["endpoint"]
        # 발표 주기 보정 없이 window_start를 그대로 쓴다, 정합은 Airflow 스케줄의 책임.
        base_date = window.window_start.strftime("%Y%m%d")
        base_time = window.window_start.strftime("%H%M")

        for nx, ny in params["grids"]:
            key = f"grid-{nx:03d}x{ny:03d}"
            if key in skip:
                continue

            url = (
                f"{_BASE_URL}/{endpoint}?authKey={_api_key()}&dataType=JSON"
                f"&numOfRows={_NUM_OF_ROWS}&pageNo=1"
                f"&base_date={base_date}&base_time={base_time}&nx={nx}&ny={ny}"
            )
            try:
                response = client.get(url)
            except httpx.RequestError:
                yield FetchResult(key=key, payload=None, error=FetchErrorKind.TRANSIENT, expected_total=None)
                continue

            category = classify_http_status(response.status_code)
            if category is FetchErrorKind.FATAL:
                # HTTP 레벨의 인증키 오류 등 확정적 원인인 경우, 즉시 중단.
                yield FetchResult(key=key, payload=None, error=category, expected_total=None)
                return
            elif category is not None:
                # HTTP 상태 5xx (TRANSIENT) 또는 4xx (PERMANENT)
                yield FetchResult(key=key, payload=None, error=category, expected_total=None)
                continue

            # HTTP 200 OK일 경우 본문의 resultCode 확인
            try:
                body = json.loads(response.content)
                if not isinstance(body, dict):
                    raise TypeError("응답이 JSON 객체가 아님")
                result_code = body.get("response", {}).get("header", {}).get("resultCode")
            except (json.JSONDecodeError, TypeError):
                yield FetchResult(key=key, payload=None, error=FetchErrorKind.TRANSIENT, expected_total=None)
                continue

            api_category = _classify_result_code(result_code)
            if api_category is None:
                yield FetchResult(key=key, payload=response.content, error=None, expected_total=None)
            elif api_category is FetchErrorKind.FATAL:
                yield FetchResult(key=key, payload=None, error=api_category, expected_total=None)
                return
            else:  # TRANSIENT 또는 PERMANENT
                yield FetchResult(key=key, payload=None, error=api_category, expected_total=None)

    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]:
        """long 조각들을 pivot 설정에 따라 wide로 합친다."""
        root_key = config.adapter_params["root_key"]
        pivot = config.adapter_params["pivot"]
        key_field, value_field = pivot["key"], pivot["value"]

        groups: dict[tuple, dict] = {}
        order: list[tuple] = []
        for chunk in chunks:
            body = json.loads(chunk)
            for item in _extract(body, root_key):
                group_key = tuple(
                    sorted((k, v) for k, v in item.items() if k not in (key_field, value_field))
                )
                if group_key not in groups:
                    groups[group_key] = dict(group_key)
                    order.append(group_key)
                groups[group_key][item[key_field]] = item[value_field]

        return [groups[k] for k in order]
