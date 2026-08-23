"""기상청 API 허브 어댑터.

기상청 초단기 실황·예보(10분)와 단기예보(3시간)를 수집한다. 이 API는 전체 행 수를
미리 알려주지 않아 `expected_total`이 항상 None이고, 완결도는 행이 아니라
(성공 격자 / 계획 격자)로 계산되어 manifest의 `missing.basis`가 `parts`가 된다.
`base_date`·`base_time`은 window로 계산한다.

`time_rule`로 발표 주기에 맞춰 window를 API가 허용하는 시각으로 보정한다:

| time_rule | 소스 | 규칙 | 예시 |
| --- | --- | --- | --- |
| `hourly` | 초단기실황 | 매시 정각 | 14:40 → 14:00 |
| `half_hourly` | 초단기예보 | 매시 30분 | 14:40 → 14:30 |
| `vilage_fcst` | 단기예보 | 02, 05, 08...시 정각 | 05:05 → 02:00 |

격자 하나의 행 수가 `numOfRows`(1000)를 넘으면(단기예보가 대표적: 3일치×14개
카테고리=1052건) 응답 본문의 `totalCount`를 읽어 `pageNo`를 늘려가며 남은 페이지를
전부 받는다. 여러 페이지를 받았어도 바깥에는 격자당 하나의 `FetchResult`만 내보낸다
— 페이지는 이 어댑터 내부에서만 존재하는 구현 세부사항이고, 재시도·skip은 여전히
격자 단위(`grid-060x127`)로만 이뤄진다. 페이지 중 하나라도 실패하면 그 격자 전체를
실패로 처리해 다음 라운드가 처음부터(1페이지부터) 다시 받는다 — 부분 페이지만
따로 재시도하면 "이 격자는 몇 페이지짜리였는지"를 라운드 사이에 기억할 방법이
없기 때문이다.

주의:
- 인증키(`KMA_APIHUB_KEY`)는 로그·bronze에 남지 않게 마스킹한다.
- 캐스팅은 검증 엔진의 `types`가 담당한다. pivot은 구조만 바꾸고 값은 문자열 그대로 둔다.
- 실패 범주 판정은 base의 규칙을 따른다.
- 페이지가 1개뿐이면(대부분의 경우) 원본 응답 바이트를 그대로 넘긴다. 2개 이상을
  합칠 때만 페이지들의 item 배열을 이어붙인 JSON을 새로 만든다.
"""

from __future__ import annotations

import copy
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import httpx

from adapters.base import (
    FetchErrorKind,
    FetchResult,
    adapter,
    classify_http_status,
    run_concurrent,
)

if TYPE_CHECKING:
    from config.schema import SourceConfig

    from adapters.base import Window

_BASE_URL = "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0"
_NUM_OF_ROWS = 1000  # 격자 하나당 한 시각의 관측·예보 항목 수를 넉넉히 덮는 상한
_EXPECTED_GRID_COUNT = 34
# adapter_params에 concurrency를 선언하지 않은 소스는 순차로 돈다(seoul_openapi와
# 같은 opt-in 관례). 단기예보(getVilageFcst)만 격자당 페이지 2장이 필요해 무거워서
# concurrency를 켠다 — 실측(2026-08-23): 순차 50.89초 -> 4개씩 병렬로 크게 단축.
_DEFAULT_CONCURRENCY = 1


def _api_key() -> str:
    return os.environ["KMA_APIHUB_KEY"]


def _classify_result_code(code: str | None) -> FetchErrorKind | None:
    """본문의 resultCode를 실패 범주로 매핑한다."""
    if not isinstance(code, str):
        return FetchErrorKind.PERMANENT

    if code == "00":
        return None  # NORMAL_SERVICE (성공)
    if code in {"03", "04", "05", "22"}:
        # 03: 데이터 없음(미발표), 04: HTTP 에러, 05: 연결 실패, 22: 제한 초과
        return FetchErrorKind.TRANSIENT
    if code in {"20", "21", "30", "31", "32", "33"}:
        # 20, 21, 30, 31, 32, 33: 인증/권한 에러
        return FetchErrorKind.FATAL

    return FetchErrorKind.PERMANENT


def _adjust_base_time(dt: datetime, rule: str | None) -> datetime:
    """기상청 API가 허용하는 가장 최근의 base_time으로 내림한다."""
    if not rule:
        return dt

    if rule == "hourly":
        # 매시 정각 (초단기실황. 매시 40분 발표. 예: 14:40 -> 14:00, 14:39 -> 13:00)
        eval_dt = dt if dt.minute >= 40 else dt - timedelta(hours=1)
        return eval_dt.replace(minute=0, second=0, microsecond=0)

    if rule == "half_hourly":
        # 매시 30분 (초단기예보. 예: 14:20 -> 13:30, 14:40 -> 14:30)
        if dt.minute < 30:
            return (dt - timedelta(hours=1)).replace(minute=30, second=0, microsecond=0)
        return dt.replace(minute=30, second=0, microsecond=0)

    if rule == "vilage_fcst":
        # 단기예보: 02:00, 05:00, 08:00, 11:00, 14:00, 17:00, 20:00, 23:00
        # API 발표는 기준시간 + 10분부터 이루어지므로, 현재 10분 미만이면 한 턴 앞당겨야 함
        eval_dt = dt if dt.minute >= 10 else dt - timedelta(hours=1)
        if eval_dt.hour < 2:
            return (eval_dt - timedelta(days=1)).replace(
                hour=23, minute=0, second=0, microsecond=0
            )

        k = (eval_dt.hour - 2) // 3
        base_hour = 2 + 3 * k
        return eval_dt.replace(hour=base_hour, minute=0, second=0, microsecond=0)

    raise ValueError(f"알 수 없는 time_rule: {rule}")


def _extract(body: dict, root_key: str) -> list[dict]:
    """root_key의 점 표기 경로를 따라가 행 배열을 꺼낸다. 경로가 없으면 빈 리스트."""
    node: object = body
    for segment in root_key.split("."):
        if not isinstance(node, dict):
            return []
        node = node.get(segment)
    return node if isinstance(node, list) else []


def _merge_pages(bodies: list[dict], root_key: str) -> bytes:
    """여러 페이지의 item 배열을 하나로 이어붙인 JSON 본문(bytes)을 만든다.

    첫 페이지의 envelope(header 등)을 그대로 쓰고 item 배열만 전체 페이지를
    이어붙인 것으로 교체한다.
    """
    merged = copy.deepcopy(bodies[0])
    node = merged
    segments = root_key.split(".")
    for segment in segments[:-1]:
        node = node[segment]
    node[segments[-1]] = [item for body in bodies for item in _extract(body, root_key)]
    return json.dumps(merged).encode()


@dataclass(frozen=True, slots=True)
class _GridOutcome:
    """격자 하나(페이지 여러 장 포함)를 전부 받은 뒤의 결과. 스레드풀 워커가
    돌려주는 값이라 순수 데이터로 둔다."""

    payload: bytes | None
    error: FetchErrorKind | None


def _fetch_grid(
    client: httpx.Client,
    endpoint: str,
    root_key: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
) -> _GridOutcome:
    """격자 하나를 필요한 만큼 페이지를 넘겨가며 받아 하나의 결과로 합친다.

    페이지 중 하나라도 실패하면 그 격자 전체를 실패로 처리한다 — 부분 페이지만 따로
    재시도하면 "이 격자는 몇 페이지짜리였는지"를 라운드 사이에 기억할 방법이 없다.
    """
    base_url = (
        f"{_BASE_URL}/{endpoint}?authKey={_api_key()}&dataType=JSON"
        f"&numOfRows={_NUM_OF_ROWS}"
        f"&base_date={base_date}&base_time={base_time}&nx={nx}&ny={ny}"
    )

    page_bodies: list[dict] = []
    first_page_raw: bytes | None = None
    total_pages = 1
    page_no = 1

    while page_no <= total_pages:
        url = f"{base_url}&pageNo={page_no}"
        try:
            response = client.get(url)
        except httpx.RequestError:
            return _GridOutcome(payload=None, error=FetchErrorKind.TRANSIENT)

        category = classify_http_status(response.status_code)
        if category is not None:
            # FATAL(인증키 오류)이든 TRANSIENT/PERMANENT든 판정만 돌려준다 —
            # "전체 중단할지"는 호출자(fetch)가 결정한다.
            return _GridOutcome(payload=None, error=category)

        # HTTP 200 OK일 경우 본문의 resultCode 확인
        try:
            body = json.loads(response.content)
            if not isinstance(body, dict):
                raise TypeError("응답이 JSON 객체가 아님")
            result_code = body.get("response", {}).get("header", {}).get("resultCode")
        except (json.JSONDecodeError, TypeError):
            return _GridOutcome(payload=None, error=FetchErrorKind.TRANSIENT)

        api_category = _classify_result_code(result_code)
        if api_category is not None:
            return _GridOutcome(payload=None, error=api_category)

        page_bodies.append(body)
        if page_no == 1:
            first_page_raw = response.content
            total_count = body.get("response", {}).get("body", {}).get("totalCount")
            if isinstance(total_count, int) and total_count > _NUM_OF_ROWS:
                total_pages = math.ceil(total_count / _NUM_OF_ROWS)

        page_no += 1

    payload = (
        first_page_raw if len(page_bodies) == 1 else _merge_pages(page_bodies, root_key)
    )
    return _GridOutcome(payload=payload, error=None)


@adapter("kma_apihub")
class KmaApiHubAdapter:
    """기상청 API 허브 공용 격자 반복 어댑터."""

    @staticmethod
    def planned_parts(
        config: SourceConfig,
        window: Window,
    ) -> frozenset[str]:
        """Deadline 전에 config의 exact 34-grid 요청 계획을 고정한다.

        KMA는 응답에서 전체 조각 수를 주지 않는다. 따라서 iterator가 deadline으로
        첫 미방문 grid 전에 끊겨도 누락을 기록하려면 fetch를 시작하기 전에 전체 key를
        만들어야 한다. 중복 grid는 34개처럼 보여도 한 번만 호출되므로 hard fail한다.
        """
        del window
        grids = config.adapter_params.get("grids")
        if type(grids) is not list or len(grids) != _EXPECTED_GRID_COUNT:
            raise ValueError("KMA source의 grids는 정확히 34개여야 합니다")

        keys: list[str] = []
        for grid in grids:
            if (
                type(grid) not in {list, tuple}
                or len(grid) != 2
                or type(grid[0]) is not int
                or type(grid[1]) is not int
            ):
                raise ValueError("KMA grid는 정확히 두 builtin integer여야 합니다")
            keys.append(f"grid-{grid[0]:03d}x{grid[1]:03d}")
        if len(set(keys)) != _EXPECTED_GRID_COUNT:
            raise ValueError("KMA source의 34개 grid는 중복될 수 없습니다")
        return frozenset(keys)

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
        root_key = params["root_key"]
        time_rule = params.get("time_rule")

        # time_rule 규칙에 따라 API 유효 시각으로 보정
        adjusted_time = _adjust_base_time(window.window_start, time_rule)
        base_date = adjusted_time.strftime("%Y%m%d")
        base_time = adjusted_time.strftime("%H%M")

        grid_items = [
            (f"grid-{nx:03d}x{ny:03d}", nx, ny)
            for nx, ny in params["grids"]
            if f"grid-{nx:03d}x{ny:03d}" not in skip
        ]

        def fetch_one_grid(item: tuple[str, int, int]) -> _GridOutcome:
            _key, nx, ny = item
            return _fetch_grid(client, endpoint, root_key, base_date, base_time, nx, ny)

        # concurrency 미선언 소스는 순차로 돈다(seoul_openapi와 같은 opt-in 관례).
        concurrency = max(1, int(params.get("concurrency", _DEFAULT_CONCURRENCY)))
        if concurrency == 1:
            for item in grid_items:
                key, _nx, _ny = item
                outcome = fetch_one_grid(item)
                yield FetchResult(
                    key=key, payload=outcome.payload, error=outcome.error, expected_total=None
                )
                if outcome.error is FetchErrorKind.FATAL:
                    # 모든 격자가 같은 인증키를 쓰므로 나머지도 같은 이유로 실패한다.
                    return
            return

        for (key, _nx, _ny), outcome in run_concurrent(
            grid_items, concurrency, fetch_one_grid, "kma-grid"
        ):
            yield FetchResult(
                key=key, payload=outcome.payload, error=outcome.error, expected_total=None
            )

    @staticmethod
    def normalize(chunks: list[bytes], config: SourceConfig) -> list[dict]:
        """조각들을 pivot 설정에 따라 합친다."""
        root_key = config.adapter_params["root_key"]
        pivot = config.adapter_params["pivot"]
        key_field, value_field = pivot["key"], pivot["value"]

        groups: dict[tuple, dict] = {}
        order: list[tuple] = []
        for chunk in chunks:
            body = json.loads(chunk)
            for item in _extract(body, root_key):
                group_key = tuple(
                    sorted(
                        (k, v)
                        for k, v in item.items()
                        if k not in (key_field, value_field)
                    )
                )
                if group_key not in groups:
                    groups[group_key] = dict(group_key)
                    order.append(group_key)
                groups[group_key][item[key_field]] = item[value_field]

        return [groups[k] for k in order]
