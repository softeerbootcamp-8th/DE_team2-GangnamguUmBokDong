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

주의:
- 인증키(`KMA_APIHUB_KEY`)는 로그·bronze에 남지 않게 마스킹한다.
- 캐스팅은 검증 엔진의 `types`가 담당한다. pivot은 구조만 바꾸고 값은 문자열 그대로 둔다.
- 실패 범주 판정은 base의 규칙을 따른다.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import httpx

from adapters.base import FetchErrorKind, FetchResult, adapter, classify_http_status

if TYPE_CHECKING:
    from adapters.base import Window
    from config.schema import SourceConfig

_BASE_URL = "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0"
_NUM_OF_ROWS = 1000  # 격자 하나당 한 시각의 관측·예보 항목 수를 넉넉히 덮는 상한


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
            return (eval_dt - timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)
        
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
        time_rule = params.get("time_rule")
        
        # time_rule 규칙에 따라 API 유효 시각으로 보정
        adjusted_time = _adjust_base_time(window.window_start, time_rule)
        base_date = adjusted_time.strftime("%Y%m%d")
        base_time = adjusted_time.strftime("%H%M")

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
                    sorted((k, v) for k, v in item.items() if k not in (key_field, value_field))
                )
                if group_key not in groups:
                    groups[group_key] = dict(group_key)
                    order.append(group_key)
                groups[group_key][item[key_field]] = item[value_field]

        return [groups[k] for k in order]
