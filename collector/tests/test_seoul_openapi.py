"""#6 서울 열린데이터광장 어댑터 테스트: 페이지네이션, RESULT.CODE 분류, normalize."""

from __future__ import annotations

import json

import httpx
import pytest

from datetime import datetime
from zoneinfo import ZoneInfo

from adapters.base import FetchErrorKind, Window
from adapters.seoul_openapi import SeoulOpenApiAdapter

KST = ZoneInfo("Asia/Seoul")


class _StubConfig:
    def __init__(self, adapter_params):
        self.adapter_params = adapter_params


def _config(page_size=2):
    return _StubConfig({"service": "bikeList", "page_size": page_size, "root_key": "rentBikeStatus.row"})


def _body(code="INFO-000", total=3, rows=None):
    return json.dumps(
        {
            "rentBikeStatus": {
                "list_total_count": total,
                "RESULT": {"CODE": code, "MESSAGE": "ok"},
                "row": rows if rows is not None else [],
            }
        }
    ).encode()


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setenv("SEOUL_OPENAPI_KEY", "secret-key-123")


def test_fetch_paginates_until_total_is_covered():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        if "/1/2/" in str(request.url):
            return httpx.Response(200, content=_body(total=3, rows=[{"a": "1"}, {"a": "2"}]))
        if "/3/3/" in str(request.url):
            return httpx.Response(200, content=_body(total=3, rows=[{"a": "3"}]))
        raise AssertionError(f"unexpected url: {request.url}")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(page_size=2), window=None, client=client))

    assert [r.key for r in results] == ["page-00001-00002", "page-00003-00003"]
    assert results[0].expected_total == 3
    assert results[1].expected_total is None  # 첫 조각에만 싣는다
    assert all(r.error is None for r in results)
    assert len(calls) == 2


def test_fetch_masks_api_key_from_chunk_keys():
    def handler(request):
        return httpx.Response(200, content=_body(total=1, rows=[{"a": "1"}]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(page_size=2), window=None, client=client))

    for r in results:
        assert "secret-key-123" not in r.key


def test_fetch_skips_keys_already_collected(monkeypatch):
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=_body(total=3, rows=[{"a": "3"}]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(
        SeoulOpenApiAdapter.fetch(
            _config(page_size=2), window=None, client=client,
            skip=frozenset({"page-00001-00002"}), expected_total=3,
        )
    )

    assert [r.key for r in results] == ["page-00003-00003"]
    assert len(calls) == 1  # page1은 호출되지 않는다


def test_fetch_classifies_auth_error_as_fatal():
    def handler(request):
        return httpx.Response(200, content=_body(code="INFO-100"))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert len(results) == 1
    assert results[0].error is FetchErrorKind.FATAL


def test_fetch_classifies_server_error_as_transient():
    def handler(request):
        return httpx.Response(200, content=_body(code="ERROR-500"))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert results[0].error is FetchErrorKind.TRANSIENT


def test_fetch_classifies_other_error_as_permanent():
    def handler(request):
        return httpx.Response(200, content=_body(code="ERROR-336"))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert results[0].error is FetchErrorKind.PERMANENT


def test_fetch_treats_empty_result_info_200_as_success():
    def handler(request):
        return httpx.Response(200, content=_body(code="INFO-200", total=0, rows=[]))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert len(results) == 1
    assert results[0].error is None


def test_fetch_yields_raw_response_unmodified():
    raw = _body(total=1, rows=[{"a": "1"}])

    def handler(request):
        return httpx.Response(200, content=raw)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(_config(), window=None, client=client))

    assert results[0].payload == raw


def test_normalize_concatenates_rows_across_chunks():
    chunk1 = _body(total=3, rows=[{"a": "1"}, {"a": "2"}])
    chunk2 = _body(total=3, rows=[{"a": "3"}])

    rows = SeoulOpenApiAdapter.normalize([chunk1, chunk2], _config())

    assert rows == [{"a": "1"}, {"a": "2"}, {"a": "3"}]


def test_normalize_tolerates_missing_row_key():
    chunk_without_rows = json.dumps({"rentBikeStatus": {"RESULT": {"CODE": "INFO-200"}}}).encode()
    chunk_with_rows = _body(total=1, rows=[{"a": "1"}])

    rows = SeoulOpenApiAdapter.normalize([chunk_without_rows, chunk_with_rows], _config())

    assert rows == [{"a": "1"}]


class TestPathSuffixTemplate:
    """`path_suffix`는 소스마다 다른 URL 꼬리를 yaml로 표현하는 장치다.

    `tbCycleRentData`는 `/{날짜}/{시}`를 받는다. 그런데 윈도우 시작 시각의 시(hour)를
    그대로 쓰면 매시 마지막 5분이 통째로 누락된다 — 19:00 윈도우가 19시대를 요청해
    버려서 18:55~19:00 대여를 아무도 가져가지 않는다. 직전 순간(`window_last`)의 시를
    쓰면 19:00 윈도우가 18시대를 한 번 더 완결시키고 19:05부터 19시대로 넘어간다.
    """

    def _capture(self, params, window):
        calls = []

        def handler(request):
            calls.append(str(request.url))
            return httpx.Response(200, content=_body(total=1, rows=[{"a": "1"}]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        list(SeoulOpenApiAdapter.fetch(_StubConfig(params), window=window, client=client))
        return calls

    def _params(self, suffix):
        return {"service": "tbCycleRentData", "page_size": 1000,
                "root_key": "rentBikeStatus.row", "path_suffix": suffix}

    def test_window_start_is_available(self):
        window = Window(
            window_start=datetime(2026, 8, 18, 18, 55, tzinfo=KST),
            window_end=datetime(2026, 8, 18, 19, 0, tzinfo=KST),
        )

        calls = self._capture(self._params("/{window_start:%Y-%m-%d}/{window_start:%H}"), window)

        assert calls[0].endswith("/2026-08-18/18/")

    def test_window_end_is_available(self):
        window = Window(
            window_start=datetime(2026, 8, 18, 18, 55, tzinfo=KST),
            window_end=datetime(2026, 8, 18, 19, 0, tzinfo=KST),
        )

        calls = self._capture(self._params("/{window_end:%H}"), window)

        assert calls[0].endswith("/19/")

    def test_window_last_is_the_instant_before_the_window(self):
        window = Window(
            window_start=datetime(2026, 8, 18, 19, 0, tzinfo=KST),
            window_end=datetime(2026, 8, 18, 19, 5, tzinfo=KST),
        )

        calls = self._capture(self._params("/{window_last:%Y-%m-%d}/{window_last:%H}"), window)

        assert calls[0].endswith("/2026-08-18/18/")

    def test_window_last_keeps_mid_hour_windows_on_their_own_hour(self):
        window = Window(
            window_start=datetime(2026, 8, 18, 19, 5, tzinfo=KST),
            window_end=datetime(2026, 8, 18, 19, 10, tzinfo=KST),
        )

        calls = self._capture(self._params("/{window_last:%H}"), window)

        assert calls[0].endswith("/19/")

    def test_window_last_rolls_the_date_back_at_midnight(self):
        window = Window(
            window_start=datetime(2026, 8, 19, 0, 0, tzinfo=KST),
            window_end=datetime(2026, 8, 19, 0, 5, tzinfo=KST),
        )

        calls = self._capture(self._params("/{window_last:%Y-%m-%d}/{window_last:%H}"), window)

        assert calls[0].endswith("/2026-08-18/23/")

    def test_hour_is_zero_padded(self):
        window = Window(
            window_start=datetime(2026, 8, 18, 4, 5, tzinfo=KST),
            window_end=datetime(2026, 8, 18, 4, 10, tzinfo=KST),
        )

        calls = self._capture(self._params("/{window_last:%H}"), window)

        assert calls[0].endswith("/04/")
