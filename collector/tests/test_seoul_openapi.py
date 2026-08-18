"""#6 서울 열린데이터광장 어댑터 테스트: 페이지네이션, RESULT.CODE 분류, normalize."""

from __future__ import annotations

import json

import httpx
import pytest

from adapters.base import FetchErrorKind
from adapters.seoul_openapi import SeoulOpenApiAdapter


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


def test_population_fetch_uses_configured_poi_range_and_does_not_stop_at_gap():
    config = _StubConfig(
        {
            "service": "citydata_ppltn",
            "page_size": 1000,
            "root_key": "SeoulRtd.citydata_ppltn",
            "poi_start": 117,
            "poi_end": 121,
        }
    )
    calls = []

    def handler(request):
        calls.append(str(request.url))
        poi_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        if poi_id == "POI119":
            body = {"RESULT": {"RESULT.CODE": "INFO-200"}}
        else:
            body = {
                "RESULT": {"RESULT.CODE": "INFO-000"},
                "SeoulRtd.citydata_ppltn": [{"AREA_CD": poi_id}],
            }
        return httpx.Response(200, content=json.dumps(body).encode())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(config, window=None, client=client))

    assert [result.key for result in results] == [
        "poi-POI117",
        "poi-POI118",
        "poi-POI119",
        "poi-POI120",
        "poi-POI121",
    ]
    assert results[0].expected_total == 5
    assert all(result.error is None for result in results)
    assert any("/POI121/" in url for url in calls)


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
