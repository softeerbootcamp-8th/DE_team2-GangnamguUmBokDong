"""#6 서울 열린데이터광장 어댑터 테스트: 페이지네이션, RESULT.CODE 분류, normalize."""

from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

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
    assert SeoulOpenApiAdapter.planned_parts(config, window=None) == frozenset(
        f"poi-POI{i:03d}" for i in range(117, 122)
    )
    results = list(SeoulOpenApiAdapter.fetch(config, window=None, client=client))

    assert [result.key for result in results] == [
        "poi-POI117",
        "poi-POI118",
        "poi-POI119",
        "poi-POI120",
        "poi-POI121",
    ]
    # POI 범위는 기대 row 수가 아니다. INFO-200도 정상 성공 조각이므로 pipeline이
    # 조각 기준 completeness를 쓰도록 expected_total은 전달하지 않는다.
    assert all(result.expected_total is None for result in results)
    assert all(result.error is None for result in results)
    assert any("/POI121/" in url for url in calls)


def test_population_fetch_requests_pois_concurrently_and_preserves_order():
    import threading
    import time

    config = _StubConfig(
        {
            "service": "citydata_ppltn",
            "page_size": 1000,
            "root_key": "SeoulRtd.citydata_ppltn",
            "poi_start": 1,
            "poi_end": 8,
            "concurrency": 4,
        }
    )
    lock = threading.Lock()
    live = {"now": 0, "max": 0}

    def handler(request):
        poi_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        with lock:
            live["now"] += 1
            live["max"] = max(live["max"], live["now"])
        try:
            time.sleep(0.05)
            body = {
                "RESULT": {"RESULT.CODE": "INFO-000"},
                "SeoulRtd.citydata_ppltn": [{"AREA_CD": poi_id}],
            }
            return httpx.Response(200, content=json.dumps(body).encode())
        finally:
            with lock:
                live["now"] -= 1

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(config, window=None, client=client))

    assert live["max"] > 1
    assert [result.key for result in results] == [f"poi-POI{i:03d}" for i in range(1, 9)]
    assert all(result.error is None for result in results)


def test_population_fetch_honors_skip_under_concurrency():
    config = _StubConfig(
        {
            "service": "citydata_ppltn",
            "page_size": 1000,
            "root_key": "SeoulRtd.citydata_ppltn",
            "poi_start": 1,
            "poi_end": 4,
            "concurrency": 2,
        }
    )
    calls = []

    def handler(request):
        poi_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        calls.append(poi_id)
        body = {
            "RESULT": {"RESULT.CODE": "INFO-000"},
            "SeoulRtd.citydata_ppltn": [{"AREA_CD": poi_id}],
        }
        return httpx.Response(200, content=json.dumps(body).encode())

    client = httpx.Client(transport=httpx.MockTransport(handler))
    results = list(SeoulOpenApiAdapter.fetch(
        config, window=None, client=client,
        skip=frozenset({"poi-POI002", "poi-POI003"}),
    ))

    assert [result.key for result in results] == ["poi-POI001", "poi-POI004"]
    assert set(calls) == {"POI001", "POI004"}


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


# ---------------------------------------------------------------------------
# 페이지 병렬 조회
#
# 실측(2026-08-19): tbCycleRentData의 1000행 페이지 응답이 시점에 따라 0.6~7.2초로
# 흔들린다. 피크 시간대(18시)는 17페이지라 순차로 72.7초가 걸렸고 fetch 예산은
# 150초다. 같은 페이지 집합을 스레드풀로 받으면 4워커 17.0초(4.3배),
# 8워커 10.5초(6.9배)였고 행 수는 정확히 일치했다.
#
# 순서를 유지하는 이유: 완료 순서대로 내보내면 조각 키 순서가 흔들려 기존 계약
# (그리고 이 파일의 다른 테스트들)이 깨진다. 앞쪽 페이지를 기다리는 동안에도
# 뒤쪽 요청은 이미 나가 있으므로 전체 소요는 병렬과 같다.
# ---------------------------------------------------------------------------


def _concurrent_config(page_size=2, concurrency=4):
    return _StubConfig({
        "service": "bikeList", "page_size": page_size,
        "root_key": "rentBikeStatus.row", "concurrency": concurrency,
    })


def _paged_handler(total, page_size, *, delay=0.0, overrides=None, calls=None, live=None):
    """total건을 page_size로 나눠 응답하는 핸들러. delay로 지연을 흉내낸다.

    `live`를 주면 동시 진행 중인 요청 수의 최댓값을 `live["max"]`에 기록한다.
    """
    import threading
    import time

    lock = threading.Lock()
    state = {"now": 0}

    def handler(request):
        url = str(request.url)
        if calls is not None:
            with lock:
                calls.append(url)
        if live is not None:
            with lock:
                state["now"] += 1
                live["max"] = max(live.get("max", 0), state["now"])
        try:
            if delay:
                time.sleep(delay)
            for marker, response in (overrides or {}).items():
                if marker in url:
                    return response
            import re
            start, end = (int(x) for x in re.search(r"/(\d+)/(\d+)/", url).groups())
            rows = [{"a": str(i)} for i in range(start, min(end, total) + 1)]
            return httpx.Response(200, content=_body(total=total, rows=rows))
        finally:
            if live is not None:
                with lock:
                    state["now"] -= 1

    return handler


class TestConcurrentPagination:
    def test_pages_are_requested_concurrently(self):
        """지연 0.2초 × 8페이지. 순차면 1.6초, 4워커면 1페이지(순차 발견) + 2배치."""
        import time

        live = {}
        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(8, 1, delay=0.2, live=live))
        )
        started = time.monotonic()
        results = list(
            SeoulOpenApiAdapter.fetch(_concurrent_config(page_size=1, concurrency=4),
                                      window=None, client=client)
        )
        elapsed = time.monotonic() - started

        assert len(results) == 8
        assert all(r.error is None for r in results)
        assert live["max"] > 1, "요청이 하나씩만 나갔다 — 병렬이 아니다"
        assert elapsed < 1.2, f"순차(1.6s)보다 빨라야 한다: {elapsed:.2f}s"

    def test_page_order_is_preserved(self):
        client = httpx.Client(transport=httpx.MockTransport(_paged_handler(8, 1, delay=0.05)))
        results = list(
            SeoulOpenApiAdapter.fetch(_concurrent_config(page_size=1, concurrency=4),
                                      window=None, client=client)
        )

        assert [r.key for r in results] == [f"page-{i:05d}-{i:05d}" for i in range(1, 9)]
        assert results[0].expected_total == 8
        assert all(r.expected_total is None for r in results[1:])

    def test_default_is_sequential_so_other_sources_are_unchanged(self):
        """concurrency를 선언하지 않은 소스는 동작이 그대로여야 한다."""
        live = {}
        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(6, 1, delay=0.02, live=live))
        )
        results = list(SeoulOpenApiAdapter.fetch(_config(page_size=1), window=None, client=client))

        assert len(results) == 6
        assert live["max"] == 1, "concurrency 미선언 소스는 순차여야 한다"

    def test_skip_is_honored_under_concurrency(self):
        calls = []
        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(4, 1, calls=calls))
        )
        results = list(SeoulOpenApiAdapter.fetch(
            _concurrent_config(page_size=1, concurrency=4), window=None, client=client,
            skip=frozenset({"page-00002-00002", "page-00003-00003"}), expected_total=4,
        ))

        assert [r.key for r in results] == ["page-00001-00001", "page-00004-00004"]
        assert not any("/2/2/" in c or "/3/3/" in c for c in calls)

    def test_transient_on_one_page_reports_only_that_page(self):
        client = httpx.Client(transport=httpx.MockTransport(_paged_handler(
            4, 1, overrides={"/3/3/": httpx.Response(500, content=b"boom")},
        )))
        results = list(SeoulOpenApiAdapter.fetch(
            _concurrent_config(page_size=1, concurrency=4), window=None, client=client))

        by_key = {r.key: r for r in results}
        assert by_key["page-00003-00003"].error is FetchErrorKind.TRANSIENT
        assert by_key["page-00001-00001"].error is None
        assert by_key["page-00004-00004"].error is None

    def test_fatal_stops_the_whole_fetch(self):
        client = httpx.Client(transport=httpx.MockTransport(_paged_handler(
            6, 1, overrides={"/3/3/": httpx.Response(200, content=_body(code="INFO-100", total=6))},
        )))
        results = list(SeoulOpenApiAdapter.fetch(
            _concurrent_config(page_size=1, concurrency=4), window=None, client=client))

        assert results[-1].error is FetchErrorKind.FATAL
        assert results[-1].key == "page-00003-00003"
        keys = [r.key for r in results]
        assert "page-00004-00004" not in keys, "FATAL 이후 조각을 더 내보내면 안 된다"

    def test_abandoning_the_generator_does_not_block_on_the_pool(self):
        """fetch_with_rounds는 마감 시한을 넘기면 순회를 중단하고 제너레이터를 버린다.
        스레드풀을 `with`로 감싸면 shutdown(wait=True)가 되어 큐에 남은 페이지가
        끝날 때까지 블록되고, 마감 시한 방어가 무력화된다."""
        import time

        client = httpx.Client(
            transport=httpx.MockTransport(_paged_handler(40, 1, delay=0.3))
        )
        iterator = SeoulOpenApiAdapter.fetch(
            _concurrent_config(page_size=1, concurrency=4), window=None, client=client)
        next(iterator)  # 1페이지만 받고 버린다

        started = time.monotonic()
        iterator.close()
        elapsed = time.monotonic() - started

        assert elapsed < 1.5, f"풀 종료가 큐를 기다렸다: {elapsed:.2f}s"


class TestTopLevelResultCode:
    """시작 인덱스가 total을 넘으면 서울 API는 래퍼 없이 최상단에 CODE만 준다
    (실측: `{"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}`).
    래퍼 안의 RESULT.CODE만 보면 None이 되어 PERMANENT로 오판한다."""

    def test_top_level_info_200_is_success_not_permanent(self):
        body = json.dumps({"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}).encode()
        client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=body)))

        results = list(SeoulOpenApiAdapter.fetch(_config(page_size=2), window=None, client=client))

        assert [r.error for r in results] == [None]

    def test_top_level_error_code_is_still_classified(self):
        body = json.dumps({"CODE": "INFO-100", "MESSAGE": "인증키 오류"}).encode()
        client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=body)))

        results = list(SeoulOpenApiAdapter.fetch(_config(page_size=2), window=None, client=client))

        assert results[0].error is FetchErrorKind.FATAL


class TestForecastFlatten:
    """citydata_ppltn의 `FCST_PPLTN` 중첩 배열이 슬롯 컬럼으로 펼쳐지는지 확인한다."""

    @staticmethod
    def _config():
        return _StubConfig(
            {"service": "citydata_ppltn", "page_size": 1000, "root_key": "SeoulRtd.citydata_ppltn"}
        )

    @staticmethod
    def _chunk(row: dict) -> bytes:
        return json.dumps({"SeoulRtd.citydata_ppltn": [row]}).encode()

    @staticmethod
    def _forecast(hour: int, pop: int) -> dict:
        return {
            "FCST_TIME": f"2026-08-19 {hour:02d}:00",
            "FCST_CONGEST_LVL": "여유",
            "FCST_PPLTN_MIN": str(pop),
            "FCST_PPLTN_MAX": str(pop + 500),
        }

    def test_twelve_slots_become_flat_columns(self):
        row = {"AREA_CD": "POI001", "FCST_YN": "Y",
               "FCST_PPLTN": [self._forecast(10 + i, 1000 * i) for i in range(12)]}
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert len(rows) == 1
        assert rows[0]["FCST_1_TIME"] == "2026-08-19 10:00"
        assert rows[0]["FCST_1_PPLTN_MIN"] == "0"
        assert rows[0]["FCST_12_TIME"] == "2026-08-19 21:00"
        assert rows[0]["FCST_12_PPLTN_MAX"] == "11500"
        assert rows[0]["FCST_12_CONGEST_LVL"] == "여유"

    def test_nested_key_is_removed_so_parquet_only_sees_scalars(self):
        row = {"AREA_CD": "POI001", "FCST_PPLTN": [self._forecast(10, 100)]}
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert "FCST_PPLTN" not in rows[0]
        assert all(not isinstance(v, (list, dict)) for v in rows[0].values())

    def test_slots_are_numbered_by_time_not_response_order(self):
        row = {"AREA_CD": "POI001",
               "FCST_PPLTN": [self._forecast(15, 300), self._forecast(13, 100), self._forecast(14, 200)]}
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert rows[0]["FCST_1_TIME"] == "2026-08-19 13:00"
        assert rows[0]["FCST_2_TIME"] == "2026-08-19 14:00"
        assert rows[0]["FCST_3_TIME"] == "2026-08-19 15:00"

    def test_short_array_leaves_remaining_slots_absent(self):
        row = {"AREA_CD": "POI001", "FCST_PPLTN": [self._forecast(10 + i, 100) for i in range(5)]}
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert rows[0]["FCST_5_TIME"] == "2026-08-19 14:00"
        assert not [k for k in rows[0] if k.startswith(("FCST_6_", "FCST_7_", "FCST_12_"))]

    def test_missing_forecast_array_keeps_other_columns(self):
        row = {"AREA_CD": "POI001", "FCST_YN": "N", "AREA_PPLTN_MIN": "100"}
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert rows[0] == {"AREA_CD": "POI001", "FCST_YN": "N", "AREA_PPLTN_MIN": "100"}

    def test_entries_without_time_are_dropped(self):
        row = {"AREA_CD": "POI001",
               "FCST_PPLTN": [{"FCST_PPLTN_MIN": "100"}, self._forecast(11, 200)]}
        rows = SeoulOpenApiAdapter.normalize([self._chunk(row)], self._config())

        assert rows[0]["FCST_1_TIME"] == "2026-08-19 11:00"
        assert "FCST_2_TIME" not in rows[0]

    def test_other_sources_are_untouched(self):
        config = _StubConfig({"service": "bikeList", "page_size": 1000, "root_key": "rentBikeStatus.row"})
        chunk = json.dumps({"rentBikeStatus": {"row": [{"stationId": "ST-1", "FCST_PPLTN": [{"x": 1}]}]}}).encode()

        rows = SeoulOpenApiAdapter.normalize([chunk], config)

        assert rows[0]["FCST_PPLTN"] == [{"x": 1}]
