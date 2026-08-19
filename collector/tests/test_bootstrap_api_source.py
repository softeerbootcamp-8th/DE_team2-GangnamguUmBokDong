"""과거 조회 API 입력 테스트: 시각 형식·페이지네이션·재시도."""

import json
from datetime import date

import httpx
import pytest

from bootstrap import api_source as api_source_module
from bootstrap.api_source import FetchFailed, _hour, fetch_by_date
from bootstrap.config import BootstrapConfig


def _cfg(page_size=2):
    return BootstrapConfig.model_validate({
        "kind": "history_api",
        "service": "bikeListHist",
        "time_format": "%Y%m%d%H",
        "page_size": page_size,
        "window": {"from_column": "stationDt", "format": "%Y%m%d%H"},
    })


def _body(total, rows):
    return json.dumps({
        "rentBikeStatus": {
            "list_total_count": total,
            "RESULT": {"CODE": "INFO-000", "MESSAGE": "ok"},
            "row": rows,
        }
    }).encode()


def _row(hour, station):
    return {"stationId": station, "stationDt": f"202608{17}{hour:02d}"}


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setenv("SEOUL_OPENAPI_KEY", "secret-key-123")


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """재시도 백오프가 실제로 잠들면 이 파일의 재시도 테스트들이 초 단위로 느려진다.

    기본 sleep은 호출부(`_hour`)가 매번 `time.sleep`을 참조하므로, 여기서
    `time.sleep`을 patch하면 명시적으로 `sleep=`을 넘기지 않는 모든 호출에 적용된다.
    """
    monkeypatch.setattr(api_source_module.time, "sleep", lambda seconds: None)


class TestTimeFormat:
    def test_requests_ten_digit_hour(self):
        """8자리를 주면 API가 조용히 최신 스냅샷을 반환하므로 형식을 못 박는다."""
        seen = []

        def handler(request):
            seen.append(str(request.url))
            return httpx.Response(200, content=_body(1, [_row(0, "ST-1")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert "/2026081700/" in seen[0]
        assert all(len(u.rstrip("/").rsplit("/", 1)[-1]) == 10 for u in seen)

    def test_covers_all_twenty_four_hours(self):
        seen = []

        def handler(request):
            seen.append(str(request.url).rstrip("/").rsplit("/", 1)[-1])
            return httpx.Response(200, content=_body(1, [_row(0, "ST-1")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert sorted(seen) == sorted(f"20260817{h:02d}" for h in range(24))


class TestPagination:
    def test_follows_total_count_across_pages(self):
        def handler(request):
            url = str(request.url)
            if "/1/2/" in url:
                return httpx.Response(200, content=_body(3, [_row(0, "ST-1"), _row(0, "ST-2")]))
            return httpx.Response(200, content=_body(3, [_row(0, "ST-3")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        rows = fetch_by_date(_cfg(page_size=2), date(2026, 8, 17), client=client, concurrency=1)

        assert len(rows) == 24 * 3

    def test_empty_hour_contributes_nothing(self):
        def handler(request):
            return httpx.Response(200, content=json.dumps(
                {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}).encode())

        client = httpx.Client(transport=httpx.MockTransport(handler))
        rows = fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert rows == []


class TestRetry:
    def test_retries_transient_failure_then_succeeds(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(500, content=b"boom")
            return httpx.Response(200, content=_body(1, [_row(0, "ST-1")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        rows = fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert len(rows) == 24

    def test_raises_after_retries_are_exhausted(self):
        def handler(request):
            return httpx.Response(500, content=b"boom")

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with pytest.raises(FetchFailed):
            fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1, max_retries=1)

    def test_api_key_is_not_in_the_error_message(self):
        def handler(request):
            return httpx.Response(500, content=b"boom")

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with pytest.raises(FetchFailed) as excinfo:
            fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1, max_retries=0)

        assert "secret-key-123" not in str(excinfo.value)


class TestBackoff:
    def test_sleeps_with_exponential_backoff_between_retries(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] <= 2:
                return httpx.Response(500, content=b"boom")
            return httpx.Response(200, content=_body(1, [_row(0, "ST-1")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        sleeps = []

        rows = _hour(_cfg(), "2026081700", client, max_retries=2, sleep=sleeps.append)

        assert rows == [_row(0, "ST-1")]
        assert sleeps == [1, 2]

    def test_does_not_sleep_after_the_last_attempt(self):
        def handler(request):
            return httpx.Response(500, content=b"boom")

        client = httpx.Client(transport=httpx.MockTransport(handler))
        sleeps = []

        with pytest.raises(FetchFailed):
            _hour(_cfg(), "2026081700", client, max_retries=1, sleep=sleeps.append)

        # 시도 2번(초기 + 재시도 1번) -> 사이에 한 번만 자고, 마지막 실패 뒤엔 안 잔다
        assert sleeps == [1]

    def test_fatal_error_does_not_sleep(self):
        def handler(request):
            return httpx.Response(200, content=b"<RESULT><CODE>INFO-100</CODE>"
                                                b"<MESSAGE>bad key</MESSAGE></RESULT>")

        client = httpx.Client(transport=httpx.MockTransport(handler))
        sleeps = []

        with pytest.raises(FetchFailed):
            _hour(_cfg(), "2026081700", client, max_retries=2, sleep=sleeps.append)

        assert sleeps == []


class TestWrapperResultCode:
    def test_wrapper_result_code_failure_raises_fetch_failed(self):
        """정상 JSON이지만 wrapper 안 RESULT.CODE가 실패 코드면 조용히 넘어가지 않고 실패해야 한다."""
        def handler(request):
            body = json.dumps({
                "rentBikeStatus": {
                    "list_total_count": 0,
                    "RESULT": {"CODE": "ERROR-500", "MESSAGE": "서버 오류"},
                    "row": [],
                }
            }).encode()
            return httpx.Response(200, content=body)

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with pytest.raises(FetchFailed):
            fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1, max_retries=0)


class TestAuthKeyError:
    def test_auth_key_error_xml_fails_immediately_without_retry(self):
        """INFO-100(인증키 오류)은 XML로 오고, 재시도하지 말고 즉시 실패해야 한다."""
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            xml_body = (
                b"<RESULT><CODE>INFO-100</CODE>"
                b"<MESSAGE><![CDATA[\xec\x9d\xb8\xec\xa6\x9d\xed\x82\xa4\xea\xb0\x80 "
                b"\xec\x9c\xa0\xed\x9a\xa8\xed\x95\x98\xec\xa7\x80 \xec\x95\x8a\xec\x8a\xb5\xeb\x8b\x88\xeb\x8b\xa4."
                b"]]></MESSAGE></RESULT>"
            )
            return httpx.Response(200, content=xml_body)

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with pytest.raises(FetchFailed):
            fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1, max_retries=2)

        # 24시각 x 1회만 호출되어야 한다 (재시도 없음)
        assert calls["n"] == 24

    def test_no_data_info_200_still_returns_empty_regression(self):
        """INFO-200(데이터 없음)은 기존처럼 빈 결과로 정상 처리되어야 한다."""
        def handler(request):
            return httpx.Response(200, content=json.dumps(
                {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}).encode())

        client = httpx.Client(transport=httpx.MockTransport(handler))
        rows = fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert rows == []
