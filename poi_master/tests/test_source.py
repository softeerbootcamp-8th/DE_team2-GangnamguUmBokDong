"""서울시 첨부파일 발견과 다운로드 계약을 검증한다."""

import httpx
import pytest

from source import (
    DOWNLOAD_URL,
    PoiSourceError,
    fetch_source_assets,
    parse_attachments,
)

PAGE_URL = "https://example.test/dataset"


def _row(filename: str, sequence: str, modified_date: str = "2026.04.02.") -> str:
    """실제 페이지와 같은 핵심 속성을 가진 첨부 표 행을 만든다."""
    return (
        "<tr><td>1</td><td>데이터</td>"
        f"<td><span>{filename}</span></td><td>0.13</td><td>{modified_date}</td>"
        f"<td><a onclick=\"downloadFile('{sequence}')\">내려받기</a></td></tr>"
    )


def _page(*rows: str) -> str:
    """첨부 표 행을 HTML document로 감싼다."""
    return f"<html><body><table>{''.join(rows)}</table></body></html>"


def test_parser_uses_filename_row_and_dynamic_sequence() -> None:
    """행 번호가 아니라 파일명과 같은 행의 바뀔 수 있는 sequence를 결합한다."""
    html = _page(
        _row("실시간 도시데이터 매뉴얼.pdf", "22"),
        _row("서울시 주요 125장소 목록.xlsx", "91", "2027.01.03."),
        _row("서울시 주요 125장소 영역.zip", "92", "2027.01.03."),
    )

    list_attachment, areas_attachment = parse_attachments(html)

    assert list_attachment.sequence == "91"
    assert list_attachment.modified_date == "2027.01.03."
    assert list_attachment.declared_place_count == 125
    assert areas_attachment.sequence == "92"
    assert areas_attachment.declared_place_count == 125


def test_parser_rejects_different_declared_place_counts() -> None:
    """목록과 영역 파일명이 서로 다른 장소 수를 선언하면 다운로드 전에 거부한다."""
    html = _page(
        _row("서울시 주요 121장소 목록.xlsx", "23"),
        _row("서울시 주요 125장소 영역.zip", "24"),
    )

    with pytest.raises(PoiSourceError, match="선언한 장소 수가 다릅니다"):
        parse_attachments(html)


@pytest.mark.parametrize(
    "html",
    [
        "",
        _page(_row("서울시 주요 121장소 목록.xlsx", "23")),
        _page(
            _row("서울시 주요 121장소 목록.xlsx", "23"),
            _row("서울시 주요 122장소 목록.xlsx", "25"),
            _row("서울시 주요 121장소 영역.zip", "24"),
        ),
        _page(
            "<tr><td>서울시 주요 121장소 목록.xlsx</td><td>2026.04.02.</td></tr>",
            _row("서울시 주요 121장소 영역.zip", "24"),
        ),
    ],
)
def test_parser_rejects_missing_duplicate_or_unbound_rows(html: str) -> None:
    """두 역할이 exact 한 개가 아니거나 sequence가 없으면 실패한다."""
    with pytest.raises(PoiSourceError):
        parse_attachments(html)


def test_fetch_downloads_both_current_sequences() -> None:
    """페이지에서 발견한 최신 sequence로 목록과 영역을 각각 POST한다."""
    requested_sequences: list[str] = []
    html = _page(
        _row("서울시 주요 121장소 목록.xlsx", "301"),
        _row("서울시 주요 121장소 영역.zip", "302"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        """페이지 GET과 첨부 POST에 fixture 응답을 반환한다."""
        if str(request.url) == PAGE_URL:
            return httpx.Response(200, text=html)
        assert str(request.url) == DOWNLOAD_URL
        form = dict(
            item.split("=", 1) for item in request.content.decode("ascii").split("&")
        )
        requested_sequences.append(form["seq"])
        return httpx.Response(200, content=b"PK\x03\x04fixture")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assets = fetch_source_assets(client, page_url=PAGE_URL)

    assert requested_sequences == ["301", "302"]
    assert assets.list_bytes.startswith(b"PK")
    assert assets.areas_bytes.startswith(b"PK")
    assert assets.page_url == PAGE_URL


def test_fetch_rejects_html_error_disguised_as_download() -> None:
    """HTTP 200이어도 첨부 대신 HTML 오류 본문이면 게시 입력으로 받지 않는다."""
    html = _page(
        _row("서울시 주요 121장소 목록.xlsx", "23"),
        _row("서울시 주요 121장소 영역.zip", "24"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        """다운로드 endpoint에서 HTML 오류를 흉내 낸다."""
        if str(request.url) == PAGE_URL:
            return httpx.Response(200, text=html)
        return httpx.Response(200, content=b"<html>error</html>")

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(PoiSourceError, match="ZIP 계열"),
    ):
        fetch_source_assets(client, page_url=PAGE_URL)
