"""Page-based collection helpers."""

from collections.abc import Callable
import time


class PageCollectionError(Exception):
    """Raised when a page fails after all retry attempts."""


def fetch_page_with_retry(
    *,
    page_no: int,
    fetch_page: Callable[[int], None],
    max_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> None:
    """Fetch one API page with retry.

    Args:
        page_no: API page number to fetch.
        fetch_page: Function that fetches a single page.
        max_attempts: Maximum number of attempts including the first attempt.
        retry_delay_seconds: Delay between retry attempts.

    Raises:
        PageCollectionError: If all attempts fail.
    """
    for attempt_no in range(1, max_attempts + 1):
        try:
            fetch_page(page_no)
            return
        except Exception as exc:
            print(
                f"page={page_no} "
                f"attempt={attempt_no}/{max_attempts} "
                f"failed: {exc}"
            )

            if attempt_no == max_attempts:
                raise PageCollectionError(
                    f"page {page_no} failed after {max_attempts} attempts"
                ) from exc

            time.sleep(retry_delay_seconds)


def collect_pages(
    *,
    total_pages: int,
    fetch_page: Callable[[int], None],
    max_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> list[int]:
    """Collect all pages while preserving failed page numbers.

    Args:
        total_pages: Total number of API pages.
        fetch_page: Function that fetches a single page.
        max_attempts: Maximum attempts for each page.
        retry_delay_seconds: Delay between retry attempts.

    Returns:
        list[int]: Pages that failed after all retry attempts.
    """
    failed_pages: list[int] = []

    for page_no in range(1, total_pages + 1):
        try:
            fetch_page_with_retry(
                page_no=page_no,
                fetch_page=fetch_page,
                max_attempts=max_attempts,
                retry_delay_seconds=retry_delay_seconds,
            )
        except PageCollectionError:
            failed_pages.append(page_no)

    return failed_pages