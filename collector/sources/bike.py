"""Bike API collection functions."""

import os

import requests

SEOUL_API_BASE_URL = "http://openapi.seoul.go.kr:8088"
BIKE_SERVICE_NAME = "bikeList"

BIKE_RESPONSE_KEY = "rentBikeStatus"

PAGE_SIZE = 1000


def fetch_bike_page(page_no: int) -> dict:
    """Fetch one page from the Seoul bike API.

    Args:
        page_no: One-based API page number.

    Returns:
        dict: Raw JSON response from the API.

    Raises:
        RuntimeError: If the API key is missing or the response is invalid.
        requests.RequestException: If the HTTP request fails.
    """
    api_key = os.getenv("SEOUL_API_KEY")

    if not api_key:
        raise RuntimeError("SEOUL_API_KEY is not set.")

    start_index = (page_no - 1) * PAGE_SIZE + 1
    end_index = page_no * PAGE_SIZE

    url = (
        f"{SEOUL_API_BASE_URL}/"
        f"{api_key}/json/"
        f"{BIKE_SERVICE_NAME}/"
        f"{start_index}/{end_index}/"
    )

    response = requests.get(
        url,
        timeout=10,
    )
    response.raise_for_status()

    payload = response.json()
    print(payload.keys())
    service = payload.get(BIKE_RESPONSE_KEY)

    if service is None:
        raise RuntimeError(
            f"Missing service response: {BIKE_RESPONSE_KEY}"
        )

    if service is None:
        raise RuntimeError(
            f"Missing service response: {BIKE_SERVICE_NAME}"
        )

    result = service.get("RESULT", {})
    result_code = result.get("CODE")

    if result_code != "INFO-000":
        raise RuntimeError(
            f"Seoul API error: "
            f"code={result_code}, "
            f"message={result.get('MESSAGE')}"
        )

    return payload