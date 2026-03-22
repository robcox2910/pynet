"""JSON client -- structured conversations with web APIs.

When your program talks to a web API, it sends and receives **JSON**
-- a structured format both sides understand. This module wraps the
HTTP client to make JSON API calls simple.
"""

import json

from pynet.errors import HTTPError
from pynet.http import Response, get, post


def json_get(url: str, headers: dict[str, str] | None = None) -> dict[str, object]:
    """Send a GET request and parse the JSON response.

    Args:
        url: The API URL to request.
        headers: Optional extra headers.

    Returns:
        The parsed JSON response as a dict.

    Raises:
        HTTPError: If the response is not valid JSON.

    """
    response = get(url, headers=headers)
    return _parse_json_body(response)


def json_post(
    url: str,
    data: dict[str, object],
    headers: dict[str, str] | None = None,
) -> dict[str, object]:
    """Send a POST request with a JSON body and parse the response.

    Args:
        url: The API URL to send to.
        data: The data to send (will be serialized to JSON).
        headers: Optional extra headers.

    Returns:
        The parsed JSON response as a dict.

    Raises:
        HTTPError: If the response is not valid JSON.

    """
    all_headers = {"Content-Type": "application/json"}
    if headers:
        all_headers.update(headers)
    body = json.dumps(data)
    response = post(url, body=body, headers=all_headers)
    return _parse_json_body(response)


def _parse_json_body(response: Response) -> dict[str, object]:
    """Parse a response body as JSON.

    Args:
        response: The HTTP response.

    Returns:
        The parsed JSON as a dict.

    Raises:
        HTTPError: If the body is not valid JSON.

    """
    try:
        result = json.loads(response.body)
    except json.JSONDecodeError as exc:
        msg = f"Response body is not valid JSON: {exc}"
        raise HTTPError(msg) from exc
    if not isinstance(result, dict):
        msg = "Expected a JSON object, got a different type"
        raise HTTPError(msg)
    from typing import cast

    return cast(dict[str, object], result)
