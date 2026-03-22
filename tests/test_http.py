"""Test HTTP response parsing."""

import pytest

from pynet.errors import HTTPError
from pynet.http import Response, _parse_response

STATUS_OK = 200
STATUS_NOT_FOUND = 404


class TestParseResponse:
    """Test the _parse_response function."""

    def test_valid_response_parsed_correctly(self) -> None:
        """Parse a well-formed HTTP response into status, headers, and body."""
        raw = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<h1>Hello</h1>"

        result = _parse_response(raw)

        assert result.status == STATUS_OK
        assert result.headers["Content-Type"] == "text/html"
        assert result.body == "<h1>Hello</h1>"

    def test_empty_response_raises_http_error(self) -> None:
        """An empty response string raise HTTPError."""
        with pytest.raises(HTTPError, match="Empty response"):
            _parse_response("")

    def test_whitespace_only_response_raises_http_error(self) -> None:
        """A whitespace-only response raise HTTPError."""
        with pytest.raises(HTTPError, match="Empty response"):
            _parse_response("   \n  ")

    def test_malformed_status_line_raises_http_error(self) -> None:
        """A status line without a status code raise HTTPError."""
        with pytest.raises(HTTPError, match="Malformed status line"):
            _parse_response("GARBAGE\r\n\r\nbody")

    def test_response_with_multiple_headers(self) -> None:
        """Parse multiple headers from a response."""
        raw = (
            "HTTP/1.1 404 Not Found\r\n"
            "Content-Type: text/plain\r\n"
            "X-Custom: hello\r\n"
            "\r\n"
            "Not Found"
        )

        result = _parse_response(raw)

        assert result.status == STATUS_NOT_FOUND
        assert result.headers["Content-Type"] == "text/plain"
        assert result.headers["X-Custom"] == "hello"
        assert result.body == "Not Found"

    def test_response_with_empty_body(self) -> None:
        """Parse a response that has headers but no body."""
        raw = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"

        result = _parse_response(raw)

        assert result.status == STATUS_OK
        assert result.body == ""


class TestResponse:
    """Test the Response class directly."""

    def test_response_stores_attributes(self) -> None:
        """Response store status, headers, and body."""
        headers = {"Content-Type": "application/json"}
        response = Response(status=STATUS_OK, headers=headers, body='{"ok": true}')

        assert response.status == STATUS_OK
        assert response.headers == headers
        assert response.body == '{"ok": true}'

    def test_response_status_is_int(self) -> None:
        """Response status is an integer."""
        response = Response(status=STATUS_NOT_FOUND, headers={}, body="")

        assert isinstance(response.status, int)
        assert response.status == STATUS_NOT_FOUND
