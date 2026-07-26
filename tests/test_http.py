"""Test HTTP response parsing."""

import pytest

from pynet.errors import HTTPError, NetConnectionError
from pynet.http import Response, _parse_response, _request_target, get, post
from pynet.url import parse_url

STATUS_OK = 200
STATUS_NOT_FOUND = 404


class TestRequestTarget:
    """Test that the request line keeps the query string."""

    def test_path_only_when_no_query(self) -> None:
        """A URL with no query yields just the path."""
        assert _request_target(parse_url("http://api/users")) == "/users"

    def test_query_is_reattached(self) -> None:
        """The query string is put back on the request target."""
        assert _request_target(parse_url("http://api/users?page=2")) == "/users?page=2"

    def test_multiple_query_params_are_encoded(self) -> None:
        """Multiple query params are url-encoded onto the target."""
        target = _request_target(parse_url("http://api/search?q=hello world&n=3"))

        assert target.startswith("/search?")
        assert "q=hello+world" in target
        assert "n=3" in target


class TestHttpsNotSupported:
    """Test that https:// URLs fail with a clear error instead of plaintext."""

    def test_get_https_raises(self) -> None:
        """GET on an https:// URL raises a clear HTTPS-not-supported error."""
        with pytest.raises(NetConnectionError, match="HTTPS is not supported"):
            get("https://example.com/")

    def test_post_https_raises(self) -> None:
        """POST on an https:// URL raises a clear HTTPS-not-supported error."""
        with pytest.raises(NetConnectionError, match="HTTPS is not supported"):
            post("https://example.com/", body="hi")


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
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nX-Custom: hello\r\n\r\nNot Found"
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
