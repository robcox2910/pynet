"""Test HTTP response parsing."""

import socket
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from pynet.errors import HTTPError, NetConnectionError
from pynet.http import Response, _parse_response, _request_target, get, post
from pynet.url import parse_url

STATUS_OK = 200
STATUS_NOT_FOUND = 404
STATUS_CREATED = 201


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

        result = _parse_response(raw.encode())

        assert result.status == STATUS_OK
        assert result.headers["Content-Type"] == "text/html"
        assert result.body == "<h1>Hello</h1>"

    def test_empty_response_raises_http_error(self) -> None:
        """An empty response string raise HTTPError."""
        with pytest.raises(HTTPError, match="Empty response"):
            _parse_response(b"")

    def test_whitespace_only_response_raises_http_error(self) -> None:
        """A whitespace-only response raise HTTPError."""
        with pytest.raises(HTTPError, match="Empty response"):
            _parse_response(b"   \n  ")

    def test_malformed_status_line_raises_http_error(self) -> None:
        """A status line without a status code raise HTTPError."""
        with pytest.raises(HTTPError, match="Malformed status line"):
            _parse_response(b"GARBAGE\r\n\r\nbody")

    def test_response_with_multiple_headers(self) -> None:
        """Parse multiple headers from a response."""
        raw = (
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nX-Custom: hello\r\n\r\nNot Found"
        )

        result = _parse_response(raw.encode())

        assert result.status == STATUS_NOT_FOUND
        assert result.headers["Content-Type"] == "text/plain"
        assert result.headers["X-Custom"] == "hello"
        assert result.body == "Not Found"

    def test_response_with_empty_body(self) -> None:
        """Parse a response that has headers but no body."""
        raw = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"

        result = _parse_response(raw.encode())

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


class TestResponseParsingEdgeCases:
    """Real servers send things a naive parser trips over."""

    def test_non_numeric_status_is_an_http_error(self) -> None:
        """Regression: 'HTTP/1.1 abc' used to raise a bare ValueError."""
        with pytest.raises(HTTPError, match="status"):
            _parse_response(b"HTTP/1.1 abc OK\r\n\r\n")

    def test_chunked_body_is_reassembled(self) -> None:
        """Regression: chunk sizes used to be left in the body."""
        raw = (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"5\r\nhello\r\n7\r\n, world\r\n0\r\n\r\n"
        )
        assert _parse_response(raw).body == "hello, world"

    def test_chunk_size_header_is_case_insensitive(self) -> None:
        """Header names don't care about capital letters."""
        raw = b"HTTP/1.1 200 OK\r\ntransfer-encoding: Chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n"
        assert _parse_response(raw).body == "hi"

    def test_broken_chunk_is_an_http_error(self) -> None:
        """A chunk size that isn't hex is reported, not crashed on."""
        raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nhi\r\n0\r\n\r\n"
        with pytest.raises(HTTPError, match="chunk"):
            _parse_response(raw)

    def test_content_length_counts_bytes_not_letters(self) -> None:
        """Regression: Content-Length was applied after decoding, so emoji broke it."""
        body = "héllo 🙂".encode()
        raw = b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n"
        assert _parse_response(raw + body + b"EXTRA").body == "héllo 🙂"

    def test_body_shorter_than_content_length_is_an_http_error(self) -> None:
        """If the connection ends early, we say so instead of returning half a page."""
        with pytest.raises(HTTPError, match="ended early"):
            _parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")

    def test_negative_chunk_size_is_an_http_error(self) -> None:
        """Regression: a chunk size of -6 sent the parser round in circles forever (property test)."""
        raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n-6\r\n"
        with pytest.raises(HTTPError, match="chunk"):
            _parse_response(raw)

    def test_negative_content_length_is_an_http_error(self) -> None:
        """Regression: Content-Length: -2 quietly chopped two bytes off the body."""
        with pytest.raises(HTTPError, match="Content-Length"):
            _parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: -2\r\n\r\nhello")

    def test_negative_status_code_is_an_http_error(self) -> None:
        """Regression: int() read the status code "-5" as minus five."""
        with pytest.raises(HTTPError, match="status"):
            _parse_response(b"HTTP/1.1 -5 OK\r\n\r\n")

    def test_content_length_with_underscore_is_an_http_error(self) -> None:
        """Regression: int() read "1_0" as 10, as Python numbers allow, but HTTP doesn't."""
        with pytest.raises(HTTPError, match="Content-Length"):
            _parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 1_0\r\n\r\n0123456789")

    def test_content_length_with_plus_sign_is_an_http_error(self) -> None:
        """Regression: int() read "+3" as 3; HTTP sizes are plain digits."""
        with pytest.raises(HTTPError, match="Content-Length"):
            _parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: +3\r\n\r\nabc")

    def test_chunk_size_with_0x_is_an_http_error(self) -> None:
        """Regression: int(..., 16) read "0x3" as 3; chunk sizes are bare hex digits."""
        raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0x3\r\nabc\r\n0\r\n\r\n"
        with pytest.raises(HTTPError, match="chunk"):
            _parse_response(raw)

    def test_non_numeric_content_length_is_an_http_error(self) -> None:
        """A Content-Length that isn't a number is reported clearly."""
        with pytest.raises(HTTPError, match="Content-Length"):
            _parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: lots\r\n\r\nhi")

    def test_chunked_body_without_final_chunk_is_an_http_error(self) -> None:
        """If the empty end-of-body chunk never arrives, the reply was cut off."""
        raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n"
        with pytest.raises(HTTPError, match="final chunk"):
            _parse_response(raw)

    def test_chunk_cut_off_midway_is_an_http_error(self) -> None:
        """A chunk shorter than its label promised means the reply was cut off."""
        raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\na\r\nhi"
        with pytest.raises(HTTPError, match="middle of a chunk"):
            _parse_response(raw)


@contextmanager
def _one_shot_server(reply: bytes) -> Iterator[int]:
    """Serve one connection on a free port with a canned reply; yield the port."""
    listener = socket.create_server(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    received: list[bytes] = []

    def serve() -> None:
        conn, _ = listener.accept()
        with conn:
            received.append(conn.recv(65536))
            conn.sendall(reply)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        thread.join(timeout=5)
        listener.close()


class TestAgainstARealSocket:
    """get() and post() talk to a real (tiny) server on this computer."""

    def test_get_reads_a_real_response(self) -> None:
        """A full round trip over TCP returns the parsed response."""
        reply = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
        with _one_shot_server(reply) as port:
            response = get(f"http://127.0.0.1:{port}/hello")
        assert response.status == STATUS_OK
        assert response.body == "ok"

    def test_get_decodes_a_chunked_response(self) -> None:
        """Chunked replies over a real socket arrive reassembled."""
        reply = (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n"
        )
        with _one_shot_server(reply) as port:
            assert get(f"http://127.0.0.1:{port}/").body == "abcdef"

    def test_post_sends_body_and_reads_reply(self) -> None:
        """post() sends its body and parses the server's answer."""
        reply = b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n"
        with _one_shot_server(reply) as port:
            assert post(f"http://127.0.0.1:{port}/items", body="x=1").status == STATUS_CREATED

    def test_refused_connection_is_a_net_connection_error(self) -> None:
        """Nobody listening? That's a clear NetConnectionError."""
        listener = socket.create_server(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.close()
        with pytest.raises(NetConnectionError):
            get(f"http://127.0.0.1:{port}/")
