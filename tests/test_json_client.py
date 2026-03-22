"""Test JSON client body parsing."""

import pytest

from pynet.errors import HTTPError
from pynet.http import Response
from pynet.json_client import _parse_json_body

STATUS_OK = 200


class TestParseJsonBody:
    """Test the _parse_json_body function."""

    def test_valid_json_parsed(self) -> None:
        """Parse a valid JSON object from a response body."""
        response = Response(status=STATUS_OK, headers={}, body='{"name": "Alice", "age": 30}')

        result = _parse_json_body(response)

        assert result == {"name": "Alice", "age": 30}

    def test_invalid_json_raises_http_error(self) -> None:
        """Invalid JSON in the response body raise HTTPError."""
        response = Response(status=STATUS_OK, headers={}, body="not valid json {{{")

        with pytest.raises(HTTPError, match="not valid JSON"):
            _parse_json_body(response)

    def test_non_object_json_raises_http_error(self) -> None:
        """A JSON array (not an object) raise HTTPError."""
        response = Response(status=STATUS_OK, headers={}, body='[1, 2, 3]')

        with pytest.raises(HTTPError, match="Expected a JSON object"):
            _parse_json_body(response)

    def test_empty_object_parsed(self) -> None:
        """An empty JSON object is valid."""
        response = Response(status=STATUS_OK, headers={}, body="{}")

        result = _parse_json_body(response)

        assert result == {}

    def test_nested_json_parsed(self) -> None:
        """Parse nested JSON objects."""
        body = '{"user": {"name": "Bob"}, "active": true}'
        response = Response(status=STATUS_OK, headers={}, body=body)

        result = _parse_json_body(response)

        assert result["active"] is True
        assert isinstance(result["user"], dict)
