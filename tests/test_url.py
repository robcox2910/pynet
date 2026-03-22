"""Test URL parsing."""

import pytest

from pynet.url import URL, parse_url

# Named constants for default ports.
HTTP_DEFAULT_PORT = 80
HTTPS_DEFAULT_PORT = 443
CUSTOM_PORT = 8080


class TestParseUrl:
    """Test the parse_url function."""

    def test_full_url_with_all_parts(self) -> None:
        """Parse a URL that has scheme, host, port, path, query, and fragment."""
        result = parse_url("https://example.com:8080/api/users?page=2&sort=name#results")

        assert result.scheme == "https"
        assert result.host == "example.com"
        assert result.port == CUSTOM_PORT
        assert result.path == "/api/users"
        assert result.query == {"page": "2", "sort": "name"}
        assert result.fragment == "results"

    def test_http_default_port(self) -> None:
        """Default port for http is 80."""
        result = parse_url("http://example.com/")

        assert result.port == HTTP_DEFAULT_PORT

    def test_https_default_port(self) -> None:
        """Default port for https is 443."""
        result = parse_url("https://example.com/")

        assert result.port == HTTPS_DEFAULT_PORT

    def test_query_params_parsed_into_dict(self) -> None:
        """Query parameters are split into key-value pairs."""
        result = parse_url("http://example.com/?color=red&size=large")

        assert result.query == {"color": "red", "size": "large"}

    def test_missing_path_defaults_to_slash(self) -> None:
        """A URL with no path defaults to /."""
        result = parse_url("http://example.com")

        assert result.path == "/"

    def test_fragment_parsing(self) -> None:
        """Fragment after # is extracted."""
        result = parse_url("http://example.com/page#section-2")

        assert result.fragment == "section-2"

    def test_no_scheme_defaults_to_http(self) -> None:
        """A URL with no scheme defaults to http."""
        result = parse_url("example.com/path")

        assert result.scheme == "http"
        assert result.host == "example.com"
        assert result.path == "/path"
        assert result.port == HTTP_DEFAULT_PORT

    def test_returns_url_dataclass(self) -> None:
        """parse_url return type is a URL dataclass."""
        result = parse_url("http://example.com/")

        assert isinstance(result, URL)

    def test_query_key_without_value(self) -> None:
        """Query key with no value is stored with empty string."""
        result = parse_url("http://example.com/?flag")

        assert result.query == {"flag": ""}
