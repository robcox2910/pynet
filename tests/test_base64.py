"""Test Base64 encoding and decoding."""

import pytest

from pynet.base64 import b64_decode, b64_encode


class TestB64Encode:
    """Test the b64_encode function."""

    def test_empty_string(self) -> None:
        """Encode an empty string to an empty string."""
        assert b64_encode("") == ""

    def test_single_char(self) -> None:
        """Encode a single character with two padding characters."""
        assert b64_encode("H") == "SA=="

    def test_two_chars(self) -> None:
        """Encode two characters with one padding character."""
        assert b64_encode("He") == "SGU="

    def test_three_chars_no_padding(self) -> None:
        """Encode three characters with no padding needed."""
        assert b64_encode("Hel") == "SGVs"

    def test_five_chars(self) -> None:
        """Encode five characters (known vector: Hello -> SGVsbG8=)."""
        assert b64_encode("Hello") == "SGVsbG8="

    def test_hello_world(self) -> None:
        """Encode 'Hello, World!' to its known Base64 form."""
        assert b64_encode("Hello, World!") == "SGVsbG8sIFdvcmxkIQ=="

    def test_padding_alignment(self) -> None:
        """Encode 'ab' and verify single-pad output."""
        assert b64_encode("ab") == "YWI="

    def test_encodes_raw_bytes(self) -> None:
        """Encode raw binary bytes (e.g. a PNG header), not just text."""
        assert b64_encode(bytes([0x89, 0x50, 0x4E])) == "iVBO"

    def test_bytes_and_str_agree_for_ascii(self) -> None:
        """Encoding bytes matches encoding the equivalent text."""
        assert b64_encode(b"Hello") == b64_encode("Hello")


class TestB64Decode:
    """Test the b64_decode function."""

    def test_empty_string(self) -> None:
        """Decode an empty string to an empty string."""
        assert b64_decode("") == ""

    def test_single_char_padded(self) -> None:
        """Decode 'SA==' back to 'H'."""
        assert b64_decode("SA==") == "H"

    def test_two_chars_padded(self) -> None:
        """Decode 'SGU=' back to 'He'."""
        assert b64_decode("SGU=") == "He"

    def test_three_chars_no_padding(self) -> None:
        """Decode 'SGVs' back to 'Hel'."""
        assert b64_decode("SGVs") == "Hel"

    def test_five_chars(self) -> None:
        """Decode 'SGVsbG8=' back to 'Hello'."""
        assert b64_decode("SGVsbG8=") == "Hello"

    def test_invalid_character_raises(self) -> None:
        """Raise ValueError for characters outside the Base64 alphabet."""
        with pytest.raises(ValueError, match="Invalid Base64 character"):
            b64_decode("!!!!")


class TestB64RoundTrip:
    """Test that encoding then decoding returns the original input."""

    @pytest.mark.parametrize(
        "text",
        [
            "",
            "a",
            "ab",
            "abc",
            "Hello",
            "Hello, World!",
            "The quick brown fox jumps over the lazy dog",
        ],
    )
    def test_round_trip(self, text: str) -> None:
        """Encode then decode and recover the original text."""
        assert b64_decode(b64_encode(text)) == text
