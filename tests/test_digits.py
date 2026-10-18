"""Plain digits -- the shared helper refuses everything but 0-9 (and a-f for hex)."""

import pytest

from pynet._digits import is_plain_digits


@pytest.mark.parametrize("text", ["0", "7", "0123456789", "007"])
def test_ascii_digits_are_plain(text: str) -> None:
    """Runs of 0-9 are plain digits."""
    assert is_plain_digits(text)


@pytest.mark.parametrize("text", ["", "-5", "+3", "1_0", " 7 ", "²", "٣", "0x3", "ff"])
def test_everything_else_is_not_plain_decimal(text: str) -> None:
    """Empty text, signs, spaces, underscores, superscripts and other alphabets' digits are refused."""
    assert not is_plain_digits(text)


@pytest.mark.parametrize("text", ["ff", "0A", "1f3"])
def test_hexadecimal_allows_a_to_f(text: str) -> None:
    """With ``hexadecimal`` the letters a-f (either case) join the digits."""
    assert is_plain_digits(text, hexadecimal=True)


@pytest.mark.parametrize("text", ["", "0x3", "g", "-1"])
def test_hexadecimal_still_refuses_the_rest(text: str) -> None:
    """Hex mode still refuses empty text, a ``0x`` prefix, other letters and signs."""
    assert not is_plain_digits(text, hexadecimal=True)
