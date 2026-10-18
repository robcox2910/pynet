"""Plain digits -- the one place that says what counts as a digit.

Python's ``str.isdigit`` and ``int()`` are kinder than internet protocols:
they accept ``"²"``, Arabic-Indic digits like ``"٣"``, and ``int()`` also
reads ``"-5"``, ``"+3"``, ``"1_0"`` and ``" 7 "``. Ports, status codes,
sizes and IPv4 pieces are only ever the ASCII digits, so the HTTP parser,
the URL parser and the DNS check all ask this module instead.
"""

DECIMAL_DIGITS = frozenset("0123456789")
HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


def is_plain_digits(text: str, *, hexadecimal: bool = False) -> bool:
    """Say whether ``text`` is one or more plain digits (0-9, or 0-9 and a-f if ``hexadecimal``)."""
    return bool(text) and set(text) <= (HEX_DIGITS if hexadecimal else DECIMAL_DIGITS)
