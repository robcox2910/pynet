"""Encoding photos for email -- binary to text.

Implement Base64 encoding and decoding from scratch. Base64 converts
arbitrary binary data into a safe ASCII string using 64 printable
characters. Think of it as translating a photograph into a sequence
of letters so you can paste it into an email.

The algorithm groups every 3 bytes (24 bits) into four 6-bit chunks,
then maps each chunk to a character from the Base64 alphabet. If the
input length is not a multiple of 3, the output is padded with '='.
"""

from __future__ import annotations

_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_PAD = "="

BITS_PER_BYTE = 8
BITS_PER_SEXTET = 6
SEXTET_MASK = 0x3F
INPUT_GROUP_SIZE = 3
OUTPUT_GROUP_SIZE = 4


def b64_encode(data: str) -> str:
    """Encode a text string to Base64.

    Convert each group of 3 bytes into 4 Base64 characters, padding
    with '=' when the input length is not a multiple of 3.

    Args:
        data: The plain-text string to encode.

    Returns:
        The Base64-encoded string.

    """
    raw = data.encode("utf-8")
    result: list[str] = []

    for i in range(0, len(raw), INPUT_GROUP_SIZE):
        group = raw[i : i + INPUT_GROUP_SIZE]
        # Combine bytes into a single integer.
        combined = 0
        for byte in group:
            combined = (combined << BITS_PER_BYTE) | byte

        # Shift left to fill a full 24-bit block if group is short.
        padding_bytes = INPUT_GROUP_SIZE - len(group)
        combined <<= padding_bytes * BITS_PER_BYTE

        # Extract four 6-bit sextets from the 24-bit block.
        sextets: list[int] = []
        for _ in range(OUTPUT_GROUP_SIZE):
            sextets.append(combined & SEXTET_MASK)
            combined >>= BITS_PER_SEXTET
        sextets.reverse()

        # Map sextets to characters, replacing trailing ones with padding.
        chars_to_write = OUTPUT_GROUP_SIZE - padding_bytes
        for j, sextet in enumerate(sextets):
            if j < chars_to_write:
                result.append(_ALPHABET[sextet])
            else:
                result.append(_PAD)

    return "".join(result)


def b64_decode(encoded: str) -> str:
    """Decode a Base64 string back to plain text.

    Reverse the encoding process: map each Base64 character back to
    its 6-bit value, reassemble into bytes, and strip padding.

    Args:
        encoded: The Base64-encoded string.

    Returns:
        The decoded plain-text string.

    Raises:
        ValueError: If the input contains invalid Base64 characters.

    """
    if not encoded:
        return ""

    # Build a reverse lookup table.
    decode_map: dict[str, int] = {char: idx for idx, char in enumerate(_ALPHABET)}

    # Process groups of 4 characters at a time, just like encode does 3 bytes.
    result_bytes: list[int] = []

    for i in range(0, len(encoded), OUTPUT_GROUP_SIZE):
        group = encoded[i : i + OUTPUT_GROUP_SIZE]

        # Count padding in this group.
        pad_count = group.count(_PAD)

        # Look up 6-bit values (treat padding as 0).
        values: list[int] = []
        for char in group:
            if char == _PAD:
                values.append(0)
            elif char in decode_map:
                values.append(decode_map[char])
            else:
                msg = f"Invalid Base64 character: {char!r}"
                raise ValueError(msg)

        # Combine four 6-bit values into a 24-bit number.
        combined = 0
        for val in values:
            combined = (combined << BITS_PER_SEXTET) | val

        # Extract 3 bytes from the 24-bit number.
        bytes_to_extract = INPUT_GROUP_SIZE - pad_count
        extracted: list[int] = []
        for _ in range(INPUT_GROUP_SIZE):
            extracted.append(combined & 0xFF)
            combined >>= BITS_PER_BYTE
        extracted.reverse()

        result_bytes.extend(extracted[:bytes_to_extract])

    return bytes(result_bytes).decode("utf-8")
