"""Encoding photos for email -- binary to text.

Implement Base64 encoding and decoding from scratch. Base64 converts
arbitrary binary data into a safe ASCII string using 64 printable
characters. Think of it as translating a photograph into a sequence
of letters so you can paste it into an email.

The algorithm groups every 3 bytes (24 bits) into four 6-bit chunks,
then maps each chunk to a character from the Base64 alphabet. If the
input length is not a multiple of 3, the output is padded with '='.

``b64_encode`` accepts raw ``bytes`` (like the contents of a photo)
*or* a text string for convenience. ``b64_decode`` in this from-scratch
version turns Base64 back into a text string, while ``b64_decode_bytes``
recovers the raw bytes -- the photo comes back exactly as it left.
"""

_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_PAD = "="

BITS_PER_BYTE = 8
BITS_PER_SEXTET = 6
SEXTET_MASK = 0x3F
INPUT_GROUP_SIZE = 3
OUTPUT_GROUP_SIZE = 4
MAX_PADDING = 2
BYTE_MASK = 0xFF


def b64_encode(data: bytes | str) -> str:
    """Encode raw bytes (or text) to Base64.

    Convert each group of 3 bytes into 4 Base64 characters, padding
    with '=' when the input length is not a multiple of 3. Pass real
    binary like a photo's bytes, or a string (encoded as UTF-8 for you).

    Args:
        data: The bytes to encode, or a text string.

    Returns:
        The Base64-encoded string.

    """
    raw = data.encode("utf-8") if isinstance(data, str) else data
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


def b64_decode_bytes(encoded: str) -> bytes:
    """Decode a Base64 string back to the raw bytes it came from.

    Unlike ``b64_decode`` the result is never forced through UTF-8, so
    binary data (a photo, a random key) survives the round trip. The
    input must be strictly valid: a length that is a multiple of 4,
    alphabet characters only, and at most two ``=`` signs, at the end.

    Args:
        encoded: The Base64-encoded string.

    Returns:
        The decoded bytes.

    Raises:
        ValueError: If the input has invalid characters, length or padding.

    """
    if not encoded:
        return b""
    if len(encoded) % OUTPUT_GROUP_SIZE:
        msg = f"Invalid Base64 length: {len(encoded)} is not a multiple of 4"
        raise ValueError(msg)

    body = encoded.rstrip(_PAD)
    pad_count = len(encoded) - len(body)
    if pad_count > MAX_PADDING:
        msg = f"Invalid Base64 padding: {pad_count} '=' signs"
        raise ValueError(msg)

    decode_map: dict[str, int] = {char: idx for idx, char in enumerate(_ALPHABET)}
    result = bytearray()
    bit_buffer = 0
    bit_count = 0
    for char in body:
        if char not in decode_map:
            msg = f"Invalid Base64 character: {char!r}"
            raise ValueError(msg)
        bit_buffer = (bit_buffer << BITS_PER_SEXTET) | decode_map[char]
        bit_count += BITS_PER_SEXTET
        if bit_count >= BITS_PER_BYTE:
            bit_count -= BITS_PER_BYTE
            result.append((bit_buffer >> bit_count) & BYTE_MASK)
            bit_buffer &= (1 << bit_count) - 1
    return bytes(result)


def b64_decode(encoded: str) -> str:
    """Decode a Base64 string back to plain text.

    Decode with ``b64_decode_bytes`` and read the bytes as UTF-8.

    Args:
        encoded: The Base64-encoded string.

    Returns:
        The decoded plain-text string.

    Raises:
        ValueError: If the input is not valid Base64 or not valid UTF-8.

    """
    return b64_decode_bytes(encoded).decode("utf-8")
