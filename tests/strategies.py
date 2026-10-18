"""Strategies -- recipes for random messages and random ways to cut up bytes.

A strategy tells Hypothesis how to invent one kind of input. Each one here
is a named function, named for what it makes, with a one-line docstring.
``tests/test_properties.py`` and ``tests/test_model.py`` both use them.
"""

from itertools import pairwise

import pytest
from hypothesis import settings
from hypothesis import strategies as st

from pynet.messaging import Message, MessageType
from pynet.messaging.codec import NEEDS_ID, NEEDS_METHOD, JsonValue

# A message body is built from at most this many plain pieces. Hypothesis
# allows 100 by default, which makes each message slow to invent; small
# bodies find the same bugs, and a failure shrinks to a small body anyway.
MOST_PIECES_PER_BODY = 5
# A list or dict inside a body holds at most this many items. Unbounded
# ones make bodies slow to invent and find nothing new.
MOST_ITEMS_PER_BOX = 3
# At most this many attachments, of at most this many bytes each: enough to
# cut a message across many chunks without making every example huge.
MOST_ATTACHMENTS = 3
MOST_ATTACHMENT_BYTES = 40


# --- Time limits ----------------------------------------------------------------
# pytest stops any test after 10 seconds (see addopts in pyproject.toml), so a
# hang fails fast. A normal run of 100 examples must finish well inside that.
# The thorough run (the profile in conftest.py) tries 2,000, so the property
# tests and the model check get 10 minutes instead. Each test module sets
# ``pytestmark = PROPERTY_TIMEOUT``.
THOROUGH_PROFILE = "thorough"
NORMAL_RUN_TIMEOUT_SECONDS = 10
THOROUGH_RUN_TIMEOUT_SECONDS = 600
_is_thorough_run = settings.get_current_profile_name() == THOROUGH_PROFILE
PROPERTY_TIMEOUT = pytest.mark.timeout(
    THOROUGH_RUN_TIMEOUT_SECONDS if _is_thorough_run else NORMAL_RUN_TIMEOUT_SECONDS
)


def json_text() -> st.SearchStrategy[str]:
    """Make any text that can be written as UTF-8: emoji, accents, quotes, the lot."""
    # Lone "surrogates" (category Cs) are half-characters that UTF-8 can't
    # write at all; encode_message documents a ProtocolError for them.
    return st.text(alphabet=st.characters(exclude_categories=["Cs"]))


def json_leaves() -> st.SearchStrategy[JsonValue]:
    """Make one plain JSON value: null, true/false, a whole number, a decimal or text."""
    # NaN and infinity are not JSON; the codec documents that it refuses them.
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(),
        st.floats(allow_nan=False, allow_infinity=False),
        json_text(),
    )


def lists_and_dicts_of(children: st.SearchStrategy[JsonValue]) -> st.SearchStrategy[JsonValue]:
    """Make a list, or a dict with text keys, holding values made by ``children``."""
    return st.one_of(
        st.lists(children, max_size=MOST_ITEMS_PER_BOX),
        st.dictionaries(json_text(), children, max_size=MOST_ITEMS_PER_BOX),
    )


def json_bodies() -> st.SearchStrategy[JsonValue]:
    """Make any message body: a plain value, or lists and dicts nested inside each other."""
    return st.recursive(json_leaves(), lists_and_dicts_of, max_leaves=MOST_PIECES_PER_BODY)


def call_signs() -> st.SearchStrategy[int]:
    """Make a call sign (a message id): any whole number, negative ones too."""
    return st.integers()


def method_names() -> st.SearchStrategy[str]:
    """Make a method name: any text, even an empty one."""
    return json_text()


def attachments() -> st.SearchStrategy[tuple[bytes, ...]]:
    """Make a few raw binary attachments, empty ones included."""
    return st.lists(st.binary(max_size=MOST_ATTACHMENT_BYTES), max_size=MOST_ATTACHMENTS).map(tuple)


def messages_of_kind(kind: MessageType) -> st.SearchStrategy[Message]:
    """Make any valid message of one kind: a request needs an id and a method, and so on."""
    # The rules for which kinds need an id or a method are NEEDS_ID and
    # NEEDS_METHOD in pynet/messaging/codec.py; the decoder refuses the rest.
    return st.builds(
        Message,
        st.just(kind),
        call_signs() if kind in NEEDS_ID else st.none() | call_signs(),
        method_names() if kind in NEEDS_METHOD else st.none() | method_names(),
        json_bodies(),
        attachments(),
    )


def messages() -> st.SearchStrategy[Message]:
    """Make any valid message, of any kind."""
    return st.one_of([messages_of_kind(kind) for kind in MessageType])


@st.composite
def chunks_of(draw: st.DrawFn, data: bytes) -> list[bytes]:
    """Cut ``data`` into non-empty chunks at random places, like a network would."""
    inside = range(1, len(data))  # a cut after byte 1, 2, ... but not at either end
    cuts = draw(st.sets(st.sampled_from(inside))) if inside else set[int]()
    edges = [0, *sorted(cuts), len(data)]
    return [data[start:end] for start, end in pairwise(edges) if end > start]
