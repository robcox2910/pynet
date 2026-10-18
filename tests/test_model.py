"""Model check -- a conversation must arrive exactly like a plain list of what was sent.

A property test checks one rule for many inputs. A *model check* goes
further: Hypothesis invents a random *story* -- send a request, send a
reply, let 7 bytes through, send a push, let 40 bytes through -- and plays
it on two things at once:

- the real receiving end: a ``FramedConnection`` reading from a pretend
  pipe, feeding every frame to a ``MessageDecoder``;
- a **model**: a plain ``list`` of the messages sent, which we already
  trust to be right.

After every step the receiver must have got exactly the start of that
list, in order, and nothing else. At the end of every story the rest of
the bytes go through and the line hangs up: then the receiver must have
the whole list, and must say the line closed cleanly, between frames. If
they ever disagree, Hypothesis shrinks the story to the shortest list of
steps that still goes wrong.

The model is deliberately *not* a copy of the framing or codec code. It is
the simplest thing that is obviously correct, so when the two disagree,
the receiver is the one to suspect.

The bytes travel through a ``PretendPipe`` (``tests/pretend_pipe.py``):
no sockets, no threads, no sleeping, and the story decides exactly where
the byte river is cut.

See ``docs/property-testing.md`` for the walk-through.
"""

import unittest
from typing import cast

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, rule

from pynet.framing import MAX_FRAME_SIZE, PEER_CLOSED_MESSAGE, encode_frame
from pynet.messaging import Message, MessageDecoder, MessageType, encode_message
from pynet.messaging.codec import JsonValue
from tests.pretend_pipe import FrameInbox
from tests.strategies import PROPERTY_TIMEOUT, call_signs, json_bodies, messages, messages_of_kind

pytestmark = PROPERTY_TIMEOUT  # see tests/strategies.py

# One delivery lets through at most this many bytes. Small bites cut
# headers and envelopes in half far more often than big ones.
MOST_BYTES_PER_DELIVERY = 64


def delivery_sizes() -> st.SearchStrategy[int]:
    """Make how many waiting bytes one delivery lets through: at least one."""
    return st.integers(min_value=1, max_value=MOST_BYTES_PER_DELIVERY)


# --- The machine: one story, played on the receiver and on a list ------------


class ConversationMachine(RuleBasedStateMachine):
    """Send random messages, let the bytes through in random bites, and compare with a list."""

    # Call signs of requests we've sent. A reply answers one of these,
    # or (to be awkward) a call sign nobody ever used.
    calls = Bundle("calls")

    def __init__(self) -> None:
        """Start with an empty pipe, a fresh receiver and an empty list."""
        super().__init__()
        self.inbox = FrameInbox(max_frame_size=MAX_FRAME_SIZE)
        self.decoder = MessageDecoder()
        self.waiting = bytearray()  # sent, but not yet let through the pipe
        self.received: list[Message] = []
        self.model: list[Message] = []

    def send(self, message: Message) -> None:
        """Frame and queue every part of ``message``, and write it in the list."""
        for payload in encode_message(message):
            self.waiting += encode_frame(payload)
        self.model.append(message)

    def receive(self) -> list[Message]:
        """Decode every frame the pipe has completed; return the messages they finish."""
        finished: list[Message] = []
        for frame in self.inbox.frames():
            message = self.decoder.feed(frame)
            if message is not None:
                finished.append(message)
        self.received += finished
        return finished

    @rule(target=calls, request=messages_of_kind(MessageType.REQUEST))
    def send_request(self, request: Message) -> int:
        """Send a request, and remember its call sign for a later reply."""
        self.send(request)
        assert request.id is not None  # messages_of_kind gives every request an id
        return request.id

    @rule(call=st.one_of(calls, call_signs()), body=json_bodies())
    def send_reply(self, call: int, body: JsonValue) -> None:
        """Send a reply to a call sign we used before, or to a brand-new one."""
        self.send(Message(MessageType.RESPONSE, call, None, body))

    @rule(message=messages())
    def send_anything(self, message: Message) -> None:
        """Send any valid message at all: a push, a ping, an error, with attachments."""
        self.send(message)

    @rule(size=delivery_sizes())
    def deliver_some_bytes(self, size: int) -> None:
        """Let ``size`` waiting bytes through; any messages they finish are the next ones sent."""
        chunk = bytes(self.waiting[:size])
        del self.waiting[:size]
        if chunk:
            self.inbox.pipe.hand_over(chunk)
        already = len(self.received)
        finished = self.receive()
        assert repr(finished) == repr(self.model[already : already + len(finished)])

    @invariant()
    def received_the_start_of_the_list(self) -> None:
        """After every step, the receiver has exactly the first messages sent, in order."""
        assert repr(self.received) == repr(self.model[: len(self.received)])
        assert self.inbox.goodbye is None

    def teardown(self) -> None:
        """At the end of the story, let the rest through and hang up: everything arrived, cleanly."""
        if self.waiting:
            self.inbox.pipe.hand_over(bytes(self.waiting))
        self.inbox.pipe.hang_up()
        self.receive()
        assert repr(self.received) == repr(self.model)
        assert self.inbox.goodbye == PEER_CLOSED_MESSAGE


# why: 10 steps find the same bugs (every deliberate bug shrank to 4 steps or
# fewer), and each step invents a whole message; 50 (the default) took most
# of the 10-second limit
ConversationMachine.TestCase.settings = settings(stateful_step_count=10)
# pytest runs this like any other test class: one run plays 100 random stories.
# (Hypothesis doesn't say what type ``TestCase`` is, so ``cast`` tells pyright.)
TestConversationModel = cast("type[unittest.TestCase]", ConversationMachine.TestCase)
