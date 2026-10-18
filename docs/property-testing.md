# Property Testing (However the Bytes Are Cut)

## The Scissor-Happy Postman

Imagine you've built a new postal service. How do you check it works?

The usual way: post one letter, wait for it to arrive, open it and smile.
That's an **example test**. It's useful, but it only tells you about the
one letter you thought of.

Now imagine a scissor-happy postman. He posts thousands of letters --
short ones, long ones, empty envelopes, letters full of emoji -- and before
each delivery he snips the whole sack of post into random strips. One strip
might hold half an envelope; the next, three whole letters and a corner of
a fourth. Then he checks a single rule:

> However the post is cut up, the reader gets back exactly the letters
> that were sent, in order, with nothing extra.

That rule is a **property**. It doesn't say what any one letter looks like.
It says something that must be true for *every* letter, cut *every* way.

That is exactly what a network does to your bytes. TCP is a river with no
gaps, and nothing promises that one `recv` brings one message (see
[Framing](concepts/framing.md)). The postman just does it on purpose.

When the postman finds a sack that breaks the rule, he doesn't hand you a
sack of 300 strips and walk off. He keeps throwing letters and strips
away, checking it still goes wrong each time, until what's left is as
small as it can be. That is called **shrinking**, and it's the best part:
the smallest failing input usually points straight at the bug.

In Python, the postman is a library called
[Hypothesis](https://hypothesis.readthedocs.io/). PyNet's property tests
live in `tests/test_properties.py`, and a step-by-step check of a whole
conversation lives in `tests/test_model.py`. No test opens a socket, starts
a thread or sleeps: the bytes go through a pretend pipe
(`tests/pretend_pipe.py`) that hands the reader exactly the strips the test
chose, one strip per read.

## A Property from PyNet

Here's the postman's own rule, for [framing](concepts/framing.md), with
its two strategies:

```python
def payloads() -> st.SearchStrategy[list[bytes]]:
    """Make a list of message payloads: any bytes, empty ones included."""
    return st.lists(st.binary())


@st.composite
def chunks_of(draw: st.DrawFn, data: bytes) -> list[bytes]:
    """Cut ``data`` into non-empty chunks at random places, like a network would."""
    inside = range(1, len(data))  # a cut after byte 1, 2, ... but not at either end
    cuts = draw(st.sets(st.sampled_from(inside))) if inside else set[int]()
    edges = [0, *sorted(cuts), len(data)]
    return [data[start:end] for start, end in pairwise(edges) if end > start]


@given(sent=payloads(), data=st.data())
def test_frames_survive_any_chunking(sent: list[bytes], data: st.DataObject) -> None:
    """However the bytes are cut up, the receiver gets exactly the frames sent, then a clean close."""
    wire = b"".join(encode_frame(payload) for payload in sent)
    inbox = FrameInbox(max_frame_size=MAX_FRAME_SIZE)
    for chunk in data.draw(chunks_of(wire)):
        inbox.pipe.hand_over(chunk)

    received, goodbye = read_until_hang_up(inbox)

    assert received == sent
    assert goodbye == PEER_CLOSED_MESSAGE
```

Line by line:

- **`payloads()`** is a **strategy**: a recipe for random inputs. It makes
  a list of byte strings: none at all, one empty one, ten long ones full of
  zero bytes, whatever it likes.
- **`chunks_of(data)`** is the postman's scissors. It picks a random set of
  places to cut, sorts them, and slices `data` at each one. `@st.composite`
  lets a strategy `draw` from other strategies, so it can choose cuts that
  fit *these* bytes.
- **`@given(sent=payloads(), data=st.data())`** runs the test many times
  (100 in a normal run). `st.data()` lets the test draw more inside itself,
  which we need because the cuts depend on how long `wire` turned out.
- **`wire`** is every frame glued together, exactly as the sender would
  write them, back to back.
- **`hand_over(chunk)`** queues one strip in the pretend pipe. The real
  `FramedConnection` reads from it, one strip per read.
- **`read_until_hang_up`** hangs up the pipe and collects every frame,
  plus the message of the `ConnectionClosedError` that ends the stream.
- **The two `assert`s** are the rule: the same frames, in the same order,
  and then a clean `"peer closed"`, not `"peer closed mid-frame"`. Nothing
  missing, nothing extra.

Notice what the test *doesn't* say. It never says "`hi` becomes
`00 00 00 02 h i`". It doesn't need to know any answers, only a rule
that's always true.

Some properties are easy to cheat. **A Base64 that does nothing at all --
hand the bytes back unchanged -- passes `test_base64_round_trip`**, so
`test_base64_encode_matches_the_standard_library` checks the real answer
too. PyNet has eleven more properties:

| Rule | Test |
|---|---|
| Encoding any bytes gives exactly what Python's own `base64` gives | `test_base64_encode_matches_the_standard_library` |
| Any text decodes to what `base64` gives, and is refused exactly when it refuses | `test_base64_decode_matches_the_standard_library` |
| Decoding what we encoded gives back the very same bytes | `test_base64_round_trip` |
| However the bytes are cut, a WebSocket `FrameReader` gets exactly the frames sent, unmasked | `test_websocket_frames_survive_any_chunking` |
| Every frame `encode_message` makes, fed to one decoder, gives back the same messages | `test_messages_survive_the_codec` |
| Any bytes parse into an HTTP `Response` or raise `HTTPError`, never anything else | `test_http_parser_only_raises_http_error` |
| Any bytes parse into an HTTP head or raise `HandshakeError` | `test_handshake_parser_only_raises_handshake_error` |
| Reading WebSocket frames from any bytes ends only in the reader's own three errors | `test_websocket_reader_only_raises_its_errors` |
| Any frames fed to a `MessageDecoder` give messages, `None` or `ProtocolError` | `test_message_decoder_only_raises_protocol_error` |
| Any text parses into a `URL` or raises `ValueError` | `test_url_parser_only_raises_value_error` |
| `is_valid_ipv4` agrees with Python's own `ipaddress` | `test_ipv4_check_agrees_with_ipaddress` |

The `base64` and `ipaddress` ones are **reference checks**: we compare with
something we already trust, written a completely different way. The "only
raises" ones feed a parser junk and let through only the error its
docstring promises. They have no `assert` at all: any other exception, or a
hang stopped by the 10-second timeout, fails the test by itself.

## Checking Against a Model

One letter at a time is fine, but a real conversation is a story: a
request goes out, 7 bytes arrive, a push goes out, 40 more bytes arrive.
So the postman keeps a **notebook**: a plain Python list of every message
he has sent. That list is the **model**. After every step of a random
story, the real receiver must have got exactly the start of the notebook.
Here is part of `tests/test_model.py`:

```python
class ConversationMachine(RuleBasedStateMachine):
    """Send random messages, let the bytes through in random bites, and compare with a list."""

    calls = Bundle("calls")

    def __init__(self) -> None:
        """Start with an empty pipe, a fresh receiver and an empty list."""
        super().__init__()
        self.inbox = FrameInbox(max_frame_size=MAX_FRAME_SIZE)
        self.decoder = MessageDecoder()
        self.waiting = bytearray()  # sent, but not yet let through the pipe
        self.received: list[Message] = []
        self.model: list[Message] = []

    @rule(target=calls, request=messages_of_kind(MessageType.REQUEST))
    def send_request(self, request: Message) -> int:
        """Send a request, and remember its call sign for a later reply."""
        self.send(request)
        assert request.id is not None  # messages_of_kind gives every request an id
        return request.id

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


TestConversationModel = cast("type[unittest.TestCase]", ConversationMachine.TestCase)
```

The real file has rules to send a reply to an earlier call sign (or a
brand-new one) and to send any message at all, plus a `teardown` that
lets the last bytes through, hangs up, and checks the whole notebook
arrived and the line closed cleanly.

Piece by piece:

- **`RuleBasedStateMachine`** is a test that plays a story instead of
  checking one input. Hypothesis picks which step comes next.
- **`@rule`** marks a step the story may take. Its arguments come from
  strategies, like `delivery_sizes()`, which picks 1 to 64 bytes.
- **`Bundle("calls")`** is a pocket for things made earlier.
  `target=calls` puts each request's call sign in it, so a later reply can
  answer a call that really happened.
- **`@invariant()`** runs after *every* step: the receiver may be behind,
  but never wrong and never ahead.
- **`.TestCase`** turns the machine into a test class pytest can run. One
  run plays 100 random stories of up to 10 steps.

The `repr` in each `assert` is on purpose: `1 == 1.0` and `True == 1` in
Python, but a body that went in as `1` must not come back as `1.0`.

The notebook is a list on purpose, not a copy of the framing or codec
code. If the model had the same bug as the code, they would agree with each
other and the bug would never show. A list is too simple to be wrong.

## Try It Yourself

Let's break the message codec on purpose and watch the postman find it.

Open `src/pynet/messaging/codec.py` and change one line in
`encode_message`, as if you'd forgotten that zero is a perfectly good call
sign:

```python
    if msg.id:  # was: if msg.id is not None:
```

Run the property tests and the model check:

```bash
uv run pytest tests/test_properties.py tests/test_model.py --no-cov
```

Hypothesis reports this (trimmed to the interesting part):

```
    @given(sent=st.lists(messages()))
>   def test_messages_survive_the_codec(sent: list[Message]) -> None:

E           pynet.errors.ProtocolError: request requires an id
E           Failing test case: test_messages_survive_the_codec(
E               # The test always failed when commented parts were varied together.
E               sent=[Message(
E                    <MessageType.REQUEST: 'request'>,
E                    0,
E                    '',  # or any other generated value
E                    None,  # or any other generated value
E                    (),  # or any other generated value
E                )],
E           )
```

Look at how small that is. Out of lists of messages with nested bodies,
emoji and attachments, Hypothesis shrank it to **one request with call
sign `0`**, and the comments say the method, body and attachments don't
matter. In Python, `if 0:` counts as false, just like `if None:`, so the
id was never written into the envelope, and the decoder refused a request
with no id.

`test_messages_survive_the_codec` caught it, and the model check failed too
(its story was a reply to call sign `0`). Every other property stayed green.
The example tests all still pass: they use call signs like 42. Only the
property notices.

Now put the line back to `if msg.id is not None:` and run the tests again.
They pass.

!!! tip "Older Hypothesis versions"
    Older versions of Hypothesis print `Falsifying example:` instead of
    `Failing test case:`. It means the same thing.

## A Bug We Found

The first run of `test_ipv4_check_agrees_with_ipaddress` failed on PyNet's
real code, with no deliberate bug:

```
    | AssertionError: assert True == False
    |  +  where True = is_valid_ipv4('0.0.0.٠')
    |  +  and   False = ipaddress_says_valid('0.0.0.٠')
    | Failing test case: test_ipv4_check_agrees_with_ipaddress(
    |     text='0.0.0.٠',
    | )
```

That last piece isn't a zero. It's `٠`, the Arabic-Indic digit zero. Our
check said `0.0.0.٠` was a fine IPv4 address; Python's `ipaddress` said no.
The same run found a second failure: `0.0.0.²` (a superscript two) crashed
with `ValueError` instead of returning `False`.

The old code asked `part.isdigit()`, which says yes to digits from every
alphabet and to symbols like `²`. Then `int("٠")` happily gave 0, while
`int("²")` blew up. The example tests used addresses like `192.168.1.1`.
Nobody thought to try another alphabet's digits; the postman's strategy
mixes a few in on purpose.

The fix accepts only the ten digits 0-9:

```python
    if not (part.isascii() and part.isdigit()) or (len(part) > 1 and part[0] == "0"):
    # was: if not part.isdigit() or (len(part) > 1 and part[0] == "0"):
```

Both inputs are now example tests in `tests/test_dns.py`:

```python
    def test_digits_from_other_alphabets_are_not_valid(self) -> None:
        """Regression: an Arabic-Indic zero passed as an octet, found by a property test."""
        assert is_valid_ipv4("0.0.0.\u0660") is False
```

`"\u0660"` is Python's way of writing `٠` with only plain keyboard
characters: `\u` means "the character with this Unicode number", and 0660
is the Arabic-Indic zero. That keeps the test readable in any editor, and
nobody can mistake it for a dot or a real 0.

The long run of `test_http_parser_only_raises_http_error` found one more:
a chunked body whose chunk size was `-6` sent the parser round in circles
forever, until the timeout stopped it. A hang can't be shrunk, so we cut
that input down by hand. The cause was Python's `int()`, which is kinder
than HTTP: it reads `-6`, `+3`, `1_0` and even `0x3`. Status codes, sizes
and chunk sizes are now read as plain digits only; anything else is an
`HTTPError`.

## Run It Yourself

Run just the property tests, and the model check (100 examples each, the same every time):

```bash
uv run pytest tests/test_properties.py tests/test_model.py --no-cov
```

The `--no-cov` turns off the coverage check, which expects the *whole* test suite to run
and would fail on two files.

Run them the long way (2,000 random examples each, different every run) to hunt for rarer
bugs. It takes a few minutes:

```bash
uv run pytest tests/test_properties.py tests/test_model.py --no-cov --hypothesis-profile=thorough
```

If the model check ever fails in the normal run, you may see a **Timeout**
instead of a short story. Hypothesis shrinks a failing story by replaying it many
times, and that can use up the 10-second limit before it finishes. Run it again with
`--hypothesis-profile=thorough`: its limit is 600 seconds, so shrinking has time to
finish and the report shows the shrunk story.

The normal run uses a fixed seed, so it tries the same inputs every time and a passing
build means the same thing on every computer. The thorough run is random, so each time you
run it the postman brings brand-new letters and cuts them in brand-new places. Both
settings live in `conftest.py` at the top of the project.

## Your Turn

Can you write your own property? Try [WebSocket](concepts/websocket-real.md)
masking: "masking any bytes twice with the same 4-byte key gives them back"
(`apply_mask` in `pynet.ws`). What's the strategy? What's the `assert`?
Then break `apply_mask` on purpose, say by using only the first three
bytes of the key, and see what Hypothesis shrinks it to. Does your
round-trip notice? What second property would?
