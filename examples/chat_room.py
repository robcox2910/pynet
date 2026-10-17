"""A walkie-talkie group channel -- one person talks and everyone hears it.

The server keeps a list of everyone in the room. A member *requests*
``say``; the server *pushes* the line to every member (the speaker too)
and answers with how many people heard it. When a member hangs up, its
``on_close`` callback takes it off the list.

Run the demo (two members, Alice and Bob, chat on a throwaway port)::

    uv run python examples/chat_room.py
"""

from pynet.loop import Event, run, wait_for
from pynet.messaging import JsonValue, Message, Peer, connect, serve

HOST = "127.0.0.1"
ANY_PORT = 0
DEMO_TIMEOUT = 5.0
SCRIPT = [("alice", "hello room"), ("bob", "hi alice")]


class ChatRoom:
    """Remember who is in the room and pass every line on to all of them."""

    def __init__(self) -> None:
        """Start with an empty room."""
        self.members: set[Peer] = set()

    def join(self, peer: Peer) -> None:
        """Set up a newly connected member (``serve`` calls this)."""
        self.members.add(peer)
        peer.on_close(lambda _cause: self.members.discard(peer))

        async def say(msg: Message) -> JsonValue:
            """Push the line to every member; answer with how many heard it."""
            for member in list(self.members):
                await member.push("said", msg.body)
            return {"heard_by": len(self.members)}

        peer.on("say", say)


class Member:
    """One person in the room, writing down every line they hear."""

    def __init__(self, name: str, expected: int) -> None:
        """Remember ``name`` and how many lines to wait for."""
        self.name = name
        self.heard: list[str] = []
        self._expected = expected
        self._done = Event()
        self.peer: Peer | None = None

    def setup(self, peer: Peer) -> None:
        """Listen for ``said`` pushes (``connect`` calls this)."""
        self.peer = peer

        async def said(msg: Message) -> None:
            """Write down one line from the room."""
            line = msg.body if isinstance(msg.body, dict) else {}
            self.heard.append(f"{self.name} hears {line.get('name')}: {line.get('text')}")
            if len(self.heard) >= self._expected:
                self._done.set()

        peer.on("said", said)

    async def say(self, text: str) -> None:
        """Ask the room to pass ``text`` on to everyone."""
        if self.peer is None:
            message = f"{self.name} has not connected yet"
            raise RuntimeError(message)
        await self.peer.request("say", {"name": self.name, "text": text})

    async def wait_until_heard_all(self) -> None:
        """Wait until every expected line has arrived."""
        await wait_for(self._done.wait(), DEMO_TIMEOUT)


async def demo() -> list[str]:
    """Run a two-person chat and return what each member heard."""
    room = ChatRoom()
    server = await serve(HOST, ANY_PORT, room.join, heartbeat_interval=None)
    members = {name: Member(name, len(SCRIPT)) for name in ("alice", "bob")}
    for member in members.values():
        await connect(HOST, server.port, setup=member.setup, heartbeat_interval=None)
    try:
        for name, text in SCRIPT:
            await members[name].say(text)
        for member in members.values():
            await member.wait_until_heard_all()
    finally:
        for member in members.values():
            if member.peer is not None:
                await member.peer.close()
        server.close()
        await server.wait_closed()
    return [line for member in members.values() for line in member.heard]


def main() -> None:
    """Run the demo and print everything that was heard."""
    for line in run(demo()):
        print(line)


if __name__ == "__main__":
    main()
