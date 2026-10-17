"""A nurse checking a pulse -- "are you still there?" every few beats.

The client ``Peer`` pings the server every ``INTERVAL`` seconds. The
server here is hand-made so you can watch it: it prints each ping and
answers with a pong. After a few pings it *freezes* -- the socket stays
open, as if the machine hung or a cable was pulled somewhere along the
way, so the client never sees a goodbye. Only the missing pongs give it
away: once more than ``MAX_MISSED`` pings in a row go unanswered, the
client declares the server dead with ``HeartbeatTimeoutError``.

Run the demo (it takes well under a second)::

    uv run python examples/heartbeat_demo.py
"""

from pynet.errors import ConnectionClosedError
from pynet.framing import FramedConnection
from pynet.loop import Event, get_running_loop, run, wait_for
from pynet.messaging import Message, MessageDecoder, MessageType, Peer, connect, encode_message
from pynet.streams import StreamReader, StreamWriter, start_server

HOST = "127.0.0.1"
ANY_PORT = 0
INTERVAL = 0.1
MAX_MISSED = 2
PINGS_BEFORE_FREEZE = 3
DEMO_TIMEOUT = 5.0


async def answer_then_freeze(conn: FramedConnection, finished: Event) -> None:
    """Answer the first few pings, then go silent until the demo ends."""
    decoder = MessageDecoder()
    answered = 0
    while answered < PINGS_BEFORE_FREEZE:
        msg = decoder.feed(await conn.recv_frame())
        if msg is not None and msg.type is MessageType.PING:
            print(f"server: ping {msg.body} -> pong")
            for frame in encode_message(Message(MessageType.PONG, msg.id, body=msg.body)):
                await conn.send_frame(frame)
            answered += 1
    print("server: *freezes* (socket still open, nobody answering)")
    await finished.wait()


async def demo() -> None:
    """Connect, watch the pings, then watch the freeze being noticed."""
    finished = Event()

    async def handle(reader: StreamReader, writer: StreamWriter) -> None:
        """Run the hand-made server for one connection."""
        conn = FramedConnection(reader, writer)
        try:
            await answer_then_freeze(conn, finished)
        except ConnectionClosedError:
            pass
        finally:
            conn.abort()

    def watch(peer: Peer) -> None:
        """Print how and when the client's connection ends."""
        began = get_running_loop().time()

        def closed(cause: BaseException | None) -> None:
            """Report the close cause and how long the connection lived."""
            alive = get_running_loop().time() - began
            print(f"client: declared dead after {alive:.1f}s: {type(cause).__name__}")

        peer.on_close(closed)

    server = await start_server(handle, HOST, ANY_PORT)
    client = await connect(
        HOST, server.port, setup=watch, heartbeat_interval=INTERVAL, max_missed_pongs=MAX_MISSED
    )
    try:
        await wait_for(client.wait_closed(), DEMO_TIMEOUT)
    finally:
        finished.set()
        await client.close()
        server.close()
        await server.wait_closed()


def main() -> None:
    """Run the heartbeat demo."""
    run(demo())


if __name__ == "__main__":
    main()
