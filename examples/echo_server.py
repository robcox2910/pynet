"""A parrot on the phone line -- whatever you say, it says straight back.

Run it, then talk to it from another terminal::

    uv run python examples/echo_server.py
    nc 127.0.0.1 8888

Press Ctrl-C to stop the server.
"""

from pynet.loop import run, wait_for
from pynet.streams import StreamReader, StreamWriter, open_connection, start_server

HOST = "127.0.0.1"
PORT = 8888
ANY_PORT = 0
SELF_TEST_MESSAGE = b"hello\n"
SELF_TEST_TIMEOUT = 5.0
READ_SIZE = 1024


async def echo(reader: StreamReader, writer: StreamWriter) -> None:
    """Send every line back until the client hangs up."""
    while not reader.at_eof():
        data = await reader.read(READ_SIZE)
        if not data:
            break
        writer.write(data)
        await writer.drain()


async def serve_forever() -> None:
    """Start the echo server and keep it running."""
    async with await start_server(echo, HOST, PORT) as server:
        print(f"Echo server listening on {HOST}:{server.port} (Ctrl-C to stop)")
        await server.wait_closed()


async def round_trip() -> bytes:
    """Start a server on a free port, send one line, return the echo."""
    async with await start_server(echo, HOST, ANY_PORT) as server:
        reader, writer = await open_connection(HOST, server.port)
        writer.write(SELF_TEST_MESSAGE)
        await writer.drain()
        reply = await wait_for(reader.read_until(), SELF_TEST_TIMEOUT)
        writer.close()
        await writer.wait_closed()
        return reply


def self_test() -> bytes:
    """Round-trip one message through a throwaway server and return the reply."""
    return run(round_trip())


def main() -> None:
    """Run the echo server until Ctrl-C."""
    try:
        run(serve_forever())
    except KeyboardInterrupt:
        print("Bye!")


if __name__ == "__main__":
    main()
