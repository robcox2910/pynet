"""A parrot on a real WebSocket line -- every message comes straight back.

Run it, then connect from a browser (see docs/concepts/websocket-real.md
for a tiny page to paste) or from Python::

    uv run python examples/ws_echo.py

Press Ctrl-C to stop the server.
"""

from pynet.loop import run
from pynet.ws import WebSocket, serve_websocket

HOST = "127.0.0.1"
PORT = 8765


async def echo(ws: WebSocket) -> None:
    """Send every message back, text as text and bytes as bytes."""
    while True:
        message = await ws.recv()  # raises WebSocketClosed when they hang up
        if isinstance(message, str):
            await ws.send_text(message)
        else:
            await ws.send_bytes(message)


async def serve_forever() -> None:
    """Start the echo server and keep it running."""
    async with await serve_websocket(echo, HOST, PORT) as server:
        print(f"WebSocket echo server on ws://{HOST}:{server.port}/ (Ctrl-C to stop)")
        await server.wait_closed()


def main() -> None:
    """Run the echo server until Ctrl-C."""
    try:
        run(serve_forever())
    except KeyboardInterrupt:
        print("Bye!")


if __name__ == "__main__":
    main()
