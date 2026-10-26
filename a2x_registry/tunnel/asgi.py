"""Adapt a FastAPI WebSocket to the tunnel server's connection interface.

The registry HTTP server and the tunnel share one TCP port. A request whose
path is ``/tunnel`` is upgraded and then handled by the existing tunnel code.
"""

from __future__ import annotations

from typing import Any

from fastapi import WebSocket
from websockets.exceptions import ConnectionClosed
from websockets.frames import Close

from .deps import get_tunnel_server


class AsgiWebSocketAdapter:
    def __init__(self, websocket: WebSocket) -> None:
        client = websocket.client
        self.remote_address = (client.host, client.port) if client else None
        self._ws = websocket
        self._closed = False

    def __aiter__(self) -> "AsgiWebSocketAdapter":
        return self

    async def __anext__(self) -> str:
        if self._closed:
            raise StopAsyncIteration
        message = await self._ws.receive()
        kind = message.get("type")
        if kind == "websocket.disconnect":
            raise StopAsyncIteration
        text = message.get("text")
        if text is not None:
            return text
        data = message.get("bytes")
        if data is not None:
            return data.decode("utf-8")
        raise StopAsyncIteration

    async def recv(self) -> str:
        try:
            return await self.__anext__()
        except StopAsyncIteration as exc:
            raise ConnectionClosed(Close(1000, ""), None) from exc

    async def send(self, data: str | bytes) -> None:
        if isinstance(data, bytes):
            await self._ws.send_bytes(data)
        else:
            await self._ws.send_text(data)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self._closed:
            return
        self._closed = True
        await self._ws.close(code=code, reason=reason)


async def tunnel_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    server: Any = get_tunnel_server()
    if server is None:
        await websocket.close(code=1013, reason="tunnel disabled")
        return
    await server.handle_connection(AsgiWebSocketAdapter(websocket))
