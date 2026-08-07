"""WebSocket tunnel lifecycle and status access."""

from __future__ import annotations

from typing import Any

from .config import TunnelConfig
from .server import WebSocketTunnelServer


_server: WebSocketTunnelServer | None = None


def _registry_getter():
    from a2x_registry.backend.routers.dataset import get_registry_service

    return get_registry_service()


async def startup_tunnel() -> None:
    global _server
    config = TunnelConfig.from_env()
    if not config.enabled:
        _server = None
        return
    if _server is not None:
        await _server.stop()
    _server = WebSocketTunnelServer(config, _registry_getter)
    await _server.start()


async def shutdown_tunnel() -> None:
    global _server
    server, _server = _server, None
    if server is not None:
        await server.stop()


def tunnel_status() -> dict[str, Any]:
    if _server is not None:
        return _server.status()
    config = TunnelConfig.from_env()
    return {
        "enabled": config.enabled,
        "host": config.host,
        "port": config.port,
        "connected_devices": 0,
        "bound_services": 0,
        "pending_requests": 0,
    }


def get_tunnel_server() -> WebSocketTunnelServer | None:
    return _server
