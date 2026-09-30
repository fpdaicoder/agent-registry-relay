"""Reverse TCP tunnel lifecycle and status access."""

from __future__ import annotations

from typing import Any

from .config import TcpTunnelConfig
from .server import TcpTunnelServer


_server: TcpTunnelServer | None = None


def _registry_getter():
    from a2x_registry.backend.routers.dataset import get_registry_service

    return get_registry_service()


async def startup_tcp_tunnel() -> None:
    global _server
    config = TcpTunnelConfig.from_env()
    if not config.enabled:
        _server = None
        return
    if _server is not None:
        await _server.stop()
    _server = TcpTunnelServer(config, _registry_getter)
    await _server.start()


async def shutdown_tcp_tunnel() -> None:
    global _server
    server, _server = _server, None
    if server is not None:
        await server.stop()


def tcp_tunnel_status() -> dict[str, Any]:
    if _server is not None:
        return _server.status()
    config = TcpTunnelConfig.from_env()
    return {
        "enabled": config.enabled,
        "host": config.host,
        "port": config.port,
        "connected_devices": 0,
        "bound_targets": 0,
        "bound_services": 0,
        "active_bridges": 0,
        "port_range": [config.port_range_min, config.port_range_max],
    }


def get_tcp_tunnel_server() -> TcpTunnelServer | None:
    return _server
