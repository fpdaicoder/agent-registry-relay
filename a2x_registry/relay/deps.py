"""Relay lifecycle and FastAPI dependency seams."""

from __future__ import annotations

from typing import Optional

from fastapi import HTTPException

from .config import RelayConfig
from .service import RelayService


_service: Optional[RelayService] = None


def _registry_getter():
    from a2x_registry.backend.routers.dataset import get_registry_service

    return get_registry_service()


def _tunnel_getter():
    from a2x_registry.tunnel.deps import get_tunnel_server

    return get_tunnel_server()


async def startup_relay() -> None:
    global _service
    config = RelayConfig.from_env()
    if not config.enabled:
        _service = None
        return
    if _service is not None:
        await _service.close()
    _service = RelayService(
        config,
        _registry_getter,
        tunnel_getter=_tunnel_getter,
    )


async def shutdown_relay() -> None:
    global _service
    service, _service = _service, None
    if service is not None:
        await service.close()


def set_relay_service(service: Optional[RelayService]) -> None:
    """Test seam for isolated relay router tests."""
    global _service
    _service = service


def require_relay_service() -> RelayService:
    global _service
    if _service is not None:
        return _service

    config = RelayConfig.from_env()
    if not config.enabled:
        raise HTTPException(
            status_code=404,
            detail={"code": "relay_disabled", "message": "A2A relay is not enabled"},
        )
    # Uvicorn initializes the service during startup. Lazy construction keeps
    # direct TestClient usage and embedded apps functional.
    _service = RelayService(
        config,
        _registry_getter,
        tunnel_getter=_tunnel_getter,
    )
    return _service
