"""Artifact relay lifecycle and FastAPI dependencies."""

from __future__ import annotations

from fastapi import HTTPException

from .config import ArtifactRelayConfig
from .service import ArtifactRelayService


_service: ArtifactRelayService | None = None


async def startup_artifact_relay() -> None:
    global _service
    config = ArtifactRelayConfig.from_env()
    if not config.enabled:
        _service = None
        return
    if _service is not None:
        await _service.stop()
    _service = ArtifactRelayService(config)
    await _service.start()


async def shutdown_artifact_relay() -> None:
    global _service
    service, _service = _service, None
    if service is not None:
        await service.stop()


def get_artifact_relay_service() -> ArtifactRelayService | None:
    return _service


def require_artifact_relay_service() -> ArtifactRelayService:
    if _service is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "artifact_relay_disabled",
                "message": "Artifact relay is not enabled",
            },
        )
    return _service

