"""Group chat lifecycle and FastAPI dependencies.

The service is held as a module-level singleton so routers can depend on it
without importing the startup machinery, and so a test can install a
memory-backed instance directly.
"""

from __future__ import annotations

import logging

from fastapi import HTTPException

from .config import GroupChatConfig
from .service import GroupChatService
from .sqlstore import build_store

logger = logging.getLogger(__name__)

_service: GroupChatService | None = None


async def startup_groupchat() -> None:
    """Start the module when enabled. Never raises on a disabled module."""
    global _service
    config = GroupChatConfig.from_env()
    if not config.enabled:
        _service = None
        return

    if _service is not None:
        await _service.stop()
    service = GroupChatService(config, build_store(config))
    await service.start()
    _service = service
    logger.info("Group chat enabled (backend=%s)", config.backend)


async def shutdown_groupchat() -> None:
    global _service
    service, _service = _service, None
    if service is not None:
        await service.stop()


def get_groupchat_service() -> GroupChatService | None:
    return _service


def set_groupchat_service(service: GroupChatService | None) -> None:
    """Install a service instance. Used by startup and by test fixtures."""
    global _service
    _service = service


def require_groupchat_service() -> GroupChatService:
    if _service is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "groupchat_disabled",
                "message": "Group chat is not enabled on this registry",
            },
        )
    return _service
