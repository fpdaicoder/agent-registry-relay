"""Task module lifecycle and FastAPI dependencies.

Mirrors ``groupchat/deps.py``: the service is a module-level singleton so
routers can depend on it without importing the startup machinery, and so a test
can install a memory-backed instance directly. The one addition is the periodic
sweeper loop, owned here (the lifecycle layer) rather than started from
``TasksService.start`` so the test fixtures stay free of background tasks.
"""

from __future__ import annotations

import logging

from fastapi import HTTPException

from .config import TasksConfig
from .service import TasksService
from .sqlstore import build_store

logger = logging.getLogger(__name__)

_service: TasksService | None = None


async def startup_tasks() -> None:
    """Start the module when enabled. Never raises on a disabled module."""
    global _service
    config = TasksConfig.from_env()
    if not config.enabled:
        _service = None
        return

    if _service is not None:
        await _service.stop()
    service = TasksService(config, build_store(config))
    await service.start()
    service.start_sweeper()
    _service = service
    logger.info("Tasks enabled (backend=%s)", config.backend)


async def shutdown_tasks() -> None:
    global _service
    service, _service = _service, None
    if service is not None:
        await service.stop()


def get_tasks_service() -> TasksService | None:
    return _service


def set_tasks_service(service: TasksService | None) -> None:
    """Install a service instance. Used by startup and by test fixtures."""
    global _service
    _service = service


def require_tasks_service() -> TasksService:
    if _service is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "tasks_disabled",
                "message": "Personal task continuation is not enabled on this registry",
            },
        )
    return _service