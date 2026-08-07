"""HTTP status surface for the optional WebSocket tunnel."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends

from a2x_registry.auth.deps import authorize
from a2x_registry.common.auth_context import AuthContext

from .deps import tunnel_status


router = APIRouter(prefix="/api/tunnel", tags=["websocket-tunnel"])


@router.get("/status")
async def get_tunnel_status(
    _ctx: Optional[AuthContext] = Depends(authorize),
) -> dict[str, Any]:
    return tunnel_status()
