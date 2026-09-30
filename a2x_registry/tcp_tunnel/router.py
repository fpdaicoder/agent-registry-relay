"""HTTP status surface for the optional reverse TCP tunnel."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends

from a2x_registry.auth.deps import authorize
from a2x_registry.common.auth_context import AuthContext

from .deps import tcp_tunnel_status


router = APIRouter(prefix="/api/tcp-tunnel", tags=["tcp-tunnel"])


@router.get("/status")
async def get_tcp_tunnel_status(
    _ctx: Optional[AuthContext] = Depends(authorize),
) -> dict[str, Any]:
    return tcp_tunnel_status()
