"""FastAPI surface for A2A relay and derived Agent Cards."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, Response

from a2x_registry.auth.deps import authorize
from a2x_registry.common.auth_context import AuthContext

from .deps import require_relay_service
from .errors import RelayError
from .service import RelayService


router = APIRouter(tags=["a2a-relay"])


def _error_response(exc: RelayError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


async def _bounded_body(request: Request, limit: int) -> bytes:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared = int(content_length)
        except ValueError as exc:
            raise RelayError(400, "relay_invalid_length", "Content-Length must be an integer") from exc
        if declared < 0:
            raise RelayError(400, "relay_invalid_length", "Content-Length must be non-negative")
        if declared > limit:
            raise RelayError(413, "relay_request_too_large", "A2A request exceeds relay limit")

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise RelayError(413, "relay_request_too_large", "A2A request exceeds relay limit")
        chunks.append(chunk)
    return b"".join(chunks)


def _json_content_type(request: Request) -> bool:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    return content_type == "application/json" or content_type.endswith("+json")


@router.post("/a2a/{dataset}/{service_id}")
async def relay_a2a(
    dataset: str,
    service_id: str,
    request: Request,
    _ctx: Optional[AuthContext] = Depends(authorize),
    service: RelayService = Depends(require_relay_service),
) -> Response:
    if not _json_content_type(request):
        return _error_response(
            RelayError(415, "relay_content_type_unsupported", "A2A relay requires JSON content")
        )
    try:
        body = await _bounded_body(request, service.config.max_body_bytes)
        return await service.forward(dataset, service_id, body, dict(request.headers))
    except RelayError as exc:
        return _error_response(exc)


@router.get("/api/datasets/{dataset}/services/{service_id}/agent-card")
async def get_agent_card(
    dataset: str,
    service_id: str,
    route: str = "direct",
    _ctx: Optional[AuthContext] = Depends(authorize),
    service: RelayService = Depends(require_relay_service),
) -> Response:
    try:
        return JSONResponse(service.agent_card(dataset, service_id, route))
    except RelayError as exc:
        return _error_response(exc)
