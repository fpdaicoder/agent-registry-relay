"""HTTP API for resumable, token-scoped artifact relay transfers."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from .deps import require_artifact_relay_service
from .errors import ArtifactRelayError
from .service import ArtifactRelayService, DownloadSlice


router = APIRouter(prefix="/api/artifact-relay", tags=["artifact-relay"])


class CreateTransferRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=1024)
    byteLength: int = Field(gt=0)
    sha256: str
    mediaType: str = Field(default="application/octet-stream", min_length=1)
    ttlSeconds: int | None = Field(default=None, gt=0)


def _error_response(exc: ArtifactRelayError) -> JSONResponse:
    content = {
        "error": {
            "code": exc.code,
            "message": exc.message,
        }
    }
    if exc.details:
        content["error"]["details"] = exc.details
    headers = {}
    if exc.status_code == 401:
        headers["WWW-Authenticate"] = "Bearer"
    return JSONResponse(
        status_code=exc.status_code,
        content=content,
        headers=headers,
    )


def _bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise ArtifactRelayError(
            401,
            "artifact_token_required",
            "Bearer token is required",
        )
    scheme, separator, token = authorization.strip().partition(" ")
    if (
        not separator
        or scheme.lower() != "bearer"
        or not token.strip()
    ):
        raise ArtifactRelayError(
            401,
            "artifact_token_required",
            "Bearer token is required",
        )
    return token.strip()


def _download_response(
    request: Request,
    service: ArtifactRelayService,
    selection: DownloadSlice,
) -> Response:
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(selection.length),
        "Content-Disposition": service.content_disposition(selection.filename),
        "ETag": f'"sha256:{selection.sha256}"',
        "X-Artifact-SHA256": selection.sha256,
    }
    if selection.partial:
        headers["Content-Range"] = (
            f"bytes {selection.start}-{selection.end}/{selection.total}"
        )
    if request.method == "HEAD":
        return Response(
            status_code=206 if selection.partial else 200,
            media_type="application/octet-stream",
            headers=headers,
        )
    return StreamingResponse(
        service.stream_download(selection),
        status_code=206 if selection.partial else 200,
        media_type="application/octet-stream",
        headers=headers,
    )


@router.get("/status")
async def get_artifact_relay_status(
    x_artifact_relay_key: Annotated[
        str | None,
        Header(alias="X-Artifact-Relay-Key"),
    ] = None,
    service: ArtifactRelayService = Depends(require_artifact_relay_service),
) -> Response:
    try:
        service.require_create_token(x_artifact_relay_key)
        return JSONResponse(await service.status())
    except ArtifactRelayError as exc:
        return _error_response(exc)


@router.post("/transfers", status_code=201)
async def create_transfer(
    payload: CreateTransferRequest,
    x_artifact_relay_key: Annotated[
        str | None,
        Header(alias="X-Artifact-Relay-Key"),
    ] = None,
    service: ArtifactRelayService = Depends(require_artifact_relay_service),
) -> Response:
    try:
        service.require_create_token(x_artifact_relay_key)
        transfer = await service.create_transfer(
            filename=payload.filename,
            byte_length=payload.byteLength,
            sha256=payload.sha256,
            media_type=payload.mediaType,
            ttl_seconds=payload.ttlSeconds,
        )
        return JSONResponse(status_code=201, content=transfer)
    except ArtifactRelayError as exc:
        return _error_response(exc)


@router.put("/transfers/{transfer_id}")
async def upload_chunk(
    transfer_id: str,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    content_range: Annotated[str | None, Header()] = None,
    content_length: Annotated[int | None, Header()] = None,
    service: ArtifactRelayService = Depends(require_artifact_relay_service),
) -> Response:
    try:
        metadata = await service.append_chunk(
            transfer_id,
            _bearer_token(authorization),
            content_range,
            content_length,
            request.stream(),
        )
        return JSONResponse(metadata)
    except ArtifactRelayError as exc:
        return _error_response(exc)


@router.get("/transfers/{transfer_id}/status")
async def get_transfer_status(
    transfer_id: str,
    authorization: Annotated[str | None, Header()] = None,
    service: ArtifactRelayService = Depends(require_artifact_relay_service),
) -> Response:
    try:
        metadata = await service.transfer_status(
            transfer_id,
            _bearer_token(authorization),
        )
        return JSONResponse(metadata)
    except ArtifactRelayError as exc:
        return _error_response(exc)


@router.api_route(
    "/transfers/{transfer_id}",
    methods=["GET", "HEAD"],
)
async def download_artifact(
    transfer_id: str,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    range_header: Annotated[str | None, Header(alias="Range")] = None,
    service: ArtifactRelayService = Depends(require_artifact_relay_service),
) -> Response:
    try:
        selection = await service.prepare_download(
            transfer_id,
            _bearer_token(authorization),
            range_header,
        )
        return _download_response(request, service, selection)
    except ArtifactRelayError as exc:
        return _error_response(exc)


@router.api_route(
    "/uri/{transfer_id}/{uri_token}",
    methods=["GET", "HEAD"],
)
async def download_artifact_by_signed_uri(
    transfer_id: str,
    uri_token: str,
    request: Request,
    range_header: Annotated[str | None, Header(alias="Range")] = None,
    service: ArtifactRelayService = Depends(require_artifact_relay_service),
) -> Response:
    try:
        selection = await service.prepare_uri_download(
            transfer_id,
            uri_token,
            range_header,
        )
        return _download_response(request, service, selection)
    except ArtifactRelayError as exc:
        return _error_response(exc)


@router.delete("/transfers/{transfer_id}", status_code=204)
async def delete_transfer(
    transfer_id: str,
    authorization: Annotated[str | None, Header()] = None,
    service: ArtifactRelayService = Depends(require_artifact_relay_service),
) -> Response:
    try:
        await service.delete_transfer(
            transfer_id,
            _bearer_token(authorization),
        )
        return Response(status_code=204)
    except ArtifactRelayError as exc:
        return _error_response(exc)
