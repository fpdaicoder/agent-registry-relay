"""Standalone FastAPI control API and WebSocket data plane."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

from fastapi import FastAPI, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from .config import StreamProxyConfig
from .service import (
    Role,
    StreamProxyError,
    StreamProxyService,
)


class CreateSessionRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=1024)
    byteLength: int = Field(gt=0)
    sha256: str
    ttlSeconds: int | None = Field(default=None, gt=0)


def _error_response(error: StreamProxyError, status_code: int) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": error.code,
                "message": error.message,
            }
        },
    )


async def _send_websocket_error(
    websocket: WebSocket,
    error: StreamProxyError,
) -> None:
    try:
        await websocket.send_json(
            {
                "type": "error",
                "code": error.code,
                "message": error.message,
            }
        )
    except Exception:
        pass


def create_app(config: StreamProxyConfig) -> FastAPI:
    service = StreamProxyService(config)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(
        title="Agent Registry Stream Proxy",
        version="0.1.0-prototype",
        lifespan=lifespan,
    )
    app.state.stream_proxy = service

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"status": "ok", "mode": "prototype"}

    @app.post("/api/stream-proxy/sessions", status_code=201)
    async def create_session(
        payload: CreateSessionRequest,
        x_stream_proxy_key: Annotated[
            str | None,
            Header(alias="X-Stream-Proxy-Key"),
        ] = None,
    ) -> Response:
        try:
            service.require_create_token(x_stream_proxy_key)
            created = await service.create_session(
                filename=payload.filename,
                byte_length=payload.byteLength,
                sha256=payload.sha256,
                ttl_seconds=payload.ttlSeconds,
            )
            return JSONResponse(status_code=201, content=created)
        except StreamProxyError as error:
            status_code = (
                401
                if error.code == "stream_create_token_invalid"
                else 429
                if error.code == "stream_capacity_exceeded"
                else 400
            )
            return _error_response(error, status_code)

    @app.get("/api/stream-proxy/status")
    async def status(
        x_stream_proxy_key: Annotated[
            str | None,
            Header(alias="X-Stream-Proxy-Key"),
        ] = None,
    ) -> Response:
        try:
            service.require_create_token(x_stream_proxy_key)
            return JSONResponse(await service.status())
        except StreamProxyError as error:
            return _error_response(error, 401)

    @app.get("/api/stream-proxy/sessions/{transfer_id}")
    async def session_status(
        transfer_id: str,
        x_stream_proxy_key: Annotated[
            str | None,
            Header(alias="X-Stream-Proxy-Key"),
        ] = None,
    ) -> Response:
        try:
            service.require_create_token(x_stream_proxy_key)
            return JSONResponse(service.get_session(transfer_id).public_status())
        except StreamProxyError as error:
            status_code = (
                401
                if error.code == "stream_create_token_invalid"
                else 410
                if error.code == "stream_session_expired"
                else 404
            )
            return _error_response(error, status_code)

    @app.post(
        "/api/stream-proxy/sessions/{transfer_id}/cancel",
        status_code=204,
    )
    async def cancel_session(
        transfer_id: str,
        x_stream_proxy_key: Annotated[
            str | None,
            Header(alias="X-Stream-Proxy-Key"),
        ] = None,
    ) -> Response:
        try:
            service.require_create_token(x_stream_proxy_key)
            await service.cancel_session(service.get_session(transfer_id))
            return Response(status_code=204)
        except StreamProxyError as error:
            status_code = (
                401
                if error.code == "stream_create_token_invalid"
                else 404
            )
            return _error_response(error, status_code)

    @app.websocket("/v1/stream/{transfer_id}/{role}")
    async def stream(
        websocket: WebSocket,
        transfer_id: str,
        role: Literal["sender", "receiver"],
    ) -> None:
        await websocket.accept(subprotocol="a2x.artifact.stream.v1")
        session = None
        attached = False
        try:
            session = service.get_session(transfer_id)
            auth = await asyncio.wait_for(
                websocket.receive_json(),
                timeout=config.auth_timeout_seconds,
            )
            if auth.get("type") != "auth":
                raise StreamProxyError(
                    "stream_auth_required",
                    "The first frame must authenticate the stream role",
                )
            service.authenticate(session, role, str(auth.get("token", "")))
            resume_offset = auth.get("resumeOffset", 0)
            await service.attach(
                session,
                role,
                websocket,
                resume_offset=resume_offset,
            )
            attached = True
            await websocket.send_json(
                {
                    "type": "registered",
                    "transferId": transfer_id,
                    "role": role,
                    "byteLength": session.byte_length,
                    "sha256": session.sha256,
                }
            )
            await service.announce_pair(session)
            while True:
                message = await websocket.receive()
                message_type = message.get("type")
                if message_type == "websocket.disconnect":
                    break
                if message_type != "websocket.receive":
                    continue
                binary = message.get("bytes")
                text = message.get("text")
                if binary is not None:
                    if role != "sender":
                        raise StreamProxyError(
                            "stream_binary_role_invalid",
                            "Only the sender may send binary frames",
                        )
                    await service.forward_data(session, websocket, binary)
                    continue
                if text is None:
                    continue
                try:
                    control = json.loads(text)
                except json.JSONDecodeError as error:
                    raise StreamProxyError(
                        "stream_control_invalid",
                        "Control frame must be a JSON object",
                    ) from error
                if not isinstance(control, dict):
                    raise StreamProxyError(
                        "stream_control_invalid",
                        "Control frame must be a JSON object",
                    )
                control_type = control.get("type")
                if control_type == "ping":
                    await websocket.send_json({"type": "pong"})
                elif role == "receiver" and control_type == "ack":
                    await service.acknowledge(
                        session,
                        websocket,
                        control.get("offset"),
                    )
                elif role == "sender" and control_type == "fin":
                    await service.finish(session, websocket, control)
                elif role == "receiver" and control_type == "complete":
                    await service.complete(session, websocket, control)
                elif control_type == "cancel":
                    await service.cancel_session(session)
                    break
                else:
                    raise StreamProxyError(
                        "stream_control_unexpected",
                        f"Unexpected {role} control frame: {control_type}",
                    )
        except asyncio.TimeoutError:
            await _send_websocket_error(
                websocket,
                StreamProxyError(
                    "stream_auth_timeout",
                    "Stream role authentication timed out",
                ),
            )
            await websocket.close(code=1008, reason="authentication timeout")
        except WebSocketDisconnect:
            pass
        except StreamProxyError as error:
            service.metrics["protocolErrors"] += 1
            await _send_websocket_error(websocket, error)
            try:
                await websocket.close(code=1008, reason=error.code[:120])
            except Exception:
                pass
        except Exception as error:
            if session is not None:
                await service.fail_session(
                    session,
                    code="stream_internal_error",
                    message=str(error),
                )
            else:
                try:
                    await websocket.close(code=1011, reason="internal error")
                except Exception:
                    pass
        finally:
            if attached and session is not None:
                await service.detach(session, role, websocket)

    return app
