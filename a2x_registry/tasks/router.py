"""HTTP and WebSocket surface for the personal task-continuation data plane.

The caller is resolved through the same ``authorize`` / ``require_principal``
dependencies as the rest of the registry, and the device is ``ctx.key_id`` —
the id of the API key that authenticated the request. Every mutating route
takes the caller's ``lease_fence`` from the body and hands it to the service,
which validates it inside the same atomic ``UPDATE`` that advances the task.

The WebSocket route is read-only by construction; writes go through HTTP POST
so there is exactly one mutation path, and it is the one with the transactions.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from a2x_registry.auth.deps import authorize, get_auth_store, require_principal
from a2x_registry.common.auth_context import AuthContext

from .deps import require_tasks_service
from .errors import TaskError
from .service import TasksService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["tasks"])

_HEARTBEAT_SECONDS = 30
_IDLE_LIMIT_SECONDS = 300


def _error(exc: TaskError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


async def _run(coro: Any) -> Any:
    try:
        return await coro
    except TaskError as exc:
        return _error(exc)


def _require_fence(body: dict[str, Any]) -> int:
    fence = body.get("fence")
    if isinstance(fence, bool) or not isinstance(fence, int) or fence < 0:
        raise TaskError(400, "task_fence_invalid", "fence must be a non-negative integer")
    return fence


def _default_namespace(ctx: AuthContext) -> str:
    if ctx.namespaces:
        return sorted(ctx.namespaces)[0]
    return "default"


# ── Tasks ────────────────────────────────────────────────────────────────


@router.post("/api/tasks")
async def create_task(
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - malformed JSON is a client error
        return _error(TaskError(400, "task_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(TaskError(400, "task_body_invalid", "Body must be a JSON object"))

    result = await _run(
        service.create_task(
            actor=ctx.principal_id,
            owner_dataset=body.get("owner_dataset") or _default_namespace(ctx),
            title=body.get("title"),
            payload=body.get("payload"),
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return JSONResponse(
        status_code=201,
        content={
            "task_id": result.id,
            "title": result.title,
            "owner_dataset": result.owner_dataset,
            "state": result.state,
            "seq_counter": result.seq_counter,
        },
    )


@router.get("/api/tasks")
async def list_tasks(
    state: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    result = await _run(
        service.list_tasks(actor=ctx.principal_id, state=state, limit=limit)
    )
    if isinstance(result, JSONResponse):
        return result
    return {"tasks": result}


@router.get("/api/tasks/status")
async def tasks_status(
    _ctx: Optional[AuthContext] = Depends(authorize),
    service: TasksService = Depends(require_tasks_service),
):
    return service.status()


@router.get("/api/tasks/{task_id}")
async def get_task(
    task_id: int,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    result = await _run(service.get_task(actor=ctx.principal_id, task_id=task_id))
    if isinstance(result, JSONResponse):
        return result
    return result


@router.patch("/api/tasks/{task_id}")
async def update_task(
    task_id: int,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return _error(TaskError(400, "task_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(TaskError(400, "task_body_invalid", "Body must be a JSON object"))

    result = await _run(
        service.update_task(
            actor=ctx.principal_id,
            task_id=task_id,
            title=body.get("title"),
            archived=body.get("archived"),
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return {
        "task_id": result.id,
        "title": result.title,
        "archived": result.archived_at is not None,
    }


# ── Lease ────────────────────────────────────────────────────────────────


@router.post("/api/tasks/{task_id}/acquire")
async def acquire(
    task_id: int,
    request: Request,
    mode: Optional[str] = Query(default="force"),
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    if not isinstance(body, dict):
        body = {}

    result = await _run(
        service.acquire(
            actor=ctx.principal_id,
            device=ctx.key_id,
            task_id=task_id,
            ttl_seconds=body.get("ttl_seconds"),
            mode=mode or "force",
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


@router.post("/api/tasks/{task_id}/renew")
async def renew(
    task_id: int,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return _error(TaskError(400, "task_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(TaskError(400, "task_body_invalid", "Body must be a JSON object"))
    try:
        fence = _require_fence(body)
    except TaskError as exc:
        return _error(exc)

    result = await _run(
        service.renew(
            actor=ctx.principal_id,
            device=ctx.key_id,
            task_id=task_id,
            fence=fence,
            ttl_seconds=body.get("ttl_seconds"),
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


@router.post("/api/tasks/{task_id}/release")
async def release(
    task_id: int,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return _error(TaskError(400, "task_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(TaskError(400, "task_body_invalid", "Body must be a JSON object"))
    try:
        fence = _require_fence(body)
    except TaskError as exc:
        return _error(exc)

    result = await _run(
        service.release(
            actor=ctx.principal_id,
            device=ctx.key_id,
            task_id=task_id,
            fence=fence,
            next_state=body.get("next_state"),
            checkpoint=body.get("checkpoint"),
            checkpoint_seq=body.get("checkpoint_seq"),
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


@router.post("/api/tasks/{task_id}/checkpoint")
async def checkpoint(
    task_id: int,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return _error(TaskError(400, "task_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(TaskError(400, "task_body_invalid", "Body must be a JSON object"))
    try:
        fence = _require_fence(body)
    except TaskError as exc:
        return _error(exc)
    checkpoint = body.get("checkpoint")
    checkpoint_seq = body.get("checkpoint_seq")
    if checkpoint_seq is None or isinstance(checkpoint_seq, bool) or not isinstance(checkpoint_seq, int):
        return _error(TaskError(400, "task_checkpoint_invalid", "checkpoint_seq must be an integer"))

    result = await _run(
        service.set_checkpoint(
            actor=ctx.principal_id,
            device=ctx.key_id,
            task_id=task_id,
            fence=fence,
            checkpoint=checkpoint if isinstance(checkpoint, dict) else {},
            checkpoint_seq=checkpoint_seq,
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


# ── Messages ─────────────────────────────────────────────────────────────


@router.post("/api/tasks/{task_id}/messages")
async def append_message(
    task_id: int,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return _error(TaskError(400, "task_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(TaskError(400, "task_body_invalid", "Body must be a JSON object"))
    try:
        fence = _require_fence(body)
    except TaskError as exc:
        return _error(exc)
    if "payload" not in body:
        return _error(TaskError(400, "task_payload_required", "payload is required"))

    result = await _run(
        service.append_message(
            actor=ctx.principal_id,
            device=ctx.key_id,
            task_id=task_id,
            fence=fence,
            kind=body.get("kind", "text"),
            payload=body.get("payload"),
            client_msg_id=body.get("client_msg_id"),
            reply_to=body.get("reply_to"),
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return JSONResponse(
        status_code=201,
        content={
            "task_id": result.task_id,
            "seq": result.seq,
            "message_id": result.message_id,
            "client_msg_id": result.client_msg_id,
            "kind": result.kind,
            "trusted": False,
            "created_at": result.created_at,
        },
    )


@router.get("/api/tasks/{task_id}/messages")
async def fetch_messages(
    task_id: int,
    after_seq: Optional[int] = Query(default=None, ge=0),
    before_seq: Optional[int] = Query(default=None, ge=0),
    limit: Optional[int] = Query(default=None, ge=1),
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    result = await _run(
        service.fetch_messages(
            actor=ctx.principal_id,
            device=ctx.key_id,
            task_id=task_id,
            after_seq=after_seq,
            before_seq=before_seq,
            limit=limit,
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


@router.post("/api/tasks/{task_id}/messages/{seq}/ack")
async def ack(
    task_id: int,
    seq: int,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    result = await _run(
        service.ack(actor=ctx.principal_id, device=ctx.key_id, task_id=task_id, seq=seq)
    )
    if isinstance(result, JSONResponse):
        return result
    return result


# ── Retirement ───────────────────────────────────────────────────────────


@router.post("/api/tasks/{task_id}/retire")
async def retire(
    task_id: int,
    ctx: AuthContext = Depends(require_principal),
    service: TasksService = Depends(require_tasks_service),
):
    result = await _run(service.retire(actor=ctx.principal_id, task_id=task_id))
    if isinstance(result, JSONResponse):
        return result
    return result


# ── Subscription ─────────────────────────────────────────────────────────


@router.websocket("/api/tasks/{task_id}/subscribe")
async def subscribe(
    websocket: WebSocket,
    task_id: int,
    after_seq: int = 0,
):
    """Stream frames for one task. Read-only.

    Authentication happens before ``accept()``: an unauthenticated socket is
    closed with 4401 and never enters the Hub.
    """
    service = _service_or_none()
    if service is None:
        await websocket.close(code=1011, reason="tasks_disabled")
        return

    ctx = await _authenticate_websocket(websocket)
    if ctx is None:
        await websocket.close(code=4401, reason="authentication required")
        return

    try:
        task = await service.store.get_task(task_id)
    except Exception:  # noqa: BLE001 - storage failure must not hang the socket
        logger.exception("tasks websocket task lookup failed")
        await websocket.close(code=1011, reason="storage unavailable")
        return
    if task is None or task.owner_id != ctx.principal_id:
        await websocket.close(code=4404, reason="task not found")
        return

    await websocket.accept()
    connection_id = uuid.uuid4().hex
    subscriber = service.hub.register(connection_id, ctx.principal_id)
    service.hub.subscribe(subscriber, task_id)

    try:
        page = await service.store.fetch_page(
            task_id, after_seq=after_seq, limit=service.config.max_page_size
        )
        await websocket.send_json(
            {
                "type": "backlog",
                "task_id": task_id,
                "messages": [
                    message.to_wire(trusted=message.sender_id == ctx.principal_id)
                    for message in page.messages
                ],
                "next_seq": page.next_seq,
                "has_more": page.has_more,
                "latest_seq": page.latest_seq,
            }
        )
    except Exception:  # noqa: BLE001
        logger.exception("tasks backlog send failed")
        await websocket.close(code=1011, reason="backlog failed")
        service.hub.unregister(connection_id)
        return

    writer = asyncio.create_task(_pump(websocket, subscriber))
    reader = asyncio.create_task(_read_acks(websocket, service, task_id, ctx))
    try:
        done, pending = await asyncio.wait(
            {writer, reader}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
    except WebSocketDisconnect:
        pass
    finally:
        service.hub.unregister(connection_id)
        for task in (writer, reader):
            if not task.done():
                task.cancel()


async def _pump(websocket: WebSocket, subscriber: Any) -> None:
    while True:
        try:
            frame = await asyncio.wait_for(subscriber.queue.get(), timeout=_HEARTBEAT_SECONDS)
        except asyncio.TimeoutError:
            try:
                await websocket.send_json({"type": "ping"})
            except Exception:  # noqa: BLE001
                return
            continue
        try:
            await websocket.send_json(frame)
        except Exception:  # noqa: BLE001
            return


async def _read_acks(
    websocket: WebSocket,
    service: TasksService,
    task_id: int,
    ctx: AuthContext,
) -> None:
    """Consume client frames. Only ``ack`` is honored; this channel is read-only."""
    while True:
        try:
            raw = await asyncio.wait_for(websocket.receive_json(), timeout=_IDLE_LIMIT_SECONDS)
        except asyncio.TimeoutError:
            return
        except WebSocketDisconnect:
            return
        except Exception:  # noqa: BLE001 - malformed frame, keep the socket open
            continue
        if not isinstance(raw, dict) or raw.get("type") != "ack":
            continue
        seq = raw.get("seq")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            continue
        if not ctx.key_id:
            continue
        try:
            await service.store.advance_cursor(task_id, ctx.key_id, seq)
        except Exception:  # noqa: BLE001
            logger.exception("tasks ack failed: task=%s device=%s", task_id, ctx.key_id)


async def _authenticate_websocket(websocket: WebSocket) -> Optional[AuthContext]:
    from a2x_registry.auth.deps import get_auth_store

    store = get_auth_store()
    if store is None:
        return None

    header = websocket.headers.get("authorization")
    token: Optional[str] = None
    if header:
        parts = header.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1].strip() or None
    if token is None:
        token = websocket.query_params.get("token") or None
    if token is None:
        return None

    try:
        return store.authenticate(token)
    except Exception:  # noqa: BLE001
        return None


def _service_or_none() -> Optional[TasksService]:
    from .deps import get_tasks_service

    return get_tasks_service()