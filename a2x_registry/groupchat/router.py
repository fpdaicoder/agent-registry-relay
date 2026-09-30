"""HTTP and WebSocket surface for the group chat data plane.

Every route resolves the caller through the same ``authorize`` dependency the
rest of the registry uses, so namespace-level auth and the group chat module
stay consistent. The mutating routes additionally layer
``require_principal``, because group chat is a per-identity feature: unlike
service registration, there is no sensible anonymous behavior — an anonymous
caller cannot be a group member.

The WebSocket route is read-only by construction: it only ever reads frames
off the server queue. All writes go through HTTP POST, so there is exactly one
code path that mutates a group, and it is the one with the transactions.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from a2x_registry.auth.deps import authorize, get_auth_store, require_principal
from a2x_registry.common.auth_context import AuthContext

from .deps import require_groupchat_service
from .errors import GroupChatError
from .models import ROLES
from .service import GroupChatService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["groupchat"])

_HEARTBEAT_SECONDS = 30
_IDLE_LIMIT_SECONDS = 300


def _error(exc: GroupChatError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


async def _run(coro: Any) -> Any:
    try:
        return await coro
    except GroupChatError as exc:
        return _error(exc)


def _with_principal_kinds(members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Decorate member rows with their principal's kind and display name.

    Group chat stores only principal ids, so a roster on its own renders as
    opaque credential strings with no way to tell an Agent from a person. The
    auth store is the one place that knows which principals are Agents, so the
    distinction is resolved here rather than pushed down into the service — the
    group chat data plane stays free of an auth dependency, and an
    uninitialized auth store degrades to the plain rows instead of failing the
    listing.

    An Agent is named after **its owner**, not itself: a bare
    ``agent-default`` says nothing about whose Agent a row is, while the
    inviter's handle reads as "this is root's Agent" — which is the question a
    roster is actually being read to answer. Agents join on a targeted
    invitation from the principal that owns them, so ``invited_by`` is exactly
    that owner; when there is none (an agent added by some future path) the
    Agent's own handle is the only thing left to fall back to.

    ``display_name`` is filled only when the row does not already carry one, so
    a client that overlays its own label still wins over the derived name.
    """
    store = get_auth_store()
    if store is None:
        return members
    enriched: list[dict[str, Any]] = []
    for row in members:
        principal = store.get_principal(str(row.get("principal_id") or ""))
        is_agent = principal is not None and principal.kind == "agent"
        decorated = dict(row)
        decorated["is_agent"] = is_agent

        name = ""
        if is_agent:
            owner = store.get_principal(str(row.get("invited_by") or ""))
            name = (owner.handle if owner is not None else "") or (
                principal.handle if principal is not None else ""
            )
        elif principal is not None:
            name = principal.handle or ""
        if name and "display_name" not in decorated:
            decorated["display_name"] = name
        enriched.append(decorated)
    return enriched


# ── Groups ───────────────────────────────────────────────────────────────


@router.post("/api/groups")
async def create_group(
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - malformed JSON is a client error
        return _error(GroupChatError(400, "groupchat_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(GroupChatError(400, "groupchat_body_invalid", "Body must be a JSON object"))

    result = await _run(
        service.create_group(
            actor=ctx.principal_id,
            name=body.get("name", ""),
            # The owning namespace is a governance field: it decides quotas
            # and who may create groups. It never participates in membership
            # checks, so a group may hold members from other namespaces.
            owner_dataset=body.get("owner_dataset") or _default_namespace(ctx),
            join_policy=body.get("join_policy", "invite_only"),
            max_members=body.get("max_members"),
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return JSONResponse(
        status_code=201,
        content={
            "group_id": result.id,
            "name": result.name,
            "owner_dataset": result.owner_dataset,
            "seq_counter": result.seq_counter,
        },
    )


@router.get("/api/groups")
async def list_groups(
    limit: int = Query(default=50, ge=1, le=500),
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(service.list_groups(actor=ctx.principal_id, limit=limit))
    if isinstance(result, JSONResponse):
        return result
    return {"groups": result}


@router.get("/api/groups/status")
async def groupchat_status(
    _ctx: Optional[AuthContext] = Depends(authorize),
    service: GroupChatService = Depends(require_groupchat_service),
):
    return service.status()


@router.get("/api/groups/{group_id}")
async def get_group(
    group_id: int,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(service.get_group(actor=ctx.principal_id, group_id=group_id))
    if isinstance(result, JSONResponse):
        return result
    if isinstance(result, dict) and isinstance(result.get("members"), list):
        result = {**result, "members": _with_principal_kinds(result["members"])}
    return result


@router.patch("/api/groups/{group_id}")
async def update_group(
    group_id: int,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return _error(GroupChatError(400, "groupchat_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(GroupChatError(400, "groupchat_body_invalid", "Body must be a JSON object"))

    result = await _run(
        service.update_group(
            actor=ctx.principal_id,
            group_id=group_id,
            name=body.get("name"),
            join_policy=body.get("join_policy"),
            muted=body.get("muted"),
            archived=body.get("archived"),
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return {
        "group_id": result.id,
        "name": result.name,
        "join_policy": result.join_policy,
        "muted": result.muted,
        "archived": result.archived_at is not None,
    }


@router.delete("/api/groups/{group_id}")
async def delete_group(
    group_id: int,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    """Physically delete a group. Owner only, irreversible.

    Distinct from ``PATCH {"archived": true}``, which retires the group but
    keeps its history. This removes the group, its members, its invitations
    and its messages.
    """
    result = await _run(service.delete_group(actor=ctx.principal_id, group_id=group_id))
    if isinstance(result, JSONResponse):
        return result
    return result


# ── Invitations ──────────────────────────────────────────────────────────


@router.post("/api/groups/{group_id}/invites")
async def create_invite(
    group_id: int,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    if not isinstance(body, dict):
        body = {}

    # Resolve whether a targeted invitee is an Agent. The service enforces the
    # "members may only invite agents" rule; the auth store is the one place a
    # principal's kind is known, so it is looked up here and passed down.
    invitee_id = body.get("invitee_id")
    invitee_is_agent = False
    if invitee_id:
        auth_store = get_auth_store()
        if auth_store is not None:
            invitee = auth_store.get_principal(invitee_id)
            invitee_is_agent = invitee is not None and invitee.kind == "agent"

    result = await _run(
        service.create_invite(
            actor=ctx.principal_id,
            group_id=group_id,
            invitee_id=invitee_id,
            max_uses=body.get("max_uses"),
            ttl_seconds=body.get("ttl_seconds"),
            invitee_is_agent=invitee_is_agent,
        )
    )
    if isinstance(result, JSONResponse):
        return result
    # The plaintext token appears exactly here and is never persisted, so a
    # stolen database row cannot be replayed against /api/invites/{id}/accept.
    return JSONResponse(status_code=201, content=result)


@router.post("/api/invites/{token}/accept")
async def accept_invite(
    token: str,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(service.accept_invite(actor=ctx.principal_id, token=token))
    if isinstance(result, JSONResponse):
        return result
    return result


# ── Members ──────────────────────────────────────────────────────────────


@router.get("/api/groups/{group_id}/members")
async def list_members(
    group_id: int,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(service.list_members(actor=ctx.principal_id, group_id=group_id))
    if isinstance(result, JSONResponse):
        return result
    return {"members": _with_principal_kinds(result)}


@router.delete("/api/groups/{group_id}/members/me")
async def leave_group(
    group_id: int,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(service.remove_member(actor=ctx.principal_id, group_id=group_id))
    if isinstance(result, JSONResponse):
        return result
    return result


@router.delete("/api/groups/{group_id}/members/{principal_id}")
async def remove_member(
    group_id: int,
    principal_id: str,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(
        service.remove_member(
            actor=ctx.principal_id, group_id=group_id, target=principal_id
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


@router.post("/api/groups/{group_id}/members/{principal_id}/ban")
async def ban_member(
    group_id: int,
    principal_id: str,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(
        service.remove_member(
            actor=ctx.principal_id, group_id=group_id, target=principal_id, ban=True
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


@router.put("/api/groups/{group_id}/members/{principal_id}/role")
async def set_role(
    group_id: int,
    principal_id: str,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    role = body.get("role") if isinstance(body, dict) else None
    if not isinstance(role, str) or role not in ROLES:
        return _error(
            GroupChatError(400, "groupchat_role_invalid", f"role must be one of {ROLES}")
        )
    result = await _run(
        service.set_member_role(
            actor=ctx.principal_id, group_id=group_id, target=principal_id, role=role
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


# ── Messages ─────────────────────────────────────────────────────────────


@router.post("/api/groups/{group_id}/messages")
async def send_message(
    group_id: int,
    request: Request,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return _error(GroupChatError(400, "groupchat_body_invalid", "Body must be JSON"))
    if not isinstance(body, dict):
        return _error(GroupChatError(400, "groupchat_body_invalid", "Body must be a JSON object"))
    if "payload" not in body and "content" not in body:
        return _error(
            GroupChatError(400, "groupchat_payload_required", "payload is required")
        )

    mentions = body.get("mentions") or []
    if not isinstance(mentions, list):
        return _error(
            GroupChatError(400, "groupchat_mentions_invalid", "mentions must be a list")
        )

    result = await _run(
        service.send_message(
            actor=ctx.principal_id,
            group_id=group_id,
            payload=body.get("payload", body.get("content")),
            kind=body.get("kind", "text"),
            client_msg_id=body.get("client_msg_id"),
            mentions=[str(m) for m in mentions],
            reply_to=body.get("reply_to"),
        )
    )
    if isinstance(result, JSONResponse):
        return result
    # ``trusted`` is False unconditionally: a group message is data written
    # by another principal, and the receiving Agent must not execute it.
    return JSONResponse(
        status_code=201,
        content={
            "group_id": result.group_id,
            "seq": result.seq,
            "message_id": result.message_id,
            "client_msg_id": result.client_msg_id,
            "kind": result.kind,
            "mentions": list(result.mentions),
            "trusted": False,
            "created_at": result.created_at,
        },
    )


@router.get("/api/groups/{group_id}/messages")
async def fetch_messages(
    group_id: int,
    after_seq: Optional[int] = Query(default=None, ge=0),
    before_seq: Optional[int] = Query(default=None, ge=0),
    limit: Optional[int] = Query(default=None, ge=1),
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(
        service.fetch_messages(
            actor=ctx.principal_id,
            group_id=group_id,
            after_seq=after_seq,
            before_seq=before_seq,
            limit=limit,
        )
    )
    if isinstance(result, JSONResponse):
        return result
    return result


@router.post("/api/groups/{group_id}/messages/{seq}/ack")
async def ack(
    group_id: int,
    seq: int,
    ctx: AuthContext = Depends(require_principal),
    service: GroupChatService = Depends(require_groupchat_service),
):
    result = await _run(
        service.ack(actor=ctx.principal_id, group_id=group_id, seq=seq)
    )
    if isinstance(result, JSONResponse):
        return result
    return result


# ── Subscription ─────────────────────────────────────────────────────────


@router.websocket("/api/groups/{group_id}/subscribe")
async def subscribe(
    websocket: WebSocket,
    group_id: int,
    after_seq: int = 0,
):
    """Stream frames for one group. Read-only.

    Authentication happens before ``accept()``: an unauthenticated socket is
    closed with 4401 and never enters the Hub, so there is no window in which
    a connection is registered but not yet authorized.
    """
    service = _service_or_none()
    if service is None:
        await websocket.close(code=1011, reason="groupchat_disabled")
        return

    ctx = await _authenticate_websocket(websocket)
    if ctx is None:
        await websocket.close(code=4401, reason="authentication required")
        return

    try:
        # A subscription is a read path: it delivers backlog and live frames.
        # An archived group still streams, so a member can finish reading a
        # retired conversation. Only deletion ends the subscription.
        group = await service.store.get_group(group_id)
        membership = await service.store.get_membership(group_id, ctx.principal_id)
    except Exception:  # noqa: BLE001 - storage failure must not hang the socket
        logger.exception("groupchat websocket membership lookup failed")
        await websocket.close(code=1011, reason="storage unavailable")
        return
    if group is None:
        await websocket.close(code=4404, reason="group not found")
        return
    if membership is None or membership.status != "active":
        await websocket.close(code=4403, reason="not a member of this group")
        return

    await websocket.accept()
    connection_id = uuid.uuid4().hex
    subscriber = service.hub.register(connection_id, ctx.principal_id)
    service.hub.subscribe(subscriber, group_id)

    # Catch up first, then stream. Anything published between the catch-up
    # query and the subscription below would otherwise be invisible until the
    # next pull; sending the backlog before registering makes the overlap
    # produce duplicates (harmless, the client dedupes on seq) instead of
    # gaps (which would need a full resync).
    try:
        page = await service.store.fetch_page(
            group_id, after_seq=after_seq, limit=service.config.max_page_size
        )
        await websocket.send_json(
            {
                "type": "backlog",
                "group_id": group_id,
                "messages": [m.to_wire(trusted=False) for m in page.messages],
                "next_seq": page.next_seq,
                "has_more": page.has_more,
                "latest_seq": page.latest_seq,
            }
        )
    except Exception:  # noqa: BLE001
        logger.exception("groupchat backlog send failed")
        await websocket.close(code=1011, reason="backlog failed")
        service.hub.unregister(connection_id)
        return

    writer = asyncio.create_task(_pump(websocket, subscriber))
    reader = asyncio.create_task(_read_acks(websocket, service, group_id, ctx.principal_id))
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
    """Drain the subscriber queue and forward frames, with a keepalive."""
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
    service: GroupChatService,
    group_id: int,
    principal_id: str,
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
        if not isinstance(raw, dict):
            continue
        if raw.get("type") != "ack":
            try:
                await websocket.send_json(
                    {
                        "type": "error",
                        "code": "groupchat_frame_unsupported",
                        "message": "This channel is read-only; send messages over HTTP",
                    }
                )
            except Exception:  # noqa: BLE001
                return
            continue
        seq = raw.get("seq")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            continue
        try:
            await service.store.advance_cursor(group_id, principal_id, seq)
        except Exception:  # noqa: BLE001
            logger.exception("groupchat ack failed: group=%s principal=%s", group_id, principal_id)


async def _authenticate_websocket(websocket: WebSocket) -> Optional[AuthContext]:
    """Resolve the caller's identity from the handshake.

    Accepts ``Authorization: Bearer <token>`` (the same credential the REST
    surface uses) or a ``?token=`` query parameter, because browser
    WebSocket clients cannot set headers. Reuses the auth store directly so
    there is exactly one token validation implementation.
    """
    from a2x_registry.auth.deps import get_auth_store

    store = get_auth_store()
    if store is None:
        # Registry runs in anonymous mode: no identity to resolve, and group
        # chat cannot function without one.
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


def _service_or_none() -> Optional[GroupChatService]:
    from .deps import get_groupchat_service

    return get_groupchat_service()

def _default_namespace(ctx: AuthContext) -> str:
    """Pick a namespace to attribute a new group to.

    A group must be owned by some namespace for quota and audit purposes. An
    admin with access to everything gets ``default``; everyone else gets one
    of the namespaces they can actually see, so the quota lands where they
    have standing.
    """
    if ctx.namespaces:
        return sorted(ctx.namespaces)[0]
    return "default"
