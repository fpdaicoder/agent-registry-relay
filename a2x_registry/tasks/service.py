"""Business rules for the personal task-continuation data plane.

Everything that decides *whether* an operation is allowed lives here; the store
decides how it is persisted and the router decides how it is exposed. Two rules
mirror ``groupchat/service.py`` and are security bugs, not correctness bugs:

* **Identity comes from the caller, never from the payload.** ``actor`` is the
  authenticated principal, ``device`` is the authenticated key id; neither is
  ever read out of a request body.
* **A task message is data, not instruction.** Messages from the owner's own
  devices may carry ``trusted=True`` on read; anything else is ``trusted=False``
  by construction, with no code path that raises it.

The correctness core is the fencing contract: the service never decides "who
holds the lease" by reading state and then writing — it hands the caller's
``lease_fence`` to the store, which checks it inside the same atomic ``UPDATE``
that advances the task. See ``sqlstore.py`` and the design doc §4.
"""

from __future__ import annotations

import asyncio
import logging
import time
import unicodedata
from typing import Any, Optional

from .config import TasksConfig
from .delivery import Hub
from .errors import TaskError
from .models import (
    CLIENT_WRITABLE_KINDS,
    MESSAGE_KINDS,
    TASK_STATES,
    Task,
    TaskMessage,
)
from .sqlstore import TasksStore

logger = logging.getLogger(__name__)

MAX_TITLE_CHARS = 200


def _clean_title(title: Any) -> str:
    """Normalize a task title for storage.

    Rejects non-text and control/format characters (the same spoofing concern
    as group names: two distinct tasks rendering identically).
    """
    if title is None:
        return ""
    if not isinstance(title, str):
        raise TaskError(400, "task_title_invalid", "Task title must be text")
    trimmed = title.strip()
    if len(trimmed) > MAX_TITLE_CHARS:
        raise TaskError(
            400,
            "task_title_invalid",
            f"Task title must be at most {MAX_TITLE_CHARS} characters",
        )
    for character in trimmed:
        if unicodedata.category(character) in {"Cc", "Cf", "Cs"}:
            raise TaskError(
                400,
                "task_title_invalid",
                "Task title must not contain control or formatting characters",
            )
    return trimmed


class TasksService:
    def __init__(
        self,
        config: TasksConfig,
        store: TasksStore,
        *,
        hub: Optional[Hub] = None,
    ) -> None:
        self.config = config
        self.store = store
        self.hub = hub if hub is not None else Hub()
        # Per-task send rate limiting, in-process (Redis in a multi-instance
        # deployment; the shape of the check is the same either way).
        self._send_windows: dict[int, list[float]] = {}
        self._sweeper: Optional[asyncio.Task] = None
        self._sweeper_interval = config.sweeper_interval_seconds

    async def start(self) -> None:
        await self.store.start()

    async def stop(self) -> None:
        self._cancel_sweeper()
        await self.store.stop()

    # ── Sweeper (crash recovery; TTL only, fence is the truth) ───────────

    def start_sweeper(self) -> None:
        """Launch the periodic expired-lease reaper.

        Not called from :meth:`start` so that the test fixtures — which run a
        single event loop per test — are never left with a dangling background
        task. The production lifecycle (``deps``) starts it explicitly.
        """
        if self._sweeper is not None:
            return
        self._sweeper = asyncio.create_task(self._sweep_forever())

    def _cancel_sweeper(self) -> None:
        if self._sweeper is not None:
            self._sweeper.cancel()
            self._sweeper = None

    async def _sweep_forever(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._sweeper_interval)
                try:
                    await self.sweep()
                except Exception:  # noqa: BLE001 - a tick must not kill the loop
                    logger.exception("tasks sweeper tick failed")
        except asyncio.CancelledError:
            pass

    async def sweep(self) -> int:
        """Release every lease whose TTL has elapsed. Returns the count."""
        return await self.store.reap_expired(time.time())

    # ── Ownership / device helpers ───────────────────────────────────────

    async def _load_owned(self, task_id: int, actor: str) -> Task:
        """Resolve the task as the caller's own, or 404 (do not leak it)."""
        task = await self.store.get_task(task_id)
        if task is None or task.owner_id != actor:
            raise TaskError(404, "task_not_found", "Task not found")
        return task

    @staticmethod
    def _require_device(device: Optional[str]) -> str:
        if not device:
            raise TaskError(
                401,
                "task_device_required",
                "A device API key is required; the credential carries no key_id",
            )
        return device

    def _require_writable(self, task: Task) -> None:
        if task.is_archived:
            raise TaskError(
                409, "task_archived", "Task is archived; unarchive it before writing"
            )
        if task.is_terminal:
            raise TaskError(
                409, "task_terminal", f"Task is in a terminal state ({task.state})"
            )

    def _lease_ttl(self, ttl_seconds: Optional[int]) -> int:
        ttl = self.config.lease_ttl_seconds if ttl_seconds is None else ttl_seconds
        if not self.config.min_lease_ttl_seconds <= ttl <= self.config.max_lease_ttl_seconds:
            raise TaskError(
                400,
                "task_ttl_invalid",
                f"ttl_seconds must be between {self.config.min_lease_ttl_seconds} "
                f"and {self.config.max_lease_ttl_seconds}",
            )
        return ttl

    # ── Tasks ────────────────────────────────────────────────────────────

    async def create_task(
        self,
        *,
        actor: str,
        owner_dataset: str,
        title: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> Task:
        if not actor:
            raise TaskError(401, "task_unauthenticated", "Authentication required")
        if payload is not None:
            self._check_payload_size(payload)
        clean_title = _clean_title(title)
        owned = await self.store.count_tasks_owned_by(actor)
        if owned >= self.config.max_tasks_per_principal:
            raise TaskError(
                429,
                "task_quota_exceeded",
                f"Principal already owns {owned} tasks "
                f"(limit {self.config.max_tasks_per_principal})",
            )
        return await self.store.create_task(
            owner_id=actor,
            owner_dataset=owner_dataset,
            title=clean_title,
            payload=payload,
        )

    async def get_task(self, *, actor: str, task_id: int) -> dict[str, Any]:
        task = await self._load_owned(task_id, actor)
        recent = await self.store.fetch_reverse_page(
            task_id, before_seq=task.seq_counter + 1, limit=self.config.default_page_size
        )
        return {
            "task": _task_to_wire(task),
            "recent_messages": [
                message.to_wire(trusted=message.sender_id == actor)
                for message in recent.messages
            ],
        }

    async def list_tasks(
        self, *, actor: str, state: Optional[str] = None, limit: int
    ) -> list[dict[str, Any]]:
        if state is not None and state not in TASK_STATES:
            raise TaskError(
                400, "task_state_invalid", f"state must be one of {TASK_STATES}"
            )
        tasks = await self.store.list_tasks_for(actor, state=state, limit=limit)
        return [_task_to_wire(task) for task in tasks]

    async def update_task(
        self,
        *,
        actor: str,
        task_id: int,
        title: Optional[str] = None,
        archived: Optional[bool] = None,
    ) -> Task:
        task = await self._load_owned(task_id, actor)
        updated = await self.store.update_task(
            task_id,
            title=_clean_title(title) if title is not None else None,
            archived=archived,
        )
        if updated is None:
            raise TaskError(404, "task_not_found", "Task not found")
        return updated

    # ── Lease ────────────────────────────────────────────────────────────

    async def acquire(
        self,
        *,
        actor: str,
        device: Optional[str],
        task_id: int,
        ttl_seconds: Optional[int] = None,
        mode: str = "force",
    ) -> dict[str, Any]:
        key_id = self._require_device(device)
        if mode not in ("force", "graceful"):
            raise TaskError(
                400, "task_mode_invalid", "mode must be 'force' or 'graceful'"
            )
        ttl = self._lease_ttl(ttl_seconds)
        task = await self._load_owned(task_id, actor)
        if task.is_archived:
            raise TaskError(409, "task_archived", "Task is archived")
        if task.is_terminal:
            raise TaskError(
                409, "task_terminal", f"Task is in a terminal state ({task.state})"
            )

        previous_holder = task.lease_holder
        # Graceful handoff: nudge the current holder to flush + release, then
        # wait a bounded time. This is a soft path, never a precondition — the
        # force acquire below always runs, so a dead device can't wedge a task.
        if mode == "graceful" and previous_holder and previous_holder != key_id:
            await self._preempt(task)

        acquired = await self.store.acquire(
            task_id, device_id=key_id, ttl_seconds=ttl, now=time.time()
        )
        if acquired is None:
            # Lost a race with a terminal transition or a concurrent delete.
            raise TaskError(409, "task_terminal", "Task can no longer be acquired")

        # Tell the *previous* holder's live connections it has been fenced off.
        # Best-effort: the fencing truth is the fence, this is just the wake-up.
        if previous_holder is not None:
            self.hub.publish(
                task_id,
                {
                    "type": "lease_lost",
                    "task_id": task_id,
                    "fence": acquired.lease_fence,
                },
            )
        return {
            "task_id": task_id,
            "fence": acquired.lease_fence,
            "holder": acquired.lease_holder,
            "expires_at": acquired.lease_expires_at,
            "state": acquired.state,
        }

    async def _preempt(self, task: Task) -> None:
        """Soft wake-up for a graceful handoff. Best-effort only."""
        self.hub.publish(
            task.id,
            {"type": "lease_preempt", "task_id": task.id},
        )
        # Offline-agent wake-up via relay/tunnel is a documented, best-effort
        # extension (design §4.4); here it is a hook with no data-plane
        # dependency. The force acquire is what actually takes the lease.
        timeout = self.config.graceful_timeout_seconds
        if timeout > 0:
            await asyncio.sleep(timeout)

    async def renew(
        self,
        *,
        actor: str,
        device: Optional[str],
        task_id: int,
        fence: int,
        ttl_seconds: Optional[int] = None,
    ) -> dict[str, Any]:
        key_id = self._require_device(device)
        task = await self._load_owned(task_id, actor)
        self._require_writable(task)
        ttl = self._lease_ttl(ttl_seconds)
        await self.store.renew(task_id, fence=fence, ttl_seconds=ttl, now=time.time())
        return {"task_id": task_id, "fence": fence, "expires_at": time.time() + ttl}

    async def release(
        self,
        *,
        actor: str,
        device: Optional[str],
        task_id: int,
        fence: int,
        next_state: Optional[str] = None,
        checkpoint: Optional[dict[str, Any]] = None,
        checkpoint_seq: Optional[int] = None,
    ) -> dict[str, Any]:
        key_id = self._require_device(device)
        task = await self._load_owned(task_id, actor)
        if task.is_archived:
            raise TaskError(409, "task_archived", "Task is archived")
        state = next_state or "input_required"
        if state not in TASK_STATES or state == "working":
            raise TaskError(
                400,
                "task_state_invalid",
                f"release next_state must be one of {TASK_STATES} and not 'working'",
            )
        # Preserve the existing resume point when the holder releases without
        # a new checkpoint; the checkpoint is the cross-device resume fuel.
        cp = checkpoint if checkpoint is not None else task.checkpoint
        cp_seq = checkpoint_seq if checkpoint_seq is not None else task.checkpoint_seq
        if cp is not None and not isinstance(cp, dict):
            raise TaskError(400, "task_checkpoint_invalid", "checkpoint must be a JSON object")
        if cp_seq < 0:
            raise TaskError(400, "task_checkpoint_invalid", "checkpoint_seq must be >= 0")

        await self.store.release(
            task_id,
            fence=fence,
            next_state=state,
            checkpoint=cp,
            checkpoint_seq=cp_seq,
            now=time.time(),
        )
        return {"task_id": task_id, "state": state, "released": True}

    async def set_checkpoint(
        self,
        *,
        actor: str,
        device: Optional[str],
        task_id: int,
        fence: int,
        checkpoint: dict[str, Any],
        checkpoint_seq: int,
    ) -> dict[str, Any]:
        key_id = self._require_device(device)
        task = await self._load_owned(task_id, actor)
        self._require_writable(task)
        if not isinstance(checkpoint, dict):
            raise TaskError(400, "task_checkpoint_invalid", "checkpoint must be a JSON object")
        if checkpoint_seq < 0:
            raise TaskError(400, "task_checkpoint_invalid", "checkpoint_seq must be >= 0")
        self._check_payload_size(checkpoint)
        await self.store.checkpoint(
            task_id, fence=fence, checkpoint=checkpoint, checkpoint_seq=checkpoint_seq
        )
        return {"task_id": task_id, "checkpoint_seq": checkpoint_seq}

    # ── Messages ─────────────────────────────────────────────────────────

    async def append_message(
        self,
        *,
        actor: str,
        device: Optional[str],
        task_id: int,
        fence: int,
        kind: str = "text",
        payload: Any = None,
        client_msg_id: Optional[str] = None,
        reply_to: Optional[int] = None,
    ) -> TaskMessage:
        key_id = self._require_device(device)
        task = await self._load_owned(task_id, actor)
        self._require_writable(task)
        if kind not in MESSAGE_KINDS:
            raise TaskError(400, "task_kind_invalid", f"kind must be one of {MESSAGE_KINDS}")
        if kind not in CLIENT_WRITABLE_KINDS:
            raise TaskError(400, "task_kind_reserved", "event and system entries are written by the server only")
        if payload is None:
            raise TaskError(400, "task_payload_required", "payload is required")
        self._check_rate(task_id)
        self._check_payload_size(payload)

        message = await self.store.append_message(
            task_id,
            sender_id=actor,
            device_id=key_id,
            kind=kind,
            payload=payload,
            fence=fence,
            client_msg_id=client_msg_id,
            reply_to=reply_to,
        )
        self._broadcast(task_id, message, owner_id=actor)
        return message

    async def fetch_messages(
        self,
        *,
        actor: str,
        device: Optional[str],
        task_id: int,
        after_seq: Optional[int] = None,
        before_seq: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        task = await self._load_owned(task_id, actor)
        if after_seq is not None and before_seq is not None:
            raise TaskError(
                400, "task_cursor_conflict", "Specify either after_seq or before_seq, not both"
            )
        page_size = min(limit or self.config.default_page_size, self.config.max_page_size)
        if page_size < 1:
            raise TaskError(400, "task_limit_invalid", "limit must be >= 1")

        if before_seq is not None:
            page = await self.store.fetch_reverse_page(
                task_id, before_seq=before_seq, limit=page_size
            )
        else:
            start = after_seq
            if start is None and device:
                # Existing device resumes at its watermark; a brand-new device
                # (no cursor row) resumes at the authoritative checkpoint, so it
                # never replays the whole history from seq=1.
                cursor = await self.store.get_cursor(task_id, device)
                start = cursor if cursor is not None else task.checkpoint_seq
            elif start is None:
                start = task.checkpoint_seq
            page = await self.store.fetch_page(task_id, after_seq=start, limit=page_size)

        return {
            "messages": [
                message.to_wire(trusted=message.sender_id == actor)
                for message in page.messages
            ],
            "next_seq": page.next_seq,
            "has_more": page.has_more,
            "latest_seq": page.latest_seq,
            "checkpoint_seq": task.checkpoint_seq,
        }

    async def ack(
        self,
        *,
        actor: str,
        device: Optional[str],
        task_id: int,
        seq: int,
    ) -> dict[str, Any]:
        key_id = self._require_device(device)
        await self._load_owned(task_id, actor)
        if seq < 0:
            raise TaskError(400, "task_seq_invalid", "seq must be >= 0")
        latest = await self.store.latest_seq(task_id)
        if seq > latest:
            raise TaskError(
                409, "task_seq_ahead", f"seq {seq} is ahead of the task's latest seq {latest}"
            )
        cursor = await self.store.advance_cursor(task_id, key_id, seq)
        return {"task_id": task_id, "last_read_seq": cursor}

    # ── Retirement ───────────────────────────────────────────────────────

    async def retire(self, *, actor: str, task_id: int) -> dict[str, Any]:
        task = await self._load_owned(task_id, actor)
        if task.is_archived:
            raise TaskError(409, "task_archived", "Task is already archived")
        if not task.is_terminal:
            raise TaskError(
                409,
                "task_not_retirable",
                "Only a completed / failed / canceled task can be retired",
            )
        retired = await self.store.retire(task_id)
        if not retired:
            raise TaskError(404, "task_not_found", "Task not found")
        logger.info("tasks task retired: task=%s owner=%s", task_id, actor)
        return {"task_id": task_id, "status": "retired"}

    # ── Fan-out / sizing helpers ─────────────────────────────────────────

    def _broadcast(self, task_id: int, message: TaskMessage, *, owner_id: str) -> int:
        return self.hub.publish(
            task_id,
            {"type": "message", **message.to_wire(trusted=message.sender_id == owner_id)},
        )

    def _check_payload_size(self, payload: Any) -> None:
        import json

        try:
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise TaskError(400, "task_payload_invalid", "Payload must be JSON-serializable") from exc
        if len(encoded) > self.config.max_message_bytes:
            raise TaskError(
                413,
                "task_message_too_large",
                f"Payload exceeds the {self.config.max_message_bytes} byte limit",
            )

    def _check_rate(self, task_id: int) -> None:
        now = time.time()
        window = self.config.rate_limit_window_seconds
        stamps = self._send_windows.setdefault(task_id, [])
        cutoff = now - window
        stamps[:] = [stamp for stamp in stamps if stamp > cutoff]
        if len(stamps) >= self.config.rate_limit_per_task:
            raise TaskError(
                429,
                "task_rate_limited",
                f"Task exceeded {self.config.rate_limit_per_task} messages "
                f"per {window}s",
            )
        stamps.append(now)

    def status(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "backend": self.store.backend_name,
            "maxMessageBytes": self.config.max_message_bytes,
            "maxTasksPerPrincipal": self.config.max_tasks_per_principal,
            "leaseTtlSeconds": self.config.lease_ttl_seconds,
            "rateLimitPerTask": self.config.rate_limit_per_task,
            **self.hub.status(),
        }


def _task_to_wire(task: Task) -> dict[str, Any]:
    return {
        "task_id": task.id,
        "owner_id": task.owner_id,
        "owner_dataset": task.owner_dataset,
        "state": task.state,
        "title": task.title,
        "seq_counter": task.seq_counter,
        "checkpoint": task.checkpoint,
        "checkpoint_seq": task.checkpoint_seq,
        "lease_holder": task.lease_holder,
        "lease_fence": task.lease_fence,
        "lease_expires_at": task.lease_expires_at,
        "archived": task.archived_at is not None,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }