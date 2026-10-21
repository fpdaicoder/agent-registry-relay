"""Storage layer for the personal task-continuation data plane.

Two backends implement the same :class:`TasksStore` interface, mirroring
``groupchat/sqlstore.py``:

* :class:`MemoryStore` — process-local, used by the test suite so it needs no
  database. Not suitable for production.
* :class:`PostgresStore` — the real backend; ``psycopg`` is imported lazily.

The whole point of this module is the **fencing-token contract**: every
mutating operation that can push a task forward carries a ``lease_fence``, and
the fence is checked inside the same single ``UPDATE`` that advances state —
never in a separate ``SELECT``, which would reopen the split-brain the fence
exists to close. The three fence-sensitive writes share one failure shape:

* ``acquire`` unconditionally bumps the fence (``fence + 1``) provided the
  task is not terminal; the returned fence is the caller's new epoch.
* ``renew`` / ``release`` / ``checkpoint`` carry the caller's fence and match
  it in ``WHERE id = %s AND lease_fence = %s``; zero rows => ``409
  task_fence_stale``.
* ``append_message`` fuses the fence check and the sequence allocation into
  ``UPDATE tk_tasks SET seq_counter = seq_counter + 1
        WHERE id = %s AND lease_fence = %s RETURNING seq_counter``
  so a felled device can neither allocate a seq nor insert a row.

``lease_expires_at`` (the TTL) is *only* crash recovery, handled by
:meth:`reap_expired`; it is deliberately not part of the fence check because
clock skew across instances must never decide "who is the holder".
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Optional, Sequence

from .errors import TaskError
from .models import (
    TERMINAL_STATES,
    Task,
    TaskCursor,
    TaskMessage,
    TaskPage,
)


class TasksStore(ABC):
    """Persistence interface. See the module docstring for the fence contract."""

    backend_name: str = "abstract"

    async def start(self) -> None:  # pragma: no cover - optional hook
        return None

    async def stop(self) -> None:  # pragma: no cover - optional hook
        return None

    # ── Tasks ────────────────────────────────────────────────────────────

    @abstractmethod
    async def create_task(
        self,
        *,
        owner_id: str,
        owner_dataset: str,
        title: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> Task:
        """Insert the task row and its ``system`` bootstrap message (seq=1).

        Both writes happen in one transaction. The returned task has
        ``seq_counter == 1`` because the creation event occupies seq=1.
        """

    @abstractmethod
    async def get_task(self, task_id: int) -> Optional[Task]:
        ...

    @abstractmethod
    async def list_tasks_for(
        self, owner_id: str, *, state: Optional[str] = None, limit: int
    ) -> list[Task]:
        """The owner's non-archived tasks, optionally filtered to one state."""

    @abstractmethod
    async def update_task(
        self,
        task_id: int,
        *,
        title: Optional[str] = None,
        archived: Optional[bool] = None,
    ) -> Optional[Task]:
        ...

    @abstractmethod
    async def count_tasks_owned_by(self, owner_id: str) -> int:
        """Used to enforce ``max_tasks_per_principal``."""

    # ── Lease ────────────────────────────────────────────────────────────

    @abstractmethod
    async def acquire(
        self,
        task_id: int,
        *,
        device_id: str,
        ttl_seconds: int,
        now: float,
    ) -> Optional[Task]:
        """Bump the fence and hand the lease to ``device_id``.

        Unconditional except for the terminal-state guard. Returns ``None``
        when the task is missing or terminal; the caller distinguishes the two
        by resolving the task first.
        """

    @abstractmethod
    async def renew(
        self, task_id: int, *, fence: int, ttl_seconds: int, now: float
    ) -> None:
        """Extend ``lease_expires_at``. Raises 409 when the fence is stale."""

    @abstractmethod
    async def release(
        self,
        task_id: int,
        *,
        fence: int,
        next_state: str,
        checkpoint: Optional[dict[str, Any]],
        checkpoint_seq: int,
        now: float,
    ) -> None:
        """Hand the lease back. Checkpoint and release are one atomic UPDATE."""

    @abstractmethod
    async def checkpoint(
        self,
        task_id: int,
        *,
        fence: int,
        checkpoint: dict[str, Any],
        checkpoint_seq: int,
    ) -> None:
        """Write the authoritative resume point. Fence-guarded."""

    @abstractmethod
    async def reap_expired(self, now: float) -> int:
        """Release every lease whose TTL has elapsed. Crash recovery only."""

    # ── Log ──────────────────────────────────────────────────────────────

    @abstractmethod
    async def append_message(
        self,
        task_id: int,
        *,
        sender_id: str,
        device_id: Optional[str],
        kind: str,
        payload: dict[str, Any],
        fence: int,
        client_msg_id: Optional[str] = None,
        reply_to: Optional[int] = None,
    ) -> TaskMessage:
        """Allocate the next seq and insert, atomically and fence-guarded.

        Idempotent on ``(task_id, sender_id, client_msg_id)``: a repeat
        returns the previously stored message instead of inserting a duplicate.
        """

    @abstractmethod
    async def fetch_page(self, task_id: int, *, after_seq: int, limit: int) -> TaskPage:
        ...

    @abstractmethod
    async def fetch_reverse_page(
        self, task_id: int, *, before_seq: int, limit: int
    ) -> TaskPage:
        """Backwards paging; cursor-based, never OFFSET."""

    @abstractmethod
    async def latest_seq(self, task_id: int) -> int:
        ...

    # ── Cursor ───────────────────────────────────────────────────────────

    @abstractmethod
    async def advance_cursor(self, task_id: int, device_id: str, seq: int) -> int:
        """Raise ``last_read_seq`` to ``seq`` if that is an increase."""

    @abstractmethod
    async def get_cursor(self, task_id: int, device_id: str) -> Optional[int]:
        """The device's read watermark, or ``None`` if it has never read this
        task. A brand-new device falls back to ``checkpoint_seq`` (the
        authoritative resume point) rather than replaying from seq=1.
        """

    # ── Retirement ───────────────────────────────────────────────────────

    @abstractmethod
    async def retire(self, task_id: int) -> bool:
        """Delete the intermediate state after a terminal task is done.

        Removes ``tk_messages``, ``tk_cursors`` and the checkpoint, but keeps
        the ``tk_tasks`` row (which is retired via ``archived_at`` instead).
        Returns False when the task does not exist.
        """


# ─────────────────────────────────────────────────────────────────────────
# In-memory backend (tests)
# ─────────────────────────────────────────────────────────────────────────


class MemoryStore(TasksStore):
    """Process-local store. Correct under one event loop, nothing more."""

    backend_name = "memory"

    def __init__(self) -> None:
        self._tasks: dict[int, Task] = {}
        self._messages: dict[int, list[TaskMessage]] = {}
        self._cursors: dict[tuple[int, str], int] = {}
        self._idempotency: dict[tuple[int, str, str], TaskMessage] = {}
        self._next_task_id = 1
        self._lock = asyncio.Lock()

    def _guard_fence(self, task_id: int, fence: int) -> Task:
        """Fetch the task and reject a stale fence; 404 when it is gone."""
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskError(404, "task_not_found", "Task not found")
        if task.lease_fence != fence:
            raise TaskError(
                409,
                "task_fence_stale",
                f"Task fence is {task.lease_fence}, caller used {fence}",
            )
        return task

    async def create_task(
        self,
        *,
        owner_id: str,
        owner_dataset: str,
        title: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> Task:
        async with self._lock:
            now = time.time()
            task = Task(
                id=self._next_task_id,
                owner_id=owner_id,
                owner_dataset=owner_dataset,
                title=title,
                seq_counter=1,
                created_at=now,
                updated_at=now,
            )
            self._next_task_id += 1
            self._tasks[task.id] = task
            event = {"event": "task_created", "title": title}
            if payload is not None:
                event["payload"] = payload
            self._messages[task.id] = [
                TaskMessage(
                    task_id=task.id,
                    seq=1,
                    sender_id=owner_id,
                    kind="system",
                    payload=event,
                    message_id=uuid.uuid4().hex,
                    created_at=now,
                )
            ]
            return task

    async def get_task(self, task_id: int) -> Optional[Task]:
        return self._tasks.get(task_id)

    async def list_tasks_for(
        self, owner_id: str, *, state: Optional[str] = None, limit: int
    ) -> list[Task]:
        found = [
            task
            for task in self._tasks.values()
            if task.owner_id == owner_id
            and task.archived_at is None
            and (state is None or task.state == state)
        ]
        found.sort(key=lambda task: task.id, reverse=True)
        return found[:limit]

    async def update_task(
        self,
        task_id: int,
        *,
        title: Optional[str] = None,
        archived: Optional[bool] = None,
    ) -> Optional[Task]:
        task = self._tasks.get(task_id)
        if task is None:
            return None
        if title is not None:
            task.title = title
        if archived is not None:
            task.archived_at = time.time() if archived else None
        return task

    async def count_tasks_owned_by(self, owner_id: str) -> int:
        return sum(
            1
            for task in self._tasks.values()
            if task.owner_id == owner_id and task.archived_at is None
        )

    async def acquire(
        self,
        task_id: int,
        *,
        device_id: str,
        ttl_seconds: int,
        now: float,
    ) -> Optional[Task]:
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task.state in TERMINAL_STATES:
                return None
            task.lease_fence += 1
            task.lease_holder = device_id
            task.lease_expires_at = now + ttl_seconds
            task.state = "working"
            task.updated_at = now
            return task

    async def renew(
        self, task_id: int, *, fence: int, ttl_seconds: int, now: float
    ) -> None:
        async with self._lock:
            task = self._guard_fence(task_id, fence)
            task.lease_expires_at = now + ttl_seconds
            task.updated_at = now

    async def release(
        self,
        task_id: int,
        *,
        fence: int,
        next_state: str,
        checkpoint: Optional[dict[str, Any]],
        checkpoint_seq: int,
        now: float,
    ) -> None:
        async with self._lock:
            task = self._guard_fence(task_id, fence)
            task.lease_holder = None
            task.lease_expires_at = None
            task.state = next_state
            task.checkpoint = checkpoint
            task.checkpoint_seq = checkpoint_seq
            task.updated_at = now

    async def checkpoint(
        self,
        task_id: int,
        *,
        fence: int,
        checkpoint: dict[str, Any],
        checkpoint_seq: int,
    ) -> None:
        async with self._lock:
            task = self._guard_fence(task_id, fence)
            task.checkpoint = checkpoint
            task.checkpoint_seq = checkpoint_seq
            task.updated_at = time.time()

    async def reap_expired(self, now: float) -> int:
        async with self._lock:
            reaped = 0
            for task in self._tasks.values():
                if task.lease_holder is not None and task.lease_expires_at is not None:
                    if task.lease_expires_at <= now:
                        task.lease_holder = None
                        task.lease_expires_at = None
                        task.state = "input_required"
                        task.updated_at = now
                        reaped += 1
            return reaped

    async def append_message(
        self,
        task_id: int,
        *,
        sender_id: str,
        device_id: Optional[str],
        kind: str,
        payload: dict[str, Any],
        fence: int,
        client_msg_id: Optional[str] = None,
        reply_to: Optional[int] = None,
    ) -> TaskMessage:
        async with self._lock:
            if client_msg_id:
                existing = self._idempotency.get((task_id, sender_id, client_msg_id))
                if existing is not None:
                    return existing
            task = self._guard_fence(task_id, fence)
            now = time.time()
            task.seq_counter += 1
            message = TaskMessage(
                task_id=task_id,
                seq=task.seq_counter,
                sender_id=sender_id,
                device_id=device_id,
                kind=kind,
                payload=payload,
                message_id=uuid.uuid4().hex,
                client_msg_id=client_msg_id,
                reply_to=reply_to,
                created_at=now,
            )
            self._messages.setdefault(task_id, []).append(message)
            if client_msg_id:
                self._idempotency[(task_id, sender_id, client_msg_id)] = message
            return message

    async def fetch_page(self, task_id: int, *, after_seq: int, limit: int) -> TaskPage:
        log = self._messages.get(task_id, [])
        latest = self._tasks[task_id].seq_counter if task_id in self._tasks else 0
        window = [m for m in log if m.seq > after_seq]
        selected = window[: limit + 1]
        has_more = len(selected) > limit
        selected = selected[:limit]
        next_seq = selected[-1].seq if selected else after_seq
        return TaskPage(
            messages=selected, next_seq=next_seq, has_more=has_more, latest_seq=latest
        )

    async def fetch_reverse_page(
        self, task_id: int, *, before_seq: int, limit: int
    ) -> TaskPage:
        log = self._messages.get(task_id, [])
        latest = self._tasks[task_id].seq_counter if task_id in self._tasks else 0
        window = [m for m in log if m.seq < before_seq]
        selected = window[-limit:]
        has_more = len(window) > limit
        next_seq = selected[0].seq if selected else before_seq
        return TaskPage(
            messages=selected, next_seq=next_seq, has_more=has_more, latest_seq=latest
        )

    async def latest_seq(self, task_id: int) -> int:
        task = self._tasks.get(task_id)
        return task.seq_counter if task is not None else 0

    async def advance_cursor(self, task_id: int, device_id: str, seq: int) -> int:
        if task_id not in self._tasks:
            raise TaskError(404, "task_not_found", "Task not found")
        current = self._cursors.get((task_id, device_id), 0)
        if seq > current:
            self._cursors[(task_id, device_id)] = seq
            current = seq
        return current

    async def get_cursor(self, task_id: int, device_id: str) -> Optional[int]:
        if task_id not in self._tasks:
            return None
        return self._cursors.get((task_id, device_id))

    async def retire(self, task_id: int) -> bool:
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            self._messages.pop(task_id, None)
            self._cursors = {k: v for k, v in self._cursors.items() if k[0] != task_id}
            for key in [k for k in self._idempotency if k[0] == task_id]:
                del self._idempotency[key]
            task.checkpoint = None
            task.checkpoint_seq = 0
            return True


# ─────────────────────────────────────────────────────────────────────────
# PostgreSQL backend
# ─────────────────────────────────────────────────────────────────────────


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tk_tasks (
  id               bigserial   PRIMARY KEY,
  owner_id         text        NOT NULL,
  owner_dataset    text        NOT NULL,
  state            text        NOT NULL DEFAULT 'pending',
  title            text        NOT NULL DEFAULT '',
  seq_counter      bigint      NOT NULL DEFAULT 0,
  checkpoint       jsonb,
  checkpoint_seq   bigint      NOT NULL DEFAULT 0,
  lease_holder     text,
  lease_fence      bigint      NOT NULL DEFAULT 0,
  lease_expires_at timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  archived_at      timestamptz
);

CREATE TABLE IF NOT EXISTS tk_messages (
  id            bigserial   PRIMARY KEY,
  task_id       bigint      NOT NULL REFERENCES tk_tasks(id) ON DELETE CASCADE,
  seq           bigint      NOT NULL,
  sender_id     text        NOT NULL,
  device_id     text,
  kind          text        NOT NULL,
  payload       jsonb       NOT NULL,
  message_id    text        NOT NULL,
  client_msg_id text,
  reply_to      bigint,
  created_at    timestamptz NOT NULL DEFAULT now(),
  deleted_at    timestamptz,
  UNIQUE (task_id, seq)
);

-- Idempotency scope includes task_id: an Agent reusing a deterministic
-- client_msg_id after a restart must not collide with an unrelated task.
CREATE UNIQUE INDEX IF NOT EXISTS tk_messages_idem
  ON tk_messages (task_id, sender_id, client_msg_id)
  WHERE client_msg_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS tk_messages_task_seq_desc
  ON tk_messages (task_id, seq DESC);

CREATE TABLE IF NOT EXISTS tk_cursors (
  task_id       bigint NOT NULL REFERENCES tk_tasks(id) ON DELETE CASCADE,
  device_id     text   NOT NULL,
  last_read_seq bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (task_id, device_id)
);

CREATE INDEX IF NOT EXISTS tk_tasks_owner ON tk_tasks (owner_id, id DESC);
CREATE INDEX IF NOT EXISTS tk_tasks_state ON tk_tasks (state);
CREATE INDEX IF NOT EXISTS tk_tasks_lease_expires ON tk_tasks (lease_expires_at)
  WHERE lease_holder IS NOT NULL;
"""

_TERMINAL_SQL = "state NOT IN ('completed', 'failed', 'canceled')"


def _iso(epoch: Optional[float]) -> Optional[str]:
    if epoch is None:
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _epoch(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return value.timestamp()


def _json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


class PostgresStore(TasksStore):
    """PostgreSQL-backed store. Requires the optional ``psycopg`` driver.

    Uses ``psycopg`` (v3) in async mode. Every method acquires a connection
    for the duration of one transaction.
    """

    backend_name = "postgres"

    def __init__(self, dsn: str, *, schema: str = "public") -> None:
        self._dsn = dsn
        self._schema = schema
        self._pool: Any = None

    async def start(self) -> None:
        try:
            from psycopg_pool import AsyncConnectionPool
        except ImportError as exc:  # pragma: no cover - driver is optional
            raise TaskError(
                500,
                "tasks_driver_missing",
                "The postgres backend requires 'psycopg[binary,pool]' to be installed",
            ) from exc

        self._pool = AsyncConnectionPool(
            self._dsn,
            min_size=1,
            max_size=10,
            open=False,
            kwargs={"autocommit": True},
        )
        await self._pool.open(wait=True, timeout=10)
        await self._ensure_schema()

    async def stop(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            await pool.close()

    async def _ensure_schema(self) -> None:
        async with self._pool.connection() as conn:
            if self._schema != "public":
                await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"')
                await conn.execute(f'SET search_path TO "{self._schema}"')
            for statement in _SCHEMA_SQL.split(";"):
                if statement.strip():
                    await conn.execute(statement)

    @staticmethod
    async def _fence_failure(conn: Any, task_id: int, fence: int) -> TaskError:
        """Turn a 0-row fence-guarded write into the right structured error."""
        row = await (
            await conn.execute(
                "SELECT lease_fence FROM tk_tasks WHERE id = %s", (task_id,)
            )
        ).fetchone()
        if row is None:
            return TaskError(404, "task_not_found", "Task not found")
        return TaskError(
            409,
            "task_fence_stale",
            f"Task fence is {int(row['lease_fence'])}, caller used {fence}",
        )

    # ── Tasks ────────────────────────────────────────────────────────────

    async def create_task(
        self,
        *,
        owner_id: str,
        owner_dataset: str,
        title: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> Task:
        now = time.time()
        event = {"event": "task_created", "title": title}
        if payload is not None:
            event["payload"] = payload
        async with self._pool.connection() as conn:
            async with conn.transaction():
                row = await (
                    await conn.execute(
                        """
                        INSERT INTO tk_tasks
                          (owner_id, owner_dataset, title, seq_counter)
                        VALUES (%s, %s, %s, 1)
                        RETURNING *
                        """,
                        (owner_id, owner_dataset, title),
                    )
                ).fetchone()
                task = _row_to_task(row)
                await conn.execute(
                    """
                    INSERT INTO tk_messages
                      (task_id, seq, sender_id, kind, payload, message_id, created_at)
                    VALUES (%s, 1, %s, 'system', %s, %s, %s)
                    """,
                    (task.id, owner_id, json.dumps(event), uuid.uuid4().hex, _iso(now)),
                )
        return task

    async def get_task(self, task_id: int) -> Optional[Task]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute("SELECT * FROM tk_tasks WHERE id = %s", (task_id,))
            ).fetchone()
        return _row_to_task(row) if row else None

    async def list_tasks_for(
        self, owner_id: str, *, state: Optional[str] = None, limit: int
    ) -> list[Task]:
        if state is not None:
            sql = (
                "SELECT * FROM tk_tasks WHERE owner_id = %s AND state = %s "
                "AND archived_at IS NULL ORDER BY id DESC LIMIT %s"
            )
            params: Sequence[Any] = (owner_id, state, limit)
        else:
            sql = (
                "SELECT * FROM tk_tasks WHERE owner_id = %s AND archived_at IS NULL "
                "ORDER BY id DESC LIMIT %s"
            )
            params = (owner_id, limit)
        async with self._pool.connection() as conn:
            rows = await (await conn.execute(sql, params)).fetchall()
        return [_row_to_task(row) for row in rows]

    async def update_task(
        self,
        task_id: int,
        *,
        title: Optional[str] = None,
        archived: Optional[bool] = None,
    ) -> Optional[Task]:
        assignments: list[str] = []
        params: list[Any] = []
        if title is not None:
            assignments.append("title = %s")
            params.append(title)
        if archived is not None:
            assignments.append("archived_at = %s")
            params.append(_iso(time.time()) if archived else None)
        if not assignments:
            return await self.get_task(task_id)
        params.append(task_id)
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    f"UPDATE tk_tasks SET {', '.join(assignments)} "
                    "WHERE id = %s RETURNING *",
                    tuple(params),
                )
            ).fetchone()
        return _row_to_task(row) if row else None

    async def count_tasks_owned_by(self, owner_id: str) -> int:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT count(*) AS n FROM tk_tasks "
                    "WHERE owner_id = %s AND archived_at IS NULL",
                    (owner_id,),
                )
            ).fetchone()
        return int(row["n"]) if row else 0

    # ── Lease ────────────────────────────────────────────────────────────

    async def acquire(
        self,
        task_id: int,
        *,
        device_id: str,
        ttl_seconds: int,
        now: float,
    ) -> Optional[Task]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    f"""
                    UPDATE tk_tasks
                    SET lease_fence = lease_fence + 1,
                        lease_holder = %s,
                        lease_expires_at = %s,
                        state = 'working',
                        updated_at = now()
                    WHERE id = %s AND {_TERMINAL_SQL}
                    RETURNING *
                    """,
                    (device_id, _iso(now + ttl_seconds), task_id),
                )
            ).fetchone()
        return _row_to_task(row) if row else None

    async def renew(
        self, task_id: int, *, fence: int, ttl_seconds: int, now: float
    ) -> None:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    UPDATE tk_tasks
                    SET lease_expires_at = %s, updated_at = now()
                    WHERE id = %s AND lease_fence = %s
                    RETURNING id
                    """,
                    (_iso(now + ttl_seconds), task_id, fence),
                )
            ).fetchone()
        if row is None:
            raise await self._fence_failure_conn(task_id, fence)

    async def release(
        self,
        task_id: int,
        *,
        fence: int,
        next_state: str,
        checkpoint: Optional[dict[str, Any]],
        checkpoint_seq: int,
        now: float,
    ) -> None:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    UPDATE tk_tasks
                    SET lease_holder = NULL,
                        lease_expires_at = NULL,
                        state = %s,
                        checkpoint = %s,
                        checkpoint_seq = %s,
                        updated_at = now()
                    WHERE id = %s AND lease_fence = %s
                    RETURNING id
                    """,
                    (
                        next_state,
                        json.dumps(checkpoint) if checkpoint is not None else None,
                        checkpoint_seq,
                        task_id,
                        fence,
                    ),
                )
            ).fetchone()
        if row is None:
            raise await self._fence_failure_conn(task_id, fence)

    async def checkpoint(
        self,
        task_id: int,
        *,
        fence: int,
        checkpoint: dict[str, Any],
        checkpoint_seq: int,
    ) -> None:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    UPDATE tk_tasks
                    SET checkpoint = %s, checkpoint_seq = %s, updated_at = now()
                    WHERE id = %s AND lease_fence = %s
                    RETURNING id
                    """,
                    (json.dumps(checkpoint), checkpoint_seq, task_id, fence),
                )
            ).fetchone()
        if row is None:
            raise await self._fence_failure_conn(task_id, fence)

    async def reap_expired(self, now: float) -> int:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    UPDATE tk_tasks
                    SET lease_holder = NULL,
                        lease_expires_at = NULL,
                        state = 'input_required',
                        updated_at = now()
                    WHERE lease_holder IS NOT NULL AND lease_expires_at < %s
                    RETURNING id
                    """,
                    (_iso(now),),
                )
            ).fetchall()
        return len(row)

    # ── Log ──────────────────────────────────────────────────────────────

    async def append_message(
        self,
        task_id: int,
        *,
        sender_id: str,
        device_id: Optional[str],
        kind: str,
        payload: dict[str, Any],
        fence: int,
        client_msg_id: Optional[str] = None,
        reply_to: Optional[int] = None,
    ) -> TaskMessage:
        now = time.time()
        async with self._pool.connection() as conn:
            async with conn.transaction():
                if client_msg_id:
                    existing = await (
                        await conn.execute(
                            "SELECT * FROM tk_messages WHERE task_id = %s "
                            "AND sender_id = %s AND client_msg_id = %s",
                            (task_id, sender_id, client_msg_id),
                        )
                    ).fetchone()
                    if existing is not None:
                        return _row_to_message(existing)

                # Fence check fused with sequence allocation: a felled device
                # matches no row and can neither take a seq nor insert a message.
                seq_row = await (
                    await conn.execute(
                        "UPDATE tk_tasks SET seq_counter = seq_counter + 1 "
                        "WHERE id = %s AND lease_fence = %s RETURNING seq_counter",
                        (task_id, fence),
                    )
                ).fetchone()
                if seq_row is None:
                    current = await (
                        await conn.execute(
                            "SELECT lease_fence FROM tk_tasks WHERE id = %s", (task_id,)
                        )
                    ).fetchone()
                    if current is None:
                        raise TaskError(404, "task_not_found", "Task not found")
                    raise TaskError(
                        409,
                        "task_fence_stale",
                        f"Task fence is {int(current['lease_fence'])}, "
                        f"caller used {fence}",
                    )
                seq = int(seq_row["seq_counter"])
                message_id = uuid.uuid4().hex
                row = await (
                    await conn.execute(
                        """
                        INSERT INTO tk_messages
                          (task_id, seq, sender_id, device_id, kind, payload,
                           message_id, client_msg_id, reply_to, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            task_id,
                            seq,
                            sender_id,
                            device_id,
                            kind,
                            json.dumps(payload),
                            message_id,
                            client_msg_id,
                            reply_to,
                            _iso(now),
                        ),
                    )
                ).fetchone()
        return _row_to_message(row)

    async def fetch_page(self, task_id: int, *, after_seq: int, limit: int) -> TaskPage:
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM tk_messages WHERE task_id = %s AND seq > %s "
                    "ORDER BY seq ASC LIMIT %s",
                    (task_id, after_seq, limit + 1),
                )
            ).fetchall()
            latest = await PostgresStore._latest_seq(conn, task_id)
            has_more = len(rows) > limit
            rows = rows[:limit]
            messages = [_row_to_message(row) for row in rows]
        next_seq = messages[-1].seq if messages else after_seq
        return TaskPage(
            messages=messages, next_seq=next_seq, has_more=has_more, latest_seq=latest
        )

    async def fetch_reverse_page(
        self, task_id: int, *, before_seq: int, limit: int
    ) -> TaskPage:
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM tk_messages WHERE task_id = %s AND seq < %s "
                    "ORDER BY seq DESC LIMIT %s",
                    (task_id, before_seq, limit + 1),
                )
            ).fetchall()
            latest = await PostgresStore._latest_seq(conn, task_id)
            has_more = len(rows) > limit
            rows = rows[:limit]
            messages = [_row_to_message(row) for row in reversed(rows)]
        next_seq = messages[0].seq if messages else before_seq
        return TaskPage(
            messages=messages, next_seq=next_seq, has_more=has_more, latest_seq=latest
        )

    async def latest_seq(self, task_id: int) -> int:
        async with self._pool.connection() as conn:
            return await PostgresStore._latest_seq(conn, task_id)

    @staticmethod
    async def _latest_seq(conn: Any, task_id: int) -> int:
        row = await (
            await conn.execute(
                "SELECT seq_counter FROM tk_tasks WHERE id = %s", (task_id,)
            )
        ).fetchone()
        return int(row["seq_counter"]) if row else 0

    # ── Cursor ───────────────────────────────────────────────────────────

    async def advance_cursor(self, task_id: int, device_id: str, seq: int) -> int:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    INSERT INTO tk_cursors (task_id, device_id, last_read_seq)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (task_id, device_id) DO UPDATE SET
                      last_read_seq = GREATEST(tk_cursors.last_read_seq, EXCLUDED.last_read_seq)
                    RETURNING last_read_seq
                    """,
                    (task_id, device_id, seq),
                )
            ).fetchone()
        if row is None:
            raise TaskError(404, "task_not_found", "Task not found")
        return int(row["last_read_seq"])

    async def get_cursor(self, task_id: int, device_id: str) -> Optional[int]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT last_read_seq FROM tk_cursors "
                    "WHERE task_id = %s AND device_id = %s",
                    (task_id, device_id),
                )
            ).fetchone()
        return int(row["last_read_seq"]) if row else None

    # ── Retirement ───────────────────────────────────────────────────────

    async def retire(self, task_id: int) -> bool:
        async with self._pool.connection() as conn:
            async with conn.transaction():
                task = await (
                    await conn.execute(
                        "SELECT id FROM tk_tasks WHERE id = %s", (task_id,)
                    )
                ).fetchone()
                if task is None:
                    return False
                await conn.execute("DELETE FROM tk_messages WHERE task_id = %s", (task_id,))
                await conn.execute("DELETE FROM tk_cursors WHERE task_id = %s", (task_id,))
                await conn.execute(
                    "UPDATE tk_tasks SET checkpoint = NULL, checkpoint_seq = 0, "
                    "updated_at = now() WHERE id = %s",
                    (task_id,),
                )
        return True

    async def _fence_failure_conn(self, task_id: int, fence: int) -> TaskError:
        async with self._pool.connection() as conn:
            return await PostgresStore._fence_failure(conn, task_id, fence)


def _row_to_task(row: Any) -> Task:
    return Task(
        id=int(row["id"]),
        owner_id=row["owner_id"],
        owner_dataset=row["owner_dataset"],
        state=row["state"],
        title=row["title"],
        seq_counter=int(row["seq_counter"]),
        checkpoint=_json(row["checkpoint"]),
        checkpoint_seq=int(row["checkpoint_seq"]),
        lease_holder=row["lease_holder"],
        lease_fence=int(row["lease_fence"]),
        lease_expires_at=_epoch(row["lease_expires_at"]),
        archived_at=_epoch(row["archived_at"]),
        created_at=_epoch(row["created_at"]) or 0.0,
        updated_at=_epoch(row["updated_at"]) or 0.0,
    )


def _row_to_message(row: Any) -> TaskMessage:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return TaskMessage(
        task_id=int(row["task_id"]),
        seq=int(row["seq"]),
        sender_id=row["sender_id"],
        device_id=row["device_id"],
        kind=row["kind"],
        payload=payload or {},
        message_id=row["message_id"],
        client_msg_id=row["client_msg_id"],
        reply_to=int(row["reply_to"]) if row["reply_to"] is not None else None,
        created_at=_epoch(row["created_at"]) or 0.0,
        deleted_at=_epoch(row["deleted_at"]),
    )


def build_store(config: Any) -> TasksStore:
    """Construct the store named by the configuration."""
    if config.backend == "memory":
        return MemoryStore()
    return PostgresStore(config.dsn, schema=config.schema)