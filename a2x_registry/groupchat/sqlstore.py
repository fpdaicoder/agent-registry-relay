"""Storage layer for the group chat data plane.

Two backends implement the same :class:`GroupChatStore` interface:

* :class:`MemoryStore` — process-local, used by the test suite so that it
  needs no database. It is *not* suitable for production: it does not
  survive a restart, and its concurrency guarantees hold only inside one
  event loop.
* :class:`PostgresStore` — the real backend. ``psycopg`` is an optional
  dependency, imported lazily so that a deployment without PostgreSQL never
  needs the driver installed.

The interface is deliberately narrow. Two rules matter for correctness and
are enforced by both backends rather than by callers:

1. **Sequence allocation and message insertion are one atomic unit.**
   ``append_message`` takes the next ``seq`` and inserts the row inside a
   single transaction, holding a row lock on the group. Callers never
   allocate a sequence themselves, so two concurrent senders cannot collide.
2. **An invitation's ``used_count`` is incremented under the same condition
   that checks it.** ``consume_invite`` performs a conditional increment and
   reports failure when no row matched, which is what makes a single-use
   invite single-use under concurrency.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import secrets
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Optional, Sequence

from .errors import GroupChatError
from .models import (
    Group,
    Invite,
    Membership,
    Message,
    Page,
)


class GroupChatStore(ABC):
    """Persistence interface. See the module docstring for the two invariants."""

    backend_name: str = "abstract"

    async def start(self) -> None:  # pragma: no cover - optional hook
        return None

    async def stop(self) -> None:  # pragma: no cover - optional hook
        return None

    # ── Groups ───────────────────────────────────────────────────────────

    @abstractmethod
    async def create_group(
        self,
        *,
        name: str,
        owner_id: str,
        owner_dataset: str,
        join_policy: str,
        max_members: int,
    ) -> Group:
        """Create the group with its owner membership and bootstrap event.

        All three writes happen in one transaction. A partially-created
        group would be unrecoverable — nobody could be added to it, because
        no member exists who could invite.
        """

    @abstractmethod
    async def get_group(self, group_id: int) -> Optional[Group]:
        ...

    @abstractmethod
    async def list_groups_for(self, principal_id: str, *, limit: int) -> list[Group]:
        """Groups this principal is an active member of."""

    @abstractmethod
    async def update_group(
        self,
        group_id: int,
        *,
        name: Optional[str] = None,
        join_policy: Optional[str] = None,
        muted: Optional[bool] = None,
        archived: Optional[bool] = None,
    ) -> Optional[Group]:
        ...

    @abstractmethod
    async def delete_group(self, group_id: int) -> bool:
        """Physically remove a group and everything it owns.

        Returns False when the group did not exist. The members, invitations,
        messages and mentions rows go with it — every one of them is scoped to
        this group and meaningless without it. This is irreversible: only the
        owner may reach it, and the service layer records the act in the audit
        log before calling here.
        """

    @abstractmethod
    async def count_groups_owned_by(self, principal_id: str) -> int:
        """Used to enforce ``max_groups_per_principal``."""

    # ── Memberships ──────────────────────────────────────────────────────

    @abstractmethod
    async def get_membership(
        self, group_id: int, principal_id: str
    ) -> Optional[Membership]:
        ...

    @abstractmethod
    async def list_members(
        self, group_id: int, *, statuses: Sequence[str] | None = None
    ) -> list[Membership]:
        ...

    @abstractmethod
    async def upsert_membership(self, membership: Membership) -> Membership:
        ...

    @abstractmethod
    async def set_membership_status(
        self,
        group_id: int,
        principal_id: str,
        *,
        status: str,
        role: Optional[str] = None,
        joined_at: Optional[float] = None,
    ) -> Optional[Membership]:
        ...

    @abstractmethod
    async def count_members(self, group_id: int) -> int:
        """Count of members occupying a seat (``invited`` + ``active``)."""

    @abstractmethod
    async def advance_cursor(
        self, group_id: int, principal_id: str, seq: int
    ) -> int:
        """Raise ``last_read_seq`` to ``seq`` if that is an increase.

        Returns the resulting cursor. Monotonic by construction: a late ACK
        from a second device must not rewind the cursor and cause the whole
        history to be replayed.
        """

    # ── Invitations ──────────────────────────────────────────────────────

    @abstractmethod
    async def create_invite(self, invite: Invite) -> Invite:
        ...

    @abstractmethod
    async def get_invite(self, invite_id: str) -> Optional[Invite]:
        ...

    @abstractmethod
    async def find_invite_by_token_hash(self, token_hash: str) -> Optional[Invite]:
        """Look an invitation up by the digest of its token.

        Tokens are high-entropy, so a plain equality lookup on the digest is
        safe; the caller still compares with ``hmac.compare_digest`` to keep
        the code path uniform with the other secret comparisons.
        """

    @abstractmethod
    async def consume_invite(self, invite_id: str, *, now: float) -> bool:
        """Atomically redeem one use. Returns False if it was already exhausted."""

    # ── Log ──────────────────────────────────────────────────────────────

    @abstractmethod
    async def append_message(
        self,
        group_id: int,
        *,
        sender_id: str,
        kind: str,
        payload: dict[str, Any],
        client_msg_id: Optional[str] = None,
        mentions: Sequence[str] = (),
        reply_to: Optional[int] = None,
    ) -> Message:
        """Allocate the next ``seq`` and insert, atomically.

        Idempotent on ``(group_id, sender_id, client_msg_id)``: a repeat
        returns the previously stored message instead of inserting a duplicate.
        """

    @abstractmethod
    async def fetch_page(
        self,
        group_id: int,
        *,
        after_seq: int,
        limit: int,
    ) -> Page:
        ...

    @abstractmethod
    async def fetch_reverse_page(
        self,
        group_id: int,
        *,
        before_seq: int,
        limit: int,
    ) -> Page:
        """Backwards paging for history scrollback; still cursor-based, never OFFSET."""

    @abstractmethod
    async def latest_seq(self, group_id: int) -> int:
        ...


# ─────────────────────────────────────────────────────────────────────────
# In-memory backend (tests)
# ─────────────────────────────────────────────────────────────────────────


class MemoryStore(GroupChatStore):
    """Process-local store. Correct under one event loop, nothing more."""

    backend_name = "memory"

    def __init__(self) -> None:
        self._groups: dict[int, Group] = {}
        self._members: dict[tuple[int, str], Membership] = {}
        self._invites: dict[str, Invite] = {}
        self._messages: dict[int, list[Message]] = {}
        self._idempotency: dict[tuple[int, str, str], Message] = {}
        self._next_group_id = 1
        self._lock = asyncio.Lock()

    async def create_group(
        self,
        *,
        name: str,
        owner_id: str,
        owner_dataset: str,
        join_policy: str,
        max_members: int,
    ) -> Group:
        async with self._lock:
            now = time.time()
            group = Group(
                id=self._next_group_id,
                name=name,
                owner_id=owner_id,
                owner_dataset=owner_dataset,
                join_policy=join_policy,
                max_members=max_members,
                seq_counter=1,
                created_at=now,
            )
            self._next_group_id += 1
            self._groups[group.id] = group
            self._members[(group.id, owner_id)] = Membership(
                group_id=group.id,
                principal_id=owner_id,
                role="owner",
                status="active",
                joined_at=now,
                last_read_seq=1,
            )
            self._messages[group.id] = [
                Message(
                    group_id=group.id,
                    seq=1,
                    sender_id=owner_id,
                    kind="system",
                    payload={"event": "group_created", "name": name},
                    message_id=uuid.uuid4().hex,
                    created_at=now,
                )
            ]
            return group

    async def get_group(self, group_id: int) -> Optional[Group]:
        return self._groups.get(group_id)

    async def list_groups_for(self, principal_id: str, *, limit: int) -> list[Group]:
        found = [
            self._groups[key[0]]
            for key, member in self._members.items()
            if key[1] == principal_id
            and member.status == "active"
            and key[0] in self._groups
            # An archived group is retired: it stays readable by direct id but
            # drops out of every listing, the same way it does in PostgreSQL.
            and self._groups[key[0]].archived_at is None
        ]
        found.sort(key=lambda group: group.id, reverse=True)
        return found[:limit]

    async def update_group(
        self,
        group_id: int,
        *,
        name: Optional[str] = None,
        join_policy: Optional[str] = None,
        muted: Optional[bool] = None,
        archived: Optional[bool] = None,
    ) -> Optional[Group]:
        group = self._groups.get(group_id)
        if group is None:
            return None
        if name is not None:
            group.name = name
        if join_policy is not None:
            group.join_policy = join_policy
        if muted is not None:
            group.muted = muted
        if archived is not None:
            group.archived_at = time.time() if archived else None
        return group

    async def count_groups_owned_by(self, principal_id: str) -> int:
        return sum(
            1
            for group in self._groups.values()
            if group.owner_id == principal_id and group.archived_at is None
        )

    async def delete_group(self, group_id: int) -> bool:
        async with self._lock:
            if self._groups.pop(group_id, None) is None:
                return False
            for key in [k for k in self._members if k[0] == group_id]:
                del self._members[key]
            for invite_id in [
                i for i, inv in self._invites.items() if inv.group_id == group_id
            ]:
                del self._invites[invite_id]
            self._messages.pop(group_id, None)
            for key in [k for k in self._idempotency if k[0] == group_id]:
                del self._idempotency[key]
            return True

    async def get_membership(
        self, group_id: int, principal_id: str
    ) -> Optional[Membership]:
        return self._members.get((group_id, principal_id))

    async def list_members(
        self, group_id: int, *, statuses: Sequence[str] | None = None
    ) -> list[Membership]:
        members = [
            member for (gid, _), member in self._members.items() if gid == group_id
        ]
        if statuses is not None:
            allowed = set(statuses)
            members = [m for m in members if m.status in allowed]
        members.sort(key=lambda m: (m.joined_at or 0.0, m.principal_id))
        return members

    async def upsert_membership(self, membership: Membership) -> Membership:
        self._members[(membership.group_id, membership.principal_id)] = membership
        return membership

    async def set_membership_status(
        self,
        group_id: int,
        principal_id: str,
        *,
        status: str,
        role: Optional[str] = None,
        joined_at: Optional[float] = None,
    ) -> Optional[Membership]:
        membership = self._members.get((group_id, principal_id))
        if membership is None:
            return None
        membership.status = status
        if role is not None:
            membership.role = role
        if joined_at is not None:
            membership.joined_at = joined_at
        return membership

    async def count_members(self, group_id: int) -> int:
        return sum(
            1
            for (gid, _), member in self._members.items()
            if gid == group_id and member.status in ("invited", "active")
        )

    async def advance_cursor(self, group_id: int, principal_id: str, seq: int) -> int:
        membership = self._members.get((group_id, principal_id))
        if membership is None:
            raise GroupChatError(404, "groupchat_not_a_member", "Not a member of this group")
        if seq > membership.last_read_seq:
            membership.last_read_seq = seq
        return membership.last_read_seq

    async def create_invite(self, invite: Invite) -> Invite:
        self._invites[invite.id] = invite
        return invite

    async def get_invite(self, invite_id: str) -> Optional[Invite]:
        return self._invites.get(invite_id)

    async def find_invite_by_token_hash(self, token_hash: str) -> Optional[Invite]:
        for invite in self._invites.values():
            if hmac.compare_digest(invite.token_hash, token_hash):
                return invite
        return None

    async def consume_invite(self, invite_id: str, *, now: float) -> bool:
        async with self._lock:
            invite = self._invites.get(invite_id)
            if invite is None or invite.is_expired(now) or invite.is_exhausted():
                return False
            invite.used_count += 1
            return True

    async def append_message(
        self,
        group_id: int,
        *,
        sender_id: str,
        kind: str,
        payload: dict[str, Any],
        client_msg_id: Optional[str] = None,
        mentions: Sequence[str] = (),
        reply_to: Optional[int] = None,
    ) -> Message:
        async with self._lock:
            if client_msg_id:
                existing = self._idempotency.get((group_id, sender_id, client_msg_id))
                if existing is not None:
                    return existing

            group = self._groups.get(group_id)
            if group is None:
                raise GroupChatError(404, "groupchat_group_not_found", "Group not found")

            now = time.time()
            group.seq_counter += 1
            message = Message(
                group_id=group_id,
                seq=group.seq_counter,
                sender_id=sender_id,
                kind=kind,
                payload=payload,
                message_id=uuid.uuid4().hex,
                client_msg_id=client_msg_id,
                reply_to=reply_to,
                mentions=tuple(mentions),
                created_at=now,
            )
            self._messages.setdefault(group_id, []).append(message)
            if client_msg_id:
                self._idempotency[(group_id, sender_id, client_msg_id)] = message
            return message

    async def fetch_page(self, group_id: int, *, after_seq: int, limit: int) -> Page:
        log = self._messages.get(group_id, [])
        latest = self._groups[group_id].seq_counter if group_id in self._groups else 0
        window = [m for m in log if m.seq > after_seq]
        selected = window[: limit + 1]
        has_more = len(selected) > limit
        selected = selected[:limit]
        next_seq = selected[-1].seq if selected else after_seq
        return Page(
            messages=selected,
            next_seq=next_seq,
            has_more=has_more,
            latest_seq=latest,
        )

    async def fetch_reverse_page(
        self, group_id: int, *, before_seq: int, limit: int
    ) -> Page:
        log = self._messages.get(group_id, [])
        latest = self._groups[group_id].seq_counter if group_id in self._groups else 0
        window = [m for m in log if m.seq < before_seq]
        selected = window[-limit:]
        has_more = len(window) > limit
        next_seq = selected[0].seq if selected else before_seq
        return Page(
            messages=selected,
            next_seq=next_seq,
            has_more=has_more,
            latest_seq=latest,
        )

    async def latest_seq(self, group_id: int) -> int:
        group = self._groups.get(group_id)
        return group.seq_counter if group is not None else 0


# ─────────────────────────────────────────────────────────────────────────
# PostgreSQL backend
# ─────────────────────────────────────────────────────────────────────────


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS gc_groups (
  id            bigserial PRIMARY KEY,
  name          text        NOT NULL,
  owner_id      text        NOT NULL,
  owner_dataset text        NOT NULL,
  join_policy   text        NOT NULL DEFAULT 'invite_only',
  max_members   int         NOT NULL DEFAULT 500,
  seq_counter   bigint      NOT NULL DEFAULT 0,
  muted         boolean     NOT NULL DEFAULT false,
  archived_at   timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gc_members (
  group_id      bigint      NOT NULL REFERENCES gc_groups(id) ON DELETE CASCADE,
  principal_id  text        NOT NULL,
  role          text        NOT NULL,
  status        text        NOT NULL,
  muted         boolean     NOT NULL DEFAULT false,
  invited_by    text,
  dataset       text,
  service_id    text,
  joined_at     timestamptz,
  last_read_seq bigint      NOT NULL DEFAULT 0,
  PRIMARY KEY (group_id, principal_id)
);

CREATE TABLE IF NOT EXISTS gc_invites (
  id          uuid        PRIMARY KEY,
  group_id    bigint      NOT NULL REFERENCES gc_groups(id) ON DELETE CASCADE,
  inviter_id  text        NOT NULL,
  invitee_id  text,
  mode        text        NOT NULL,
  token_hash  text        NOT NULL UNIQUE,
  expires_at  timestamptz,
  max_uses    int,
  used_count  int         NOT NULL DEFAULT 0,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gc_messages (
  id            bigserial   PRIMARY KEY,
  group_id      bigint      NOT NULL REFERENCES gc_groups(id) ON DELETE CASCADE,
  seq           bigint      NOT NULL,
  sender_id     text        NOT NULL,
  kind          text        NOT NULL,
  payload       jsonb       NOT NULL,
  message_id    text        NOT NULL,
  client_msg_id text,
  reply_to      bigint,
  created_at    timestamptz NOT NULL DEFAULT now(),
  deleted_at    timestamptz,
  UNIQUE (group_id, seq)
);

CREATE TABLE IF NOT EXISTS gc_message_mentions (
  group_id     bigint NOT NULL,
  seq          bigint NOT NULL,
  principal_id text   NOT NULL,
  PRIMARY KEY (group_id, seq, principal_id),
  FOREIGN KEY (group_id, seq) REFERENCES gc_messages(group_id, seq) ON DELETE CASCADE
);

-- Idempotency scope includes group_id: an Agent reusing a deterministic
-- client_msg_id after a restart must not collide with an unrelated group.
CREATE UNIQUE INDEX IF NOT EXISTS gc_messages_idem
  ON gc_messages (group_id, sender_id, client_msg_id)
  WHERE client_msg_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS gc_messages_group_seq_desc
  ON gc_messages (group_id, seq DESC);
CREATE INDEX IF NOT EXISTS gc_members_principal
  ON gc_members (principal_id);
CREATE INDEX IF NOT EXISTS gc_mentions_principal
  ON gc_message_mentions (principal_id, group_id);
"""


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


class PostgresStore(GroupChatStore):
    """PostgreSQL-backed store. Requires the optional ``psycopg`` driver.

    Uses ``psycopg`` (v3) in async mode. Every method acquires a connection
    from the pool for the duration of one transaction; no connection state
    leaks between calls.
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
            raise GroupChatError(
                500,
                "groupchat_driver_missing",
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

    def _connect(self) -> Any:
        if self._pool is None:
            raise GroupChatError(
                503, "groupchat_store_unavailable", "Group chat store is not started"
            )
        return self._pool.connection()

    # ── Groups ───────────────────────────────────────────────────────────

    async def create_group(
        self,
        *,
        name: str,
        owner_id: str,
        owner_dataset: str,
        join_policy: str,
        max_members: int,
    ) -> Group:
        now = time.time()
        async with self._pool.connection() as conn:
            async with conn.transaction():
                row = await (
                    await conn.execute(
                        """
                        INSERT INTO gc_groups
                          (name, owner_id, owner_dataset, join_policy, max_members, seq_counter)
                        VALUES (%s, %s, %s, %s, %s, 1)
                        RETURNING *
                        """,
                        (name, owner_id, owner_dataset, join_policy, max_members),
                    )
                ).fetchone()
                group = _row_to_group(row)
                await conn.execute(
                    """
                    INSERT INTO gc_members
                      (group_id, principal_id, role, status, joined_at, last_read_seq)
                    VALUES (%s, %s, 'owner', 'active', %s, 1)
                    """,
                    (group.id, owner_id, _iso(now)),
                )
                await conn.execute(
                    """
                    INSERT INTO gc_messages
                      (group_id, seq, sender_id, kind, payload, message_id, created_at)
                    VALUES (%s, 1, %s, 'system', %s, %s, %s)
                    """,
                    (
                        group.id,
                        owner_id,
                        json.dumps({"event": "group_created", "name": name}),
                        uuid.uuid4().hex,
                        _iso(now),
                    ),
                )
        return group

    async def get_group(self, group_id: int) -> Optional[Group]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM gc_groups WHERE id = %s", (group_id,)
                )
            ).fetchone()
        return _row_to_group(row) if row else None

    async def list_groups_for(self, principal_id: str, *, limit: int) -> list[Group]:
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """
                    SELECT g.* FROM gc_groups g
                    JOIN gc_members m ON m.group_id = g.id
                    WHERE m.principal_id = %s AND m.status = 'active'
                      AND g.archived_at IS NULL
                    ORDER BY g.id DESC
                    LIMIT %s
                    """,
                    (principal_id, limit),
                )
            ).fetchall()
        return [_row_to_group(row) for row in rows]

    async def update_group(
        self,
        group_id: int,
        *,
        name: Optional[str] = None,
        join_policy: Optional[str] = None,
        muted: Optional[bool] = None,
        archived: Optional[bool] = None,
    ) -> Optional[Group]:
        assignments: list[str] = []
        params: list[Any] = []
        if name is not None:
            assignments.append("name = %s")
            params.append(name)
        if join_policy is not None:
            assignments.append("join_policy = %s")
            params.append(join_policy)
        if muted is not None:
            assignments.append("muted = %s")
            params.append(muted)
        if archived is not None:
            assignments.append("archived_at = %s")
            params.append(_iso(time.time()) if archived else None)
        if not assignments:
            return await self.get_group(group_id)

        params.append(group_id)
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    f"UPDATE gc_groups SET {', '.join(assignments)} "
                    "WHERE id = %s RETURNING *",
                    tuple(params),
                )
            ).fetchone()
        return _row_to_group(row) if row else None

    async def count_groups_owned_by(self, principal_id: str) -> int:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT count(*) AS n FROM gc_groups "
                    "WHERE owner_id = %s AND archived_at IS NULL",
                    (principal_id,),
                )
            ).fetchone()
        return int(row["n"]) if row else 0

    async def delete_group(self, group_id: int) -> bool:
        """One statement: every gc_* table carries ON DELETE CASCADE.

        Relies on the foreign keys rather than issuing a delete per table, so
        a future table added with the same cascade is covered automatically and
        the operation stays a single atomic unit.
        """
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "DELETE FROM gc_groups WHERE id = %s RETURNING id", (group_id,)
                )
            ).fetchone()
        return row is not None

    # ── Memberships ──────────────────────────────────────────────────────

    async def get_membership(
        self, group_id: int, principal_id: str
    ) -> Optional[Membership]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM gc_members WHERE group_id = %s AND principal_id = %s",
                    (group_id, principal_id),
                )
            ).fetchone()
        return _row_to_membership(row) if row else None

    async def list_members(
        self, group_id: int, *, statuses: Sequence[str] | None = None
    ) -> list[Membership]:
        if statuses:
            async with self._pool.connection() as conn:
                rows = await (
                    await conn.execute(
                        "SELECT * FROM gc_members WHERE group_id = %s "
                        "AND status = ANY(%s) ORDER BY joined_at NULLS LAST, principal_id",
                        (group_id, list(statuses)),
                    )
                ).fetchall()
        else:
            async with self._pool.connection() as conn:
                rows = await (
                    await conn.execute(
                        "SELECT * FROM gc_members WHERE group_id = %s "
                        "ORDER BY joined_at NULLS LAST, principal_id",
                        (group_id,),
                    )
                ).fetchall()
        return [_row_to_membership(row) for row in rows]

    async def upsert_membership(self, membership: Membership) -> Membership:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    INSERT INTO gc_members
                      (group_id, principal_id, role, status, muted, invited_by,
                       dataset, service_id, joined_at, last_read_seq)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (group_id, principal_id) DO UPDATE SET
                      role = EXCLUDED.role,
                      status = EXCLUDED.status,
                      muted = EXCLUDED.muted,
                      invited_by = EXCLUDED.invited_by,
                      dataset = EXCLUDED.dataset,
                      service_id = EXCLUDED.service_id,
                      joined_at = EXCLUDED.joined_at,
                      last_read_seq = GREATEST(
                        gc_members.last_read_seq, EXCLUDED.last_read_seq)
                    RETURNING *
                    """,
                    (
                        membership.group_id,
                        membership.principal_id,
                        membership.role,
                        membership.status,
                        membership.muted,
                        membership.invited_by,
                        membership.dataset,
                        membership.service_id,
                        _iso(membership.joined_at),
                        membership.last_read_seq,
                    ),
                )
            ).fetchone()
        return _row_to_membership(row)

    async def set_membership_status(
        self,
        group_id: int,
        principal_id: str,
        *,
        status: str,
        role: Optional[str] = None,
        joined_at: Optional[float] = None,
    ) -> Optional[Membership]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    UPDATE gc_members
                    SET status = %s,
                        role = COALESCE(%s, role),
                        joined_at = COALESCE(%s, joined_at)
                    WHERE group_id = %s AND principal_id = %s
                    RETURNING *
                    """,
                    (status, role, _iso(joined_at), group_id, principal_id),
                )
            ).fetchone()
        return _row_to_membership(row) if row else None

    async def count_members(self, group_id: int) -> int:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT count(*) AS n FROM gc_members "
                    "WHERE group_id = %s AND status IN ('invited', 'active')",
                    (group_id,),
                )
            ).fetchone()
        return int(row["n"]) if row else 0

    async def advance_cursor(self, group_id: int, principal_id: str, seq: int) -> int:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    UPDATE gc_members
                    SET last_read_seq = GREATEST(last_read_seq, %s)
                    WHERE group_id = %s AND principal_id = %s
                    RETURNING last_read_seq
                    """,
                    (seq, group_id, principal_id),
                )
            ).fetchone()
        if row is None:
            raise GroupChatError(404, "groupchat_not_a_member", "Not a member of this group")
        return int(row["last_read_seq"])

    # ── Invitations ──────────────────────────────────────────────────────

    async def create_invite(self, invite: Invite) -> Invite:
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO gc_invites
                  (id, group_id, inviter_id, invitee_id, mode, token_hash,
                   expires_at, max_uses, used_count, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    invite.id,
                    invite.group_id,
                    invite.inviter_id,
                    invite.invitee_id,
                    invite.mode,
                    invite.token_hash,
                    _iso(invite.expires_at),
                    invite.max_uses,
                    invite.used_count,
                    _iso(invite.created_at),
                ),
            )
        return invite

    async def get_invite(self, invite_id: str) -> Optional[Invite]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM gc_invites WHERE id = %s", (invite_id,)
                )
            ).fetchone()
        return _row_to_invite(row) if row else None

    async def find_invite_by_token_hash(self, token_hash: str) -> Optional[Invite]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM gc_invites WHERE token_hash = %s", (token_hash,)
                )
            ).fetchone()
        return _row_to_invite(row) if row else None

    async def consume_invite(self, invite_id: str, *, now: float) -> bool:
        """Conditional increment — the check and the write are one statement.

        A ``SELECT`` followed by an ``UPDATE`` would let two concurrent
        redeems both observe ``used_count < max_uses`` and both succeed,
        which breaks single-use invitations.
        """
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    UPDATE gc_invites
                    SET used_count = used_count + 1
                    WHERE id = %s
                      AND (expires_at IS NULL OR expires_at > %s)
                      AND (max_uses IS NULL OR used_count < max_uses)
                    RETURNING used_count
                    """,
                    (invite_id, _iso(now)),
                )
            ).fetchone()
        return row is not None

    # ── Log ──────────────────────────────────────────────────────────────

    async def append_message(
        self,
        group_id: int,
        *,
        sender_id: str,
        kind: str,
        payload: dict[str, Any],
        client_msg_id: Optional[str] = None,
        mentions: Sequence[str] = (),
        reply_to: Optional[int] = None,
    ) -> Message:
        now = time.time()
        async with self._pool.connection() as conn:
            async with conn.transaction():
                if client_msg_id:
                    existing = await (
                        await conn.execute(
                            "SELECT * FROM gc_messages WHERE group_id = %s "
                            "AND sender_id = %s AND client_msg_id = %s",
                            (group_id, sender_id, client_msg_id),
                        )
                    ).fetchone()
                    if existing is not None:
                        return _row_to_message(existing, await self._mentions(
                            conn, group_id, int(existing["seq"])
                        ))

                # Row lock on the group serializes concurrent senders; the
                # lock is held only for the counter bump plus one insert.
                seq_row = await (
                    await conn.execute(
                        "UPDATE gc_groups SET seq_counter = seq_counter + 1 "
                        "WHERE id = %s RETURNING seq_counter",
                        (group_id,),
                    )
                ).fetchone()
                if seq_row is None:
                    raise GroupChatError(
                        404, "groupchat_group_not_found", "Group not found"
                    )
                seq = int(seq_row["seq_counter"])
                message_id = uuid.uuid4().hex
                row = await (
                    await conn.execute(
                        """
                        INSERT INTO gc_messages
                          (group_id, seq, sender_id, kind, payload, message_id,
                           client_msg_id, reply_to, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            group_id,
                            seq,
                            sender_id,
                            kind,
                            json.dumps(payload),
                            message_id,
                            client_msg_id,
                            reply_to,
                            _iso(now),
                        ),
                    )
                ).fetchone()
                if mentions:
                    for principal_id in dict.fromkeys(mentions):
                        await conn.execute(
                            "INSERT INTO gc_message_mentions (group_id, seq, principal_id) "
                            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                            (group_id, seq, principal_id),
                        )
        return _row_to_message(row, tuple(mentions))

    @staticmethod
    async def _mentions(conn: Any, group_id: int, seq: int) -> tuple[str, ...]:
        rows = await (
            await conn.execute(
                "SELECT principal_id FROM gc_message_mentions "
                "WHERE group_id = %s AND seq = %s ORDER BY principal_id",
                (group_id, seq),
            )
        ).fetchall()
        return tuple(row["principal_id"] for row in rows)

    async def fetch_page(self, group_id: int, *, after_seq: int, limit: int) -> Page:
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM gc_messages WHERE group_id = %s AND seq > %s "
                    "ORDER BY seq ASC LIMIT %s",
                    (group_id, after_seq, limit + 1),
                )
            ).fetchall()
            latest = await self._latest_seq(conn, group_id)
            has_more = len(rows) > limit
            rows = rows[:limit]
            messages = [await self._hydrate(conn, row) for row in rows]
        next_seq = messages[-1].seq if messages else after_seq
        return Page(
            messages=messages, next_seq=next_seq, has_more=has_more, latest_seq=latest
        )

    async def fetch_reverse_page(
        self, group_id: int, *, before_seq: int, limit: int
    ) -> Page:
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM gc_messages WHERE group_id = %s AND seq < %s "
                    "ORDER BY seq DESC LIMIT %s",
                    (group_id, before_seq, limit + 1),
                )
            ).fetchall()
            latest = await self._latest_seq(conn, group_id)
            has_more = len(rows) > limit
            rows = rows[:limit]
            messages = [await self._hydrate(conn, row) for row in reversed(rows)]
        next_seq = messages[0].seq if messages else before_seq
        return Page(
            messages=messages, next_seq=next_seq, has_more=has_more, latest_seq=latest
        )

    async def latest_seq(self, group_id: int) -> int:
        async with self._pool.connection() as conn:
            return await self._latest_seq(conn, group_id)

    @staticmethod
    async def _latest_seq(conn: Any, group_id: int) -> int:
        row = await (
            await conn.execute(
                "SELECT seq_counter FROM gc_groups WHERE id = %s", (group_id,)
            )
        ).fetchone()
        return int(row["seq_counter"]) if row else 0

    async def _hydrate(self, conn: Any, row: Any) -> Message:
        return _row_to_message(row, await self._mentions(conn, row["group_id"], row["seq"]))


def _row_to_group(row: Any) -> Group:
    return Group(
        id=int(row["id"]),
        name=row["name"],
        owner_id=row["owner_id"],
        owner_dataset=row["owner_dataset"],
        join_policy=row["join_policy"],
        max_members=int(row["max_members"]),
        seq_counter=int(row["seq_counter"]),
        muted=bool(row["muted"]),
        archived_at=_epoch(row["archived_at"]),
        created_at=_epoch(row["created_at"]) or 0.0,
    )


def _row_to_membership(row: Any) -> Membership:
    return Membership(
        group_id=int(row["group_id"]),
        principal_id=row["principal_id"],
        role=row["role"],
        status=row["status"],
        muted=bool(row["muted"]),
        invited_by=row["invited_by"],
        dataset=row["dataset"],
        service_id=row["service_id"],
        joined_at=_epoch(row["joined_at"]),
        last_read_seq=int(row["last_read_seq"]),
    )


def _row_to_invite(row: Any) -> Invite:
    return Invite(
        id=str(row["id"]),
        group_id=int(row["group_id"]),
        inviter_id=row["inviter_id"],
        invitee_id=row["invitee_id"],
        mode=row["mode"],
        token_hash=row["token_hash"],
        expires_at=_epoch(row["expires_at"]),
        max_uses=row["max_uses"],
        used_count=int(row["used_count"]),
        created_at=_epoch(row["created_at"]) or 0.0,
    )


def _row_to_message(row: Any, mentions: tuple[str, ...]) -> Message:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return Message(
        group_id=int(row["group_id"]),
        seq=int(row["seq"]),
        sender_id=row["sender_id"],
        kind=row["kind"],
        payload=payload or {},
        message_id=row["message_id"],
        client_msg_id=row["client_msg_id"],
        reply_to=int(row["reply_to"]) if row["reply_to"] is not None else None,
        mentions=mentions,
        created_at=_epoch(row["created_at"]) or 0.0,
        deleted_at=_epoch(row["deleted_at"]),
    )


def build_store(config: Any) -> GroupChatStore:
    """Construct the store named by the configuration."""
    if config.backend == "memory":
        return MemoryStore()
    return PostgresStore(config.dsn, schema=config.schema)


def new_invite_token() -> str:
    return secrets.token_urlsafe(32)
