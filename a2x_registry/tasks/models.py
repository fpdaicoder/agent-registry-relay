"""Data models for the personal task-continuation data plane.

Plain dataclasses rather than Pydantic models, mirroring ``groupchat/models.py``:
values are constructed by the store layer from database rows and are already
type-checked by PostgreSQL. The closed enums below are persisted as ``text``
with a check constraint, so adding a value does not force a migration.

The vocabulary that already existed in ``groupchat/models.py`` — ``TASK_STATES``
and ``MESSAGE_KINDS`` — is *re-declared here* rather than imported, so this
module stays decoupled from group chat (the two may drift once tasks gains its
own lease notions). Keep them in sync deliberately.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

# Task lifecycle. ``input_required`` is the "a human must decide" signal: the
# lease is released and the task waits for a person to re-acquire it.
TASK_STATES = (
    "pending",
    "working",
    "input_required",
    "completed",
    "failed",
    "canceled",
)

# Once a task reaches one of these it can never be acquired or appended again.
TERMINAL_STATES = ("completed", "failed", "canceled")

# Message kinds carried in a task's log. ``event``/``system`` are reserved for
# the server; a client may only write ``text``, ``tool_result`` or
# ``a2x_request``.
MESSAGE_KINDS = ("text", "event", "system", "a2x_request", "tool_result")

# Kinds a client is allowed to write (the service enforces the rest).
CLIENT_WRITABLE_KINDS = ("text", "tool_result", "a2x_request")


def _check(value: str, allowed: tuple[str, ...], field_name: str) -> str:
    if value not in allowed:
        raise ValueError(f"{field_name} must be one of {allowed}, got {value!r}")
    return value


@dataclass
class Task:
    """One personal task. The lease columns make it a cross-device entity.

    ``lease_fence`` is the monotonically increasing takeover token (see the
    design doc §4); ``lease_expires_at`` is only crash recovery. ``checkpoint``
    / ``checkpoint_seq`` are the authoritative resume point written atomically
    on release.
    """

    id: int
    owner_id: str
    owner_dataset: str
    state: str = "pending"
    title: str = ""
    seq_counter: int = 0
    checkpoint: Optional[dict] = None
    checkpoint_seq: int = 0
    lease_holder: Optional[str] = None
    lease_fence: int = 0
    lease_expires_at: Optional[float] = None
    archived_at: Optional[float] = None
    created_at: float = 0.0
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        _check(self.state, TASK_STATES, "state")
        if not self.owner_id:
            raise ValueError("task owner_id must not be empty")

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def is_archived(self) -> bool:
        return self.archived_at is not None


@dataclass
class TaskMessage:
    """One entry in a task's log.

    ``sender_id`` records the principal (the human, or their Agent), while
    ``device_id`` records which device key wrote it — the two are split so an
    audit can tell *whom* and *where* apart. Personal tasks drop ``mentions``.
    """

    task_id: int
    seq: int
    sender_id: str
    kind: str
    payload: dict[str, Any] = field(default_factory=dict)
    message_id: Optional[str] = None
    device_id: Optional[str] = None
    client_msg_id: Optional[str] = None
    reply_to: Optional[int] = None
    created_at: float = 0.0
    deleted_at: Optional[float] = None

    def __post_init__(self) -> None:
        _check(self.kind, MESSAGE_KINDS, "kind")
        if self.seq <= 0:
            raise ValueError("message seq must be positive")

    @property
    def is_event(self) -> bool:
        return self.kind in ("event", "system")

    def to_wire(self, *, trusted: bool = False) -> dict[str, Any]:
        """Serialize for the subscription channel and the pull endpoint.

        ``trusted`` is always server-derived and never read from storage. The
        owner's own entries may be marked trusted; anything from another
        principal is untrusted by construction.
        """
        return {
            "task_id": self.task_id,
            "seq": self.seq,
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "device_id": self.device_id,
            "kind": self.kind,
            "payload": self.payload,
            "reply_to": self.reply_to,
            "trusted": trusted,
            "created_at": self.created_at,
            "deleted_at": self.deleted_at,
        }


@dataclass
class TaskCursor:
    """A device's read watermark. Best-effort: ``checkpoint_seq`` is authoritative."""

    task_id: int
    device_id: str
    last_read_seq: int = 0


@dataclass
class TaskPage:
    """A slice of a task log, plus what the caller needs to ask for more."""

    messages: list[TaskMessage]
    next_seq: int
    has_more: bool
    latest_seq: int

    def to_wire(self, *, trusted: bool = False) -> dict[str, Any]:
        return {
            "messages": [message.to_wire(trusted=trusted) for message in self.messages],
            "next_seq": self.next_seq,
            "has_more": self.has_more,
            "latest_seq": self.latest_seq,
        }