"""Data models for the group chat data plane.

Plain dataclasses rather than Pydantic models: these are constructed by the
store layer from database rows (or from the in-memory backend), and the values
arriving from PostgreSQL are already type-checked by the column types. The
FastAPI request bodies that *need* validation are Pydantic models in
``router.py``.

The vocabulary here is deliberately narrow — every enum below is closed, so
``sqlstore`` can persist them as ``text`` with a check constraint rather than
needing a migration each time a value is added.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

# Membership roles. ``readonly`` can read and receive but never write.
ROLES = ("owner", "admin", "member", "readonly")

# Membership lifecycle. ``banned`` is terminal: no invitation path may leave it.
STATUSES = ("invited", "active", "left", "kicked", "banned")

# Message kinds. The distinction that matters is ``text``/``tool_result``
# (background data) versus ``a2a_request`` (an explicit request to act).
MESSAGE_KINDS = ("text", "event", "system", "a2a_request", "tool_result")

# How an invitation can be redeemed.
INVITE_MODES = ("targeted", "open")

# Who currently holds a task lease.
HOLDER_KINDS = ("agent", "human")

# Task lifecycle. ``input_required`` is the "a human must decide" signal.
TASK_STATES = (
    "pending",
    "working",
    "input_required",
    "completed",
    "failed",
    "canceled",
)


def _check(value: str, allowed: tuple[str, ...], field_name: str) -> str:
    if value not in allowed:
        raise ValueError(f"{field_name} must be one of {allowed}, got {value!r}")
    return value


@dataclass
class Group:
    id: int
    name: str
    owner_id: str
    owner_dataset: str
    join_policy: str = "invite_only"
    max_members: int = 500
    seq_counter: int = 0
    muted: bool = False
    archived_at: Optional[float] = None
    created_at: float = 0.0

    def __post_init__(self) -> None:
        _check(self.join_policy, ("invite_only", "approval", "link", "open"), "join_policy")
        if not self.name:
            raise ValueError("group name must not be empty")
        if not self.owner_id:
            raise ValueError("group owner_id must not be empty")


@dataclass
class Membership:
    group_id: int
    principal_id: str
    role: str = "member"
    status: str = "invited"
    muted: bool = False
    invited_by: Optional[str] = None
    joined_at: Optional[float] = None
    last_read_seq: int = 0
    # Optional Agent binding: the (dataset, service_id) this principal is
    # reachable at for delivery. Both are None for a human member with no
    # registered Agent, which simply means "deliver nothing, they poll".
    dataset: Optional[str] = None
    service_id: Optional[str] = None

    def __post_init__(self) -> None:
        _check(self.role, ROLES, "role")
        _check(self.status, STATUSES, "status")
        if not self.principal_id:
            raise ValueError("membership principal_id must not be empty")

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    @property
    def binding(self) -> Optional[tuple[str, str]]:
        """Return the Agent binding as ``(dataset, service_id)``, or ``None``."""
        if self.dataset and self.service_id:
            return (self.dataset, self.service_id)
        return None


@dataclass
class Invite:
    id: str
    group_id: int
    inviter_id: str
    mode: str = "targeted"
    invitee_id: Optional[str] = None
    token_hash: str = ""
    expires_at: Optional[float] = None
    max_uses: Optional[int] = 1
    used_count: int = 0
    created_at: float = 0.0

    def __post_init__(self) -> None:
        _check(self.mode, INVITE_MODES, "mode")
        if self.mode == "targeted" and not self.invitee_id:
            raise ValueError("targeted invitations require invitee_id")
        if not self.token_hash:
            raise ValueError("invitation token_hash must not be empty")

    def is_expired(self, now: float) -> bool:
        return self.expires_at is not None and self.expires_at <= now

    def is_exhausted(self) -> bool:
        return self.max_uses is not None and self.used_count >= self.max_uses


@dataclass
class Message:
    """One entry in a group's log.

    Messages and membership events share the group's ``seq`` space, so this
    covers both: ``kind == "event"`` entries carry a membership transition in
    ``payload`` and have ``sender_id`` set to the acting principal.
    """

    group_id: int
    seq: int
    sender_id: str
    kind: str
    payload: dict[str, Any] = field(default_factory=dict)
    message_id: Optional[str] = None
    client_msg_id: Optional[str] = None
    reply_to: Optional[int] = None
    mentions: tuple[str, ...] = ()
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

        ``trusted`` is always server-derived and never read from storage —
        see the delivery envelope rules in the design doc. Every message
        originating from a group member is untrusted by construction.
        """
        return {
            "group_id": self.group_id,
            "seq": self.seq,
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "kind": self.kind,
            "payload": self.payload,
            "mentions": list(self.mentions),
            "reply_to": self.reply_to,
            "trusted": trusted,
            "created_at": self.created_at,
            "deleted_at": self.deleted_at,
        }


@dataclass
class Page:
    """A slice of a group log, plus what the caller needs to ask for more."""

    messages: list[Message]
    next_seq: int
    has_more: bool
    latest_seq: int

    def to_wire(self) -> dict[str, Any]:
        return {
            "messages": [message.to_wire() for message in self.messages],
            "next_seq": self.next_seq,
            "has_more": self.has_more,
            "latest_seq": self.latest_seq,
        }
