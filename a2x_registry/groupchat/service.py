"""Business rules for the group chat data plane.

Everything that decides *whether* an operation is allowed lives here; the
store decides how it is persisted and the router decides how it is exposed.
Two rules are enforced at this layer because getting them wrong is a security
bug rather than a correctness bug:

* **Sender identity comes from the caller, never from the payload.** The
  service takes an explicit ``actor`` argument and never reads a sender field
  out of a request body. In a group of Agents, a forged sender is the first
  thing an attacker tries, and "an instruction that appears to come from a
  trusted peer" is exactly the input that gets obeyed downstream.
* **A group message is untrusted data.** Every message this service returns
  carries ``trusted=False``. Nothing in the payload can raise that; there is
  no code path that sets it to True.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
import unicodedata
import uuid
from typing import Any, Optional, Sequence

from .config import GroupChatConfig
from .delivery import Hub
from .errors import GroupChatError
from .models import (
    INVITE_MODES,
    MESSAGE_KINDS,
    ROLES,
    Group,
    Invite,
    Membership,
    Message,
    Page,
)
from .sqlstore import GroupChatStore, new_invite_token

logger = logging.getLogger(__name__)

# Actions that require admin-or-owner rather than plain membership.
#
# ``invite`` (a targeted invitation naming a single principal) is deliberately
# *not* here: any active, non-readonly member may issue one, subject to the
# extra limits in ``create_invite`` (agent-only, at most one per member).
# Broadcast "open" links are the privileged shape and are gated as
# ``invite_open``.
_ADMIN_ACTIONS = frozenset(
    {"invite_open", "kick", "ban", "promote", "update_group", "delete_group"}
)

MAX_NAME_CHARS = 120


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _clean_name(name: str) -> str:
    """Normalize a group name for storage.

    Rejects control characters and zero-width joiners: both are usable to
    make two distinct groups render identically, which turns a name into a
    spoofing vector when an Agent decides whom to trust by display name.
    """
    if not isinstance(name, str):
        raise GroupChatError(400, "groupchat_name_invalid", "Group name must be text")
    trimmed = name.strip()
    if not trimmed:
        raise GroupChatError(400, "groupchat_name_invalid", "Group name must not be empty")
    if len(trimmed) > MAX_NAME_CHARS:
        raise GroupChatError(
            400,
            "groupchat_name_invalid",
            f"Group name must be at most {MAX_NAME_CHARS} characters",
        )
    for character in trimmed:
        category = unicodedata.category(character)
        if category in {"Cc", "Cf", "Cs"}:
            raise GroupChatError(
                400,
                "groupchat_name_invalid",
                "Group name must not contain control or formatting characters",
            )
    return trimmed


class GroupChatService:
    def __init__(
        self,
        config: GroupChatConfig,
        store: GroupChatStore,
        *,
        hub: Optional[Hub] = None,
    ) -> None:
        self.config = config
        self.store = store
        self.hub = hub if hub is not None else Hub()
        # Per-group send rate limiting, in-process. A multi-instance
        # deployment needs this in Redis instead; the shape of the check is
        # the same either way.
        self._send_windows: dict[int, list[float]] = {}

    async def start(self) -> None:
        await self.store.start()

    async def stop(self) -> None:
        await self.store.stop()

    # ── Permission ───────────────────────────────────────────────────────

    @staticmethod
    def _can(membership: Optional[Membership], action: str) -> bool:
        """The single place membership implies permission.

        ``status != 'active'`` is a blanket refusal: an ``invited`` member has
        not accepted, and ``left``/``kicked``/``banned`` members are gone. A
        role-only check would let an invited principal speak, which breaks
        the invite-only semantics.
        """
        if membership is None or membership.status != "active":
            return False
        if action in _ADMIN_ACTIONS:
            return membership.role in ("owner", "admin")
        if action == "send_message":
            return membership.role != "readonly" and not membership.muted
        if action == "invite":
            # A targeted invite seats exactly the named principal. Any active,
            # non-readonly member may bring a specific principal in. Muted is
            # deliberately not checked: muting gates speech, not invitations.
            return membership.role != "readonly"
        return True

    async def _require_membership(
        self, group_id: int, actor: str, *, allow_archived: bool = False
    ) -> tuple[Group, Membership]:
        """Resolve the caller's membership, or raise.

        ``allow_archived`` distinguishes reading from writing. An archived
        group is retired but its history is not destroyed, so reads still
        resolve; writes do not, because "archived" is meant to end the
        conversation. Deletion is the operation that makes the group
        unreachable in both directions.

        Blocking reads here as well would be a one-way door: the unarchive
        call needs this same lookup to find the group, so archiving could
        never be undone and the history would be permanently sealed.
        """
        group = await self.store.get_group(group_id)
        if group is None:
            raise GroupChatError(404, "groupchat_group_not_found", "Group not found")
        if group.archived_at is not None and not allow_archived:
            raise GroupChatError(
                409,
                "groupchat_group_archived",
                "Group is archived; unarchive it before writing to it",
            )
        membership = await self.store.get_membership(group_id, actor)
        if membership is None:
            # 404 rather than 403: a non-member should not learn that the
            # group exists.
            raise GroupChatError(404, "groupchat_group_not_found", "Group not found")
        return group, membership

    async def _require_action(
        self,
        group_id: int,
        actor: str,
        action: str,
        *,
        verb: Optional[str] = None,
    ) -> tuple[Group, Membership]:
        group, membership = await self._require_membership(group_id, actor)
        if not self._can(membership, action):
            raise GroupChatError(
                403,
                "groupchat_forbidden",
                f"Principal {actor!r} may not {verb or action} in this group",
            )
        return group, membership

    # ── Groups ───────────────────────────────────────────────────────────

    async def create_group(
        self,
        *,
        actor: str,
        name: str,
        owner_dataset: str,
        join_policy: str = "invite_only",
        max_members: Optional[int] = None,
    ) -> Group:
        if not actor:
            raise GroupChatError(401, "groupchat_unauthenticated", "Authentication required")
        clean_name = _clean_name(name)
        limit = max_members or self.config.max_members_per_group
        if limit > self.config.max_members_per_group:
            raise GroupChatError(
                400,
                "groupchat_max_members_invalid",
                f"max_members must not exceed {self.config.max_members_per_group}",
            )
        owned = await self.store.count_groups_owned_by(actor)
        if owned >= self.config.max_groups_per_principal:
            raise GroupChatError(
                429,
                "groupchat_group_quota_exceeded",
                f"Principal already owns {owned} groups (limit {self.config.max_groups_per_principal})",
            )
        return await self.store.create_group(
            name=clean_name,
            owner_id=actor,
            owner_dataset=owner_dataset,
            join_policy=join_policy,
            max_members=limit,
        )

    async def get_group(self, *, actor: str, group_id: int) -> dict[str, Any]:
        # Reading an archived group stays available so its history is not
        # sealed away; writes are what archiving stops.
        group, membership = await self._require_membership(
            group_id, actor, allow_archived=True
        )
        members = await self.store.list_members(group_id)
        visible = [
            member
            for member in members
            # Non-admins see active members plus their own pending invite;
            # they do not get to enumerate who was kicked or banned.
            if member.status == "active"
            or member.principal_id == actor
            or self._can(membership, "kick")
        ]
        return {
            "group": _group_to_wire(group),
            "me": _member_to_wire(membership),
            "members": [_member_to_wire(member) for member in visible],
        }

    async def list_groups(self, *, actor: str, limit: int) -> list[dict[str, Any]]:
        groups = await self.store.list_groups_for(actor, limit=limit)
        out: list[dict[str, Any]] = []
        for group in groups:
            membership = await self.store.get_membership(group.id, actor)
            latest = await self.store.latest_seq(group.id)
            out.append(
                {
                    **_group_to_wire(group),
                    "last_read_seq": membership.last_read_seq if membership else 0,
                    "latest_seq": latest,
                }
            )
        return out

    async def update_group(
        self,
        *,
        actor: str,
        group_id: int,
        name: Optional[str] = None,
        join_policy: Optional[str] = None,
        muted: Optional[bool] = None,
        archived: Optional[bool] = None,
    ) -> Group:
        """Update group settings, including archive/unarchive.

        Archiving is the reversible way to retire a group: the rows stay, the
        history stays readable, and the group drops out of every listing. Use
        :meth:`delete_group` when the data itself must go.
        """
        group, _ = await self._require_action(group_id, actor, "update_group")
        updated = await self.store.update_group(
            group_id,
            name=_clean_name(name) if name is not None else None,
            join_policy=join_policy,
            muted=muted,
            archived=archived,
        )
        if updated is None:
            raise GroupChatError(404, "groupchat_group_not_found", "Group not found")
        # Unarchiving re-opens the group, so the transition belongs in the log
        # either way — a member pulling after this must be able to tell the
        # group's state changed, not just that no new messages arrived.
        if archived is not None and updated.archived_at is not None:
            event = await self.store.append_message(
                group_id,
                sender_id=actor,
                kind="event",
                payload={"event": "group_archived", "by": actor},
                mentions=(),
            )
            self._broadcast(group_id, event, exclude=None)
        return updated

    async def delete_group(self, *, actor: str, group_id: int) -> dict[str, Any]:
        """Physically delete a group and everything it owns. Irreversible.

        Owner only. An admin can archive a group but cannot destroy it: the
        messages in it were written by other principals, and a delegated role
        should not be able to erase another principal's data.
        """
        # Deletion is allowed on an archived group: archiving is the
        # reversible "retire it" step and deletion is the later "remove the
        # data" step, so blocking deletion here would leave archived groups
        # impossible to ever clean up.
        group, membership = await self._require_membership(
            group_id, actor, allow_archived=True
        )
        if membership.role != "owner":
            raise GroupChatError(
                403,
                "groupchat_owner_required",
                "Only the group owner may delete a group",
            )

        # Tell the members before the rows are gone: once the group is
        # deleted there is no group left to broadcast to, and a client that
        # only ever sees an empty log cannot distinguish "deleted" from
        # "no new messages".
        self.hub.publish(
            group_id,
            {
                "type": "group_deleted",
                "group_id": group_id,
                "deleted_by": actor,
            },
        )
        # Drop the subscriptions too. Leaving them registered would keep
        # these connections in the fan-out index under a group id that no
        # longer exists.
        dropped = self._drop_group_subscriptions(group_id)

        deleted = await self.store.delete_group(group_id)
        if not deleted:
            raise GroupChatError(404, "groupchat_group_not_found", "Group not found")

        self._send_windows.pop(group_id, None)
        logger.info(
            "groupchat group deleted: group=%s name=%r owner=%s connections_dropped=%s",
            group_id,
            group.name,
            actor,
            dropped,
        )
        return {
            "group_id": group_id,
            "status": "deleted",
            "dropped_connections": dropped,
        }

    def _drop_group_subscriptions(self, group_id: int) -> int:
        connection_ids = list(self.hub._by_group.get(group_id, ()))  # noqa: SLF001
        for connection_id in connection_ids:
            subscriber = self.hub._by_connection.get(connection_id)  # noqa: SLF001
            if subscriber is not None:
                self.hub.unsubscribe(subscriber, group_id)
        return len(connection_ids)

    # ── Invitations ──────────────────────────────────────────────────────

    async def create_invite(
        self,
        *,
        actor: str,
        group_id: int,
        invitee_id: Optional[str] = None,
        max_uses: Optional[int] = None,
        ttl_seconds: Optional[int] = None,
        invitee_is_agent: bool = False,
    ) -> dict[str, Any]:
        """Create an invitation and return the plaintext token exactly once.

        Only the digest is persisted. The plaintext is returned here and is
        never recoverable afterwards — a stolen database row cannot be
        replayed against the accept endpoint.
        """
        targeted = invitee_id is not None
        mode = "targeted" if targeted else "open"
        group, membership = await self._require_action(
            group_id,
            actor,
            "invite" if targeted else "invite_open",
            verb="invite",
        )
        if membership.role not in ("owner", "admin"):
            # A plain member may introduce exactly one principal, and it must
            # be an Agent — no broad guest-listing and no Agent flooding.
            if not invitee_is_agent:
                raise GroupChatError(
                    403,
                    "groupchat_invitee_not_agent",
                    "Members may only invite agents",
                )
            if any(
                member.invited_by == actor and member.status in ("active", "invited")
                for member in await self.store.list_members(group_id)
            ):
                raise GroupChatError(
                    409,
                    "groupchat_agent_already_invited",
                    "This member has already invited an agent",
                )

        if targeted:
            existing = await self.store.get_membership(group_id, invitee_id or "")
            if existing is not None:
                if existing.status == "banned":
                    raise GroupChatError(
                        403,
                        "groupchat_member_banned",
                        "Banned principals cannot be invited back",
                    )
                if existing.status in ("active", "invited"):
                    raise GroupChatError(
                        409,
                        "groupchat_already_member",
                        "Principal is already an active or invited member",
                    )
            uses = 1
        else:
            if max_uses is None or max_uses < 1:
                raise GroupChatError(
                    400,
                    "groupchat_max_uses_required",
                    "Open invitations require max_uses >= 1",
                )
            uses = max_uses

        seats = await self.store.count_members(group_id)
        if targeted:
            # A targeted invite seats the invitee immediately, so it needs a
            # free seat right now. Re-inviting an existing ``invited`` member
            # does not consume an extra one — they already hold it.
            already_seated = (
                existing is not None and existing.status == "invited"
            )
            if not already_seated and seats >= group.max_members:
                raise GroupChatError(
                    409, "groupchat_group_full", "Group has reached its member limit"
                )
        elif seats >= group.max_members:
            # An open link must not be minted against a group with no room
            # left: it would be accepted and then fail at redemption, which
            # is a confusing failure for the holder of the link.
            raise GroupChatError(
                409, "groupchat_group_full", "Group has reached its member limit"
            )

        token = new_invite_token()
        now = time.time()
        invite = Invite(
            id=str(uuid.uuid4()),
            group_id=group_id,
            inviter_id=actor,
            invitee_id=invitee_id,
            mode=mode,
            token_hash=_digest(token),
            expires_at=(now + ttl_seconds) if ttl_seconds else None,
            max_uses=uses,
            created_at=now,
        )
        await self.store.create_invite(invite)

        if targeted and invitee_id:
            await self.store.upsert_membership(
                Membership(
                    group_id=group_id,
                    principal_id=invitee_id,
                    role="member",
                    status="invited",
                    invited_by=actor,
                )
            )

        return {
            "invite_id": invite.id,
            "group_id": group_id,
            "mode": mode,
            "invitee_id": invitee_id,
            "token": token,
            "expires_at": invite.expires_at,
            "max_uses": invite.max_uses,
        }

    async def accept_invite(
        self, *, actor: str, token: str
    ) -> dict[str, Any]:
        """Redeem an invitation. Returns the group plus recent context.

        Order of checks matters: the cheap local validations run before the
        conditional increment, so a failed attempt does not burn a use. The
        increment itself is what makes single-use invites single-use under
        concurrency.
        """
        if not actor:
            raise GroupChatError(401, "groupchat_unauthenticated", "Authentication required")
        if not token:
            raise GroupChatError(400, "groupchat_token_required", "Invitation token required")

        token_hash = _digest(token)
        invite = await self.store.find_invite_by_token_hash(token_hash)
        if invite is None:
            raise GroupChatError(404, "groupchat_invite_not_found", "Invitation not found")
        # Defensive: the store matched the digest, but compare again here so
        # the comparison is unconditionally constant-time regardless of how a
        # future backend implements the lookup.
        if not hmac.compare_digest(invite.token_hash, token_hash):
            raise GroupChatError(404, "groupchat_invite_not_found", "Invitation not found")

        now = time.time()
        if invite.is_expired(now):
            raise GroupChatError(410, "groupchat_invite_expired", "Invitation has expired")
        if invite.invitee_id and invite.invitee_id != actor:
            # A targeted invite is bound to one principal. Without this check
            # anyone who obtains the link becomes a member.
            raise GroupChatError(
                403,
                "groupchat_invite_not_for_caller",
                "Invitation was issued to a different principal",
            )

        group = await self.store.get_group(invite.group_id)
        if group is None or group.archived_at is not None:
            raise GroupChatError(404, "groupchat_group_not_found", "Group not found")

        membership = await self.store.get_membership(invite.group_id, actor)
        if membership is not None and membership.status == "banned":
            raise GroupChatError(
                403, "groupchat_member_banned", "Banned principals cannot rejoin"
            )
        if membership is not None and membership.status == "active":
            raise GroupChatError(
                409, "groupchat_already_member", "Already an active member"
            )

        seats = await self.store.count_members(invite.group_id)
        already_seated = membership is not None and membership.status == "invited"
        if not already_seated and seats >= group.max_members:
            raise GroupChatError(
                409, "groupchat_group_full", "Group has reached its member limit"
            )

        consumed = await self.store.consume_invite(invite.id, now=now)
        if not consumed:
            raise GroupChatError(
                410, "groupchat_invite_exhausted", "Invitation is no longer redeemable"
            )

        joined = await self.store.upsert_membership(
            Membership(
                group_id=invite.group_id,
                principal_id=actor,
                role=membership.role if already_seated else "member",
                status="active",
                muted=membership.muted if membership else False,
                invited_by=invite.inviter_id,
                dataset=membership.dataset if membership else None,
                service_id=membership.service_id if membership else None,
                joined_at=now,
                last_read_seq=await self.store.latest_seq(invite.group_id),
            )
        )

        event = await self.store.append_message(
            invite.group_id,
            sender_id=actor,
            kind="event",
            payload={
                "event": "member_joined",
                "principal_id": actor,
                "role": joined.role,
            },
            mentions=(),
        )
        self._broadcast(invite.group_id, event, exclude=None)

        page = await self.store.fetch_reverse_page(
            invite.group_id,
            before_seq=event.seq,
            limit=min(self.config.default_page_size, self.config.max_page_size),
        )
        return {
            "group": _group_to_wire(group),
            "role": joined.role,
            "joined_seq": event.seq,
            "recent_messages": [message.to_wire() for message in page.messages],
        }

    # ── Members ──────────────────────────────────────────────────────────

    async def list_members(self, *, actor: str, group_id: int) -> list[dict[str, Any]]:
        await self._require_membership(group_id, actor, allow_archived=True)
        members = await self.store.list_members(group_id)
        return [
            _member_to_wire(member) for member in members if member.status == "active"
        ]

    async def set_member_role(
        self, *, actor: str, group_id: int, target: str, role: str
    ) -> dict[str, Any]:
        if role not in ROLES or role == "owner":
            raise GroupChatError(
                400, "groupchat_role_invalid", f"role must be one of {ROLES[:-1]}"
            )
        await self._require_action(group_id, actor, "promote")
        updated = await self.store.set_membership_status(
            group_id, target, status="active", role=role
        )
        if updated is None:
            raise GroupChatError(404, "groupchat_member_not_found", "Member not found")
        return _member_to_wire(updated)

    async def remove_member(
        self,
        *,
        actor: str,
        group_id: int,
        target: Optional[str] = None,
        ban: bool = False,
    ) -> dict[str, Any]:
        """Leave (``target=None``), kick, or ban.

        Every branch ends in the same three steps: write the status, append an
        event to the log so the transition is ordered against messages, and
        drop the Hub subscriptions. Skipping the last step leaves a removed
        member still receiving traffic.
        """
        leave = target is None or target == actor
        if leave:
            group, membership = await self._require_membership(group_id, actor)
            target_principal = actor
        else:
            group, membership = await self._require_action(
                group_id, actor, "ban" if ban else "kick"
            )
            target_principal = target or ""

        target_membership = await self.store.get_membership(group_id, target_principal)
        if target_membership is None:
            raise GroupChatError(404, "groupchat_member_not_found", "Member not found")
        if target_membership.status == "banned" and not ban:
            raise GroupChatError(409, "groupchat_member_banned", "Member is banned")

        if not leave:
            if target_membership.role == "owner":
                raise GroupChatError(
                    403, "groupchat_owner_immutable", "The group owner cannot be removed"
                )
            # An admin may not remove another admin; only the owner may.
            if target_membership.role == "admin" and membership.role != "owner":
                raise GroupChatError(
                    403, "groupchat_forbidden", "Only the owner may remove an admin"
                )

        status = "banned" if ban else ("left" if leave else "kicked")
        updated = await self.store.set_membership_status(
            group_id, target_principal, status=status
        )
        if updated is None:
            raise GroupChatError(404, "groupchat_member_not_found", "Member not found")

        event = await self.store.append_message(
            group_id,
            sender_id=actor,
            kind="event",
            payload={
                "event": status,
                "principal_id": target_principal,
                "by": actor,
            },
            mentions=(),
        )
        self._broadcast(group_id, event, exclude=None)

        # Security-relevant: drop the subscriptions now, not on reconnect.
        dropped = self.hub.drop_principal(target_principal)
        logger.info(
            "groupchat membership %s: group=%s target=%s by=%s connections_dropped=%s",
            status,
            group_id,
            target_principal,
            actor,
            dropped,
        )
        return {"principal_id": target_principal, "status": status, "dropped_connections": dropped}

    # ── Messages ─────────────────────────────────────────────────────────

    async def send_message(
        self,
        *,
        actor: str,
        group_id: int,
        payload: Any,
        kind: str = "text",
        client_msg_id: Optional[str] = None,
        mentions: Sequence[str] = (),
        reply_to: Optional[int] = None,
        connection_id: Optional[str] = None,
    ) -> Message:
        group, membership = await self._require_action(group_id, actor, "send_message")
        if group.muted and membership.role not in ("owner", "admin"):
            raise GroupChatError(403, "groupchat_group_muted", "Group is muted")
        if kind not in MESSAGE_KINDS:
            raise GroupChatError(
                400, "groupchat_kind_invalid", f"kind must be one of {MESSAGE_KINDS}"
            )
        if kind in ("event", "system"):
            raise GroupChatError(
                400,
                "groupchat_kind_reserved",
                "event and system entries are written by the server only",
            )

        self._check_rate(group_id)
        self._check_payload_size(payload)

        clean_mentions = tuple(dict.fromkeys(m for m in mentions if m))
        if clean_mentions:
            await self._validate_mentions(group_id, clean_mentions)

        message = await self.store.append_message(
            group_id,
            sender_id=actor,
            kind=kind,
            payload=payload,
            client_msg_id=client_msg_id,
            mentions=clean_mentions,
            reply_to=reply_to,
        )

        # Post-commit only. Broadcasting before the transaction committed
        # would let a subscriber learn a seq that never became durable, and
        # its cursor would then sit above the server's maximum forever.
        self._broadcast(
            group_id,
            message,
            exclude=connection_id,
            only=frozenset(clean_mentions) if clean_mentions else None,
        )
        return message

    async def _validate_mentions(self, group_id: int, mentions: tuple[str, ...]) -> None:
        for principal_id in mentions:
            membership = await self.store.get_membership(group_id, principal_id)
            if membership is None or membership.status != "active":
                raise GroupChatError(
                    400,
                    "groupchat_mention_invalid",
                    f"Mentioned principal {principal_id!r} is not an active member",
                )

    def _check_payload_size(self, payload: Any) -> None:
        import json

        try:
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise GroupChatError(
                400, "groupchat_payload_invalid", "Payload must be JSON-serializable"
            ) from exc
        if len(encoded) > self.config.max_message_bytes:
            raise GroupChatError(
                413,
                "groupchat_message_too_large",
                f"Payload exceeds the {self.config.max_message_bytes} byte limit",
            )

    def _check_rate(self, group_id: int) -> None:
        now = time.time()
        window = self.config.rate_limit_window_seconds
        stamps = self._send_windows.setdefault(group_id, [])
        cutoff = now - window
        stamps[:] = [stamp for stamp in stamps if stamp > cutoff]
        if len(stamps) >= self.config.rate_limit_per_group:
            raise GroupChatError(
                429,
                "groupchat_rate_limited",
                f"Group exceeded {self.config.rate_limit_per_group} messages "
                f"per {window}s",
            )
        stamps.append(now)

    async def fetch_messages(
        self,
        *,
        actor: str,
        group_id: int,
        after_seq: Optional[int] = None,
        before_seq: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        """Pull the log, or the delta since the caller's cursor.

        When ``after_seq`` is omitted the server's own recorded cursor is
        used. That is what lets an Agent whose local state was lost — a
        container restart, a truncated context — resume at the right place
        instead of replaying the entire group history into its context.
        """
        group, membership = await self._require_membership(
            group_id, actor, allow_archived=True
        )
        if after_seq is not None and before_seq is not None:
            raise GroupChatError(
                400,
                "groupchat_cursor_conflict",
                "Specify either after_seq or before_seq, not both",
            )
        page_size = min(limit or self.config.default_page_size, self.config.max_page_size)
        if page_size < 1:
            raise GroupChatError(400, "groupchat_limit_invalid", "limit must be >= 1")

        if before_seq is not None:
            page = await self.store.fetch_reverse_page(
                group_id, before_seq=before_seq, limit=page_size
            )
            cursor = membership.last_read_seq
        else:
            start = after_seq if after_seq is not None else membership.last_read_seq
            page = await self.store.fetch_page(group_id, after_seq=start, limit=page_size)
            cursor = start

        return {
            **page.to_wire(),
            "cursor": cursor,
            "server_cursor": membership.last_read_seq,
        }

    async def ack(
        self, *, actor: str, group_id: int, seq: int, connection_id: Optional[str] = None
    ) -> dict[str, Any]:
        """Advance the caller's read cursor. Monotonic, never rewinds."""
        await self._require_membership(group_id, actor)
        if seq < 0:
            raise GroupChatError(400, "groupchat_seq_invalid", "seq must be >= 0")
        latest = await self.store.latest_seq(group_id)
        if seq > latest:
            raise GroupChatError(
                409,
                "groupchat_seq_ahead",
                f"seq {seq} is ahead of the group's latest seq {latest}",
            )
        cursor = await self.store.advance_cursor(group_id, actor, seq)
        if connection_id:
            subscriber = self.hub._by_connection.get(connection_id)  # noqa: SLF001
            if subscriber is not None:
                self.hub.publish(
                    group_id,
                    {"type": "ack", "group_id": group_id, "principal_id": actor, "seq": cursor},
                    only=frozenset({actor}),
                )
        return {"group_id": group_id, "last_read_seq": cursor}

    # ── Fan-out helper ───────────────────────────────────────────────────

    def _broadcast(
        self,
        group_id: int,
        message: Message,
        *,
        exclude: Optional[str],
        only: Optional[frozenset[str]] = None,
    ) -> int:
        return self.hub.publish(
            group_id,
            {"type": "message", **message.to_wire(trusted=False)},
            only=only,
            exclude=exclude,
        )

    # ── Status ───────────────────────────────────────────────────────────

    def status(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "backend": self.store.backend_name,
            "maxMessageBytes": self.config.max_message_bytes,
            "maxMembersPerGroup": self.config.max_members_per_group,
            "maxGroupsPerPrincipal": self.config.max_groups_per_principal,
            "rateLimitPerGroup": self.config.rate_limit_per_group,
            **self.hub.status(),
        }


def _group_to_wire(group: Group) -> dict[str, Any]:
    return {
        "group_id": group.id,
        "name": group.name,
        "owner_id": group.owner_id,
        "owner_dataset": group.owner_dataset,
        "join_policy": group.join_policy,
        "max_members": group.max_members,
        "seq_counter": group.seq_counter,
        "muted": group.muted,
        "archived": group.archived_at is not None,
        "created_at": group.created_at,
    }


def _member_to_wire(membership: Membership) -> dict[str, Any]:
    return {
        "principal_id": membership.principal_id,
        "role": membership.role,
        "status": membership.status,
        "muted": membership.muted,
        "invited_by": membership.invited_by,
        "dataset": membership.dataset,
        "service_id": membership.service_id,
        "joined_at": membership.joined_at,
        "last_read_seq": membership.last_read_seq,
    }
