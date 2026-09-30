"""Group chat service tests: membership, permissions, cursor, invites.

These exercise the service and store layers directly rather than through
HTTP, so a failure points at the rule that broke instead of at routing.
The HTTP surface has its own file.
"""

from __future__ import annotations

import asyncio

import pytest

from a2x_registry.groupchat.errors import GroupChatError
from a2x_registry.groupchat.models import Membership


def run(coro):
    """Drive one coroutine to completion.

    ``pytest-asyncio`` is not a dependency of this repository, and adding one
    for a single new module is not worth it — the store's concurrency
    invariants are exercised explicitly where they matter.
    """
    return asyncio.run(coro)


# ── Group creation ───────────────────────────────────────────────────────


def test_create_group_seats_owner_and_writes_bootstrap_event(started):
    group = run(
        started.create_group(actor="p_alice", name="triage", owner_dataset="default")
    )

    assert group.id == 1
    assert group.owner_id == "p_alice"
    # The counter starts at 1 because the creation event occupies seq=1.
    assert group.seq_counter == 1

    membership = run(started.store.get_membership(group.id, "p_alice"))
    assert membership is not None
    assert membership.role == "owner"
    assert membership.status == "active"
    # The owner is caught up on its own bootstrap event, so it does not
    # immediately see one unread message.
    assert membership.last_read_seq == 1

    page = run(started.store.fetch_page(group.id, after_seq=0, limit=10))
    assert [message.seq for message in page.messages] == [1]
    assert page.messages[0].kind == "system"


def test_group_creation_is_atomic_no_orphan_without_owner(started):
    """A group with no member would be unrepairable: nobody could invite."""
    group = run(started.create_group(actor="p_alice", name="solo", owner_dataset="default"))
    members = run(started.store.list_members(group.id))
    assert len(members) == 1
    assert members[0].status == "active"


def test_group_name_rejects_control_characters(started):
    """Two groups rendering identically would make name-based trust spoofable."""
    with pytest.raises(GroupChatError) as excinfo:
        run(
            started.create_group(
                actor="p_alice", name="triage​", owner_dataset="default"
            )
        )
    assert excinfo.value.code == "groupchat_name_invalid"


def test_group_name_rejects_empty_and_overlong(started):
    for bad in ("", "   ", "x" * 200):
        with pytest.raises(GroupChatError) as excinfo:
            run(started.create_group(actor="p_alice", name=bad, owner_dataset="default"))
        assert excinfo.value.code == "groupchat_name_invalid"


def test_group_quota_is_enforced_per_owner(config, started):
    for index in range(config.max_groups_per_principal):
        run(
            started.create_group(
                actor="p_alice", name=f"g{index}", owner_dataset="default"
            )
        )
    with pytest.raises(GroupChatError) as excinfo:
        run(started.create_group(actor="p_alice", name="one-too-many", owner_dataset="default"))
    assert excinfo.value.code == "groupchat_group_quota_exceeded"


def test_max_members_cannot_exceed_configured_ceiling(config, started):
    with pytest.raises(GroupChatError) as excinfo:
        run(
            started.create_group(
                actor="p_alice",
                name="big",
                owner_dataset="default",
                max_members=config.max_members_per_group + 1,
            )
        )
    assert excinfo.value.code == "groupchat_max_members_invalid"


# ── Non-member disclosure ────────────────────────────────────────────────


def test_non_member_cannot_read_group(started):
    group = run(started.create_group(actor="p_alice", name="private", owner_dataset="default"))
    with pytest.raises(GroupChatError) as excinfo:
        run(started.get_group(actor="p_mallory", group_id=group.id))
    # 404 rather than 403: a stranger should not learn the group exists.
    assert excinfo.value.status_code == 404


def test_non_member_cannot_send(started):
    group = run(started.create_group(actor="p_alice", name="private", owner_dataset="default"))
    with pytest.raises(GroupChatError) as excinfo:
        run(
            started.send_message(
                actor="p_mallory", group_id=group.id, payload={"text": "hello"}
            )
        )
    assert excinfo.value.status_code == 404


# ── Invitations ──────────────────────────────────────────────────────────


async def _invite_and_accept(started, group_id, inviter, invitee):
    """Async so call sites stay uniform: everything is driven by ``run``."""
    invite = await started.create_invite(
        actor=inviter, group_id=group_id, invitee_id=invitee
    )
    return await started.accept_invite(actor=invitee, token=invite["token"])


def test_targeted_invite_seats_member_as_invited_then_active(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))

    invite = run(
        started.create_invite(
            actor="p_alice", group_id=group.id, invitee_id="p_bob"
        )
    )
    # Plaintext token is returned once; only its digest is persisted.
    assert invite["token"]
    assert invite["mode"] == "targeted"

    pending = run(started.store.get_membership(group.id, "p_bob"))
    assert pending.status == "invited"
    assert pending.role == "member"

    # An invited member has not accepted, so the invite-only semantics must
    # not let them speak yet. 403 rather than 404: the invitation means they
    # legitimately know the group exists, they simply have no write access
    # until they accept.
    with pytest.raises(GroupChatError) as excinfo:
        run(started.send_message(actor="p_bob", group_id=group.id, payload={"text": "hi"}))
    assert excinfo.value.status_code == 403

    accepted = run(started.accept_invite(actor="p_bob", token=invite["token"]))
    assert accepted["role"] == "member"

    member = run(started.store.get_membership(group.id, "p_bob"))
    assert member.status == "active"
    assert member.joined_at is not None


def test_invited_member_becomes_active_but_still_cannot_send_until_accepted(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    invite = run(
        started.create_invite(actor="p_alice", group_id=group.id, invitee_id="p_bob")
    )
    run(started.accept_invite(actor="p_bob", token=invite["token"]))
    sent = run(started.send_message(actor="p_bob", group_id=group.id, payload={"text": "hi"}))
    assert sent.seq == 3


def test_accept_creates_join_event_in_the_shared_seq_space(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    accepted = run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))

    # seq=1 is the creation event, seq=2 the join event.
    assert accepted["joined_seq"] == 2
    page = run(started.store.fetch_page(group.id, after_seq=0, limit=10))
    kinds = [(message.seq, message.kind) for message in page.messages]
    assert kinds == [(1, "system"), (2, "event")]
    assert page.messages[1].payload["event"] == "member_joined"


def test_invite_token_is_bound_to_the_named_invitee(started):
    """A leaked targeted link must not admit a third party."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    invite = run(
        started.create_invite(actor="p_alice", group_id=group.id, invitee_id="p_bob")
    )
    with pytest.raises(GroupChatError) as excinfo:
        run(started.accept_invite(actor="p_mallory", token=invite["token"]))
    assert excinfo.value.code == "groupchat_invite_not_for_caller"


def test_single_use_invite_is_single_use_under_sequential_retry(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    invite = run(
        started.create_invite(actor="p_alice", group_id=group.id, invitee_id="p_bob")
    )
    run(started.accept_invite(actor="p_bob", token=invite["token"]))

    # Bob is already active, so a replay is rejected before it can consume
    # another use — and the recorded count must not have moved.
    with pytest.raises(GroupChatError):
        run(started.accept_invite(actor="p_bob", token=invite["token"]))

    stored = run(started.store.find_invite_by_token_hash(_sha256(invite["token"])))
    assert stored.used_count == 1


def test_consume_invite_is_the_only_guard_against_double_redeem(started):
    """The conditional increment must refuse the second call."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    invite = run(
        started.create_invite(actor="p_alice", group_id=group.id, invitee_id="p_bob")
    )
    token_hash = _sha256(invite["token"])
    stored = run(started.store.find_invite_by_token_hash(token_hash))

    assert run(started.store.consume_invite(stored.id, now=0.0)) is True
    assert run(started.store.consume_invite(stored.id, now=0.0)) is False


def test_expired_invite_is_rejected(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    invite = run(
        started.create_invite(
            actor="p_alice", group_id=group.id, invitee_id="p_bob", ttl_seconds=1
        )
    )
    stored = run(started.store.find_invite_by_token_hash(_sha256(invite["token"])))
    # Pin the clock forward rather than sleeping.
    assert run(started.store.consume_invite(stored.id, now=stored.expires_at + 1)) is False


def test_open_invite_requires_max_uses(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    with pytest.raises(GroupChatError) as excinfo:
        run(started.create_invite(actor="p_alice", group_id=group.id))
    assert excinfo.value.code == "groupchat_max_uses_required"


def test_open_invite_admits_multiple_distinct_principals(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    invite = run(
        started.create_invite(actor="p_alice", group_id=group.id, max_uses=2)
    )
    assert run(started.accept_invite(actor="p_bob", token=invite["token"]))["role"] == "member"
    assert run(started.accept_invite(actor="p_carol", token=invite["token"]))["role"] == "member"
    with pytest.raises(GroupChatError) as excinfo:
        run(started.accept_invite(actor="p_dave", token=invite["token"]))
    assert excinfo.value.code == "groupchat_invite_exhausted"


def test_member_can_invite_exactly_one_agent(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))  # bob = member
    invite = run(
        started.create_invite(
            actor="p_bob", group_id=group.id, invitee_id="p_agent", invitee_is_agent=True
        )
    )
    assert invite["mode"] == "targeted"
    accepted = run(started.accept_invite(actor="p_agent", token=invite["token"]))
    assert accepted["role"] == "member"
    # A second agent by the same member is refused.
    with pytest.raises(GroupChatError) as excinfo:
        run(
            started.create_invite(
                actor="p_bob", group_id=group.id, invitee_id="p_agent2", invitee_is_agent=True
            )
        )
    assert excinfo.value.code == "groupchat_agent_already_invited"


def test_member_cannot_invite_a_human(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))  # bob = member
    with pytest.raises(GroupChatError) as excinfo:
        run(
            started.create_invite(
                actor="p_bob", group_id=group.id, invitee_id="p_carol", invitee_is_agent=False
            )
        )
    assert excinfo.value.code == "groupchat_invitee_not_agent"


def test_member_cannot_issue_an_open_invite(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))  # bob = member
    with pytest.raises(GroupChatError) as excinfo:
        run(started.create_invite(actor="p_bob", group_id=group.id, max_uses=3))
    assert excinfo.value.code == "groupchat_forbidden"


def test_owner_can_still_invite_a_human(started):
    """Relaxing member invites must not narrow the owner's own powers."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    invite = run(
        started.create_invite(
            actor="p_alice", group_id=group.id, invitee_id="p_bob", invitee_is_agent=False
        )
    )
    assert invite["mode"] == "targeted"


def test_banned_principal_cannot_be_reinvited(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(started.remove_member(actor="p_alice", group_id=group.id, target="p_bob", ban=True))

    with pytest.raises(GroupChatError) as excinfo:
        run(started.create_invite(actor="p_alice", group_id=group.id, invitee_id="p_bob"))
    assert excinfo.value.code == "groupchat_member_banned"


def test_ban_is_terminal_even_with_a_valid_open_invite(started):
    """A banned principal must not slip back in through a link invite."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(started.remove_member(actor="p_alice", group_id=group.id, target="p_bob", ban=True))

    invite = run(started.create_invite(actor="p_alice", group_id=group.id, max_uses=5))
    with pytest.raises(GroupChatError) as excinfo:
        run(started.accept_invite(actor="p_bob", token=invite["token"]))
    assert excinfo.value.code == "groupchat_member_banned"


def test_kicked_member_may_rejoin_while_banned_may_not(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    kicked = run(
        started.remove_member(actor="p_alice", group_id=group.id, target="p_bob")
    )
    assert kicked["status"] == "kicked"

    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    member = run(started.store.get_membership(group.id, "p_bob"))
    assert member.status == "active"


def test_no_invite_admits_a_member_beyond_max_members(config, started):
    group = run(
        started.create_group(
            actor="p_alice", name="small", owner_dataset="default", max_members=2
        )
    )
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))

    with pytest.raises(GroupChatError) as excinfo:
        run(started.create_invite(actor="p_alice", group_id=group.id, max_uses=5))
    assert excinfo.value.code == "groupchat_group_full"


# ── Messages ─────────────────────────────────────────────────────────────


def test_send_allocates_monotonic_seq(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    seqs = [
        run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": f"m{i}"})).seq
        for i in range(5)
    ]
    assert seqs == [2, 3, 4, 5, 6]


def test_send_is_idempotent_per_sender_and_group(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    first = run(
        started.send_message(
            actor="p_alice",
            group_id=group.id,
            payload={"text": "retry me"},
            client_msg_id="c_1",
        )
    )
    second = run(
        started.send_message(
            actor="p_alice",
            group_id=group.id,
            payload={"text": "retry me"},
            client_msg_id="c_1",
        )
    )
    assert first.seq == second.seq
    assert run(started.store.latest_seq(group.id)) == first.seq


def test_idempotency_key_does_not_collide_across_groups(started):
    """An Agent reusing a deterministic key after a restart stays in its group.

    ``seq`` is per-group, so two groups both reach seq=2 here; what must not
    happen is the second send returning the *first* group's message.
    """
    first_group = run(started.create_group(actor="p_alice", name="one", owner_dataset="default"))
    second_group = run(started.create_group(actor="p_alice", name="two", owner_dataset="default"))

    first = run(
        started.send_message(
            actor="p_alice", group_id=first_group.id, payload={"text": "a"}, client_msg_id="same"
        )
    )
    second = run(
        started.send_message(
            actor="p_alice", group_id=second_group.id, payload={"text": "b"}, client_msg_id="same"
        )
    )
    assert first.group_id != second.group_id
    assert first.message_id != second.message_id
    assert second.payload["text"] == "b"


def test_idempotency_is_scoped_per_sender(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))

    alice = run(
        started.send_message(
            actor="p_alice", group_id=group.id, payload={"text": "a"}, client_msg_id="k"
        )
    )
    bob = run(
        started.send_message(
            actor="p_bob", group_id=group.id, payload={"text": "b"}, client_msg_id="k"
        )
    )
    assert alice.seq != bob.seq


def test_message_payload_size_is_bounded(config, started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    oversized = {"text": "x" * (config.max_message_bytes + 1)}
    with pytest.raises(GroupChatError) as excinfo:
        run(started.send_message(actor="p_alice", group_id=group.id, payload=oversized))
    assert excinfo.value.code == "groupchat_message_too_large"
    assert excinfo.value.status_code == 413


def test_client_cannot_write_reserved_kinds(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    for kind in ("event", "system"):
        with pytest.raises(GroupChatError) as excinfo:
            run(
                started.send_message(
                    actor="p_alice", group_id=group.id, payload={"x": 1}, kind=kind
                )
            )
        assert excinfo.value.code == "groupchat_kind_reserved"


def test_mention_must_target_an_active_member(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    with pytest.raises(GroupChatError) as excinfo:
        run(
            started.send_message(
                actor="p_alice",
                group_id=group.id,
                payload={"text": "hey"},
                mentions=["p_stranger"],
            )
        )
    assert excinfo.value.code == "groupchat_mention_invalid"


def test_readonly_member_cannot_send(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(
        started.set_member_role(
            actor="p_alice", group_id=group.id, target="p_bob", role="readonly"
        )
    )
    with pytest.raises(GroupChatError) as excinfo:
        run(started.send_message(actor="p_bob", group_id=group.id, payload={"text": "hi"}))
    assert excinfo.value.status_code == 403


def test_muted_group_blocks_plain_members_but_not_admins(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(started.update_group(actor="p_alice", group_id=group.id, muted=True))

    with pytest.raises(GroupChatError) as excinfo:
        run(started.send_message(actor="p_bob", group_id=group.id, payload={"text": "hi"}))
    assert excinfo.value.code == "groupchat_group_muted"

    # The owner still can, so a muted group is still administrable.
    assert run(
        started.send_message(actor="p_alice", group_id=group.id, payload={"text": "notice"})
    ).seq


# ── Cursor and sync ──────────────────────────────────────────────────────


def test_cursor_never_rewinds(started):
    """A late ACK from a second connection must not replay history."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": "one"}))

    forward = run(started.ack(actor="p_alice", group_id=group.id, seq=2))
    assert forward["last_read_seq"] == 2

    stale = run(started.ack(actor="p_alice", group_id=group.id, seq=1))
    assert stale["last_read_seq"] == 2


def test_fetch_without_after_seq_uses_the_server_cursor(started):
    """This is what lets an Agent whose local state was lost resume correctly."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    for index in range(3):
        run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": str(index)}))

    # Bob has never acked, but his cursor was seeded at join time.
    first = run(started.fetch_messages(actor="p_bob", group_id=group.id))
    assert first["messages"], "a fresh member must receive subsequent messages"
    run(started.ack(actor="p_bob", group_id=group.id, seq=first["next_seq"]))

    run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": "later"}))
    second = run(started.fetch_messages(actor="p_bob", group_id=group.id))
    assert len(second["messages"]) == 1
    assert second["messages"][0]["payload"]["text"] == "later"


def test_fetch_reports_latest_seq_and_has_more(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    for index in range(5):
        run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": str(index)}))

    page = run(started.fetch_messages(actor="p_alice", group_id=group.id, after_seq=0, limit=2))
    assert len(page["messages"]) == 2
    assert page["has_more"] is True
    assert page["latest_seq"] == 6
    assert page["next_seq"] == 2


def test_reverse_page_walks_history_backwards(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    for index in range(5):
        run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": str(index)}))

    page = run(
        started.fetch_messages(actor="p_alice", group_id=group.id, before_seq=6, limit=2)
    )
    assert [message["seq"] for message in page["messages"]] == [4, 5]
    assert page["has_more"] is True


def test_cannot_request_both_cursors(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    with pytest.raises(GroupChatError) as excinfo:
        run(
            started.fetch_messages(
                actor="p_alice", group_id=group.id, after_seq=1, before_seq=5
            )
        )
    assert excinfo.value.code == "groupchat_cursor_conflict"


def test_ack_ahead_of_latest_is_rejected(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    with pytest.raises(GroupChatError) as excinfo:
        run(started.ack(actor="p_alice", group_id=group.id, seq=999))
    assert excinfo.value.code == "groupchat_seq_ahead"


def test_page_size_is_capped_by_configuration(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    for index in range(5):
        run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": str(index)}))
    page = run(
        started.fetch_messages(actor="p_alice", group_id=group.id, after_seq=0, limit=10_000)
    )
    assert len(page["messages"]) <= started.config.max_page_size


# ── Removal ──────────────────────────────────────────────────────────────


def test_leave_removes_hub_subscription(started):
    """A departed member must stop receiving frames, not just lose write access."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))

    subscriber = started.hub.register("conn_bob", "p_bob")
    started.hub.subscribe(subscriber, group.id)
    assert started.hub.subscriber_count(group.id) == 1

    run(started.remove_member(actor="p_bob", group_id=group.id))

    assert started.hub.subscriber_count(group.id) == 0
    assert started.hub._by_connection.get("conn_bob") is None  # noqa: SLF001


def test_kick_drops_every_connection_of_the_target(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    for connection_id in ("c1", "c2"):
        subscriber = started.hub.register(connection_id, "p_bob")
        started.hub.subscribe(subscriber, group.id)

    result = run(
        started.remove_member(actor="p_alice", group_id=group.id, target="p_bob")
    )
    assert result["dropped_connections"] == 2
    assert started.hub.subscriber_count(group.id) == 0


def test_owner_cannot_be_removed(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(
        started.set_member_role(actor="p_alice", group_id=group.id, target="p_bob", role="admin")
    )
    with pytest.raises(GroupChatError) as excinfo:
        run(started.remove_member(actor="p_bob", group_id=group.id, target="p_alice"))
    assert excinfo.value.code == "groupchat_owner_immutable"


def test_admin_cannot_remove_another_admin(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    for principal in ("p_bob", "p_carol"):
        run(_invite_and_accept(started, group.id, "p_alice", principal))
        run(
            started.set_member_role(
                actor="p_alice", group_id=group.id, target=principal, role="admin"
            )
        )
    with pytest.raises(GroupChatError) as excinfo:
        run(started.remove_member(actor="p_bob", group_id=group.id, target="p_carol"))
    assert excinfo.value.status_code == 403


def test_owner_may_remove_an_admin(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(
        started.set_member_role(actor="p_alice", group_id=group.id, target="p_bob", role="admin")
    )
    result = run(started.remove_member(actor="p_alice", group_id=group.id, target="p_bob"))
    assert result["status"] == "kicked"


def test_leave_writes_an_event_into_the_shared_seq_space(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(started.remove_member(actor="p_bob", group_id=group.id))

    page = run(started.store.fetch_page(group.id, after_seq=0, limit=20))
    kinds = [(message.seq, message.payload.get("event")) for message in page.messages]
    assert kinds == [
        (1, "group_created"),
        (2, "member_joined"),
        (3, "left"),
    ]


def test_removal_requires_admin_role(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    for principal in ("p_bob", "p_carol"):
        run(_invite_and_accept(started, group.id, "p_alice", principal))
    with pytest.raises(GroupChatError) as excinfo:
        run(started.remove_member(actor="p_bob", group_id=group.id, target="p_carol"))
    assert excinfo.value.status_code == 403


# ── Untrusted content ────────────────────────────────────────────────────


def test_every_message_serializes_as_untrusted(started):
    """Group content is data written by a peer, never an instruction.

    The receiving Agent must not execute it, and no API response may claim
    otherwise.
    """
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": "hi"}))
    page = run(started.fetch_messages(actor="p_alice", group_id=group.id, after_seq=0))
    assert page["messages"]
    for message in page["messages"]:
        assert message["trusted"] is False


def test_caller_cannot_assert_sender_identity(started):
    """Sender is taken from the caller argument, never from the payload."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    message = run(
        started.send_message(
            actor="p_bob",
            group_id=group.id,
            payload={"text": "hi", "sender_id": "p_alice"},
        )
    )
    assert message.sender_id == "p_bob"


# ── Rate limiting ────────────────────────────────────────────────────────


def test_group_rate_limit_triggers(config, service):
    async def scenario():
        await service.start()
        tight = config.__class__(**{**config.__dict__, "rate_limit_per_group": 3})
        service.config = tight
        group = await service.create_group(
            actor="p_alice", name="busy", owner_dataset="default"
        )
        for index in range(3):
            await service.send_message(
                actor="p_alice", group_id=group.id, payload={"text": str(index)}
            )
        try:
            await service.send_message(actor="p_alice", group_id=group.id, payload={"text": "x"})
        except GroupChatError as exc:
            assert exc.code == "groupchat_rate_limited"
            assert exc.status_code == 429
        else:  # pragma: no cover - explicit failure message
            raise AssertionError("rate limit did not trigger")
        finally:
            await service.stop()

    run(scenario())


def _sha256(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# ── Archive ──────────────────────────────────────────────────────────────


def test_archive_is_reachable_through_update_group(started):
    """The store supported archiving from the start; the service must expose it."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))

    archived = run(
        started.update_group(actor="p_alice", group_id=group.id, archived=True)
    )
    assert archived.archived_at is not None

    # An archived group drops out of listings.
    assert run(started.list_groups(actor="p_alice", limit=10)) == []

    # But it stays readable by id, so the history is not sealed away.
    detail = run(started.get_group(actor="p_alice", group_id=group.id))
    assert detail["group"]["archived"] is True


def test_archive_keeps_the_history(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": "keep me"}))
    run(started.update_group(actor="p_alice", group_id=group.id, archived=True))

    # The rows are untouched, which is the whole difference from deletion.
    # The trailing entry is the archive event itself.
    page = run(started.store.fetch_page(group.id, after_seq=0, limit=10))
    assert [message.payload.get("text") for message in page.messages] == [
        None,
        "keep me",
        None,
    ]


def test_archive_writes_an_event_in_the_shared_seq_space(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(started.update_group(actor="p_alice", group_id=group.id, archived=True))

    page = run(started.store.fetch_page(group.id, after_seq=0, limit=10))
    assert page.messages[-1].payload["event"] == "group_archived"
    assert page.messages[-1].seq == 2


def test_archiving_requires_admin_role(started):
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    with pytest.raises(GroupChatError) as excinfo:
        run(started.update_group(actor="p_bob", group_id=group.id, archived=True))
    assert excinfo.value.status_code == 403


def test_rename_does_not_emit_an_archive_event(started):
    """Only an actual archive transition writes to the log."""
    group = run(started.create_group(actor="p_alice", name="triage", owner_dataset="default"))
    run(started.update_group(actor="p_alice", group_id=group.id, name="renamed"))
    assert run(started.store.latest_seq(group.id)) == 1


# ── Delete ───────────────────────────────────────────────────────────────


def test_delete_removes_the_group_and_its_rows(started):
    group = run(started.create_group(actor="p_alice", name="doomed", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(started.send_message(actor="p_alice", group_id=group.id, payload={"text": "bye"}))

    result = run(started.delete_group(actor="p_alice", group_id=group.id))
    assert result["status"] == "deleted"

    # Everything scoped to the group is gone, not just hidden.
    assert run(started.store.get_group(group.id)) is None
    assert run(started.store.list_members(group.id)) == []
    assert run(started.store.fetch_page(group.id, after_seq=0, limit=10)).messages == []
    assert run(started.store.latest_seq(group.id)) == 0


def test_delete_removes_memberships_so_the_principal_can_be_re_invited(started):
    """A deleted group must not leave a membership row behind."""
    group = run(started.create_group(actor="p_alice", name="doomed", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(started.delete_group(actor="p_alice", group_id=group.id))

    assert run(started.store.get_membership(group.id, "p_bob")) is None


def test_delete_requires_owner_not_just_admin(started):
    """A delegated role must not be able to erase other principals' messages."""
    group = run(started.create_group(actor="p_alice", name="doomed", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    run(
        started.set_member_role(actor="p_alice", group_id=group.id, target="p_bob", role="admin")
    )

    with pytest.raises(GroupChatError) as excinfo:
        run(started.delete_group(actor="p_bob", group_id=group.id))
    assert excinfo.value.code == "groupchat_owner_required"
    assert excinfo.value.status_code == 403
    # The failed attempt must not have touched anything.
    assert run(started.store.get_group(group.id)) is not None


def test_delete_by_non_member_is_not_found(started):
    group = run(started.create_group(actor="p_alice", name="doomed", owner_dataset="default"))
    with pytest.raises(GroupChatError) as excinfo:
        run(started.delete_group(actor="p_mallory", group_id=group.id))
    assert excinfo.value.status_code == 404


def test_delete_drops_subscriptions(started):
    group = run(started.create_group(actor="p_alice", name="doomed", owner_dataset="default"))
    run(_invite_and_accept(started, group.id, "p_alice", "p_bob"))
    for connection_id in ("c1", "c2"):
        subscriber = started.hub.register(connection_id, "p_bob")
        started.hub.subscribe(subscriber, group.id)

    result = run(started.delete_group(actor="p_alice", group_id=group.id))
    assert result["dropped_connections"] == 2
    assert started.hub.subscriber_count(group.id) == 0


def test_delete_broadcasts_before_the_rows_go(started):
    """Subscribers learn the group is gone; an empty pull would be ambiguous."""
    group = run(started.create_group(actor="p_alice", name="doomed", owner_dataset="default"))
    subscriber = started.hub.register("conn_bob", "p_bob")
    started.hub.subscribe(subscriber, group.id)

    run(started.delete_group(actor="p_alice", group_id=group.id))

    # The frame was queued before the subscription was dropped.
    assert not subscriber.queue.empty()
    frame = subscriber.queue.get_nowait()
    assert frame["type"] == "group_deleted"


def test_delete_frees_a_group_slot(started):
    """The quota counts live groups, so deleting one lets the owner create another."""
    group = run(started.create_group(actor="p_alice", name="doomed", owner_dataset="default"))
    run(started.delete_group(actor="p_alice", group_id=group.id))
    assert run(started.store.count_groups_owned_by("p_alice")) == 0


def test_delete_twice_is_not_found_the_second_time(started):
    group = run(started.create_group(actor="p_alice", name="doomed", owner_dataset="default"))
    run(started.delete_group(actor="p_alice", group_id=group.id))
    with pytest.raises(GroupChatError) as excinfo:
        run(started.delete_group(actor="p_alice", group_id=group.id))
    assert excinfo.value.status_code == 404


def test_deleting_one_group_leaves_others_intact(started):
    keep = run(started.create_group(actor="p_alice", name="keep", owner_dataset="default"))
    drop = run(started.create_group(actor="p_alice", name="drop", owner_dataset="default"))
    run(started.delete_group(actor="p_alice", group_id=drop.id))

    assert run(started.store.get_group(keep.id)) is not None
    assert run(started.store.latest_seq(keep.id)) == 1


# ── Store interface conformance ──────────────────────────────────────────


@pytest.mark.parametrize(
    "method",
    [
        "create_group",
        "get_group",
        "list_groups_for",
        "update_group",
        "count_groups_owned_by",
        "get_membership",
        "list_members",
        "upsert_membership",
        "set_membership_status",
        "count_members",
        "advance_cursor",
        "create_invite",
        "get_invite",
        "find_invite_by_token_hash",
        "consume_invite",
        "append_message",
        "fetch_page",
        "fetch_reverse_page",
        "latest_seq",
    ],
)
def test_store_declares_expected_operations(method):
    """Both backends must implement the full interface.

    Listed explicitly so that adding a method to the abstract base without
    implementing it in a backend fails here rather than at first use.
    """
    from a2x_registry.groupchat.sqlstore import GroupChatStore, MemoryStore

    assert hasattr(GroupChatStore, method)
    assert getattr(MemoryStore, method, None) is not None
