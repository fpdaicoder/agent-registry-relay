"""HTTP surface tests for the group chat module.

These cover the concerns that only appear once routing is involved: the
structured 404 when the module is disabled, sender identity being taken from
the credential rather than the body, and the reserved ``kind`` values that a
client must not be able to write.
"""

from __future__ import annotations


def _create(client, name="triage", **extra):
    response = client.post("/api/groups", json={"name": name, **extra})
    return response


def test_create_group_returns_201_and_owner(app_client):
    response = _create(app_client, "triage")
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["name"] == "triage"
    assert body["seq_counter"] == 1
    assert body["group_id"] == 1


def test_owner_is_read_from_the_credential_not_the_body(app_client):
    """A caller must not be able to make someone else the owner."""
    response = app_client.post(
        "/api/groups",
        json={"name": "triage", "owner_id": "someone-else"},
    )
    assert response.status_code == 201
    group_id = response.json()["group_id"]

    detail = app_client.get(f"/api/groups/{group_id}").json()
    assert detail["group"]["owner_id"] == app_client.pid("alice")


def test_group_without_a_name_is_rejected(app_client):
    response = app_client.post("/api/groups", json={})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "groupchat_name_invalid"


def test_malformed_body_is_a_client_error(app_client):
    response = app_client.post(
        "/api/groups", content=b"not json", headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "groupchat_body_invalid"


def test_send_returns_the_sequence_and_marks_content_untrusted(app_client):
    group_id = _create(app_client).json()["group_id"]
    response = app_client.post(
        f"/api/groups/{group_id}/messages",
        json={"payload": {"text": "hello"}, "client_msg_id": "c_1"},
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["seq"] == 2
    # This flag is server-derived and must never be true for group content.
    assert body["trusted"] is False


def test_sender_is_not_taken_from_the_payload(app_client):
    app_client.act_as("alice")
    group_id = _create(app_client).json()["group_id"]
    app_client.post(
        f"/api/groups/{group_id}/messages",
        json={"payload": {"text": "hi", "sender_id": "someone-else"}},
    )

    page = app_client.get(
        f"/api/groups/{group_id}/messages", params={"after_seq": 0}
    ).json()
    assert all(
        message["sender_id"] == app_client.pid("alice") for message in page["messages"]
    )


def test_missing_payload_is_rejected(app_client):
    group_id = _create(app_client).json()["group_id"]
    response = app_client.post(f"/api/groups/{group_id}/messages", json={"kind": "text"})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "groupchat_payload_required"


def test_client_cannot_inject_an_event_entry(app_client):
    """Membership events share the log; they must not be client-writable."""
    group_id = _create(app_client).json()["group_id"]
    response = app_client.post(
        f"/api/groups/{group_id}/messages",
        json={"payload": {"event": "member_joined"}, "kind": "event"},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "groupchat_kind_reserved"


def test_non_member_gets_404_not_403(app_client):
    group_id = _create(app_client).json()["group_id"]
    app_client.act_as("mallory")
    assert app_client.get(f"/api/groups/{group_id}").status_code == 404


def test_invite_accept_flow_over_http(app_client):
    group_id = _create(app_client).json()["group_id"]

    invite = app_client.post(
        f"/api/groups/{group_id}/invites", json={"invitee_id": app_client.pid("bob")}
    ).json()
    assert invite["token"]
    assert invite["mode"] == "targeted"

    # The invitee cannot act before accepting.
    app_client.act_as("bob")
    assert app_client.get(f"/api/groups/{group_id}").status_code == 200
    assert (
        app_client.post(
            f"/api/groups/{group_id}/messages", json={"payload": {"text": "hi"}}
        ).status_code
        == 403
    )

    accepted = app_client.post(f"/api/invites/{invite['token']}/accept")
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["role"] == "member"

    assert (
        app_client.post(
            f"/api/groups/{group_id}/messages", json={"payload": {"text": "hi"}}
        ).status_code
        == 201
    )


def test_accepting_someone_elses_targeted_invite_is_forbidden(app_client):
    group_id = _create(app_client).json()["group_id"]
    invite = app_client.post(
        f"/api/groups/{group_id}/invites", json={"invitee_id": app_client.pid("bob")}
    ).json()

    app_client.act_as("mallory")
    response = app_client.post(f"/api/invites/{invite['token']}/accept")
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "groupchat_invite_not_for_caller"


def test_cursor_endpoints_round_trip(app_client):
    group_id = _create(app_client).json()["group_id"]
    app_client.post(f"/api/groups/{group_id}/messages", json={"payload": {"text": "one"}})

    ack = app_client.post(f"/api/groups/{group_id}/messages/2/ack")
    assert ack.status_code == 200
    assert ack.json()["last_read_seq"] == 2

    page = app_client.get(f"/api/groups/{group_id}/messages").json()
    assert page["server_cursor"] == 2
    assert page["messages"] == []


def test_ack_beyond_latest_is_a_conflict(app_client):
    group_id = _create(app_client).json()["group_id"]
    response = app_client.post(f"/api/groups/{group_id}/messages/999/ack")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "groupchat_seq_ahead"


def test_member_rows_carry_the_principal_kind(app_client):
    """A roster must say which rows are Agents.

    Only the auth store knows a principal's kind, so the router is the layer
    that decorates the rows. Without it every client sees opaque credential
    strings and a peer instance's Agent is indistinguishable from a person.
    """
    group_id = _create(app_client).json()["group_id"]
    invite = app_client.post(
        f"/api/groups/{group_id}/invites", json={"invitee_id": app_client.pid("agent")}
    ).json()
    app_client.act_as("agent")
    app_client.post(f"/api/invites/{invite['token']}/accept")

    rows = {
        member["principal_id"]: member
        for member in app_client.get(f"/api/groups/{group_id}/members").json()["members"]
    }
    assert rows[app_client.pid("agent")]["is_agent"] is True
    # An Agent is named after the principal that invited it, so a reader can
    # tell whose Agent the row is; the badge is what separates it from that
    # principal's own row. The bootstrap admin's handle is "root".
    assert rows[app_client.pid("agent")]["display_name"] == "root"
    assert rows[app_client.pid("alice")]["display_name"] == "root"
    assert rows[app_client.pid("alice")]["is_agent"] is False

    # The same decoration applies to the roster embedded in the group detail.
    detail = app_client.get(f"/api/groups/{group_id}").json()
    detail_rows = {m["principal_id"]: m for m in detail["members"]}
    assert detail_rows[app_client.pid("agent")]["is_agent"] is True


def test_member_is_flagged_as_agent_even_when_it_joined_by_open_link(app_client):
    """Being an Agent is a property of the principal, not of how it joined."""
    group_id = _create(app_client).json()["group_id"]
    invite = app_client.post(
        f"/api/groups/{group_id}/invites", json={"max_uses": 2}
    ).json()
    app_client.act_as("agent")
    app_client.post(f"/api/invites/{invite['token']}/accept")

    app_client.act_as("alice")
    row = next(
        member
        for member in app_client.get(f"/api/groups/{group_id}/members").json()["members"]
        if member["principal_id"] == app_client.pid("agent")
    )
    assert row["is_agent"] is True


def test_leave_removes_the_caller_from_the_member_list(app_client):
    group_id = _create(app_client).json()["group_id"]
    invite = app_client.post(
        f"/api/groups/{group_id}/invites", json={"invitee_id": app_client.pid("bob")}
    ).json()
    app_client.act_as("bob")
    app_client.post(f"/api/invites/{invite['token']}/accept")

    assert app_client.delete(f"/api/groups/{group_id}/members/me").status_code == 200

    members = app_client.get(f"/api/groups/{group_id}/members").json()["members"]
    assert [member["principal_id"] for member in members] == [app_client.pid("alice")]


def test_ban_is_recorded_and_terminal(app_client):
    group_id = _create(app_client).json()["group_id"]
    invite = app_client.post(
        f"/api/groups/{group_id}/invites", json={"invitee_id": app_client.pid("bob")}
    ).json()
    app_client.act_as("bob")
    app_client.post(f"/api/invites/{invite['token']}/accept")

    app_client.act_as("alice")
    banned = app_client.post(
        f"/api/groups/{group_id}/members/{app_client.pid('bob')}/ban"
    )
    assert banned.status_code == 200
    assert banned.json()["status"] == "banned"

    with_out = app_client.post(
        f"/api/groups/{group_id}/invites", json={"invitee_id": app_client.pid("bob")}
    )
    assert with_out.status_code == 403
    assert with_out.json()["error"]["code"] == "groupchat_member_banned"


def test_status_snapshot_exposes_the_backend(app_client):
    body = app_client.get("/api/groups/status").json()
    assert body["enabled"] is True
    assert body["backend"] == "memory"
    assert "maxMessageBytes" in body


def test_module_reports_404_when_not_enabled(app_client):
    """The same contract the relay and tunnel modules use when disabled."""
    from a2x_registry.groupchat.deps import set_groupchat_service

    set_groupchat_service(None)
    response = app_client.get("/api/groups/status")
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "groupchat_disabled"


def test_routes_are_mounted_on_the_registry_app():
    """Read paths from the OpenAPI schema.

    ``app.routes`` is not usable here: this FastAPI version wraps included
    routers in an object without a ``path`` attribute, so walking the route
    list would only ever show the default docs endpoints.
    """
    from a2x_registry.backend.app import app

    paths = set(app.openapi()["paths"])
    assert "/api/groups" in paths
    assert "/api/groups/{group_id}/messages" in paths
    assert "/api/groups/{group_id}/members/me" in paths
    assert "/api/invites/{token}/accept" in paths


def test_group_is_owned_by_a_namespace_for_governance_only(app_client):
    """The owning namespace scopes quota and audit; it does not gate members."""
    group_id = _create(app_client, owner_dataset="research").json()["group_id"]
    detail = app_client.get(f"/api/groups/{group_id}").json()
    assert detail["group"]["owner_dataset"] == "research"

    # A principal from a different namespace can still be invited and join.
    invite = app_client.post(
        f"/api/groups/{group_id}/invites", json={"invitee_id": app_client.pid("bob")}
    ).json()
    app_client.act_as("bob")
    assert app_client.post(f"/api/invites/{invite['token']}/accept").status_code == 200


def test_archive_is_reachable_through_patch(app_client):
    """The store supported archiving before the service exposed it."""
    group_id = _create(app_client).json()["group_id"]

    response = app_client.patch(f"/api/groups/{group_id}", json={"archived": True})
    assert response.status_code == 200, response.text
    assert response.json()["archived"] is True

    assert app_client.get("/api/groups").json()["groups"] == []


def test_delete_group_returns_deleted(app_client):
    group_id = _create(app_client, "doomed").json()["group_id"]

    response = app_client.delete(f"/api/groups/{group_id}")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "deleted"

    # Gone, not merely hidden.
    assert app_client.get(f"/api/groups/{group_id}").status_code == 404
    assert app_client.get("/api/groups").json()["groups"] == []


def test_delete_requires_owner_not_admin(app_client):
    group_id = _create(app_client, "doomed").json()["group_id"]
    invite = app_client.post(
        f"/api/groups/{group_id}/invites", json={"invitee_id": app_client.pid("bob")}
    ).json()
    app_client.act_as("bob")
    app_client.post(f"/api/invites/{invite['token']}/accept")

    app_client.act_as("alice")
    promoted = app_client.put(
        f"/api/groups/{group_id}/members/{app_client.pid('bob')}/role",
        json={"role": "admin"},
    )
    assert promoted.status_code == 200, promoted.text

    app_client.act_as("bob")
    denied = app_client.delete(f"/api/groups/{group_id}")
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "groupchat_owner_required"


def test_delete_by_non_member_is_not_found(app_client):
    group_id = _create(app_client, "doomed").json()["group_id"]
    app_client.act_as("mallory")
    assert app_client.delete(f"/api/groups/{group_id}").status_code == 404


def test_archived_group_remains_readable_by_members(app_client):
    """Archive retires the group from listings without destroying history.

    Members can still open it by id and see the messages; it is the listing
    that drops it. Deleting is the operation that makes it unreachable.
    """
    group_id = _create(app_client).json()["group_id"]
    app_client.post(f"/api/groups/{group_id}/messages", json={"payload": {"text": "kept"}})
    app_client.patch(f"/api/groups/{group_id}", json={"archived": True})

    assert app_client.get("/api/groups").json()["groups"] == []

    detail = app_client.get(f"/api/groups/{group_id}")
    assert detail.status_code == 200
    assert detail.json()["group"]["archived"] is True

    page = app_client.get(
        f"/api/groups/{group_id}/messages", params={"after_seq": 0}
    ).json()
    assert any(
        message["payload"].get("text") == "kept" for message in page["messages"]
    )

    # Deleting an archived group is still possible, and still owner-only.
    assert app_client.delete(f"/api/groups/{group_id}").status_code == 200
