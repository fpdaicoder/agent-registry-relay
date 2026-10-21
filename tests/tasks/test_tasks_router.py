"""Task router tests: the HTTP/WS surface and its identity/trust contract.

Where ``test_service.py`` pins the fencing invariants, this file pins the
boundary rules: identity must come from the bearer credential (never the
payload), the device is the API-key id behind that credential, and reserved
message kinds are rejected at the edge.
"""

from __future__ import annotations


# ── Mounting ─────────────────────────────────────────────────────────────


def test_routes_are_mounted(app_client):
    from a2x_registry.backend.app import app

    paths = set(app.openapi()["paths"])
    assert "/api/tasks" in paths
    assert "/api/tasks/{task_id}" in paths
    assert "/api/tasks/{task_id}/acquire" in paths
    assert "/api/tasks/{task_id}/messages" in paths
    assert "/api/tasks/{task_id}/retire" in paths


# ── Identity from the credential ─────────────────────────────────────────


def test_create_task_returns_201_and_owner_is_the_caller(app_client):
    task = app_client.post("/api/tasks", json={"title": "triage"}).json()
    assert task["state"] == "pending"
    tid = task["task_id"]

    detail = app_client.get(f"/api/tasks/{tid}").json()
    assert detail["task"]["owner_id"] == app_client.pid("alice")
    assert detail["task"]["title"] == "triage"


def test_owner_comes_from_credential_not_body(app_client):
    """An ``owner_id`` smuggled into the body must be ignored."""
    app_client.act_as("mallory", "primary")
    task = app_client.post(
        "/api/tasks", json={"title": "mine", "owner_id": "someone-else"}
    ).json()
    detail = app_client.get(f"/api/tasks/{task['task_id']}").json()
    assert detail["task"]["owner_id"] == app_client.pid("mallory")


def test_non_owner_cannot_read(app_client):
    tid = app_client.post("/api/tasks", json={"title": "alice-only"}).json()["task_id"]
    app_client.act_as("mallory", "primary")
    response = app_client.get(f"/api/tasks/{tid}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "task_not_found"


# ── Device = the API key id ──────────────────────────────────────────────


def test_acquire_holder_is_the_authenticating_key_id(app_client):
    app_client.act_as("alice", "phone")
    tid = app_client.post("/api/tasks", json={"title": "t"}).json()["task_id"]

    acq = app_client.post(f"/api/tasks/{tid}/acquire", json={}).json()
    assert acq["fence"] == 1
    assert acq["holder"] == app_client.key_id("alice", "phone")


def test_message_device_id_comes_from_credential(app_client):
    app_client.act_as("alice", "phone")
    tid = app_client.post("/api/tasks", json={"title": "t"}).json()["task_id"]
    fence = app_client.post(f"/api/tasks/{tid}/acquire", json={}).json()["fence"]

    app_client.post(
        f"/api/tasks/{tid}/messages", json={"fence": fence, "payload": {"text": "hi"}}
    )
    messages = app_client.get(f"/api/tasks/{tid}/messages").json()["messages"]
    ours = [m for m in messages if m["kind"] == "text"][0]
    assert ours["device_id"] == app_client.key_id("alice", "phone")
    # Messages from the owner's own device are trusted by construction.
    assert ours["trusted"] is True


# ── Fencing over the wire ────────────────────────────────────────────────


def test_takeover_phone_to_pc_fences_the_phone(app_client):
    tid = app_client.post("/api/tasks", json={"title": "continuation"}).json()["task_id"]

    app_client.act_as("alice", "phone")
    first = app_client.post(f"/api/tasks/{tid}/acquire", json={}).json()
    assert first["fence"] == 1

    app_client.act_as("alice", "pc")
    second = app_client.post(f"/api/tasks/{tid}/acquire", json={}).json()
    assert second["fence"] == 2
    assert second["holder"] == app_client.key_id("alice", "pc")

    app_client.act_as("alice", "phone")
    response = app_client.post(
        f"/api/tasks/{tid}/messages",
        json={"fence": first["fence"], "payload": {"text": "late write"}},
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "task_fence_stale"


# ── Edge validation ──────────────────────────────────────────────────────


def test_missing_fence_is_400(app_client):
    tid = app_client.post("/api/tasks", json={"title": "t"}).json()["task_id"]
    app_client.post(f"/api/tasks/{tid}/acquire", json={})
    response = app_client.post(
        f"/api/tasks/{tid}/messages", json={"payload": {"text": "x"}}
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "task_fence_invalid"


def test_reserved_kind_is_rejected_at_the_edge(app_client):
    tid = app_client.post("/api/tasks", json={"title": "t"}).json()["task_id"]
    fence = app_client.post(f"/api/tasks/{tid}/acquire", json={}).json()["fence"]
    response = app_client.post(
        f"/api/tasks/{tid}/messages",
        json={"fence": fence, "kind": "system", "payload": {"x": 1}},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "task_kind_reserved"


def test_unauthenticated_request_is_401(app_client):
    response = app_client._client.get("/api/tasks")
    assert response.status_code == 401


# ── Module off switch ────────────────────────────────────────────────────


def test_module_reports_404_when_not_enabled(app_client):
    """The same off-switch contract as the relay, tunnel and group chat."""
    from a2x_registry.tasks.deps import set_tasks_service

    set_tasks_service(None)
    response = app_client.get("/api/tasks/status")
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "tasks_disabled"


def test_status_snapshot_exposes_the_backend(app_client):
    body = app_client.get("/api/tasks/status").json()
    assert body["enabled"] is True
    assert body["backend"] == "memory"
    assert "maxMessageBytes" in body
    assert "leaseTtlSeconds" in body