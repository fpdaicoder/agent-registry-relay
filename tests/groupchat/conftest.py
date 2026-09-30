"""Fixtures shared by the group chat tests.

The suite runs against the in-memory store: the module's contract is defined
by the abstract store interface, and the PostgreSQL backend differs only in
how it persists the same operations. Keeping the default suite free of a
database dependency means `pytest` works on a clean checkout.
"""

from __future__ import annotations

import pytest

from a2x_registry.groupchat.config import GroupChatConfig
from a2x_registry.groupchat.deps import set_groupchat_service
from a2x_registry.groupchat.service import GroupChatService
from a2x_registry.groupchat.sqlstore import MemoryStore

# Key under which the bootstrap admin's token lives in the fixture's token
# map. The admin owns everything it creates in these tests.
_ADMIN = "alice"


@pytest.fixture
def config() -> GroupChatConfig:
    return GroupChatConfig(
        enabled=True,
        backend="memory",
        max_message_bytes=4096,
        max_groups_per_principal=5,
        max_members_per_group=10,
        max_page_size=50,
        default_page_size=20,
        rate_limit_per_group=1000,
        rate_limit_window_seconds=60,
    )


@pytest.fixture
def service(config) -> GroupChatService:
    svc = GroupChatService(config, MemoryStore())
    return svc


@pytest.fixture
def installed_service(service):
    set_groupchat_service(service)
    yield service
    set_groupchat_service(None)


@pytest.fixture
def started(service) -> GroupChatService:
    """A started service, driven synchronously.

    Deliberately not an async fixture: this repository does not depend on
    ``pytest-asyncio``, and adding a plugin to the test dependencies for one
    module is a worse trade than calling ``asyncio.run`` explicitly. Each
    test therefore owns one event loop for its whole lifetime, which also
    keeps the store's ``asyncio.Lock`` valid — an object created in one loop
    and awaited in another would fail.
    """
    import asyncio

    asyncio.run(service.start())
    yield service
    asyncio.run(service.stop())


class GroupChatClient:
    """A TestClient wrapper with a switchable caller identity.

    Callers are switched by swapping the bearer token rather than by
    overriding FastAPI dependencies: the auth store is real, so these tests
    exercise the same credential path production uses. Overrides were the
    first approach here and they silently stopped applying in a full-suite
    run, which is exactly the kind of thing a test should not be able to be
    fooled by.

    Principal ids are assigned by the auth store (``u_<hex>``) rather than
    chosen by the caller, so tests refer to actors through :meth:`pid`.
    """

    def __init__(
        self,
        client,
        tokens: dict[str, str],
        principal_ids: dict[str, str],
    ):
        self._client = client
        self._tokens = tokens
        self._principal_ids = principal_ids
        self.current_principal_id = principal_ids[_ADMIN]

    def pid(self, actor: str) -> str:
        """The real principal id behind a named test actor."""
        return self._principal_ids[actor]

    @property
    def current_principal(self) -> dict[str, str]:
        return {"principal_id": self.current_principal_id}

    def act_as(self, actor: str) -> None:
        self.current_principal_id = self._principal_ids.get(actor, actor)

    def _headers(self) -> dict[str, str]:
        # ``current_principal_id`` is a real id after ``act_as``; the token map
        # is keyed by actor name, so look the token up by identity.
        for actor, principal_id in self._principal_ids.items():
            if principal_id == self.current_principal_id:
                return {"Authorization": f"Bearer {self._tokens[actor]}"}
        raise KeyError(f"no credential for principal {self.current_principal_id!r}")

    def _merge(self, kwargs: dict) -> dict:
        """Add the caller's credential without clobbering an explicit header.

        Callers that need to send a raw body pass their own ``headers``; the
        credential is merged in rather than passed twice.
        """
        headers = dict(kwargs.pop("headers", None) or {})
        headers.update(self._headers())
        kwargs["headers"] = headers
        return kwargs

    def get(self, url, **kwargs):
        return self._client.get(url, **self._merge(kwargs))

    def post(self, url, **kwargs):
        return self._client.post(url, **self._merge(kwargs))

    def put(self, url, **kwargs):
        return self._client.put(url, **self._merge(kwargs))

    def patch(self, url, **kwargs):
        return self._client.patch(url, **self._merge(kwargs))

    def delete(self, url, **kwargs):
        return self._client.delete(url, **self._merge(kwargs))


@pytest.fixture
def app_client(monkeypatch, config, tmp_path):
    """A TestClient with group chat enabled and real auth initialized.

    The module is enabled through the environment rather than by injecting a
    service object: ``TestClient`` runs the app lifespan, and that lifespan
    calls ``startup_groupchat``, so anything installed beforehand would be
    replaced. Going through the real startup path also means these tests
    exercise the same wiring production uses.
    """
    import sys

    monkeypatch.setenv("A2X_REGISTRY_HOME", str(tmp_path))
    monkeypatch.setenv("A2X_GROUPCHAT_ENABLED", "true")
    monkeypatch.setenv("A2X_GROUPCHAT_BACKEND", "memory")
    monkeypatch.setenv("A2X_GROUPCHAT_MAX_MESSAGE_BYTES", str(config.max_message_bytes))
    monkeypatch.setenv(
        "A2X_GROUPCHAT_MAX_GROUPS_PER_PRINCIPAL", str(config.max_groups_per_principal)
    )
    monkeypatch.setenv(
        "A2X_GROUPCHAT_MAX_MEMBERS_PER_GROUP", str(config.max_members_per_group)
    )
    monkeypatch.setenv("A2X_GROUPCHAT_MAX_PAGE_SIZE", str(config.max_page_size))
    monkeypatch.setenv("A2X_GROUPCHAT_DEFAULT_PAGE_SIZE", str(config.default_page_size))

    # Reload the package before importing the app, exactly as the shared
    # ``lite_app`` fixture does. Without this, a preceding test that used
    # ``lite_app`` leaves a *different* ``a2x_registry.auth.deps`` module
    # object in ``sys.modules`` than the one the already-built app holds
    # references to — so installing the auth store would land on a module the
    # running app never consults, and every request would 401.
    for name in list(sys.modules):
        if name.startswith("a2x_registry"):
            monkeypatch.delitem(sys.modules, name, raising=False)

    from fastapi.testclient import TestClient

    from a2x_registry.backend.app import app
    from a2x_registry.auth.deps import set_auth_store
    from a2x_registry.auth.store import AuthStore
    from a2x_registry.common import paths

    paths.reset_cache()

    # A real principal per named test actor, each with its own key. Ids are
    # assigned by the store, so keep the actor -> id mapping for assertions.
    store, admin_token = AuthStore.bootstrap(data_dir=tmp_path / "auth_data")
    set_auth_store(store)
    tokens = {_ADMIN: admin_token}
    principal_ids = {_ADMIN: store.list_principals()[0].id}
    for handle in ("bob", "carol", "mallory", "dave"):
        principal, token = store.create_principal(
            handle=handle, role="user", namespaces=["default"]
        )
        tokens[handle] = token
        principal_ids[handle] = principal.id

    # One Agent principal: several routes distinguish an Agent invitee from a
    # human one, so the roster needs a member whose kind is not "human".
    agent, agent_token = store.create_principal(
        handle="robot", role="user", namespaces=["default"], kind="agent"
    )
    tokens["agent"] = agent_token
    principal_ids["agent"] = agent.id

    try:
        with TestClient(app) as client:
            yield GroupChatClient(client, tokens, principal_ids)
    finally:
        set_auth_store(None)
        paths.reset_cache()
