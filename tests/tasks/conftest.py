"""Fixtures shared by the task-continuation tests.

The suite runs against the in-memory store: the module's contract is defined
by the abstract store interface, and the PostgreSQL backend differs only in
how it persists the same operations. Keeping the default suite free of a
database dependency means ``pytest`` works on a clean checkout.
"""

from __future__ import annotations

import pytest

from a2x_registry.tasks.config import TasksConfig
from a2x_registry.tasks.deps import set_tasks_service
from a2x_registry.tasks.service import TasksService
from a2x_registry.tasks.sqlstore import MemoryStore

_ADMIN = "alice"


@pytest.fixture
def config() -> TasksConfig:
    return TasksConfig(
        enabled=True,
        backend="memory",
        max_message_bytes=4096,
        max_tasks_per_principal=5,
        max_page_size=50,
        default_page_size=20,
        lease_ttl_seconds=300,
        min_lease_ttl_seconds=10,
        max_lease_ttl_seconds=3600,
        sweeper_interval_seconds=30,
        graceful_timeout_seconds=0,
        rate_limit_per_task=1000,
        rate_limit_window_seconds=60,
    )


@pytest.fixture
def service(config) -> TasksService:
    return TasksService(config, MemoryStore())


@pytest.fixture
def installed_service(service):
    set_tasks_service(service)
    yield service
    set_tasks_service(None)


@pytest.fixture
def started(service) -> TasksService:
    """A started service, driven synchronously (no pytest-asyncio, no
    background sweeper — the lifecycle layer owns that in production)."""
    import asyncio

    asyncio.run(service.start())
    yield service
    asyncio.run(service.stop())


class TasksClient:
    """A TestClient wrapper with a switchable caller identity *and* device.

    Callers are switched by swapping the bearer token. A principal may hold
    several keys (one per device), so the client tracks the device keys issued
    to each actor and lets a test switch between them — the whole point of the
    module is takeover from one device by another.
    """

    def __init__(
        self,
        client,
        actors: dict,
    ):
        self._client = client
        # actors: name -> {"principal_id": ..., "devices": {device_name: token}}
        self._actors = actors
        self.current_actor = _ADMIN
        self._current_device = _default_device(actors[_ADMIN])

    @property
    def current_principal(self) -> str:
        return self._actors[self.current_actor]["principal_id"]

    def pid(self, actor: str) -> str:
        return self._actors[actor]["principal_id"]

    def key_id(self, actor: str, device: str | None = None) -> str:
        """The real API-key id behind a named actor's (device) token."""
        from a2x_registry.auth.deps import get_auth_store

        token = self._token(actor, device)
        return get_auth_store().authenticate(token).key_id

    def _token(self, actor: str, device: str | None = None) -> str:
        devices = self._actors[actor]["devices"]
        return devices[device or _default_device(self._actors[actor])]

    def act_as(self, actor: str, device: str | None = None) -> None:
        self.current_actor = actor
        self._current_device = device or _default_device(self._actors[actor])

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token(self.current_actor, self._current_device)}"}

    def _merge(self, kwargs: dict) -> dict:
        headers = dict(kwargs.pop("headers", None) or {})
        headers.update(self._headers())
        kwargs["headers"] = headers
        return kwargs

    def get(self, url, **kwargs):
        return self._client.get(url, **self._merge(kwargs))

    def post(self, url, **kwargs):
        return self._client.post(url, **self._merge(kwargs))

    def patch(self, url, **kwargs):
        return self._client.patch(url, **self._merge(kwargs))

    def delete(self, url, **kwargs):
        return self._client.delete(url, **self._merge(kwargs))


def _default_device(actor: dict) -> str:
    return next(iter(actor["devices"]))


@pytest.fixture
def app_client(monkeypatch, config, tmp_path):
    """A TestClient with the tasks module enabled and real auth initialized.

    The module is enabled through the environment rather than by injecting a
    service object: ``TestClient`` runs the app lifespan, which calls
    ``startup_tasks``, so anything installed beforehand would be replaced.
    """
    import sys

    monkeypatch.setenv("A2X_REGISTRY_HOME", str(tmp_path))
    monkeypatch.setenv("A2X_TASKS_ENABLED", "true")
    monkeypatch.setenv("A2X_TASKS_BACKEND", "memory")
    monkeypatch.setenv("A2X_TASKS_MAX_MESSAGE_BYTES", str(config.max_message_bytes))
    monkeypatch.setenv(
        "A2X_TASKS_MAX_TASKS_PER_PRINCIPAL", str(config.max_tasks_per_principal)
    )
    monkeypatch.setenv("A2X_TASKS_MAX_PAGE_SIZE", str(config.max_page_size))
    monkeypatch.setenv("A2X_TASKS_DEFAULT_PAGE_SIZE", str(config.default_page_size))

    for name in list(sys.modules):
        if name.startswith("a2x_registry"):
            monkeypatch.delitem(sys.modules, name, raising=False)

    from fastapi.testclient import TestClient

    from a2x_registry.backend.app import app
    from a2x_registry.auth.deps import set_auth_store
    from a2x_registry.auth.store import AuthStore
    from a2x_registry.common import paths

    paths.reset_cache()

    store, admin_token = AuthStore.bootstrap(data_dir=tmp_path / "auth_data")
    set_auth_store(store)

    # alice owns two devices (a phone and a PC) so takeover can be exercised
    # through the real credential path; everyone else gets one.
    alice_principal = store.list_principals()[0]
    actors = {
        _ADMIN: {
            "principal_id": alice_principal.id,
            "devices": {"phone": admin_token},
        }
    }
    # A second device key for alice (the takeover target).
    _, pc_token = store.create_key(alice_principal.id, name="alice-pc")
    actors[_ADMIN]["devices"]["pc"] = pc_token

    for handle in ("bob", "mallory"):
        principal, token = store.create_principal(
            handle=handle, role="user", namespaces=["default"]
        )
        actors[handle] = {"principal_id": principal.id, "devices": {"primary": token}}

    try:
        with TestClient(app) as client:
            yield TasksClient(client, actors)
    finally:
        set_auth_store(None)
        paths.reset_cache()