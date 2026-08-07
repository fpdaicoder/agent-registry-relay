from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from a2x_registry.auth.deps import authorize
from a2x_registry.relay.config import RelayConfig
from a2x_registry.relay.deps import require_relay_service, set_relay_service
from a2x_registry.relay.errors import RelayError
from a2x_registry.relay.router import router
from a2x_registry.relay.service import RelayService

from .conftest import FakeRegistry, v1_card


def _config(max_body_bytes: int = 1024, max_inflight: int = 100) -> RelayConfig:
    return RelayConfig(
        enabled=True,
        public_base_url="http://registry.example:8000",
        allowed_origins=frozenset({"http://127.0.0.1:9101"}),
        max_body_bytes=max_body_bytes,
        max_inflight=max_inflight,
    )


def _app(service: RelayService) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[authorize] = lambda: None
    app.dependency_overrides[require_relay_service] = lambda: service
    return app


def test_json_rpc_is_forwarded_without_method_translation():
    observed = {}

    async def upstream(request: httpx.Request) -> httpx.Response:
        observed["body"] = await request.aread()
        observed["authorization"] = request.headers.get("authorization")
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            content=observed["body"],
        )

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    service = RelayService(
        _config(),
        lambda: FakeRegistry(v1_card()),
        client=async_client,
    )
    payload = b'{"jsonrpc":"2.0","id":"1","method":"message/send","params":{}}'

    with TestClient(_app(service)) as client:
        response = client.post(
            "/a2a/agents/target",
            content=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer registry-secret",
            },
        )

    assert response.status_code == 200
    assert response.content == payload
    assert response.headers["x-a2x-relay"] == "1"
    assert observed["body"] == payload
    assert observed["authorization"] is None


def test_derived_agent_card_uses_relay_url():
    async_client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: None))
    service = RelayService(
        _config(),
        lambda: FakeRegistry(v1_card()),
        client=async_client,
    )
    with TestClient(_app(service)) as client:
        response = client.get(
            "/api/datasets/agents/services/target/agent-card?route=relay"
        )
    assert response.status_code == 200
    assert (
        response.json()["supportedInterfaces"][0]["url"]
        == "http://registry.example:8000/a2a/agents/target"
    )


def test_request_size_and_content_type_are_enforced():
    async_client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: None))
    service = RelayService(
        _config(max_body_bytes=4),
        lambda: FakeRegistry(v1_card()),
        client=async_client,
    )
    with TestClient(_app(service)) as client:
        too_large = client.post(
            "/a2a/agents/target",
            content=b"12345",
            headers={"Content-Type": "application/json"},
        )
        wrong_type = client.post(
            "/a2a/agents/target",
            content=b"{}",
            headers={"Content-Type": "text/plain"},
        )
    assert too_large.status_code == 413
    assert too_large.json()["error"]["code"] == "relay_request_too_large"
    assert wrong_type.status_code == 415


class _EventStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self):
        yield b"data: one\\n\\n"
        yield b"data: two\\n\\n"
        yield b"data: three\\n\\n"

    async def aclose(self) -> None:
        self.closed = True


def test_sse_response_is_streamed_and_closed():
    stream = _EventStream()

    async def upstream(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=stream,
        )

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    service = RelayService(
        _config(),
        lambda: FakeRegistry(v1_card()),
        client=async_client,
    )
    with TestClient(_app(service)) as client:
        with client.stream(
            "POST",
            "/a2a/agents/target",
            content=b'{"jsonrpc":"2.0"}',
            headers={"Content-Type": "application/json"},
        ) as response:
            content = b"".join(response.iter_raw())
    assert response.status_code == 200
    assert b"data: one" in content
    assert b"data: three" in content
    assert stream.closed is True


def test_sse_holds_concurrency_slot_until_stream_closes():
    stream = _EventStream()

    async def scenario():
        async def upstream(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=stream,
            )

        async_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        service = RelayService(
            _config(max_inflight=1),
            lambda: FakeRegistry(v1_card()),
            client=async_client,
        )
        response = await service.forward(
            "agents",
            "target",
            b"{}",
            {"Content-Type": "application/json"},
        )
        assert service._inflight._value == 0
        with pytest.raises(RelayError) as exc:
            await service.forward(
                "agents",
                "target",
                b"{}",
                {"Content-Type": "application/json"},
            )
        assert exc.value.code == "relay_busy"
        await anext(response.body_iterator)
        await response.body_iterator.aclose()
        assert service._inflight._value == 1
        await async_client.aclose()

    asyncio.run(scenario())


def test_disabled_relay_returns_404(monkeypatch):
    set_relay_service(None)
    monkeypatch.setenv("A2X_RELAY_ENABLED", "false")
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[authorize] = lambda: None
    with TestClient(app) as client:
        response = client.post(
            "/a2a/agents/target",
            content=b"{}",
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "relay_disabled"

